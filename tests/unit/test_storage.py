"""
Unit tests for src/storage.py

Tests async SQLAlchemy tweet storage, deduplication, and run tracking.
Uses aiosqlite in-memory database (no PostgreSQL required).
"""

from datetime import datetime

import pytest

from src.storage import TweetStore, create_tweet_store, Run, Tweet
from tests.fixtures import make_sample_tweet, make_sample_tweets


class TestTweetStore:
    """Tests for TweetStore."""

    @pytest.fixture
    async def store(self) -> TweetStore:
        """Create a TweetStore with an in-memory aiosqlite database."""
        store = await create_tweet_store("sqlite+aiosqlite://")
        yield store
        await store.close()

    @pytest.mark.asyncio
    async def test_init_creates_tables(self, store):
        """Test that initialization creates the required tables."""
        from sqlalchemy import inspect

        async with store._engine.connect() as conn:
            table_names = await conn.run_sync(
                lambda sync_conn: inspect(sync_conn).get_table_names()
            )

        assert "runs" in table_names
        assert "tweets" in table_names

    @pytest.mark.asyncio
    async def test_start_run(self, store):
        """Test recording the start of a run."""
        await store.start_run("20260224")

        from sqlalchemy import select

        async with store._session_factory() as session:
            result = await session.execute(
                select(Run).where(Run.run_id == "20260224")
            )
            row = result.scalar_one()

        assert row.run_id == "20260224"
        assert row.started_at is not None
        assert row.completed_at is None

    @pytest.mark.asyncio
    async def test_start_run_idempotent(self, store):
        """Test that starting the same run twice doesn't error."""
        await store.start_run("20260224")
        await store.start_run("20260224")

        from sqlalchemy import select, func

        async with store._session_factory() as session:
            result = await session.execute(
                select(func.count()).select_from(Run).where(Run.run_id == "20260224")
            )
            count = result.scalar_one()

        assert count == 1

    @pytest.mark.asyncio
    async def test_store_tweets(self, store):
        """Test storing tweets."""
        await store.start_run("20260224")
        tweets = make_sample_tweets(count=5)

        inserted = await store.store_tweets(tweets, "20260224", "epstein files")

        assert inserted == 5
        assert await store.get_run_count("20260224") == 5

    @pytest.mark.asyncio
    async def test_store_tweets_deduplicates_within_run(self, store):
        """Test that duplicate tweets within the same run are skipped."""
        await store.start_run("20260224")

        tweet = make_sample_tweet(id=12345)
        await store.store_tweets([tweet], "20260224", "epstein files")
        await store.store_tweets([tweet], "20260224", "trump")

        assert await store.get_run_count("20260224") == 1

    @pytest.mark.asyncio
    async def test_store_tweets_different_runs_allowed(self, store):
        """Test that the same tweet can exist in different runs."""
        await store.start_run("20260224")
        await store.start_run("20260225")

        tweet = make_sample_tweet(id=12345)
        await store.store_tweets([tweet], "20260224", "epstein files")
        await store.store_tweets([tweet], "20260225", "epstein files")

        assert await store.get_run_count("20260224") == 1
        assert await store.get_run_count("20260225") == 1

    @pytest.mark.asyncio
    async def test_complete_run(self, store):
        """Test completing a run updates metadata."""
        await store.start_run("20260224")
        tweets = make_sample_tweets(count=3)
        await store.store_tweets(tweets, "20260224", "epstein files")

        count = await store.complete_run("20260224")

        assert count == 3

        from sqlalchemy import select

        async with store._session_factory() as session:
            result = await session.execute(
                select(Run).where(Run.run_id == "20260224")
            )
            row = result.scalar_one()

        assert row.completed_at is not None
        assert row.tweet_count == 3

    @pytest.mark.asyncio
    async def test_get_run_count_empty(self, store):
        """Test run count for non-existent run."""
        assert await store.get_run_count("nonexistent") == 0

    @pytest.mark.asyncio
    async def test_store_tweets_preserves_data(self, store):
        """Test that stored tweets contain correct data."""
        await store.start_run("20260224")
        tweet = make_sample_tweet(
            id=99999,
            text="New Epstein documents released today",
            username="testuser",
            likes=500,
            retweets=200,
            hashtags=["epstein", "breaking"],
        )

        await store.store_tweets([tweet], "20260224", "epstein files")

        from sqlalchemy import select

        async with store._session_factory() as session:
            result = await session.execute(
                select(Tweet).where(Tweet.tweet_id == "99999")
            )
            row = result.scalar_one()

        assert row.text == "New Epstein documents released today"
        assert row.username == "testuser"
        assert row.likes == 500
        assert row.retweets == 200
        assert row.topic == "epstein files"
        assert '"epstein"' in row.hashtags
        assert '"breaking"' in row.hashtags

    @pytest.mark.asyncio
    async def test_store_tweet_with_parent_tweet_id(self, store):
        """Test storing a tweet with parent_tweet_id (reply)."""
        await store.start_run("20260224")
        tweet = make_sample_tweet(
            id=88888,
            text="This is a reply",
            parent_tweet_id=99999,
        )

        await store.store_tweets([tweet], "20260224", "replies")

        from sqlalchemy import select

        async with store._session_factory() as session:
            result = await session.execute(
                select(Tweet).where(Tweet.tweet_id == "88888")
            )
            row = result.scalar_one()

        assert row.parent_tweet_id == "99999"
        assert row.topic == "replies"

    @pytest.mark.asyncio
    async def test_store_tweet_without_parent_tweet_id(self, store):
        """Test storing a tweet without parent_tweet_id (not a reply)."""
        await store.start_run("20260224")
        tweet = make_sample_tweet(id=77777)

        await store.store_tweets([tweet], "20260224", "epstein files")

        from sqlalchemy import select

        async with store._session_factory() as session:
            result = await session.execute(
                select(Tweet).where(Tweet.tweet_id == "77777")
            )
            row = result.scalar_one()

        assert row.parent_tweet_id is None

    @pytest.mark.asyncio
    async def test_close(self, store):
        """Test closing the store disposes the engine."""
        await store.close()
        # Engine should be disposed — creating a new connection will fail
        # (but we just verify it doesn't raise during close)

    @pytest.mark.asyncio
    async def test_store_tweet_with_none_created_at(self, store):
        """Test storing a tweet where created_at is None."""
        from src.scraper import ScrapedTweet

        await store.start_run("20260224")
        tweet = ScrapedTweet(
            id=1234567890,
            text="Test tweet",
            username="testuser",
            display_name="Test User",
            created_at=None,
            likes=10,
            retweets=5,
            replies=1,
            views=100,
            language="en",
            is_retweet=False,
            hashtags=[],
        )

        inserted = await store.store_tweets([tweet], "20260224", "epstein files")
        assert inserted == 1

        from sqlalchemy import select

        async with store._session_factory() as session:
            result = await session.execute(select(Tweet.created_at).limit(1))
            row = result.one()

        assert row[0] is None
