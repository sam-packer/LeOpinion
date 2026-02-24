"""
PostgreSQL Tweet Storage Module (SQLAlchemy ORM).

Persists scraped tweets to a PostgreSQL database with run metadata.
Deduplicates tweets by tweet_id within a run (same tweet may appear across topics).
Uses aiosqlite in tests, asyncpg in production.
"""

import json
import logging
from datetime import datetime

from sqlalchemy import ForeignKey, String, select, func, update
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship

from .scraper import ScrapedTweet

logger = logging.getLogger("lescraper.storage")


class Base(DeclarativeBase):
    pass


class Run(Base):
    __tablename__ = "runs"

    id: Mapped[int] = mapped_column(primary_key=True)
    run_id: Mapped[str] = mapped_column(String, unique=True)
    started_at: Mapped[datetime | None]
    completed_at: Mapped[datetime | None]
    tweet_count: Mapped[int] = mapped_column(default=0)

    tweets: Mapped[list["Tweet"]] = relationship(back_populates="run")


class Tweet(Base):
    __tablename__ = "tweets"

    id: Mapped[int] = mapped_column(primary_key=True)
    tweet_id: Mapped[str] = mapped_column(String, index=True)
    run_id: Mapped[str] = mapped_column(ForeignKey("runs.run_id"), index=True)
    text: Mapped[str]
    username: Mapped[str] = mapped_column(index=True)
    likes: Mapped[int]
    retweets: Mapped[int]
    replies: Mapped[int]
    views: Mapped[int | None]
    created_at: Mapped[datetime | None]
    is_retweet: Mapped[bool]
    hashtags: Mapped[str]  # JSON array string
    topic: Mapped[str | None]
    parent_tweet_id: Mapped[str | None] = mapped_column(String, index=True, nullable=True)
    scraped_at: Mapped[datetime | None]

    run: Mapped["Run"] = relationship(back_populates="tweets")


class TweetStore:
    """SQLAlchemy-backed async storage for scraped tweets."""

    def __init__(self, engine: AsyncEngine, session_factory: async_sessionmaker[AsyncSession]):
        self._engine = engine
        self._session_factory = session_factory

    async def init_db(self) -> None:
        """Create tables and indexes if they don't exist."""
        async with self._engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        logger.info("Tweet store initialized")

    async def start_run(self, run_id: str) -> None:
        """Record the start of a scraping run."""
        async with self._session_factory() as session:
            # Check if run already exists
            result = await session.execute(
                select(Run).where(Run.run_id == run_id)
            )
            if result.scalar_one_or_none() is None:
                session.add(Run(run_id=run_id, started_at=datetime.now()))
                await session.commit()
        logger.info(f"Run started: {run_id}")

    async def store_tweets(
        self,
        tweets: list[ScrapedTweet],
        run_id: str,
        topic: str,
    ) -> int:
        """
        Store tweets for a given run and topic.

        Deduplicates by tweet_id within the run — if the same tweet was
        already stored from a different topic, it is skipped.

        Returns:
            Number of new tweets inserted.
        """
        now = datetime.now()
        inserted = 0

        async with self._session_factory() as session:
            for tweet in tweets:
                tweet_id = str(tweet.id)

                # Skip duplicates within the same run
                result = await session.execute(
                    select(Tweet.id).where(
                        Tweet.tweet_id == tweet_id,
                        Tweet.run_id == run_id,
                    )
                )
                if result.scalar_one_or_none() is not None:
                    continue

                session.add(
                    Tweet(
                        tweet_id=tweet_id,
                        run_id=run_id,
                        text=tweet.text,
                        username=tweet.username,
                        likes=tweet.likes,
                        retweets=tweet.retweets,
                        replies=tweet.replies,
                        views=tweet.views,
                        created_at=tweet.created_at if tweet.created_at else None,
                        is_retweet=tweet.is_retweet,
                        hashtags=json.dumps(tweet.hashtags),
                        topic=topic,
                        parent_tweet_id=str(tweet.parent_tweet_id) if tweet.parent_tweet_id else None,
                        scraped_at=now,
                    )
                )
                inserted += 1

            await session.commit()

        logger.debug(f"Stored {inserted} new tweets for topic '{topic}' (run {run_id})")
        return inserted

    async def complete_run(self, run_id: str) -> int:
        """
        Mark a run as complete and update its tweet count.

        Returns:
            Total tweet count for the run.
        """
        async with self._session_factory() as session:
            result = await session.execute(
                select(func.count()).select_from(Tweet).where(Tweet.run_id == run_id)
            )
            count = result.scalar_one()

            await session.execute(
                update(Run)
                .where(Run.run_id == run_id)
                .values(completed_at=datetime.now(), tweet_count=count)
            )
            await session.commit()

        logger.info(f"Run {run_id} complete: {count} tweets stored")
        return count

    async def get_run_count(self, run_id: str) -> int:
        """Get the number of tweets stored for a run."""
        async with self._session_factory() as session:
            result = await session.execute(
                select(func.count()).select_from(Tweet).where(Tweet.run_id == run_id)
            )
            return result.scalar_one()

    async def close(self) -> None:
        """Dispose of the engine and release connections."""
        await self._engine.dispose()


async def create_tweet_store(database_url: str) -> TweetStore:
    """
    Factory: create and initialize a TweetStore.

    Args:
        database_url: SQLAlchemy async database URL
            (e.g. ``postgresql+asyncpg://...`` or ``sqlite+aiosqlite://...``)

    Returns:
        An initialized TweetStore ready for use.
    """
    engine = create_async_engine(database_url)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    store = TweetStore(engine, session_factory)
    await store.init_db()
    return store
