"""
Shared pytest fixtures for LeOpinion tests.

This module provides:
- Temporary checkpoint paths
- Mock external services (twscrape)
- Sample data fixtures
"""

from datetime import datetime
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.scraper import ScrapedTweet
from tests.fixtures import make_sample_tweet, make_sample_tweets, make_reply_tweet, make_reply_tweets


@pytest.fixture
def temp_dir(tmp_path):
    """Provide a temporary directory that cleans up after test."""
    return tmp_path


@pytest.fixture
def temp_checkpoint_file(tmp_path) -> Path:
    """Provide a temporary checkpoint file path."""
    return tmp_path / "test_checkpoint.json"


@pytest.fixture
def sample_tweet() -> ScrapedTweet:
    """Provide a single sample tweet."""
    return make_sample_tweet()


@pytest.fixture
def sample_tweets() -> list[ScrapedTweet]:
    """Provide a list of sample tweets."""
    return make_sample_tweets(count=10)


@pytest.fixture
def sample_reply_tweet() -> ScrapedTweet:
    """Provide a single reply tweet."""
    return make_reply_tweet()


@pytest.fixture
def sample_reply_tweets() -> list[ScrapedTweet]:
    """Provide a list of reply tweets."""
    return make_reply_tweets(count=5)


@pytest.fixture
def mock_twscrape_api():
    """Mock twscrape.API for Twitter scraping tests."""
    with patch("twscrape.API") as mock_api_class:
        mock_api = MagicMock()

        # Mock pool stats
        mock_api.pool.stats = AsyncMock(
            return_value={"active": 3, "total": 5, "locked": 2}
        )

        # Mock search to return async generator
        async def mock_search(*args, **kwargs):
            for i in range(5):
                mock_tweet = MagicMock()
                mock_tweet.id = 1234567890 + i
                mock_tweet.rawContent = f"Mock tweet #{i}"
                mock_tweet.user = MagicMock(username=f"user{i}", displayname=f"User {i}")
                mock_tweet.date = datetime.now()
                mock_tweet.likeCount = 100
                mock_tweet.retweetCount = 50
                mock_tweet.replyCount = 10
                mock_tweet.viewCount = 1000
                mock_tweet.lang = "en"
                mock_tweet.hashtags = ["test"]
                yield mock_tweet

        # Mock tweet_replies to return async generator
        async def mock_tweet_replies(tweet_id, *args, **kwargs):
            for i in range(3):
                mock_reply = MagicMock()
                mock_reply.id = 8888880 + i
                mock_reply.rawContent = f"Mock reply #{i} to {tweet_id}"
                mock_reply.user = MagicMock(username=f"replier{i}", displayname=f"Replier {i}")
                mock_reply.date = datetime.now()
                mock_reply.likeCount = 20
                mock_reply.retweetCount = 5
                mock_reply.replyCount = 2
                mock_reply.viewCount = 200
                mock_reply.lang = "en"
                mock_reply.hashtags = []
                yield mock_reply

        mock_api.search = mock_search
        mock_api.tweet_replies = mock_tweet_replies
        mock_api_class.return_value = mock_api
        yield mock_api_class


@pytest.fixture
def mock_env_vars(monkeypatch):
    """Set mock environment variables for testing."""
    monkeypatch.setenv("TWITTER_USERNAME", "testuser")
    monkeypatch.setenv("TWITTER_PASSWORD", "testpass")
