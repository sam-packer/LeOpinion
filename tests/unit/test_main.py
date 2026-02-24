"""
Unit tests for src/main.py

Tests the simplified scraping pipeline.
"""

from unittest.mock import AsyncMock, MagicMock, patch
import pytest

from src.main import run_pipeline


class TestRunPipeline:
    """Tests for the run_pipeline function."""

    @pytest.fixture
    def mock_config(self):
        """Create a mock config."""
        with patch("src.main.config") as mock_cfg:
            mock_cfg.setup_logging.return_value = MagicMock()
            mock_cfg.validate.return_value = []
            mock_cfg.twitter.db_path = "test_accounts.db"
            mock_cfg.app.database_url = "sqlite+aiosqlite://"
            mock_cfg.app.broad_topics = ["epstein files", "trump"]
            mock_cfg.app.broad_tweet_limit = 10
            mock_cfg.app.search_timeout = 30
            mock_cfg.app.top_tweets_for_replies = 5
            mock_cfg.app.replies_per_tweet = 10
            yield mock_cfg

    @pytest.mark.asyncio
    async def test_pipeline_fails_on_validation_errors(self, mock_config):
        """Test that pipeline fails when config validation has errors."""
        mock_config.validate.return_value = ["Missing config.yaml"]

        result = await run_pipeline()

        assert result is False

    @pytest.mark.asyncio
    async def test_pipeline_fails_with_no_accounts(self, mock_config):
        """Test that pipeline fails when no Twitter accounts are configured."""
        with patch("src.main.CheckpointManager") as MockCP, \
             patch("src.main.create_tweet_store", new_callable=AsyncMock) as mock_create_store, \
             patch("src.main.TwitterScraper") as MockScraper:

            mock_scraper = MagicMock()
            mock_scraper.fix_locks = AsyncMock()
            mock_scraper.get_account_stats = AsyncMock(return_value={"active": 0, "total": 0})
            mock_scraper.close = AsyncMock()
            MockScraper.return_value = mock_scraper

            mock_store = MagicMock()
            mock_store.start_run = AsyncMock()
            mock_store.close = AsyncMock()
            mock_create_store.return_value = mock_store

            mock_cp = MagicMock()
            mock_cp.should_resume.return_value = False
            mock_state = MagicMock()
            mock_state.run_id = "20260224"
            mock_state.topics_remaining = ["epstein files", "trump"]
            mock_state.topics_completed = []
            mock_cp.start_new_run.return_value = mock_state
            mock_cp.get_state.return_value = mock_state
            MockCP.return_value = mock_cp

            result = await run_pipeline()

            assert result is False

    @pytest.mark.asyncio
    async def test_pipeline_fails_with_no_tweets(self, mock_config):
        """Test that pipeline fails when no tweets are retrieved."""
        with patch("src.main.CheckpointManager") as MockCP, \
             patch("src.main.create_tweet_store", new_callable=AsyncMock) as mock_create_store, \
             patch("src.main.TwitterScraper") as MockScraper:

            mock_scraper = MagicMock()
            mock_scraper.fix_locks = AsyncMock()
            mock_scraper.get_account_stats = AsyncMock(return_value={"active": 2, "total": 2})
            mock_scraper.get_broad_tweets_incremental = AsyncMock(return_value=[])
            mock_scraper.close = AsyncMock()
            MockScraper.return_value = mock_scraper

            mock_store = MagicMock()
            mock_store.start_run = AsyncMock()
            mock_store.close = AsyncMock()
            mock_create_store.return_value = mock_store

            mock_cp = MagicMock()
            mock_cp.should_resume.return_value = False
            mock_state = MagicMock()
            mock_state.run_id = "20260224"
            mock_state.step1_complete = False
            mock_state.step2_complete = False
            mock_state.topics_remaining = ["epstein files", "trump"]
            mock_state.topics_completed = []
            mock_state.broad_tweets = []
            mock_cp.start_new_run.return_value = mock_state
            mock_cp.get_state.return_value = mock_state
            mock_cp.get_broad_tweets.return_value = []
            mock_cp.complete_step1.side_effect = lambda: setattr(mock_state, 'step1_complete', True)
            MockCP.return_value = mock_cp

            result = await run_pipeline()

            assert result is False

    @pytest.mark.asyncio
    async def test_pipeline_success(self, mock_config):
        """Test successful pipeline execution."""
        from tests.fixtures import make_sample_tweets, make_reply_tweets

        sample_tweets = make_sample_tweets(count=5)
        sample_replies = make_reply_tweets(count=3)

        with patch("src.main.CheckpointManager") as MockCP, \
             patch("src.main.create_tweet_store", new_callable=AsyncMock) as mock_create_store, \
             patch("src.main.TwitterScraper") as MockScraper:

            mock_scraper = MagicMock()
            mock_scraper.fix_locks = AsyncMock()
            mock_scraper.get_account_stats = AsyncMock(return_value={"active": 2, "total": 2})
            mock_scraper.get_broad_tweets_incremental = AsyncMock(return_value=sample_tweets)
            mock_scraper.fetch_replies_for_top_tweets = AsyncMock(return_value=sample_replies)
            mock_scraper.close = AsyncMock()
            MockScraper.return_value = mock_scraper

            mock_store = MagicMock()
            mock_store.start_run = AsyncMock()
            mock_store.store_tweets = AsyncMock(return_value=5)
            mock_store.complete_run = AsyncMock(return_value=8)
            mock_store.get_run_count = AsyncMock(return_value=8)
            mock_store.close = AsyncMock()
            mock_create_store.return_value = mock_store

            mock_cp = MagicMock()
            mock_cp.should_resume.return_value = False

            # Track step completion
            step1_done = {"value": False}
            step2_done = {"value": False}

            def make_state():
                mock_state = MagicMock()
                mock_state.run_id = "20260224"
                mock_state.step1_complete = step1_done["value"]
                mock_state.step2_complete = step2_done["value"]
                mock_state.topics_remaining = [] if step1_done["value"] else ["epstein files", "trump"]
                mock_state.topics_completed = ["epstein files", "trump"] if step1_done["value"] else []
                mock_state.broad_tweets = [{"id": i, "text": f"tweet {i}", "username": f"user{i}",
                                            "display_name": f"User {i}", "created_at": None,
                                            "likes": 10, "retweets": 5, "replies": 1,
                                            "views": 100, "language": "en", "hashtags": [],
                                            "is_retweet": False, "parent_tweet_id": None} for i in range(5)]
                return mock_state

            mock_cp.start_new_run.return_value = make_state()
            mock_cp.get_state.side_effect = lambda: make_state()
            mock_cp.get_broad_tweets.return_value = sample_tweets
            mock_cp.complete_step1.side_effect = lambda: step1_done.update({"value": True})
            mock_cp.complete_step2.side_effect = lambda: step2_done.update({"value": True})
            mock_cp._deserialize_tweet = MagicMock(side_effect=lambda d: sample_tweets[0])
            MockCP.return_value = mock_cp

            result = await run_pipeline()

            assert result is True
            mock_scraper.fix_locks.assert_called_once()
            mock_scraper.fetch_replies_for_top_tweets.assert_called_once()
            mock_store.start_run.assert_called_once()
            mock_cp.clear.assert_called_once()
