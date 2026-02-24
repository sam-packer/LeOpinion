"""
Unit tests for src/checkpoint.py

Tests checkpoint persistence, serialization, and pipeline state management.
"""

from datetime import datetime
from pathlib import Path

import pytest

from src.checkpoint import CheckpointManager, PipelineState
from src.scraper import ScrapedTweet


class TestPipelineState:
    """Tests for PipelineState dataclass."""

    def test_default_state(self):
        """Test default PipelineState values."""
        state = PipelineState(
            run_id="20240101",
            started_at="2024-01-01T12:00:00",
        )

        assert state.step1_complete is False
        assert state.step2_complete is False
        assert state.topics_completed == []
        assert state.topics_remaining == []
        assert state.broad_tweets == []

    def test_state_with_data(self):
        """Test PipelineState with populated data."""
        state = PipelineState(
            run_id="20240101",
            started_at="2024-01-01T12:00:00",
            step1_complete=True,
            topics_completed=["epstein files", "trump"],
        )

        assert state.step1_complete is True
        assert len(state.topics_completed) == 2


class TestCheckpointManager:
    """Tests for CheckpointManager."""

    def test_init_creates_directory(self, temp_checkpoint_file):
        """Test that CheckpointManager creates parent directory."""
        manager = CheckpointManager(str(temp_checkpoint_file))
        assert temp_checkpoint_file.parent.exists()

    def test_start_new_run(self, temp_checkpoint_file):
        """Test starting a new pipeline run."""
        manager = CheckpointManager(str(temp_checkpoint_file))
        topics = ["epstein files", "trump", "greenland"]

        state = manager.start_new_run(topics)

        assert state.run_id == datetime.now().strftime("%Y%m%d")
        assert state.topics_remaining == topics
        assert state.topics_completed == []
        assert temp_checkpoint_file.exists()

    def test_save_and_load(self, temp_checkpoint_file):
        """Test saving and loading checkpoint state."""
        manager = CheckpointManager(str(temp_checkpoint_file))
        topics = ["epstein files", "trump"]

        manager.start_new_run(topics)
        manager.get_state().step1_complete = True
        manager.save()

        manager2 = CheckpointManager(str(temp_checkpoint_file))
        loaded_state = manager2.load()

        assert loaded_state is not None
        assert loaded_state.step1_complete is True
        assert loaded_state.topics_remaining == topics

    def test_load_returns_none_if_no_file(self, temp_checkpoint_file):
        """Test that load returns None when no checkpoint exists."""
        manager = CheckpointManager(str(temp_checkpoint_file))
        assert manager.load() is None

    def test_serialize_tweet(self, temp_checkpoint_file, sample_tweet):
        """Test tweet serialization."""
        manager = CheckpointManager(str(temp_checkpoint_file))
        serialized = manager._serialize_tweet(sample_tweet)

        assert serialized["id"] == sample_tweet.id
        assert serialized["text"] == sample_tweet.text
        assert serialized["username"] == sample_tweet.username
        assert serialized["likes"] == sample_tweet.likes

    def test_deserialize_tweet(self, temp_checkpoint_file, sample_tweet):
        """Test tweet deserialization."""
        manager = CheckpointManager(str(temp_checkpoint_file))
        serialized = manager._serialize_tweet(sample_tweet)
        deserialized = manager._deserialize_tweet(serialized)

        assert deserialized.id == sample_tweet.id
        assert deserialized.text == sample_tweet.text
        assert deserialized.username == sample_tweet.username
        assert deserialized.likes == sample_tweet.likes

    def test_serialize_reply_tweet(self, temp_checkpoint_file, sample_reply_tweet):
        """Test that parent_tweet_id round-trips through serialization."""
        manager = CheckpointManager(str(temp_checkpoint_file))
        serialized = manager._serialize_tweet(sample_reply_tweet)

        assert serialized["parent_tweet_id"] == sample_reply_tweet.parent_tweet_id

        deserialized = manager._deserialize_tweet(serialized)
        assert deserialized.parent_tweet_id == sample_reply_tweet.parent_tweet_id

    def test_deserialize_tweet_without_parent_tweet_id(self, temp_checkpoint_file):
        """Test backward compatibility — old checkpoints without parent_tweet_id."""
        manager = CheckpointManager(str(temp_checkpoint_file))
        old_data = {
            "id": 123,
            "text": "old tweet",
            "username": "user",
            "display_name": "User",
            "created_at": None,
            "likes": 10,
            "retweets": 5,
            "replies": 1,
            "views": 100,
            "language": "en",
            "hashtags": [],
            "is_retweet": False,
        }

        deserialized = manager._deserialize_tweet(old_data)
        assert deserialized.parent_tweet_id is None

    def test_mark_topic_complete(self, temp_checkpoint_file, sample_tweets):
        """Test marking a topic as complete with tweets."""
        manager = CheckpointManager(str(temp_checkpoint_file))
        manager.start_new_run(["epstein files", "trump"])

        manager.mark_topic_complete("epstein files", sample_tweets[:5])
        state = manager.get_state()

        assert "epstein files" in state.topics_completed
        assert "epstein files" not in state.topics_remaining
        assert len(state.broad_tweets) == 5

    def test_mark_topic_complete_empty_tweets_triggers_retry(self, temp_checkpoint_file):
        """Test that empty tweets trigger retry mechanism."""
        manager = CheckpointManager(str(temp_checkpoint_file))
        manager.start_new_run(["epstein files"])

        manager.mark_topic_complete("epstein files", [])
        state = manager.get_state()

        assert "epstein files" in state.topics_remaining
        assert "epstein files" not in state.topics_completed
        assert state.retry_counts.get("epstein files") == 1

    def test_get_broad_tweets(self, temp_checkpoint_file, sample_tweets):
        """Test retrieving all broad tweets."""
        manager = CheckpointManager(str(temp_checkpoint_file))
        manager.start_new_run(["epstein files", "trump"])

        manager.mark_topic_complete("epstein files", sample_tweets[:5])
        manager.mark_topic_complete("trump", sample_tweets[5:])

        tweets = manager.get_broad_tweets()
        assert len(tweets) == len(sample_tweets)
        assert all(isinstance(t, ScrapedTweet) for t in tweets)

    def test_complete_steps(self, temp_checkpoint_file):
        """Test completing pipeline steps."""
        manager = CheckpointManager(str(temp_checkpoint_file))
        manager.start_new_run(["epstein files"])

        manager.complete_step1()
        assert manager.get_state().step1_complete is True

        manager.complete_step2()
        assert manager.get_state().step2_complete is True

    def test_should_resume_same_day(self, temp_checkpoint_file):
        """Test resume detection for same-day incomplete run."""
        manager = CheckpointManager(str(temp_checkpoint_file))
        manager.start_new_run(["epstein files"])
        manager.complete_step1()

        manager2 = CheckpointManager(str(temp_checkpoint_file))
        assert manager2.should_resume() is True

    def test_should_not_resume_completed_run(self, temp_checkpoint_file):
        """Test that completed runs don't trigger resume."""
        manager = CheckpointManager(str(temp_checkpoint_file))
        manager.start_new_run(["epstein files"])
        manager.complete_step2()

        manager2 = CheckpointManager(str(temp_checkpoint_file))
        assert manager2.should_resume() is False

    def test_clear(self, temp_checkpoint_file):
        """Test clearing checkpoint file."""
        manager = CheckpointManager(str(temp_checkpoint_file))
        manager.start_new_run(["epstein files"])
        assert temp_checkpoint_file.exists()

        manager.clear()
        assert not temp_checkpoint_file.exists()

    def test_set_error(self, temp_checkpoint_file):
        """Test recording an error."""
        manager = CheckpointManager(str(temp_checkpoint_file))
        manager.start_new_run(["epstein files"])

        manager.set_error("Test error message")
        state = manager.get_state()

        assert state.error == "Test error message"

    def test_get_state_raises_without_init(self, temp_checkpoint_file):
        """Test that get_state raises error without initialization."""
        manager = CheckpointManager(str(temp_checkpoint_file))

        with pytest.raises(RuntimeError, match="No active state"):
            manager.get_state()
