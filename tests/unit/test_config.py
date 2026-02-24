"""
Unit tests for src/config.py

Tests configuration loading, validation, and dataclass behavior.
"""

from unittest.mock import patch

import pytest


class TestYamlConfigLoading:
    """Tests for YAML configuration loading."""

    def test_get_yaml_returns_default_for_missing_key(self):
        """Test that _get_yaml returns default when key is missing."""
        from src.config import _get_yaml

        result = _get_yaml("nonexistent", "key", "default_value")
        assert result == "default_value"

    def test_get_yaml_section_returns_empty_dict_for_missing(self):
        """Test that _get_yaml_section returns empty dict for missing section."""
        from src.config import _get_yaml_section

        result = _get_yaml_section("nonexistent")
        assert result == {}


class TestTwitterConfig:
    """Tests for TwitterConfig dataclass."""

    def test_twitter_config_defaults(self, mock_env_vars):
        """Test TwitterConfig uses environment variables."""
        from src.config import TwitterConfig

        config = TwitterConfig()
        assert config.db_path == "accounts.db"

    def test_twitter_config_proxies_from_env(self, monkeypatch):
        """Test proxies can be loaded from environment."""
        monkeypatch.setenv("TWITTER_PROXIES", "http://proxy1:8080,http://proxy2:8080")

        from src.config import _get_proxies

        proxies = _get_proxies()
        assert len(proxies) == 2
        assert "http://proxy1:8080" in proxies


class TestAppConfig:
    """Tests for AppConfig dataclass."""

    def test_app_config_has_values(self):
        """Test AppConfig has valid values."""
        from src.config import AppConfig

        config = AppConfig()
        assert config.broad_tweet_limit > 0
        assert config.search_timeout > 0

    def test_database_url_from_env(self, monkeypatch):
        """Test that database_url reads from DATABASE_URL env var."""
        monkeypatch.setenv("DATABASE_URL", "postgresql+asyncpg://user:pass@localhost/lescraper")

        from src.config import AppConfig

        config = AppConfig()
        assert config.database_url == "postgresql+asyncpg://user:pass@localhost/lescraper"

    def test_database_url_defaults_to_empty(self, monkeypatch):
        """Test that database_url defaults to empty string when not set."""
        monkeypatch.delenv("DATABASE_URL", raising=False)

        from src.config import AppConfig

        config = AppConfig()
        assert config.database_url == ""

    def test_broad_topics_has_focused_topics(self):
        """Test that broad_topics contains focused topics."""
        from src.config import AppConfig

        config = AppConfig()
        assert len(config.broad_topics) > 0
        topics_str = " ".join(config.broad_topics).lower()
        assert "epstein" in topics_str
        assert "trump" in topics_str
        assert "greenland" in topics_str
        assert "venezuela" in topics_str

    def test_top_tweets_for_replies_default(self):
        """Test that top_tweets_for_replies has a default value."""
        from src.config import AppConfig

        config = AppConfig()
        assert config.top_tweets_for_replies == 10

    def test_replies_per_tweet_default(self):
        """Test that replies_per_tweet has a default value."""
        from src.config import AppConfig

        config = AppConfig()
        assert config.replies_per_tweet == 20


class TestConfigValidation:
    """Tests for Config.validate() method."""

    def test_validate_passes_with_config_file(self):
        """Test validation passes when config.yaml exists."""
        from src.config import Config, CONFIG_FILE

        config = Config()
        errors = config.validate()

        if CONFIG_FILE.exists():
            assert errors == []

    def test_validate_fails_without_config_file(self):
        """Test validation fails when config.yaml is missing."""
        from src.config import Config

        with patch("src.config.CONFIG_FILE") as mock_path:
            mock_path.exists.return_value = False
            mock_path.__str__ = lambda self: "/fake/config.yaml"

            config = Config()
            errors = config.validate()
            assert any("Config file" in e for e in errors)

    def test_validate_fails_without_database_url(self, monkeypatch):
        """Test validation fails when DATABASE_URL is not set."""
        monkeypatch.delenv("DATABASE_URL", raising=False)

        from src.config import Config

        config = Config()
        config.app.database_url = ""
        errors = config.validate()
        assert any("DATABASE_URL" in e for e in errors)


class TestWorkerLogFilter:
    """Tests for WorkerLogFilter."""

    def test_filter_adds_worker_info(self):
        """Test that filter adds worker_info to log records."""
        import logging

        from src.config import WorkerLogFilter, worker_context

        filter_instance = WorkerLogFilter()
        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="test.py",
            lineno=1,
            msg="test message",
            args=(),
            exc_info=None,
        )

        # Without worker context
        filter_instance.filter(record)
        assert record.worker_info == ""

        # With worker context
        worker_context.set(5)
        filter_instance.filter(record)
        assert record.worker_info == " [Worker 5]"

        # Clean up
        worker_context.set(None)
