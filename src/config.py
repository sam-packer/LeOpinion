"""
Configuration module for Twitter Scraper.

Loads application settings from config.yaml and secrets from environment variables.
"""

import logging
import os
import contextvars
from dataclasses import dataclass, field
from pathlib import Path

import yaml
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Context variable for worker ID logging
worker_context = contextvars.ContextVar("worker_id", default=None)


class WorkerLogFilter(logging.Filter):
    """Filter to inject worker ID into log records."""
    def filter(self, record):
        worker_id = worker_context.get()
        if worker_id is not None:
            record.worker_info = f" [Worker {worker_id}]"
        else:
            record.worker_info = ""
        return True


# Default config file path
CONFIG_FILE = Path(__file__).parent.parent / "config.yaml"


def _load_yaml_config() -> dict:
    """Load configuration from YAML file."""
    if CONFIG_FILE.exists():
        with open(CONFIG_FILE) as f:
            return yaml.safe_load(f) or {}
    return {}


# Load YAML config once at module import
_yaml_config = _load_yaml_config()


def _get_yaml(section: str, key: str, default=None):
    """Get a value from the YAML config."""
    return _yaml_config.get(section, {}).get(key, default)


def _get_yaml_section(section: str, default=None):
    """Get an entire section from the YAML config."""
    return _yaml_config.get(section, default or {})


@dataclass
class TwitterConfig:
    """Twitter/X credentials and settings."""
    # Secrets from .env
    username: str = field(default_factory=lambda: os.getenv("TWITTER_USERNAME", ""))
    password: str = field(default_factory=lambda: os.getenv("TWITTER_PASSWORD", ""))
    email: str = field(default_factory=lambda: os.getenv("TWITTER_EMAIL", ""))
    email_password: str = field(default_factory=lambda: os.getenv("TWITTER_EMAIL_PASSWORD", ""))

    # Settings from YAML (with .env fallback for proxies with credentials)
    db_path: str = field(default_factory=lambda: _get_yaml("twitter", "db_path", "accounts.db"))
    proxies: list[str] = field(default_factory=lambda: _get_proxies())  # pylint: disable=unnecessary-lambda


def _get_proxies() -> list[str]:
    """Get proxies from YAML or .env (for proxies with credentials)."""
    env_proxies = os.getenv("TWITTER_PROXIES", "")
    if env_proxies:
        return [p.strip() for p in env_proxies.split(",") if p.strip()]
    return _get_yaml("twitter", "proxies", []) or []


@dataclass
class AppConfig:
    """Application settings from YAML."""
    # Scraping limits
    broad_tweet_limit: int = field(
        default_factory=lambda: _get_yaml("scraping", "broad_tweet_limit", 200)
    )
    search_timeout: int = field(
        default_factory=lambda: _get_yaml("scraping", "search_timeout", 120)
    )

    # Logging
    log_level: str = field(
        default_factory=lambda: _get_yaml("logging", "level", "INFO")
    )

    # Storage (DATABASE_URL from .env — contains credentials)
    database_url: str = field(
        default_factory=lambda: os.getenv("DATABASE_URL", "")
    )

    # Reply collection
    top_tweets_for_replies: int = field(
        default_factory=lambda: _get_yaml("scraping", "top_tweets_for_replies", 10)
    )
    replies_per_tweet: int = field(
        default_factory=lambda: _get_yaml("scraping", "replies_per_tweet", 20)
    )

    # Broad search topics
    broad_topics: list[str] = field(default_factory=lambda: _get_broad_topics())  # pylint: disable=unnecessary-lambda


def _get_broad_topics() -> list[str]:
    """Get topics from YAML or use defaults."""
    topics = _get_yaml_section("broad_topics")
    if topics:
        return topics

    return [
        # Epstein
        "epstein files",
        "epstein documents",
        "epstein list",
        "epstein release",
        # Trump
        "trump",
        "trump news",
        "trump administration",
        # Greenland
        "greenland",
        "greenland trump",
        "greenland purchase",
        "greenland denmark",
        # Venezuela
        "venezuela",
        "venezuela crisis",
        "venezuela maduro",
    ]


@dataclass
class Config:
    """Main configuration container."""
    twitter: TwitterConfig = field(default_factory=TwitterConfig)
    app: AppConfig = field(default_factory=AppConfig)

    def setup_logging(self) -> logging.Logger:
        """Configure and return the application logger."""
        root = logging.getLogger()
        if root.handlers:
            for handler in root.handlers:
                root.removeHandler(handler)

        logging.basicConfig(
            level=getattr(logging, self.app.log_level.upper()),
            format="%(asctime)s - %(name)s - %(levelname)s%(worker_info)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )

        for handler in logging.getLogger().handlers:
            handler.addFilter(WorkerLogFilter())

        return logging.getLogger("leopinion")

    def validate(self) -> list[str]:
        """
        Validate configuration and return list of missing/invalid settings.

        Returns:
            List of validation error messages, empty if all valid.
        """
        errors = []

        if not CONFIG_FILE.exists():
            errors.append(f"Config file not found: {CONFIG_FILE} (copy config.yaml.example to config.yaml)")

        if not self.app.database_url:
            errors.append("DATABASE_URL not set (add it to .env)")

        return errors


# Global configuration instance
config = Config()
