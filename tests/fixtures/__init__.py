"""
Test fixtures and sample data for LeScraper tests.
"""

from datetime import datetime

from src.scraper import ScrapedTweet


def make_sample_tweet(
    id: int = 1234567890,
    text: str = "Breaking: New Epstein documents released today. #epstein",
    username: str = "testuser",
    display_name: str = "Test User",
    created_at: datetime = None,
    likes: int = 100,
    retweets: int = 50,
    replies: int = 10,
    views: int = 1000,
    language: str = "en",
    is_retweet: bool = False,
    hashtags: list[str] = None,
    parent_tweet_id: int | None = None,
) -> ScrapedTweet:
    """Create a sample ScrapedTweet for testing."""
    return ScrapedTweet(
        id=id,
        text=text,
        username=username,
        display_name=display_name,
        created_at=created_at or datetime.now(),
        likes=likes,
        retweets=retweets,
        replies=replies,
        views=views,
        language=language,
        is_retweet=is_retweet,
        hashtags=hashtags or [],
        parent_tweet_id=parent_tweet_id,
    )


def make_sample_tweets(count: int = 10, base_engagement: int = 100) -> list[ScrapedTweet]:
    """Create a list of sample tweets with varying engagement."""
    tweets = []
    for i in range(count):
        tweets.append(
            make_sample_tweet(
                id=1234567890 + i,
                text=f"Sample tweet #{i} about epstein documents and trump news",
                username=f"user{i}",
                display_name=f"User {i}",
                likes=base_engagement * (i + 1),
                retweets=base_engagement // 2 * (i + 1),
                replies=base_engagement // 10 * (i + 1),
                hashtags=["epstein"],
            )
        )
    return tweets


def make_topic_tweets(count: int = 5) -> list[ScrapedTweet]:
    """Create tweets with focused topic context."""
    texts = [
        "New Epstein files just dropped. Names we haven't seen before. This is massive.",
        "Trump announces new executive order on trade policy. Markets react immediately.",
        "Greenland officials respond to renewed US purchase interest. Denmark pushes back.",
        "Venezuela crisis deepens as Maduro cracks down on opposition. Sanctions tighten.",
        "Epstein document release reveals connections to major political figures worldwide.",
    ]
    tweets = []
    for i, text in enumerate(texts[:count]):
        tweets.append(
            make_sample_tweet(
                id=9999990 + i,
                text=text,
                username=f"newsreporter{i}",
                likes=500 * (i + 1),
                retweets=200 * (i + 1),
                hashtags=["breaking", "news"],
            )
        )
    return tweets


def make_reply_tweet(
    id: int = 8888880,
    parent_tweet_id: int = 9999990,
    text: str = "This is a reply to the original tweet. Very interesting.",
    **kwargs,
) -> ScrapedTweet:
    """Create a single reply tweet for testing."""
    defaults = dict(
        username="replier",
        display_name="Reply User",
        likes=20,
        retweets=5,
        replies=2,
        views=200,
    )
    defaults.update(kwargs)
    return make_sample_tweet(
        id=id,
        text=text,
        parent_tweet_id=parent_tweet_id,
        **defaults,
    )


def make_reply_tweets(count: int = 5, parent_tweet_id: int = 9999990) -> list[ScrapedTweet]:
    """Create a list of reply tweets for testing."""
    tweets = []
    for i in range(count):
        tweets.append(
            make_reply_tweet(
                id=8888880 + i,
                parent_tweet_id=parent_tweet_id,
                text=f"Reply #{i} to the original tweet. Interesting take.",
                username=f"replier{i}",
                display_name=f"Replier {i}",
                likes=10 * (i + 1),
                retweets=2 * (i + 1),
            )
        )
    return tweets
