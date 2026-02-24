"""
Main Orchestration Script for Twitter Scraper.

This script coordinates the scraping pipeline:
1. Init: Load config, init scraper, init storage, init checkpoint
2. Scrape: Topic scraping with checkpointing
3. Replies: Fetch replies for top engaging tweets
4. Store: Save all scraped tweets to PostgreSQL, log summary
"""

import asyncio
import logging
import sys
import time
from datetime import datetime

from .config import config
from .scraper import TwitterScraper, ScrapedTweet
from .storage import create_tweet_store
from .checkpoint import CheckpointManager

logger = logging.getLogger("lescraper.main")


async def run_pipeline() -> bool:
    """
    Run the scraping pipeline with checkpointing.

    Supports resumption after interruption — progress is saved after each topic.

    Returns:
        True if the pipeline completed successfully.
    """
    logger = config.setup_logging()
    logger.info("Twitter Scraper Pipeline")

    # Validate configuration
    errors = config.validate()
    if errors:
        for error in errors:
            logger.error(f"Configuration error: {error}")
        return False

    # Initialize checkpoint manager
    checkpoint = CheckpointManager()

    # Check for existing checkpoint to resume
    resuming = checkpoint.should_resume()
    if resuming:
        state = checkpoint.get_state()
        logger.info(f"Resuming from checkpoint: {state.run_id}")
        logger.info(
            f"  - Scrape: {'DONE' if state.step1_complete else f'{len(state.topics_completed)}/{len(state.topics_completed) + len(state.topics_remaining)} topics'}"
        )
        logger.info(f"  - Store: {'DONE' if state.step2_complete else 'PENDING'}")
    else:
        logger.info("Starting fresh pipeline run")
        checkpoint.start_new_run(topics=config.app.broad_topics)
        state = checkpoint.get_state()

    # Initialize components
    scraper = TwitterScraper(db_path=config.twitter.db_path)
    await scraper.fix_locks()

    store = await create_tweet_store(config.app.database_url)
    await store.start_run(state.run_id)

    # Check if Twitter accounts are available
    try:
        stats = await scraper.get_account_stats()
        active = stats.get("active", 0)
        total = stats.get("total", 0)

        if total == 0:
            logger.error("No Twitter accounts configured. Run 'uv run twscrape accounts' to check.")
            logger.error("Add accounts with: uv run python add_account.py <username> cookies.json")
            return False

        if active == 0:
            logger.warning(f"All {total} Twitter accounts are rate-limited or inactive")
            logger.warning("Consider adding more accounts or waiting for rate limits to reset")
        else:
            logger.info(f"Twitter accounts: {active}/{total} active")
    except Exception as e:
        logger.warning(f"Could not check Twitter account status: {e}")

    try:
        # Step 1: Scrape topics
        step1_start = time.time()
        if not state.step1_complete:
            logger.info("[Step 1/3] Scraping topics...")
            logger.info(f"Topics: {len(state.topics_remaining)} remaining, {len(state.topics_completed)} completed")

            def on_topic_done(topic: str, tweets: list[ScrapedTweet]) -> None:
                checkpoint.mark_topic_complete(topic, tweets)

            existing_tweets = checkpoint.get_broad_tweets() if state.topics_completed else []

            new_tweets = await scraper.get_broad_tweets_incremental(
                topics=config.app.broad_topics,
                limit_per_topic=config.app.broad_tweet_limit,
                on_topic_complete=on_topic_done,
                skip_topics=state.topics_completed,
                timeout=config.app.search_timeout,
            )

            broad_tweets = existing_tweets + new_tweets
            checkpoint.complete_step1()
            state = checkpoint.get_state()
        else:
            logger.info("[Step 1/3] Scraping already complete, skipping")
            broad_tweets = checkpoint.get_broad_tweets()

        elapsed_scrape = time.time() - step1_start

        if not broad_tweets:
            logger.error("No tweets retrieved. Check twscrape setup.")
            return False

        logger.info(f"Total tweets scraped: {len(broad_tweets)} ({elapsed_scrape:.1f}s)")

        # Step 2: Collect replies for top engaging tweets
        logger.info("[Step 2/3] Collecting replies for top tweets...")
        reply_tweets = await scraper.fetch_replies_for_top_tweets(
            tweets=broad_tweets,
            top_n=config.app.top_tweets_for_replies,
            replies_limit=config.app.replies_per_tweet,
            timeout=config.app.search_timeout,
        )
        logger.info(f"Collected {len(reply_tweets)} replies")

        # Step 3: Store everything to database
        step3_start = time.time()
        if not state.step2_complete:
            logger.info("[Step 3/3] Saving to database...")

            # Group tweets by topic from checkpoint state
            topic_tweet_map: dict[str, list[ScrapedTweet]] = {}
            for tweet_data in state.broad_tweets:
                # Tweets in checkpoint don't carry topic info individually,
                # so we store them all under a generic topic. The checkpoint
                # tracks which topics were completed.
                topic_tweet_map.setdefault("broad", []).append(
                    checkpoint._deserialize_tweet(tweet_data)
                )

            total_stored = 0
            for topic, tweets in topic_tweet_map.items():
                stored = await store.store_tweets(tweets, state.run_id, topic)
                total_stored += stored

            if reply_tweets:
                reply_stored = await store.store_tweets(reply_tweets, state.run_id, "replies")
                total_stored += reply_stored
                logger.info(f"Stored {reply_stored} reply tweets")

            count = await store.complete_run(state.run_id)
            checkpoint.complete_step2()
            state = checkpoint.get_state()

            logger.info(f"Stored {total_stored} new tweets ({count} total in run)")
        else:
            logger.info("[Step 3/3] Storage already complete, skipping")
            count = await store.get_run_count(state.run_id)

        elapsed_store = time.time() - step3_start

        checkpoint.clear()

        logger.info("Pipeline complete")
        logger.info(f"  Tweets scraped:  {len(broad_tweets)}")
        logger.info(f"  Replies scraped: {len(reply_tweets)}")
        logger.info(f"  Tweets in DB:    {count}")
        logger.info(f"  Scrape time:     {elapsed_scrape:.1f}s")
        logger.info(f"  Store time:      {elapsed_store:.1f}s")

        return True

    except KeyboardInterrupt:
        logger.info("Interrupted by user, progress saved to checkpoint")
        logger.info("Run again to resume from where you left off")
        raise

    except Exception as e:
        checkpoint.set_error(str(e))
        logger.exception(f"Pipeline failed: {e}")
        logger.info("Progress saved, run again to resume")
        return False

    finally:
        await scraper.close()
        await store.close()


def main():
    """Entry point for the application."""
    print("LeScraper - Twitter/X Scraper")
    print()

    try:
        success = asyncio.run(run_pipeline())
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        print("\nOperation cancelled by user")
        sys.exit(130)
    except Exception as e:
        print(f"\nFatal error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
