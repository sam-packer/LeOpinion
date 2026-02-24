# LeScraper

**Barebones Twitter/X scraper for NLP projects.**

LeScraper scrapes broad topics from Twitter using twscrape and stores the raw tweets in PostgreSQL via SQLAlchemy. No analysis, no LLM, no email — just scraping and storage. The data is meant to be consumed by downstream NLP pipelines.

## Quick Start

```bash
# 1. Install Dependencies
uv sync

# 2. Add Twitter Account
uv run add_account.py <username> cookies.json

# 3. Set DATABASE_URL in .env
DATABASE_URL=postgresql+asyncpg://user:password@localhost:5432/lescraper

# 4. Run Pipeline
uv run main.py

# Background Service (Linux/Mac)
./run.sh start | logs | status | stop
```

## Architecture

Two-step pipeline:

1. **Scrape**: Broad topic scraping across 30+ economic/consumer topics via twscrape. Incremental with checkpointing — survives interruptions.
2. **Store**: Deduplicated tweet storage in PostgreSQL (SQLAlchemy ORM). Each run is tracked with metadata.

## Key Files

- `src/main.py` — Pipeline orchestration (scrape + store)
- `src/scraper.py` — Twitter scraping via twscrape
- `src/storage.py` — PostgreSQL tweet storage (SQLAlchemy async ORM)
- `src/config.py` — YAML + env config loading
- `src/checkpoint.py` — Run state persistence for resumption

## Testing

```bash
# Run all tests
uv run pytest tests/ -v

# Run with coverage
uv run pytest tests/ --cov=src --cov-report=html
```

**When making changes:**
- Run the full test suite to catch regressions
- Update existing tests if behavior changes intentionally
- Add new tests for new functionality
- Tests are in `tests/unit/` — check there first for examples

The test suite uses aiosqlite as the async driver for tests (no PostgreSQL required). External dependencies (twscrape) are mocked so tests run fast and don't require API keys.

## Configuration

- **`config.yaml`**: App settings (scraping limits, broad topics).
- **`.env`**: Secrets (`DATABASE_URL`, proxy credentials).
