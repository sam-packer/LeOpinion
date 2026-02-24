# LeOpinion

Twitter/X scraper for NLP projects. Scrapes broad topics and stores tweets in PostgreSQL.

## Quick Start

```bash
# Install dependencies
uv sync

# Add a Twitter account
uv run add_account.py <username> cookies.json

# Set DATABASE_URL in .env
cp .env.example .env
# Edit .env with your PostgreSQL connection string

# Copy and edit config
cp config.yaml.example config.yaml

# Run the pipeline
uv run main.py
```

## How It Works

Two-step pipeline:

1. **Scrape** - Broad topic scraping across 30+ economic/consumer topics via twscrape. Incremental with checkpointing — survives interruptions.
2. **Store** - Deduplicated tweet storage in PostgreSQL (SQLAlchemy async ORM). Each run is tracked with metadata.

## Twitter Setup

1. Log into Twitter in your browser
2. Export cookies with a browser extension
3. Save as `cookies.json`
4. Run: `uv run python add_account.py <username> cookies.json`

More accounts = more parallel scraping. Proxies can be configured in `config.yaml`.

## Running

```bash
# Interactive
uv run python main.py

# Background (for VPS)
./run.sh start
./run.sh logs
./run.sh status
./run.sh stop
```

## Configuration

- **`config.yaml`** - App settings (scraping limits, broad topics, timeouts)
- **`.env`** - Secrets (`DATABASE_URL`, proxy credentials)

See `config.yaml.example` and `.env.example` for all available options.

## Production Deployment

See [DAEMONIZING.md](DAEMONIZING.md) for systemd setup with randomized timing to avoid pattern detection.

## Testing

```bash
uv run pytest tests/ -v
```

Tests use aiosqlite (in-memory) so no PostgreSQL is needed to run them.

## Troubleshooting

- **"No tweets retrieved"** - Accounts are logged out or banned. Check with `uv run twscrape accounts` and re-add via cookies.
- **Rate limiting** - Add more accounts or configure SOCKS5 proxies.
