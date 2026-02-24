# Daemonizing LeOpinion

How to run LeOpinion as a background service on a Linux VPS using systemd timers.

## Why Randomized Timers?

Running at the same time every day is an easy signal for bot detection. systemd timers support `RandomizedDelaySec`, which adds a random delay to each run so your traffic pattern looks organic.

**Fixed schedule** (`0 14 * * *`): Same time daily, trivial to fingerprint.
**Randomized schedule**: Different time each day within a window, blends in with normal traffic.

## Timer Strategies

### Option A: Twice-Daily Randomized (Recommended)

Runs twice a day in randomized windows to minimize coverage gaps.

- **Morning**: 7 AM - 12 PM (randomized daily)
- **Evening**: 5 PM - 11 PM (randomized daily)
- **Max gap**: ~14 hours
- **Min gap**: ~5 hours

This catches both daytime and evening activity with no overnight runs.

### Option B: Once-Daily

Runs once per day between 8 AM - 8 PM.

- **Max gap**: Up to 36 hours between runs
- **Use case**: Lower volume needs where a longer gap is acceptable

## Installation

### Step 1: Edit the Service File

```bash
cd systemd/
nano leopinion.service
```

Replace `yourusername` with your actual username (appears 3 times):

```ini
User=yourusername
Group=yourusername
WorkingDirectory=/home/yourusername/LeOpinion
ExecStart=/home/yourusername/.local/bin/uv run main.py
ReadWritePaths=/home/yourusername/LeOpinion
```

Run `which uv` to confirm the path to your uv binary and update `ExecStart` if needed.

### Step 2: Install systemd Files

```bash
# Copy the service file (required for both strategies)
sudo cp systemd/leopinion.service /etc/systemd/system/

# Option A: Twice-daily (recommended)
sudo cp systemd/leopinion-morning.timer /etc/systemd/system/
sudo cp systemd/leopinion-evening.timer /etc/systemd/system/

# Option B: Once-daily
sudo cp systemd/leopinion.timer /etc/systemd/system/
```

### Step 3: Enable and Start Timers

```bash
sudo systemctl daemon-reload

# Option A: Twice-daily
sudo systemctl enable leopinion-morning.timer leopinion-evening.timer
sudo systemctl start leopinion-morning.timer leopinion-evening.timer

# Option B: Once-daily
sudo systemctl enable leopinion.timer
sudo systemctl start leopinion.timer
```

### Step 4: Verify

```bash
systemctl list-timers leopinion-*
```

You should see output like:

```
NEXT                         LEFT     LAST PASSED UNIT                      ACTIVATES
Mon 2025-12-30 09:34:22 EST  2h left  -    -      leopinion-morning.timer   leopinion.service
Mon 2025-12-30 19:12:45 EST  12h left -    -      leopinion-evening.timer   leopinion.service
```

The `NEXT` times should be different each day due to randomization.

### Step 5: Test

```bash
# Run immediately
sudo systemctl start leopinion.service

# Watch logs
sudo journalctl -u leopinion.service -f

# Or check the file log
tail -f ~/LeOpinion/pipeline.log
```

## How Randomization Works

Each timer has a base time plus a random delay:

### Morning Timer
```ini
OnCalendar=*-*-* 07:00:00      # Base: 7:00 AM
RandomizedDelaySec=18000        # Random: 0-5 hours
# Effective window: 7:00 AM - 12:00 PM
```

### Evening Timer
```ini
OnCalendar=*-*-* 17:00:00      # Base: 5:00 PM
RandomizedDelaySec=21600        # Random: 0-6 hours
# Effective window: 5:00 PM - 11:00 PM
```

**Example week** (times vary each day):

```
Monday:     9:23 AM, 8:47 PM
Tuesday:   11:12 AM, 6:34 PM
Wednesday:  7:45 AM, 10:22 PM
Thursday:   8:15 AM, 7:09 PM
Friday:    10:58 AM, 5:31 PM
```

## Monitoring

### Check status

```bash
# Timer status
sudo systemctl status leopinion-morning.timer
sudo systemctl status leopinion-evening.timer

# Last service run
sudo systemctl status leopinion.service
```

### View logs

```bash
# Today's logs
sudo journalctl -u leopinion.service --since today

# Last 100 entries
sudo journalctl -u leopinion.service -n 100 --no-pager
```

### Upcoming schedule

```bash
systemctl list-timers
```

### Manual trigger

```bash
sudo systemctl start leopinion.service
```

### Disable/re-enable timers

```bash
# Stop future runs (doesn't kill in-progress runs)
sudo systemctl stop leopinion-morning.timer leopinion-evening.timer

# Stop an in-progress run
sudo systemctl stop leopinion.service

# Re-enable
sudo systemctl start leopinion-morning.timer leopinion-evening.timer
```

## Changing Timer Windows

Edit the timer files directly:

```bash
sudo nano /etc/systemd/system/leopinion-morning.timer
```

Adjust `OnCalendar` (base time) or `RandomizedDelaySec` (delay in seconds), then reload:

```bash
sudo systemctl daemon-reload
sudo systemctl restart leopinion-morning.timer
```

## Troubleshooting

### Timer isn't running

```bash
sudo systemctl enable leopinion-morning.timer
sudo systemctl start leopinion-morning.timer
sudo systemctl status leopinion-morning.timer
```

If status shows "inactive (dead)", the timer was never enabled or started.

### Config changes not taking effect

Config changes apply on the next scheduled run. To apply immediately:

```bash
sudo systemctl start leopinion.service
```

### Zero tweets scraped

Check your Twitter accounts:

```bash
cd ~/LeOpinion
uv run twscrape accounts
```

If accounts are logged out or rate-limited, re-add them:

```bash
uv run python add_account.py <username> cookies.json
```

Consider adding more accounts or configuring SOCKS5 proxies if you're consistently hitting rate limits.

## systemd File Reference

**leopinion.service** - Main service definition
- Security hardening: PrivateTmp, NoNewPrivileges, ProtectSystem
- Resource limits: 2 GB RAM max, 80% CPU quota
- 10 minute timeout
- Logs to `pipeline.log`

**leopinion-morning.timer** - Morning runs (7 AM - 12 PM)
- `Persistent=true`: catches up on missed runs after reboot

**leopinion-evening.timer** - Evening runs (5 PM - 11 PM)
- Same persistence behavior

**leopinion.timer** - Alternative single daily run (8 AM - 8 PM)
- Can create gaps up to 36 hours; twice-daily is preferred
