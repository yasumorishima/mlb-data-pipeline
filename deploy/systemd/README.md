# MLB Backfill — RPi5 systemd deployment

Runs `scripts/run_backfill.py` every 3 hours to fetch one work unit at a time
and write Parquet output to `/mnt/ssd/mlb_shared/`.

## Prerequisites (one-time setup)

Repo + venv must already exist on the RPi5:

```bash
# Clone if not already present
cd /mnt/ssd && git clone https://github.com/yasumorishima/mlb-data-pipeline.git

# Create venv and install deps
python3 -m venv /mnt/ssd/venv
/mnt/ssd/venv/bin/pip install -r /mnt/ssd/mlb-data-pipeline/requirements.txt
```

## Install

```bash
# 1. Copy unit files to user systemd directory
mkdir -p ~/.config/systemd/user
cp /mnt/ssd/mlb-data-pipeline/deploy/systemd/mlb-backfill.service ~/.config/systemd/user/
cp /mnt/ssd/mlb-data-pipeline/deploy/systemd/mlb-backfill.timer ~/.config/systemd/user/

# 2. (Optional) Discord webhook for failure + completion notifications
cat > ~/.config/mlb-backfill.env <<EOF
DISCORD_WEBHOOK_URL=https://discord.com/api/webhooks/xxx/yyy
EOF
chmod 600 ~/.config/mlb-backfill.env

# 3. Enable lingering so user timers run when logged out
sudo loginctl enable-linger $USER

# 4. Reload, enable, start
systemctl --user daemon-reload
systemctl --user enable --now mlb-backfill.timer
```

## Operations

```bash
# Timer status + next fire
systemctl --user status mlb-backfill.timer
systemctl --user list-timers | grep mlb-backfill

# Live log tail
journalctl --user -u mlb-backfill -f

# Progress table (pending/completed/failed per unit)
cd /mnt/ssd/mlb-data-pipeline
MLB_PARQUET_ROOT=/mnt/ssd/mlb_shared /mnt/ssd/venv/bin/python scripts/run_backfill.py --status

# Manual one-shot run (fires immediately)
systemctl --user start mlb-backfill.service

# Re-run a specific failed unit
MLB_DATA_TARGET=parquet MLB_PARQUET_ROOT=/mnt/ssd/mlb_shared \
  /mnt/ssd/venv/bin/python scripts/run_backfill.py --force-unit statcast_2018
```

## Timing

17 work units × 3h cadence ≈ 2 days for full backfill. Per-unit timeout is 3h
(accommodates slow Savant responses for heavy statcast years).

After all 17 units complete, orchestrator sets `mode=complete` and subsequent
timer fires become no-ops until the state file is manually reset.

## Stopping / removal

```bash
systemctl --user disable --now mlb-backfill.timer
rm ~/.config/systemd/user/mlb-backfill.{service,timer}
systemctl --user daemon-reload
```
