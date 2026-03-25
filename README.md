# mlb-data-pipeline

MLB shared data pipeline for BigQuery. Single source of truth for all baseball analytics projects.

## Architecture

```
FanGraphs API ─┐
Savant API ────┤  scripts/fetch_*.py  →  data/*.csv  →  BQ: mlb_shared.*
savant-extras ─┘
```

## BigQuery Tables (`data-platform-490901.mlb_shared`)

| Table | Source | Description |
|-------|--------|-------------|
| `fg_batting` | FanGraphs | Season batting stats (all columns, qual=50) |
| `fg_pitching` | FanGraphs | Season pitching stats (all columns, qual=30) |
| `fg_pitcher_plus` | FanGraphs | Stuff+/Location+/Pitching+ per pitch type (2020+) |
| `sc_batter_exitvelo` | Savant | Exit velocity, barrel rate |
| `sc_batter_expected` | Savant | xBA, xSLG, xwOBA |
| `sc_pitcher_exitvelo` | Savant | Exit velocity against |
| `sc_pitcher_expected` | Savant | xERA, xwOBA against |
| `sc_pitcher_arsenal` | Savant | Per-pitch-type stats |
| `sc_bat_tracking` | Savant | Bat speed, swing tilt (2024+) |
| `sc_batted_ball` | Savant | Pull/oppo rates |
| `sprint_speed` | Savant | Sprint speed (2015+) |
| `oaa` | Savant | Outs Above Average by position (2016+) |
| `oaa_team` | Savant | Team-level OAA aggregate |
| `catcher` | Savant | Pop time + framing |
| `park_factors` | Savant | Stadium park factors |
| `statcast_pitches` | Savant | Full pitch-level data (6.8M+ rows) |

## Consumers

- [baseball-mlops](https://github.com/yasumorishima/baseball-mlops) — MLB player performance prediction
- [mlb-win-probability](https://github.com/yasumorishima/mlb-win-probability) — Live win probability engine

## Usage

```bash
# Full refresh (all tables)
python scripts/fetch_fangraphs.py
python scripts/fetch_savant_leaderboards.py
python scripts/fetch_fielding_running.py
python scripts/fetch_park_factors.py

# Pitch-level (heavy — run separately)
python scripts/fetch_statcast_pitches.py --years 2015-2024

# Individual tables
python scripts/fetch_fangraphs.py --batting-only
python scripts/fetch_fielding_running.py --sprint-only --no-bq
```

## Column Sanitization (unified)

All projects use the same BQ column naming:
- `%` → `_pct` (e.g., `K%` → `K_pct`)
- `/` → `_per_` (e.g., `wFB/C` → `wFB_per_C`)
- `+` → `_plus` (e.g., `Stuff+` → `Stuff_plus`)

## Automation

Weekly refresh runs every Monday via GitHub Actions (`weekly_refresh.yml`).

## Credits

- Data: [Baseball Savant](https://baseballsavant.mlb.com/) / [FanGraphs](https://www.fangraphs.com/)
- API: [pybaseball](https://github.com/jldbc/pybaseball) / [savant-extras](https://pypi.org/project/savant-extras/)
