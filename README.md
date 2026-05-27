# mlb-data-pipeline

**MLB shared data pipeline.** Fetches from FanGraphs / Baseball Savant and writes to either local Parquet (RPi5 primary) or BigQuery (legacy).

> **Status (2026-04-17):** Phase 3 進行中。実行基盤を GitHub Actions + BigQuery → **RPi5 self-hosted + `/mnt/ssd/mlb_shared` (Parquet)** に移行中。新規の experiments / evaluation は Parquet 参照を前提。BigQuery は完全移行後に退役。

> **Data:** Published as a public Hugging Face Dataset → [yasumorishima/mlb-stats](https://huggingface.co/datasets/yasumorishima/mlb-stats).

## Architecture

```
FanGraphs API ─┐                               ┌─ MLB_DATA_TARGET=parquet
Savant API ────┤  scripts/fetch_*.py  ─────────┤    → /mnt/ssd/mlb_shared/<table>/<table>.parquet
savant-extras ─┘  (unified write_dataframe)    │
                                               └─ MLB_DATA_TARGET=bq (legacy)
                                                    → data-platform-490901.mlb_shared.*

Orchestrator (new):  scripts/run_backfill.py
  - Runs one work unit per invocation
  - Tracks progress in <PARQUET_ROOT>/.state.json (fcntl-locked)
  - Fired every 3h by deploy/systemd/mlb-backfill.timer on RPi5
```

## Tables

| Table | Source | Rows | Description |
|-------|--------|------|-------------|
| `fg_batting` | FanGraphs | ~6K/yr | Season batting stats (all columns, qual=50) |
| `fg_pitching` | FanGraphs | ~4K/yr | Season pitching stats (all columns, qual=30) |
| `fg_pitcher_plus` | FanGraphs | ~2.5K/yr | Stuff+/Location+/Pitching+ per pitch type (2020+) |
| `sc_batter_exitvelo` | Savant | ~100/yr | Exit velocity, barrel rate |
| `sc_batter_expected` | Savant | ~100/yr | xBA, xSLG, xwOBA |
| `sc_pitcher_exitvelo` | Savant | ~80/yr | Exit velocity against |
| `sc_pitcher_expected` | Savant | ~80/yr | xERA, xwOBA against |
| `sc_pitcher_arsenal` | Savant | ~80/yr | Per-pitch-type stats |
| `sc_bat_tracking` | Savant | ~100/yr | Bat speed, swing tilt (2024+ Hawk-Eye) |
| `sc_batted_ball` | Savant | ~100/yr | Pull/oppo rates |
| `sprint_speed` | Savant | ~500/yr | Sprint speed (2015+) |
| `oaa` | Savant | 2,428 | Outs Above Average by position (2016+) |
| `oaa_team` | Savant | 270 | Team-level OAA aggregate |
| `catcher` | Savant | 702 | Pop time + framing (2015+) |
| `park_factors` | Savant | 329 | Stadium park factors (2015-2025) |
| `statcast_pitches` | Savant | 6.8M+ | Full pitch-level data (2015-2025, 122 cols) |

Parquet mode writes statcast per-year (`statcast_pitches_2015.parquet` … `statcast_pitches_2025.parquet`); all other tables are single files.

## Consumers

| Project | Status | Tables Used |
|---------|--------|-------------|
| [baseball-mlops](https://github.com/yasumorishima/baseball-mlops) | Weekly Retrain 停止中 | fg_batting, fg_pitching, sc_*, sprint_speed, park_factors |
| [mlb-win-probability](https://github.com/yasumorishima/mlb-win-probability) | Cloud Run 削除済 | statcast_pitches, fg_batting, fg_pitching, sprint_speed, oaa_team, catcher, park_factors |

読み取り側は Parquet 参照に書き換え予定。書き換え完了次第 BigQuery `mlb_shared` を退役。

## Output targets

| Env var | Default | Effect |
|---------|---------|--------|
| `MLB_DATA_TARGET` | `bq` | `bq` = load to BigQuery. `parquet` = write to `MLB_PARQUET_ROOT`. |
| `MLB_PARQUET_ROOT` | `data/parquet_out` | Parquet output directory (typically `/mnt/ssd/mlb_shared` on RPi5). |

## Usage

### Manual single-fetcher run

```bash
# Parquet output on RPi5
export MLB_DATA_TARGET=parquet
export MLB_PARQUET_ROOT=/mnt/ssd/mlb_shared

python scripts/fetch_fangraphs.py
python scripts/fetch_savant_leaderboards.py
python scripts/fetch_fielding_running.py
python scripts/fetch_park_factors.py
python scripts/fetch_statcast_pitches.py --years 2015-2025

# Granular flags still work
python scripts/fetch_fangraphs.py --batting-only
python scripts/fetch_fielding_running.py --sprint-only
```

### Orchestrated backfill (recommended for RPi5)

```bash
# Show progress table
python scripts/run_backfill.py --status

# Dry-run: show what would execute next
python scripts/run_backfill.py --dry-run

# Run next pending unit (one per invocation)
python scripts/run_backfill.py

# Force re-run of a specific unit
python scripts/run_backfill.py --force-unit statcast_2018
```

The orchestrator picks the next pending (or previously-failed) unit each invocation and writes to `MLB_PARQUET_ROOT`. Progress persists in `<MLB_PARQUET_ROOT>/.state.json`. See `deploy/systemd/README.md` for the systemd timer deployment that fires this every 3h.

## Column Sanitization (unified)

All outputs use the same column naming rules via `config.sanitize_columns()`:
- `%` → `_pct` (e.g., `K%` → `K_pct`)
- `/` → `_per_` (e.g., `wFB/C` → `wFB_per_C`)
- `+` → `_plus` (e.g., `Stuff+` → `Stuff_plus`)
- trailing `-` → `_minus` (e.g., `ERA-` → `ERA_minus`)

`write_dataframe()` in `config.py` auto-applies sanitization before writing to either target.

## Data Quality

全 fetch スクリプトに組み込みバリデーション:

- **年カバレッジ**: 指定範囲の全年にデータがあるか検証
- **null 率**: 高 null カラム（>50%）を警告、年×カラムの null マトリクス
- **必須カラム**: player_id, season, 主要指標の存在確認
- **重複チェック**: player_id × season の一意性
- **BQ 検証** (bq mode only): アップロード後の行数・サイズ・スキーマ照合

## Automation

### RPi5 systemd timer (primary)

See `deploy/systemd/README.md` for install instructions. Fires every 3h, picks the next pending unit, notifies Discord on failure + final completion.

### GitHub Actions `weekly_refresh.yml` (disabled)

Cron trigger is commented out as of 2026-04-11. Manual `workflow_dispatch` still available for BQ-mode runs if needed (not actively used).

## Migration History

- **Phase 1** (2026-03-25): `statcast_pitches` を `mlb_wp` → `mlb_shared` に移行
- **Phase 2** (2026-03-26): FG stats / fielding / park_factors を `mlb_shared` に統合。mlb-win-probability の独自 fetch スクリプト削除。`mlb_statcast` データセット削除
- **Phase 3** (2026-04-17 進行中): 実行基盤を RPi5 + Parquet に移行。`MLB_DATA_TARGET` env + `run_backfill.py` 追加。BQ 依存を剥がして local Parquet を primary に。完了次第 BigQuery `mlb_shared` 退役

## Credits

- Data: [Baseball Savant](https://baseballsavant.mlb.com/) / [FanGraphs](https://www.fangraphs.com/)
- API: [pybaseball](https://github.com/jldbc/pybaseball) / [savant-extras](https://pypi.org/project/savant-extras/)
