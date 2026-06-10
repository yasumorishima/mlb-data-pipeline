# mlb-data-pipeline

**MLB shared data pipeline.** Fetches from FanGraphs / Baseball Savant and publishes Parquet to a public Hugging Face Dataset.

> **Status (2026-06-10):** 実行基盤は **GitHub Actions（ubuntu-latest, 週次）**、データ正本は **Hugging Face Dataset [yasumorishima/mlb-stats](https://huggingface.co/datasets/yasumorishima/mlb-stats)**。BigQuery は 2026-04-19 退役、RPi5 SSD は 2026-05-29 廃止（経緯は Migration History 参照）。

> **Known limitation:** FanGraphs 由来テーブル（`fg_*` + `park_factors`）は datacenter IP からの 403 ブロックにより GHA から取得不可（2026-06-10 検証）。週次 refresh で自動更新されるのは **Savant 系テーブルのみ**で、`fg_*` / `park_factors` は HF 上の 2026-04 救出スナップショットを静的保持。

## Architecture

```
FanGraphs API ─┐
Savant API ────┤  scripts/fetch_*.py  ──→  Parquet (MLB_DATA_TARGET=parquet)
savant-extras ─┘  (unified write_dataframe)      │
                                                 └─→ hf upload → HF Dataset yasumorishima/mlb-stats

Automation: .github/workflows/weekly_refresh.yml
  - Every Monday UTC 01:00 (JST 10:00) + workflow_dispatch
  - fangraphs / savant / fielding / park を fetch → Parquet → HF へ upload
  - statcast (pitch-level, heavy) は manual dispatch のみ
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

Parquet mode writes statcast per-year (`statcast_pitches_2015.parquet` … `statcast_pitches_2025.parquet`); all other tables are single files. HF 上ではテーブル名のファイルがルート直下に置かれる（例: `fg_batting.parquet`）。

| 更新区分 | テーブル |
|---|---|
| 週次自動更新（Savant） | `sc_*`, `sprint_speed`, `oaa`, `oaa_team`, `catcher` |
| 静的（FanGraphs 403、2026-04 スナップショット） | `fg_batting`, `fg_pitching`, `fg_pitcher_plus`, `park_factors` |
| 手動 dispatch のみ（重量） | `statcast_pitches` |

## Consumers

| Project | Status | Tables Used |
|---------|--------|-------------|
| [baseball-mlops](https://github.com/yasumorishima/baseball-mlops) | Weekly Retrain 停止中 | fg_batting, fg_pitching, sc_*, sprint_speed, park_factors |
| [mlb-win-probability](https://github.com/yasumorishima/mlb-win-probability) | Cloud Run 削除済 | statcast_pitches, fg_batting, fg_pitching, sprint_speed, oaa_team, catcher, park_factors |

読み取り側は HF Dataset 参照（`hf_hub_download` / `pandas.read_parquet` + HF URL）を前提に再設計する。

## Output targets

| Env var | Default | Effect |
|---------|---------|--------|
| `MLB_DATA_TARGET` | `bq` | `parquet` = write to `MLB_PARQUET_ROOT`（現行運用）。`bq` は legacy（BQ 退役済のため使用しない）。 |
| `MLB_PARQUET_ROOT` | `data/parquet_out` | Parquet output directory. |

## Usage

### GitHub Actions（primary）

```bash
# 週次 cron (Mon UTC 01:00) が全テーブル refresh + HF upload を自動実行。手動は:
gh workflow run "Weekly Data Refresh" --repo yasumorishima/mlb-data-pipeline \
  -f memo="実行意図" -f steps=all -f start_year=2015 -f end_year=2026

# 重い statcast pitch-level は明示指定のみ
gh workflow run "Weekly Data Refresh" --repo yasumorishima/mlb-data-pipeline \
  -f memo="statcast 2024" -f steps=statcast -f start_year=2024 -f end_year=2024
```

### Manual single-fetcher run

```bash
export MLB_DATA_TARGET=parquet

python scripts/fetch_fangraphs.py
python scripts/fetch_savant_leaderboards.py
python scripts/fetch_fielding_running.py
python scripts/fetch_park_factors.py
python scripts/fetch_statcast_pitches.py --years 2015-2025

# Granular flags still work
python scripts/fetch_fangraphs.py --batting-only
python scripts/fetch_fielding_running.py --sprint-only
```

### Orchestrated backfill（legacy, RPi5 時代の仕組み）

`scripts/run_backfill.py` は 1 invocation = 1 work unit の orchestrator（進捗は `<MLB_PARQUET_ROOT>/.state.json`）。RPi5 systemd timer 運用（`deploy/systemd/`）は SSD 廃止に伴い停止済みだが、スクリプト自体はローカル一括 backfill に再利用可能。

## Column Sanitization (unified)

All outputs use the same column naming rules via `config.sanitize_columns()`:
- `%` → `_pct` (e.g., `K%` → `K_pct`)
- `/` → `_per_` (e.g., `wFB/C` → `wFB_per_C`)
- `+` → `_plus` (e.g., `Stuff+` → `Stuff_plus`)
- trailing `-` → `_minus` (e.g., `ERA-` → `ERA_minus`)

`write_dataframe()` in `config.py` auto-applies sanitization before writing.

## Data Quality

全 fetch スクリプトに組み込みバリデーション:

- **年カバレッジ**: 指定範囲の全年にデータがあるか検証
- **null 率**: 高 null カラム（>50%）を警告、年×カラムの null マトリクス
- **必須カラム**: player_id, season, 主要指標の存在確認
- **重複チェック**: player_id × season の一意性

## Migration History

- **Phase 1** (2026-03-25): `statcast_pitches` を `mlb_wp` → `mlb_shared` に移行
- **Phase 2** (2026-03-26): FG stats / fielding / park_factors を `mlb_shared` に統合。mlb-win-probability の独自 fetch スクリプト削除。`mlb_statcast` データセット削除
- **Phase 3** (2026-04-17): 実行基盤を RPi5 + Parquet に移行。`MLB_DATA_TARGET` env + `run_backfill.py` 追加。17/17 work units backfill 完走
- **BQ 退役** (2026-04-19): Parquet vs BQ reconcile 後、BigQuery `mlb_shared` データセット削除
- **HF 移行** (2026-05-27〜29): RPi5 USB SSD 故障・廃止に伴い、全データを public HF Dataset [yasumorishima/mlb-stats](https://huggingface.co/datasets/yasumorishima/mlb-stats) へ移行（HF が正本）
- **Phase 4** (2026-06-10): 週次 refresh を GitHub Actions（ubuntu-latest, 無料）+ HF upload に再構築、cron 再開

## Credits

- Data: [Baseball Savant](https://baseballsavant.mlb.com/) / [FanGraphs](https://www.fangraphs.com/)
- API: [pybaseball](https://github.com/jldbc/pybaseball) / [savant-extras](https://pypi.org/project/savant-extras/)
