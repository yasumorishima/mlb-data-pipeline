# mlb-data-pipeline

**MLB shared data pipeline for BigQuery.** Single source of truth for all baseball analytics projects.

> **Status (2026-03-26):** Phase 2 完了。`baseball-mlops` / `mlb-win-probability` の全BQ参照を `mlb_shared` に統合。毎週月曜 JST 10:00 自動更新。

## Architecture

```
FanGraphs API ─┐
Savant API ────┤  scripts/fetch_*.py  →  data/*.csv  →  BQ: mlb_shared.*
savant-extras ─┘                                          │
                                                          ├── baseball-mlops (reads)
                                                          └── mlb-win-probability (reads)
```

## BigQuery Tables (`data-platform-490901.mlb_shared`)

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
| `statcast_pitches` | Savant | 6,838,542 | Full pitch-level data (2015-2024, 122 cols) |

## Consumers

| Project | Tables Used |
|---------|-------------|
| [baseball-mlops](https://github.com/yasumorishima/baseball-mlops) | fg_batting, fg_pitching, sc_*, sprint_speed, park_factors |
| [mlb-win-probability](https://github.com/yasumorishima/mlb-win-probability) | statcast_pitches, fg_batting, fg_pitching, sprint_speed, oaa_team, catcher, park_factors |

## Data Quality

全 fetch スクリプトに組み込みバリデーション:

- **年カバレッジ**: 指定範囲の全年にデータがあるか検証
- **null 率**: 高 null カラム（>50%）を警告、年×カラムの null マトリクス
- **必須カラム**: player_id, season, 主要指標の存在確認
- **重複チェック**: player_id × season の一意性
- **BQ 検証**: アップロード後の行数・サイズ・スキーマ照合
- **BQ サマリー**: 毎回実行後に全テーブルの行数・サイズ一覧を出力

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

All projects use the same BQ column naming (`config.sanitize_columns()`):
- `%` → `_pct` (e.g., `K%` → `K_pct`)
- `/` → `_per_` (e.g., `wFB/C` → `wFB_per_C`)
- `+` → `_plus` (e.g., `Stuff+` → `Stuff_plus`)
- trailing `-` → `_minus` (e.g., `ERA-` → `ERA_minus`)

## Automation

Weekly refresh runs every Monday JST 10:00 via GitHub Actions (`weekly_refresh.yml`).

### Workflow Steps

| Step | Trigger | Description |
|------|---------|-------------|
| `all` (default) | `schedule` / `workflow_dispatch` | FanGraphs + Savant + Fielding + Park Factors |
| `fangraphs` | manual | FanGraphs のみ |
| `savant` | manual | Savant leaderboards のみ |
| `fielding` | manual | Sprint speed + OAA + Catcher |
| `park` | manual | Park factors のみ |

**CI 可観測性**: 全 5 fetch スクリプトに `PYTHONUNBUFFERED=1` + ステップ別経過時間ログを追加。BQ アップロードやバリデーションのどのフェーズで停止したかをログから即座に特定できる。

## Migration History

- **Phase 1** (2026-03-25): `statcast_pitches` を `mlb_wp` → `mlb_shared` に移行
- **Phase 2** (2026-03-26): FG stats / fielding / park_factors を `mlb_shared` に統合。mlb-win-probability の独自 fetch スクリプト削除。`mlb_wp` は `play_states` のみ残存。`mlb_statcast` データセット削除

## Credits

- Data: [Baseball Savant](https://baseballsavant.mlb.com/) / [FanGraphs](https://www.fangraphs.com/)
- API: [pybaseball](https://github.com/jldbc/pybaseball) / [savant-extras](https://pypi.org/project/savant-extras/)
