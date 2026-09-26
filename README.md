# mlb-data-pipeline

**MLB shared data pipeline.** Fetches from Baseball Savant / the MLB Stats API (and, frozen, FanGraphs) and publishes Parquet to a public Hugging Face Dataset.

> **Status (2026-06-10):** 実行基盤は **GitHub Actions（ubuntu-latest, 週次）**、データ正本は **Hugging Face Dataset [yasumorishima/mlb-stats](https://huggingface.co/datasets/yasumorishima/mlb-stats)**。BigQuery は 2026-04-19 退役、RPi5 SSD は 2026-05-29 廃止（経緯は Migration History 参照）。

> **Known limitation:** FanGraphs 由来の `fg_batting` / `fg_pitching` / `fg_pitcher_plus` は取得できず、
> HF 上は **2026-04 救出スナップショット（2025 シーズンまで）の静的保持**。
> 🔴 **遮断は 2 層あり、runner に効いているのはアドレスの方**＝pybaseball は User-Agent を一切設定せず
> 正直な既定値を送るのに、run `35565978836`（2026-09-21）で 12 シーズン全部 403。
> 一方**ブラウザを詐称した UA はどこからでも** 403 + `cf-mitigated: challenge` を返し、
> `python-requests` / `curl` / UA 無しなら住宅回線から **200**（2026-09-23 実測）。
> ⇒ 旧記述の「datacenter IP」は runner については正しく、UA の層とは別物。
> **代わりに `statsapi_batting` / `statsapi_pitching`（MLB Stats API・キー不要・2026-09-23 新設）が wOBA / wRC+ / WAR / FIP / xFIP を 2026 まで週次で持つ**（FanGraphs の表ではない。差の実測はデータセットカード参照）。
`park_factors` は **2026-09-23 に Baseball Savant へ移して週次更新に復帰**
> （savant-extras 0.5.0）。それ以前は「Savant 由来」と書きながら実体が FanGraphs Guts! で、
> **HF に一度も存在しなかった**（週次ジョブは緑のまま）＝復帰ではなく**新規投入**。
>
> 🔴 **毎回 `scripts/check_outputs.py` がテーブルごとの行数を job summary に出し、
> 期待したテーブルが欠けていれば run を赤にする。** 欠けたテーブルは upload されず、
> HF 上の前回のコピーがそのまま残る（緑のまま古くなるのを防ぐのが目的）。
> `fg_*` の免除は `RECHECK_AFTER` で期限切れになり、その後は再測定するまで落ちる。
> さらに **HF の公開版にあった季節が今回の出力から消えていれば `LOST SEASONS` で upload しない**（範囲を狭めた手動実行が公開データを黙って削るのを防ぐ）。

## Architecture

```
MLB Stats API ─┐
Savant API ────┤  scripts/fetch_*.py  ──→  Parquet (MLB_DATA_TARGET=parquet)
savant-extras ─┤  (unified write_dataframe)      │
FanGraphs API ─┘  (frozen, see above)            │
                                                 └─→ hf upload → HF Dataset yasumorishima/mlb-stats

Automation: .github/workflows/weekly_refresh.yml
  - Every Monday UTC 01:00 (JST 10:00) + workflow_dispatch
  - savant / fielding / park / statsapi を fetch → check_outputs.py で監査 → 通ったテーブルだけ HF へ upload
  - end_year の既定は「全 30 球団が 1 試合以上消化した最新季」（MLB Stats API の順位表から毎回算出）
  - statcast (pitch-level, heavy) は manual dispatch のみ
```

## Tables

Rows は 2026-09-23 の run `35864013558` の実測（全季節の合計）。

| Table | Source | Rows | Description |
|-------|--------|------|-------------|
| `fg_batting` | FanGraphs | ~6K/yr | Season batting stats (all columns, qual=50) |
| `fg_pitching` | FanGraphs | ~4K/yr | Season pitching stats (all columns, qual=30) |
| `fg_pitcher_plus` | FanGraphs | ~2.5K/yr | Stuff+/Location+/Pitching+ per pitch type (2020+) |
| `sc_batter_exitvelo` | Savant | 5,668 | Exit velocity, barrel rate |
| `sc_batter_expected` | Savant | 9,835 | xBA, xSLG, xwOBA |
| `sc_pitcher_exitvelo` | Savant | 6,630 | Exit velocity against |
| `sc_pitcher_expected` | Savant | 9,832 | xERA, xwOBA against |
| `sc_pitcher_arsenal` | Savant | 17,401 | Per-pitch-type stats (2017+) |
| `sc_bat_tracking` | Savant | 644 | Bat speed, swing tilt (2024+ Hawk-Eye) |
| `sc_batted_ball` | Savant | 4,077 | Pull/oppo rates |
| `sprint_speed` | Savant | 6,665 | Sprint speed (2015+) |
| `oaa` | Savant | 2,976 | Outs Above Average by position (2016+) |
| `oaa_team` | Savant | 330 | Team-level OAA aggregate |
| `catcher` | Savant | 952 | Pop time + framing (2015+) |
| `park_factors` | Savant | 360 | Ballpark factors, 1yr + 3yr windows (2015-2026, 12 × 30) |
| `statsapi_batting` | MLB Stats API | 12,100 | Season batting + sabermetrics (wOBA, wRAA, wRC, wRC+, WAR), all players, 2015+ |
| `statsapi_pitching` | MLB Stats API | 9,832 | Season pitching + sabermetrics (FIP, xFIP, FIP-, ERA-, WAR), all players, 2015+ |
| `statcast_pitches` | Savant | 6.8M+ | Full pitch-level data (2015-2025, 122 cols) |

Parquet mode writes statcast per-year (`statcast_pitches_2015.parquet` … `statcast_pitches_2025.parquet`); all other tables are single files. HF 上ではテーブル名のファイルがルート直下に置かれる（例: `fg_batting.parquet`）。

| 更新区分 | テーブル |
|---|---|
| 週次自動更新（Savant） | `sc_*`, `sprint_speed`, `oaa`, `oaa_team`, `catcher`, `park_factors` |
| 週次自動更新（MLB Stats API・毎回全季節を取り直す＝過去季も変わりうる） | `statsapi_batting`, `statsapi_pitching` |
| 静的（FanGraphs Cloudflare チャレンジ、2026-04 スナップショット） | `fg_batting`, `fg_pitching`, `fg_pitcher_plus` |
| 手動 dispatch のみ（重量） | `statcast_pitches` |

## Consumers

| Project | Status | Tables Used |
|---------|--------|-------------|
| [baseball-mlops](https://github.com/yasumorishima/baseball-mlops) | Weekly Retrain 停止中 | fg_batting, fg_pitching, sc_*, sprint_speed, park_factors |
| [mlb-win-probability](https://github.com/yasumorishima/mlb-win-probability) | Cloud Run 削除済 | statcast_pitches, fg_batting, fg_pitching, sprint_speed, oaa_team, catcher, park_factors |
| [dbt marts](dbt/) | 稼働中（CI で毎週 build） | statsapi_*, sc_*, sprint_speed, oaa → 分析用マート 5 本（選手評価・年齢曲線入力・球種スカウティング・指標の翌年への持ち越し）。全マートに契約（列名と型を強制）。同じ SQL を BigQuery sandbox（課金アカウントなし）でも build し、全テスト通過・DuckDB と全セル一致（浮動小数は 1e-15 以内） |

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
  -f memo="実行意図" -f steps=all -f start_year=2015   # end_year は省略で自動（開幕済みの最新季）

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
python scripts/fetch_statsapi.py --start-year 2015 --end-year 2026
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
- **dbt テスト**（[dbt/](dbt/README.md)）: マートのキー一意性・値域、自前 FIP と Stats API の一致（終了シーズン・20 IP 以上で差 0.001 以内・既知の例外 1 行）、K%・BB% の分母が PA／BF であることの検算、PA = AB+BB+HBP+SF+SH+CI の恒等式、全マートの契約（列名・型・順序が宣言と違えば build しない）、スカウティング信頼性マートの全セルを別経路で計算し直す照合

## Migration History

- **Phase 1** (2026-03-25): `statcast_pitches` を `mlb_wp` → `mlb_shared` に移行
- **Phase 2** (2026-03-26): FG stats / fielding / park_factors を `mlb_shared` に統合。mlb-win-probability の独自 fetch スクリプト削除。`mlb_statcast` データセット削除
- **Phase 3** (2026-04-17): 実行基盤を RPi5 + Parquet に移行。`MLB_DATA_TARGET` env + `run_backfill.py` 追加。17/17 work units backfill 完走
- **BQ 退役** (2026-04-19): Parquet vs BQ reconcile 後、BigQuery `mlb_shared` データセット削除
- **HF 移行** (2026-05-27〜29): RPi5 USB SSD 故障・廃止に伴い、全データを public HF Dataset [yasumorishima/mlb-stats](https://huggingface.co/datasets/yasumorishima/mlb-stats) へ移行（HF が正本）
- **Phase 4** (2026-06-10): 週次 refresh を GitHub Actions（ubuntu-latest, 無料）+ HF upload に再構築、cron 再開
- **Phase 5** (2026-09-23): 出力監査（`check_outputs.py`）で通ったテーブルだけ upload・`park_factors` を Savant へ（初投入）・`statsapi_batting` / `statsapi_pitching` を新設（FanGraphs 遮断の代替）・end_year 自動化・HF の季節を失う upload を拒否
- **BigQuery sandbox** (2026-09-26): dbt マートを課金アカウントの無い GCP プロジェクトでも build（HF が正本のまま。BigQuery は同じマートの 2 つ目のエンジンで、2026-04 に退役した課金ありの保存先とは別物）。鍵を置かず Workload Identity Federation で master からだけ書き込む

## Credits

- Data: [Baseball Savant](https://baseballsavant.mlb.com/) / [MLB Stats API](https://statsapi.mlb.com/) (Copyright MLB Advanced Media, L.P., terms: <http://gdx.mlb.com/components/copyright.txt>) / [FanGraphs](https://www.fangraphs.com/)
- API: [pybaseball](https://github.com/jldbc/pybaseball) / [savant-extras](https://pypi.org/project/savant-extras/)
