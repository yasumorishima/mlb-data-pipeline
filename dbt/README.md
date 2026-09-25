# dbt marts

Analysis-ready tables built on top of the raw parquet this repo publishes to
[yasumorishima/mlb-stats](https://huggingface.co/datasets/yasumorishima/mlb-stats).
dbt-core + DuckDB, so it runs anywhere for free: DuckDB reads the parquet over
HTTPS and nothing has to be downloaded or loaded first.

## Layers

| Layer | Materialized | What happens there |
| --- | --- | --- |
| `staging/` (`stg_*`) | view | One model per raw table: rename, type, rescale percents to 0-1. No joins. |
| `intermediate/` (`int_*`) | view | League totals per season and the FIP constant. |
| `marts/` (`mart_*`) | table | What an analyst or a dashboard reads. |

| Mart | Grain | Use |
| --- | --- | --- |
| `mart_batter_season` | batter-season, PA > 0 | Player evaluation: outcomes (wOBA, wRC+, WAR) next to process (xwOBA, batted-ball mix, bat speed, sprint speed, OAA). `woba_minus_xwoba` is the luck/skill gap in one column. |
| `mart_pitcher_season` | pitcher-season | K%, BB%, K-BB%, FIP rebuilt from the table, xERA, ERA - xERA, pitch-mix breadth. |
| `mart_batter_aging_pairs` | batter, season and season + 1 | Input for aging / development curves (delta method or a hierarchical model), weighted by the harmonic mean of PA. |
| `mart_pitch_arsenal_scouting` | pitcher-season-pitch | Usage rank plus whiff and run-value percentiles within the same pitch type and season. |

## Run it

```bash
pip install -r dbt/requirements.txt
cd dbt
dbt build --profiles-dir .                                   # reads from Hugging Face
dbt build --profiles-dir . --vars "{raw_base: /path/to/dir}" # or from local parquet
```

The DuckDB file lands in `dbt/target/mlb.duckdb` (override with `MLB_MARTS_DB`).
CI (`.github/workflows/dbt_marts.yml`) runs the same build on every change to
`dbt/` and after every successful weekly refresh.

## Data tests

Besides key uniqueness and value ranges on every mart:

- **FIP reconciliation** (`assert_fip_matches_statsapi`): our FIP, with the
  league constant rebuilt from the same table, must equal the Stats API figure
  to 0.001 on every finished season with 20+ IP. As of the 2026-09-23 fetch it
  does on 5,695 of 5,696 rows (largest gap 0.0002); the one exception, a 2024
  season split across three teams, is listed by id. Partial seasons are
  excluded because the sabermetrics endpoint lags the counting stats.
- **Rate denominators** (`assert_rates_use_pa_and_bf`): K% x PA = SO and
  BB% x PA = BB for batters (BF for pitchers). A 0-1 range test cannot catch
  a K% over AB, this can.
- **PA identity** (`assert_pa_identity`): PA = AB + BB + HBP + SF + SH + CI.
  The Stats API is off by exactly one PA on 10 of 12,100 player-seasons (as of
  the 2026-09-23 fetch); those are a warning (`assert_pa_identity_off_by_one`),
  a larger gap fails.

Each of these was checked to fail on a deliberately broken model: wrong FIP
constant, an unaggregated OAA join that fans out rows, K% over HR, K% over AB,
and Savant percents left on the 0-100 scale.
