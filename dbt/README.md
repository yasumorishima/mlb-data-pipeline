# dbt marts

Analysis-ready tables built on top of the raw parquet this repo publishes to
[yasumorishima/mlb-stats](https://huggingface.co/datasets/yasumorishima/mlb-stats).
dbt-core + DuckDB, so it runs anywhere for free: DuckDB reads the parquet over
HTTPS and nothing has to be downloaded or loaded first. The same SQL also
builds on BigQuery (see [BigQuery](#bigquery-sandbox)); every model and test
passes on both (with the same one warning), and the two builds agree on every
cell (text and integers exactly, floats within 1e-15).

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
| `mart_scouting_reliability` | metric x sample-size bin | Year-to-year correlation of each pitch metric for the same pitcher and pitch type, raw and within pitch type: how far a one-season number can be trusted. |

## Run it

```bash
pip install -r dbt/requirements.txt
cd dbt
dbt build --profiles-dir .                                   # reads from Hugging Face
dbt build --profiles-dir . --vars "{raw_base: /path/to/dir}" # or from local parquet
```

The DuckDB file lands in `dbt/target/mlb.duckdb` (override with `MLB_MARTS_DB`).
CI (`.github/workflows/dbt_marts.yml`) runs the same build on every change to
`dbt/` and after every successful weekly refresh. On master, when every model and
test passes, `export_marts.py` writes the four marts to parquet and they are
published to the dataset under
[`marts/`](https://huggingface.co/datasets/yasumorishima/mlb-stats/tree/main/marts),
so dashboards and models can read them without running dbt.

## BigQuery (sandbox)

The `bigquery` target builds the same models in the GCP project
`mlb-marts-sandbox`, which has **no billing account** (BigQuery sandbox: free,
1 TiB of queries a month, tables expire after 60 days, and 10 GiB of storage
for the life of the project that deleting data does not give back). The
Hugging Face dataset stays the source of truth; BigQuery is a second engine
for the same marts, not a store.

- `load_raw_bigquery.py <revision>` copies each raw source table from one
  dataset revision into the `mlb_raw` dataset with a batch load job (free and
  allowed in the sandbox; streaming inserts and DML are not) and checks the
  row count against the parquet. A revision that is already loaded is not
  loaded again: one full build writes about 25 MB (raw 19 MB, marts 6 MB,
  measured 2026-09-26), so the lifetime allowance covers some 400 builds.
  Each raw table's expiry is set 59 days out on every load.
- `dbt build --profiles-dir . --target bigquery` then builds and tests
  everything into `mlb_marts`. Credentials: Application Default Credentials,
  or an OAuth access token in `BQ_ACCESS_TOKEN`.
- CI runs both steps in the `bigquery` job of `dbt_marts.yml`, only on master
  and only after the DuckDB build passed, with the same input revision. No key
  is stored in GitHub: the job's OIDC token is exchanged through Workload
  Identity Federation, and the provider accepts only this workflow file on this
  repository's master branch.

What had to change so one set of SQL runs on both engines:

| DuckDB-only | Portable form | Why |
| --- | --- | --- |
| `x::double` | `cast(x as {{ float_type() }})` | `dbt.type_float()` is FLOAT, which is 32-bit on DuckDB |
| `count(*) filter (where c)` | `count(case when c then 1 end)` | no FILTER clause on BigQuery |
| `arg_max(pitch_type, usage)` | `row_number()` ordered by usage, then pitch code | also fixes 13 tied pitcher-seasons whose primary pitch was arbitrary |
| `"positional"` | `{{ adapter.quote("positional") }}` | a double-quoted name is a string literal on BigQuery |
| bare `sprint_speed` from table `sprint_speed` | `s.sprint_speed` | BigQuery resolves the bare name to the table (a STRUCT of the row) |
| `accepted_values: [100, 200, ...]` | same, with `quote: false` | BigQuery will not compare INT64 with a string |
| contract `data_type: double` | `float64` on BigQuery, `double` on DuckDB | the contract types are adapter-specific |

## Data tests

Besides key uniqueness and value ranges on every mart:

- **Contracts** on every mart (`contract: {enforced: true}` in
  `models/marts/_marts.yml`): dbt refuses to build a mart whose columns,
  types or order differ from the declaration, and key columns are NOT NULL.
  The contract found that `oaa` and `fielding_runs_prevented` were published
  as DOUBLE (DuckDB types `sum(BIGINT)` as HUGEINT); they are BIGINT now.
- **Reliability recomputed** (`assert_scouting_reliability_recomputed`): every
  cell of `mart_scouting_reliability` is computed again another way (group-by
  means and joins instead of window functions and UNPIVOT) and must match.

- **FIP reconciliation** (`assert_fip_matches_statsapi`): our FIP, with the
  league constant rebuilt from the same table, must equal the Stats API figure
  to 0.001 on every finished season with 20+ IP. As of the 2026-09-23 fetch it
  does on 5,695 of 5,696 rows (largest gap 0.0002); the one exception, a 2024
  season split across three teams, is listed by id. Partial seasons are
  excluded because the sabermetrics endpoint lags the counting stats.
- **Rate denominators** (`assert_rates_use_pa_and_bf`): K% x PA = SO and
  BB% x PA = BB for batters (BF for pitchers). A 0-1 range test notices K%
  over AB only on the 56 rows with AB = 0; this fails on every row.
- **ISO** (`assert_iso_from_total_bases`) from TB - H over AB, NULL when AB = 0,
  and **single-qualifier percentiles** (`assert_single_qualifier_pctile_is_null`)
  are NULL rather than 0.
- **PA identity** (`assert_pa_identity`): PA = AB + BB + HBP + SF + SH + CI.
  The Stats API is off by exactly one PA on 10 of 12,100 player-seasons (as of
  the 2026-09-23 fetch); those are a warning (`assert_pa_identity_off_by_one`),
  a larger gap fails.

Each of these was checked to fail on a deliberately broken model: wrong FIP
constant, an unaggregated OAA join that fans out rows, K% over HR, K% over AB,
Savant percents left on the 0-100 scale; a renamed column, a changed type,
a NULL key and the old OAA sum (contracts); and a lost within-type centring,
a moved bin edge, pairs joined across pitch types and in-progress seasons
kept (reliability).
