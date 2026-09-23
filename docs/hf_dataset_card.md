---
pretty_name: MLB Shared Stats
tags:
  - baseball
  - mlb
  - statcast
  - sabermetrics
language:
  - en
configs:
  - config_name: catcher
    data_files: catcher.parquet
  - config_name: fg_batting
    data_files: fg_batting.parquet
  - config_name: fg_pitcher_plus
    data_files: fg_pitcher_plus.parquet
  - config_name: fg_pitching
    data_files: fg_pitching.parquet
  - config_name: oaa
    data_files: oaa.parquet
  - config_name: oaa_team
    data_files: oaa_team.parquet
  - config_name: park_factors
    data_files: park_factors.parquet
  - config_name: sc_bat_tracking
    data_files: sc_bat_tracking.parquet
  - config_name: sc_batted_ball
    data_files: sc_batted_ball.parquet
  - config_name: sc_batter_exitvelo
    data_files: sc_batter_exitvelo.parquet
  - config_name: sc_batter_expected
    data_files: sc_batter_expected.parquet
  - config_name: sc_pitcher_arsenal
    data_files: sc_pitcher_arsenal.parquet
  - config_name: sc_pitcher_exitvelo
    data_files: sc_pitcher_exitvelo.parquet
  - config_name: sc_pitcher_expected
    data_files: sc_pitcher_expected.parquet
  - config_name: sprint_speed
    data_files: sprint_speed.parquet
---

# MLB Shared Stats

Season-level MLB tables, refreshed weekly by
[yasumorishima/mlb-data-pipeline](https://github.com/yasumorishima/mlb-data-pipeline)
and published here as Parquet. One file per table at the repository root.

**Read the freshness column before using a table.** Not everything here is
current, and the rows in a stale table look exactly like the rows in a fresh
one.

## Tables

| File | Source | Seasons | Refreshed |
|---|---|---|---|
| `sc_batter_exitvelo.parquet` | Baseball Savant | 2015– | weekly |
| `sc_pitcher_exitvelo.parquet` | Baseball Savant | 2015– | weekly |
| `sc_batter_expected.parquet` | Baseball Savant | 2015– | weekly |
| `sc_pitcher_expected.parquet` | Baseball Savant | 2015– | weekly |
| `sc_pitcher_arsenal.parquet` | Baseball Savant | 2015– | weekly |
| `sc_batted_ball.parquet` | Baseball Savant | 2015– | weekly |
| `sc_bat_tracking.parquet` | Baseball Savant | 2024– (Hawk-Eye) | weekly |
| `sprint_speed.parquet` | Baseball Savant | 2015– | weekly |
| `oaa.parquet` | Baseball Savant | 2016– | weekly |
| `oaa_team.parquet` | Baseball Savant | 2016– | weekly |
| `catcher.parquet` | Baseball Savant | 2015– | weekly |
| `park_factors.parquet` | Baseball Savant | 2015– | weekly (added 2026-09-23) |
| `fg_batting.parquet` | FanGraphs | 2015–**2025** | ⚠️ **frozen 2026-04** |
| `fg_pitching.parquet` | FanGraphs | 2015–**2025** | ⚠️ **frozen 2026-04** |
| `fg_pitcher_plus.parquet` | FanGraphs | 2020–**2025** | ⚠️ **frozen 2026-04** |

`statcast_pitches` (pitch-level, ~6.8M rows) is fetched by manual dispatch
only and is not part of the weekly refresh.

### Why the three FanGraphs tables are frozen

FanGraphs refuses the GitHub Actions runner. The cause is the **address**,
not the client: pybaseball sets no `User-Agent`, so it sends the honest
`python-requests` default, and it still got HTTP 403 for all 12 seasons on
[run 35565978836](https://github.com/yasumorishima/mlb-data-pipeline/actions/runs/35565978836)
(2026-09-21). The same request from a home connection returns 200.

There is a second, separate layer that is easy to confuse with it: a client
claiming to be a browser gets 403 with `cf-mitigated: challenge` from
*anywhere*, including a residential line. Measured 2026-09-23.

So these three tables hold a rescue snapshot taken in 2026-04, which ends
with the 2025 season. **They contain no 2026 rows.** The pipeline reports
them on every run and the exemption has an expiry date, so this cannot
quietly become permanent.

### Why you can trust the "weekly" column

Every run audits what it produced. A table that fetched nothing used to be
skipped silently — the upload step pushes whatever files exist, so the copy
here simply stayed as it was and the job still went green. That is how
`park_factors` never arrived at all until 2026-09-23, and how the FanGraphs
tables went stale without an alarm.

Now `scripts/check_outputs.py` prints one row per expected table, publishes
only the tables that passed, and turns the run red otherwise.

## `park_factors` columns

One row per club per season, all 30 clubs in every season. 100 = neutral,
above 100 favours hitters. From Baseball Savant's Statcast park factors.

`season`, `team`, `venue_id`, `venue_name`, `n_pa_1yr`, `n_pa_3yr`,
`pf_1yr`, `pf_3yr`, `pf_3yr_years`, then `pf_hr`, `pf_1b`, `pf_2b`,
`pf_3b`, `pf_so`, `pf_bb`, `pf_obp`, `pf_hits`, `pf_woba`, `pf_wobacon`,
`pf_xwobacon`, `pf_bacon`, `pf_xbacon`, `pf_hardhit`, `pf_wobatto`.

Everything except `pf_1yr` and `n_pa_1yr` comes from a 3-year window;
`pf_3yr_years` records which window that was.

Two NaN patterns are expected, not defects:

- **8 rows of 360 have no 3-year window at all**, because the park has no
  three-year history: 2017 ATL, 2018 ATL, 2020 TEX, 2020 TOR, 2021 TEX,
  2025 OAK, 2025 TB, 2026 OAK. Their `pf_3yr*` columns are NaN while
  `pf_1yr` is filled. The club is kept rather than dropped, so a team never
  silently disappears from a season.
- **`pf_xwobacon`, `pf_xbacon` and `pf_hardhit` are NaN for all 30 clubs in
  2015 and 2016**, because those windows reach back before Statcast
  measured batted balls. Those three columns have 68 NaN, not 8.

## Subdirectories

`mlb_bat_tracking/` and `mlb_wp/` come from other projects and are **not**
touched by the weekly refresh. Their freshness is unrelated to the table
above.

## Usage

```python
import pandas as pd

base = "https://huggingface.co/datasets/yasumorishima/mlb-stats/resolve/main/"
pf = pd.read_parquet(base + "park_factors.parquet")
print(pf[pf["season"] == 2026].nlargest(3, "pf_3yr")[["team", "venue_name", "pf_3yr"]])
```

## Sources and terms

- [Baseball Savant](https://baseballsavant.mlb.com/) — MLB Advanced Media
- [FanGraphs](https://www.fangraphs.com/) — the three `fg_*` tables
- Fetched with [pybaseball](https://github.com/jldbc/pybaseball) and
  [savant-extras](https://github.com/yasumorishima/savant-extras)

This is a derived, aggregated mirror published for research and
reproducibility. No licence is asserted over the underlying data, which
remains subject to the terms of the sources above; check those before
redistributing. Open an issue on the
[pipeline repository](https://github.com/yasumorishima/mlb-data-pipeline)
for corrections.
