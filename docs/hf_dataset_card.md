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
  - config_name: statsapi_batting
    data_files: statsapi_batting.parquet
  - config_name: statsapi_pitching
    data_files: statsapi_pitching.parquet
  - config_name: mart_batter_season
    data_files: marts/mart_batter_season.parquet
  - config_name: mart_pitcher_season
    data_files: marts/mart_pitcher_season.parquet
  - config_name: mart_batter_aging_pairs
    data_files: marts/mart_batter_aging_pairs.parquet
  - config_name: mart_pitch_arsenal_scouting
    data_files: marts/mart_pitch_arsenal_scouting.parquet
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
| `sc_pitcher_arsenal.parquet` | Baseball Savant | 2017– | weekly |
| `sc_batted_ball.parquet` | Baseball Savant | 2015– | weekly |
| `sc_bat_tracking.parquet` | Baseball Savant | 2024– (Hawk-Eye) | weekly |
| `sprint_speed.parquet` | Baseball Savant | 2015– | weekly |
| `oaa.parquet` | Baseball Savant | 2016– | weekly |
| `oaa_team.parquet` | Baseball Savant | 2016– | weekly |
| `catcher.parquet` | Baseball Savant | 2015– | weekly |
| `park_factors.parquet` | Baseball Savant | 2015– | weekly (added 2026-09-23) |
| `statsapi_batting.parquet` | MLB Stats API | 2015– | weekly (added 2026-09-23) |
| `statsapi_pitching.parquet` | MLB Stats API | 2015– | weekly (added 2026-09-23) |
| `fg_batting.parquet` | FanGraphs | 2015–**2025** | ⚠️ **frozen 2026-04** |
| `fg_pitching.parquet` | FanGraphs | 2015–**2025** | ⚠️ **frozen 2026-04** |
| `fg_pitcher_plus.parquet` | FanGraphs | 2020–**2025** | ⚠️ **frozen 2026-04** |

`statcast_pitches` (pitch-level, ~6.8M rows) is fetched by manual dispatch
only and is not part of the weekly refresh.

## Marts (`marts/`)

Analysis-ready tables built from the tables above by the
[dbt project](https://github.com/yasumorishima/mlb-data-pipeline/tree/master/dbt)
and republished after every weekly refresh, only when all of its data tests
pass. Rows for the season in progress carry `is_partial = true`.

| File | Grain |
|---|---|
| `marts/mart_batter_season.parquet` | batter-season (PA > 0): wOBA, wRC+, WAR next to xwOBA, batted-ball mix, bat speed, sprint speed, OAA |
| `marts/mart_pitcher_season.parquet` | pitcher-season: K%, BB%, K-BB%, FIP, xERA, pitch-mix breadth |
| `marts/mart_batter_aging_pairs.parquet` | same batter, season and season + 1: input for aging curves |
| `marts/mart_pitch_arsenal_scouting.parquet` | pitcher-season-pitch: usage rank, whiff and run-value percentiles within type |

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

For current wOBA / wRC+ / WAR / FIP / xFIP, use `statsapi_batting` and
`statsapi_pitching` below. They are **not** FanGraphs tables.

## `statsapi_batting` / `statsapi_pitching`

From the [MLB Stats API](https://statsapi.mlb.com/api/v1/stats), keyless:
`stats=season` (counting and rate stats) joined with `stats=sabermetrics`
(wOBA, wRAA, wRC, wRC+, WAR and its components for hitters; FIP, xFIP,
FIP-, ERA-, WAR, RA9-WAR, leverage for pitchers), `playerPool=ALL`. Column
names are the API's own (`plateAppearances`, `wRcPlus`, `xfip`, …).
Responses carry "Copyright MLB Advanced Media, L.P." and point to the terms
at <http://gdx.mlb.com/components/copyright.txt>.

**MLB does not document how its sabermetrics feed is computed or where it
comes from.** What we measured, 2026-09-23, joining on MLBAM `player_id`
against the frozen FanGraphs snapshot above (every one of its 5,703 hitter
and 4,648 pitcher rows matched):

- Counting stats agree exactly (HR, SO; PA within 1).
- 2015–2021: wOBA within 0.002, wRC+ within 0.5 (FanGraphs stores
  integers), WAR within 0.2, FIP / xFIP within 0.005 — i.e. rounding.
- 2022–2025: wOBA still within 0.006, but wRC+ differs by up to 1.0 (2022,
  2023), 3.4 (2024) and 5.6 (2025), and the 2025 difference is lined up by
  club (ATH +5.1, CIN −3.1). Park factors were revised after the snapshot.
  WAR follows: up to 0.34 for hitters in 2024, and 0.39 for hitters and
  0.40 for pitchers in 2025. One 2024 pitcher differs by 0.31 in FIP and
  0.32 in xFIP; the other 433 by ≤ 0.005.

So **past seasons can change from one week to the next**; every run
refetches every season.

Rows and columns:

- One row per `(player_id, season)`, every player who appeared, not a
  qualified subset (e.g. 1,252 hitters in 2015 against 546 in the FanGraphs
  snapshot). The hitter count falls from ~1,250 to ~770 in 2022 because the
  universal DH ended pitchers batting.
- A player who changed clubs has **one row with his season total**.
  `last_team_id` is the club he finished with and `num_teams` how many he
  played for. It is not "his stats for that club".
- `is_partial` is true for the current season. `fetched_at` is when the row
  was read.
- `inningsPitched` is kept as the API's string: `"5.1"` means five and one
  third innings, not 5.1. Use `outs / 3`.
- Rates are absent where the denominator is zero: `woba` / `wRcPlus` for
  hitters with 0 PA (288 of 1,252 in 2015), `fip` / `xfip` for a pitcher
  with 0 outs. Placeholder strings such as `".---"` become null.
- In the current season a pitcher can appear in `stats=season` before
  `stats=sabermetrics` (one in 2026); his sabermetric columns are null.

The fetch fails and these two tables are not published if a response was
truncated (`totalSplits` ≠ rows returned), answered for another season,
lacks a required stat, repeats a player, has a player only the
sabermetrics side knows, or if a completed season now has fewer players
than the copy already published here. A run that checked the ids and
knows MLB removed a player (e.g. merged a duplicate id) can publish with
the `allow_statsapi_shrink` workflow input.

For every table except `statcast_pitches` (one file per season, so a
narrower run cannot remove the others), the output audit also refuses to
publish a file that lacks a season the published copy has, so a manual
run over a narrower year range cannot delete seasons from here. A
deliberate removal has to name the table in the `allow_lost_seasons`
workflow input.

The weekly run covers seasons up to the latest one in which every club
has played a game, read from the Stats API standings; before that (and
through an overseas opening series) it stops at the previous season.

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
- [MLB Stats API](https://statsapi.mlb.com/) — `statsapi_batting` and
  `statsapi_pitching`; responses state "Copyright MLB Advanced Media,
  L.P." and refer to <http://gdx.mlb.com/components/copyright.txt>
- Fetched with [pybaseball](https://github.com/jldbc/pybaseball) and
  [savant-extras](https://github.com/yasumorishima/savant-extras)

This is a derived, aggregated mirror published for research and
reproducibility. No licence is asserted over the underlying data, which
remains subject to the terms of the sources above; check those before
redistributing. Open an issue on the
[pipeline repository](https://github.com/yasumorishima/mlb-data-pipeline)
for corrections.
