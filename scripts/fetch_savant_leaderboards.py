"""
Fetch Baseball Savant leaderboard data (season-level aggregates).

Superset of both baseball-mlops and mlb-win-probability sources:
  - Batter exit velocity + barrel rate
  - Batter expected stats (xBA/xSLG/xwOBA)
  - Pitcher exit velocity (against)
  - Pitcher expected stats (against)
  - Pitcher arsenal stats (per pitch type)
  - Bat tracking (Hawk-Eye, 2024+)
  - Batted ball direction (pull/oppo rates)

Tables produced:
  mlb_shared.sc_batter_exitvelo
  mlb_shared.sc_batter_expected
  mlb_shared.sc_pitcher_exitvelo
  mlb_shared.sc_pitcher_expected
  mlb_shared.sc_pitcher_arsenal
  mlb_shared.sc_bat_tracking
  mlb_shared.sc_batted_ball

Usage:
  python scripts/fetch_savant_leaderboards.py
  python scripts/fetch_savant_leaderboards.py --no-bq
"""

from __future__ import annotations

import argparse
import time

import pandas as pd
import pybaseball as pb

from config import (
    mark_failed_validation,
    DATA_DIR,
    DATA_TARGET,
    END_SEASON,
    START_SEASON,
    fetch_with_retry,
    validate_bq_table,
    validate_dataframe,
    write_dataframe,
)

pb.cache.enable()

# Budget: Savant leaderboards is one step within the 180-min job
BUDGET_MIN = 60


def _log_elapsed(label: str, start: float, budget_min: int = BUDGET_MIN):
    elapsed_min = (time.time() - start) / 60
    print(f"  [{label}] elapsed: {elapsed_min:.1f} min / {budget_min} min budget")
    if elapsed_min > budget_min * 0.8:
        print(f"  ⚠️ WARNING: {label} used {elapsed_min:.0f}/{budget_min} min "
              f"({elapsed_min / budget_min * 100:.0f}%) — timeout risk!")


def _yearly_fetch(name, func, start, end, csv_name, **kwargs) -> pd.DataFrame:
    """Generic yearly fetch loop with retry."""
    print(f"Statcast {name} {start}-{end} ...")
    frames = []
    for year in range(start, end + 1):
        try:
            df = fetch_with_retry(func, year, **kwargs)
            df["Season"] = year
            frames.append(df)
            print(f"  [{year}] {len(df)} rows")
            time.sleep(1)
        except Exception as e:
            print(f"  [{year}] skipped: {e}")

    if not frames:
        print(f"  No {name} data")
        return pd.DataFrame()

    out = pd.concat(frames, ignore_index=True)
    # Normalize year column to lowercase 'season' for consistency with FG tables
    if "Season" in out.columns:
        out = out.rename(columns={"Season": "season"})
    path = DATA_DIR / csv_name
    out.to_csv(path, index=False)
    print(f"Saved: {path} ({len(out):,} rows)")
    return out


def fetch_batter_exitvelo(start=START_SEASON, end=END_SEASON) -> pd.DataFrame:
    return _yearly_fetch(
        "batter exit velo",
        pb.statcast_batter_exitvelo_barrels,
        start, end,
        "sc_batter_exitvelo.csv",
        minBBE=50,
    )


def fetch_batter_expected(start=START_SEASON, end=END_SEASON) -> pd.DataFrame:
    return _yearly_fetch(
        "batter expected",
        pb.statcast_batter_expected_stats,
        start, end,
        "sc_batter_expected.csv",
        minPA=50,
    )


def fetch_pitcher_exitvelo(start=START_SEASON, end=END_SEASON) -> pd.DataFrame:
    return _yearly_fetch(
        "pitcher exit velo",
        pb.statcast_pitcher_exitvelo_barrels,
        start, end,
        "sc_pitcher_exitvelo.csv",
        minBBE=50,
    )


def fetch_pitcher_expected(start=START_SEASON, end=END_SEASON) -> pd.DataFrame:
    return _yearly_fetch(
        "pitcher expected",
        pb.statcast_pitcher_expected_stats,
        start, end,
        "sc_pitcher_expected.csv",
        minPA=50,
    )


def fetch_pitcher_arsenal(start=START_SEASON, end=END_SEASON) -> pd.DataFrame:
    return _yearly_fetch(
        "pitcher arsenal",
        pb.statcast_pitcher_arsenal_stats,
        start, end,
        "sc_pitcher_arsenal.csv",
        minPA=25,
    )


def fetch_bat_tracking(start=2024, end=END_SEASON) -> pd.DataFrame:
    """Statcast bat tracking (Hawk-Eye, 2024+)."""
    print(f"Statcast bat tracking {start}-{end} ...")
    from savant_extras import bat_tracking

    frames = []
    for year in range(max(start, 2024), end + 1):
        try:
            df = bat_tracking(
                f"{year}-03-20", f"{year}-11-05",
                player_type="batter", min_swings="q",
            )
            df["Season"] = year
            frames.append(df)
            print(f"  [{year}] {len(df)} rows")
            time.sleep(2)
        except Exception as e:
            print(f"  [{year}] skipped: {e}")

    if not frames:
        print("  No bat tracking data")
        return pd.DataFrame()

    out = pd.concat(frames, ignore_index=True)
    if "Season" in out.columns:
        out = out.rename(columns={"Season": "season"})
    path = DATA_DIR / "sc_bat_tracking.csv"
    out.to_csv(path, index=False)
    print(f"Saved: {path} ({len(out):,} rows)")
    return out


def fetch_batted_ball(start=START_SEASON, end=END_SEASON) -> pd.DataFrame:
    """Statcast batted ball direction (pull/oppo rates)."""
    print(f"Statcast batted ball {start}-{end} ...")
    from savant_extras import batted_ball

    frames = []
    for year in range(start, end + 1):
        try:
            df = batted_ball(year, player_type="batter", min_bbe="q")
            df["season"] = year
            frames.append(df)
            print(f"  [{year}] {len(df)} rows")
            time.sleep(1)
        except Exception as e:
            print(f"  [{year}] skipped: {e}")

    if not frames:
        print("  No batted ball data")
        return pd.DataFrame()

    out = pd.concat(frames, ignore_index=True)
    path = DATA_DIR / "sc_batted_ball.csv"
    out.to_csv(path, index=False)
    print(f"Saved: {path} ({len(out):,} rows)")
    return out


# =====================================================================
# BQ upload
# =====================================================================
TABLE_MAP = {
    "sc_batter_exitvelo.csv": "sc_batter_exitvelo",
    "sc_batter_expected.csv": "sc_batter_expected",
    "sc_pitcher_exitvelo.csv": "sc_pitcher_exitvelo",
    "sc_pitcher_expected.csv": "sc_pitcher_expected",
    "sc_pitcher_arsenal.csv": "sc_pitcher_arsenal",
    "sc_bat_tracking.csv": "sc_bat_tracking",
    "sc_batted_ball.csv": "sc_batted_ball",
}


def write_all_tables(skip=frozenset()):
    """Write all Savant leaderboard CSVs to the configured target (BQ or Parquet).

    Tables named in ``skip`` failed validation and are not written.
    """
    for csv_name, table_name in TABLE_MAP.items():
        if table_name in skip:
            print(f"  SKIP: {table_name} failed validation")
            continue
        path = DATA_DIR / csv_name
        if not path.exists():
            print(f"  SKIP: {csv_name} not found")
            continue

        df = pd.read_csv(path)
        if df.empty:
            continue

        write_dataframe(df, table_name)


# =====================================================================
# Main
# =====================================================================
def main():
    parser = argparse.ArgumentParser(description="Fetch Savant leaderboards -> CSV + BQ")
    parser.add_argument("--start-year", type=int, default=START_SEASON)
    parser.add_argument("--end-year", type=int, default=END_SEASON)
    parser.add_argument("--no-bq", action="store_true")
    args = parser.parse_args()

    t0 = time.time()
    results = {}
    results["sc_batter_exitvelo"] = fetch_batter_exitvelo(args.start_year, args.end_year)
    _log_elapsed("batter_exitvelo", t0)
    results["sc_batter_expected"] = fetch_batter_expected(args.start_year, args.end_year)
    _log_elapsed("batter_expected", t0)
    results["sc_pitcher_exitvelo"] = fetch_pitcher_exitvelo(args.start_year, args.end_year)
    _log_elapsed("pitcher_exitvelo", t0)
    results["sc_pitcher_expected"] = fetch_pitcher_expected(args.start_year, args.end_year)
    _log_elapsed("pitcher_expected", t0)
    results["sc_pitcher_arsenal"] = fetch_pitcher_arsenal(args.start_year, args.end_year)
    _log_elapsed("pitcher_arsenal", t0)
    results["sc_bat_tracking"] = fetch_bat_tracking(2024, args.end_year)
    _log_elapsed("bat_tracking", t0)
    results["sc_batted_ball"] = fetch_batted_ball(args.start_year, args.end_year)
    _log_elapsed("batted_ball", t0)

    # Validate all fetched data. validate_dataframe returns a bool and
    # prints its verdict; a bare call lets a table that lost seasons or
    # columns overwrite the published copy and still exit 0.
    # First season each leaderboard has on Savant: bat tracking starts in
    # 2024 and pitch arsenal stats in 2017 (2015 and 2016 return only the
    # CSV header), so asking them for 2015 would fail every run.
    first_season = {"sc_bat_tracking": 2024, "sc_pitcher_arsenal": 2017}
    failed = []
    for table_name, df in results.items():
        if len(df) == 0:
            continue
        yr_range = (max(first_season.get(table_name, args.start_year),
                        args.start_year), args.end_year)
        if not validate_dataframe(df, table_name, expected_years=yr_range):
            failed.append(table_name)

    if not args.no_bq:
        write_all_tables(skip=set(failed))
        if DATA_TARGET == "bq":
            for table_name in TABLE_MAP.values():
                try:
                    validate_bq_table(table_name)
                except Exception as e:
                    print(f"  validate_bq_table({table_name}) failed: {type(e).__name__}: {e}")

    _log_elapsed("Savant leaderboards total", t0)
    print("\nSavant leaderboards fetch complete.")
    # Exit non-zero only after the tables that passed were written, so
    # one bad table does not cost the week for the others.
    if failed:
        for table in failed:
            mark_failed_validation(table)
        print(f"ERROR: {', '.join(failed)} failed validation (see the warnings "
              "above). Not written, so the previous copy stays published.")
        raise SystemExit(1)


if __name__ == "__main__":
    main()
