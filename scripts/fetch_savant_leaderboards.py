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
    BQ_FULL,
    DATA_DIR,
    END_SEASON,
    START_SEASON,
    fetch_with_retry,
    get_bq_client,
    sanitize_columns,
)

pb.cache.enable()


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
            df["Season"] = year
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


def load_all_to_bq():
    """Load all Savant leaderboard CSVs to BigQuery."""
    from google.cloud import bigquery

    client = get_bq_client()
    for csv_name, table_name in TABLE_MAP.items():
        path = DATA_DIR / csv_name
        if not path.exists():
            print(f"  SKIP: {csv_name} not found")
            continue

        df = pd.read_csv(path)
        if df.empty:
            continue

        df = sanitize_columns(df)
        table_ref = f"{BQ_FULL}.{table_name}"
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE",
            autodetect=True,
        )
        job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
        job.result()
        table = client.get_table(table_ref)
        print(f"BQ: {table_ref} -- {table.num_rows:,} rows")


# =====================================================================
# Main
# =====================================================================
def main():
    parser = argparse.ArgumentParser(description="Fetch Savant leaderboards -> CSV + BQ")
    parser.add_argument("--start-year", type=int, default=START_SEASON)
    parser.add_argument("--end-year", type=int, default=END_SEASON)
    parser.add_argument("--no-bq", action="store_true")
    args = parser.parse_args()

    fetch_batter_exitvelo(args.start_year, args.end_year)
    fetch_batter_expected(args.start_year, args.end_year)
    fetch_pitcher_exitvelo(args.start_year, args.end_year)
    fetch_pitcher_expected(args.start_year, args.end_year)
    fetch_pitcher_arsenal(args.start_year, args.end_year)
    fetch_bat_tracking(2024, args.end_year)
    fetch_batted_ball(args.start_year, args.end_year)

    if not args.no_bq:
        load_all_to_bq()

    print("\nSavant leaderboards fetch complete.")


if __name__ == "__main__":
    main()
