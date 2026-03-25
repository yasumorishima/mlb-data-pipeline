"""
Fetch FanGraphs season-level stats (batting + pitching + pitcher plus).

Superset: fetches ALL columns from pybaseball (not curated lists),
maps FanGraphs IDs to MLBAM IDs, saves CSV + loads to BQ.

Tables produced:
  mlb_shared.fg_batting         -- all FG batting stats (qual=50)
  mlb_shared.fg_pitching        -- all FG pitching stats (qual=30)
  mlb_shared.fg_pitcher_plus    -- Stuff+/Location+/Pitching+ per pitch type (2020+)

Usage:
  python scripts/fetch_fangraphs.py
  python scripts/fetch_fangraphs.py --batting-only
  python scripts/fetch_fangraphs.py --no-bq
"""

from __future__ import annotations

import argparse
import time
from pathlib import Path

import pandas as pd
import pybaseball as pb

from config import (
    BQ_FULL,
    DATA_DIR,
    END_SEASON,
    START_SEASON,
    fetch_with_retry,
    get_bq_client,
    map_fg_to_mlbam,
    sanitize_columns,
)

pb.cache.enable()

# =====================================================================
# Pitcher Plus: per-pitch-type Stuff+/Location+/Pitching+
# =====================================================================
PITCH_TYPES = ["FA", "SI", "SL", "CH", "CU", "FC", "FS", "KC"]
PLUS_OVERALL = ["Stuff+", "Location+", "Pitching+"]
PLUS_PER_PITCH = []
for _pt in PITCH_TYPES:
    PLUS_PER_PITCH.extend([f"Stf+ {_pt}", f"Loc+ {_pt}", f"Pit+ {_pt}"])


# =====================================================================
# Batting
# =====================================================================
def fetch_batting(
    start: int = START_SEASON,
    end: int = END_SEASON,
) -> pd.DataFrame:
    """Fetch FanGraphs batting stats — ALL columns, qual=50."""
    print(f"FanGraphs batting {start}-{end} ...")
    frames = []
    for year in range(start, end + 1):
        try:
            df = fetch_with_retry(pb.batting_stats, year, year, qual=50)
            df["Season"] = year
            frames.append(df)
            time.sleep(2)
            print(f"  [{year}] {len(df)} batters, {len(df.columns)} cols")
        except Exception as e:
            print(f"  [{year}] skipped: {e}")

    if not frames:
        print("ERROR: No batting data")
        return pd.DataFrame()

    combined = pd.concat(frames, ignore_index=True)

    # Rename for consistency
    combined = combined.rename(columns={"Season": "season", "Name": "name"})

    # Map to MLBAM ID (keeps both name and player_id)
    if "IDfg" in combined.columns:
        combined = map_fg_to_mlbam(combined)

    out = DATA_DIR / "fg_batting.csv"
    combined.to_csv(out, index=False)
    print(f"Saved: {out} ({len(combined):,} rows, {len(combined.columns)} cols)")
    return combined


# =====================================================================
# Pitching
# =====================================================================
def fetch_pitching(
    start: int = START_SEASON,
    end: int = END_SEASON,
) -> pd.DataFrame:
    """Fetch FanGraphs pitching stats — ALL columns, qual=30."""
    print(f"FanGraphs pitching {start}-{end} ...")
    frames = []
    for year in range(start, end + 1):
        try:
            df = fetch_with_retry(pb.pitching_stats, year, year, qual=30)
            df["Season"] = year
            frames.append(df)
            time.sleep(2)
            print(f"  [{year}] {len(df)} pitchers, {len(df.columns)} cols")
        except Exception as e:
            print(f"  [{year}] skipped: {e}")

    if not frames:
        print("ERROR: No pitching data")
        return pd.DataFrame()

    combined = pd.concat(frames, ignore_index=True)
    combined = combined.rename(columns={"Season": "season", "Name": "name"})

    if "IDfg" in combined.columns:
        combined = map_fg_to_mlbam(combined)

    out = DATA_DIR / "fg_pitching.csv"
    combined.to_csv(out, index=False)
    print(f"Saved: {out} ({len(combined):,} rows, {len(combined.columns)} cols)")
    return combined


# =====================================================================
# Pitcher Plus (Stuff+/Location+/Pitching+ per pitch type, 2020+)
# =====================================================================
def fetch_pitcher_plus(
    start: int = 2020,
    end: int = END_SEASON,
) -> pd.DataFrame:
    """Fetch per-pitch-type Stuff+/Location+/Pitching+ (2020+)."""
    print(f"Pitcher Plus stats {start}-{end} ...")
    frames = []
    for year in range(max(start, 2020), end + 1):
        try:
            df = fetch_with_retry(pb.pitching_stats, year, year, qual=30)
        except Exception as e:
            print(f"  [{year}] skipped: {e}")
            continue

        if df is None or len(df) == 0:
            continue

        # Extract IDfg + name + overall plus + per-pitch plus
        keep = ["IDfg", "Name"]
        keep += [c for c in PLUS_OVERALL if c in df.columns]
        keep += [c for c in PLUS_PER_PITCH if c in df.columns]
        df = df[[c for c in keep if c in df.columns]].copy()
        df["season"] = year

        frames.append(df)
        print(f"  [{year}] {len(df)} pitchers, {len(df.columns)} cols")
        time.sleep(2)

    if not frames:
        print("  No pitcher plus data (available from 2020)")
        return pd.DataFrame()

    combined = pd.concat(frames, ignore_index=True)
    combined = combined.rename(columns={"Name": "name"})

    if "IDfg" in combined.columns:
        combined = map_fg_to_mlbam(combined)

    out = DATA_DIR / "fg_pitcher_plus.csv"
    combined.to_csv(out, index=False)
    print(f"Saved: {out} ({len(combined):,} rows, {len(combined.columns)} cols)")
    return combined


# =====================================================================
# BQ upload
# =====================================================================
def _load_to_bq(df: pd.DataFrame, table_name: str) -> None:
    """Load DataFrame to BigQuery with sanitized columns."""
    from google.cloud import bigquery

    df_bq = sanitize_columns(df.copy())
    client = get_bq_client()
    table_ref = f"{BQ_FULL}.{table_name}"

    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        autodetect=True,
    )
    job = client.load_table_from_dataframe(df_bq, table_ref, job_config=job_config)
    job.result()

    table = client.get_table(table_ref)
    print(f"BQ: {table_ref} -- {table.num_rows:,} rows, {len(table.schema)} cols")


# =====================================================================
# Main
# =====================================================================
def main():
    parser = argparse.ArgumentParser(description="Fetch FanGraphs stats -> CSV + BQ")
    parser.add_argument("--start-year", type=int, default=START_SEASON)
    parser.add_argument("--end-year", type=int, default=END_SEASON)
    parser.add_argument("--no-bq", action="store_true")
    parser.add_argument("--batting-only", action="store_true")
    parser.add_argument("--pitching-only", action="store_true")
    parser.add_argument("--plus-only", action="store_true")
    args = parser.parse_args()

    run_all = not (args.batting_only or args.pitching_only or args.plus_only)

    bat_df = pd.DataFrame()
    pit_df = pd.DataFrame()
    plus_df = pd.DataFrame()

    if run_all or args.batting_only:
        bat_df = fetch_batting(args.start_year, args.end_year)

    if run_all or args.pitching_only:
        pit_df = fetch_pitching(args.start_year, args.end_year)

    if run_all or args.plus_only:
        plus_df = fetch_pitcher_plus(2020, args.end_year)

    if not args.no_bq:
        if len(bat_df) > 0:
            _load_to_bq(bat_df, "fg_batting")
        if len(pit_df) > 0:
            _load_to_bq(pit_df, "fg_pitching")
        if len(plus_df) > 0:
            _load_to_bq(plus_df, "fg_pitcher_plus")

    print("\nFanGraphs fetch complete.")


if __name__ == "__main__":
    main()
