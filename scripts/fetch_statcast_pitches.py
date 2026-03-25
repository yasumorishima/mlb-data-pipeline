"""
Fetch full Statcast pitch-level data and load to BigQuery.

Downloads all pitches for each season via pybaseball.statcast(),
saves as yearly parquet files, then loads to BQ year-by-year
(to avoid OOM on limited-RAM runners).

Table produced:
  mlb_shared.statcast_pitches  (6.8M+ rows, 120+ columns)

Usage:
  python scripts/fetch_statcast_pitches.py --years 2015-2024
  python scripts/fetch_statcast_pitches.py --years 2024 --append
  python scripts/fetch_statcast_pitches.py --load-only --data-dir data/statcast/
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path

import pandas as pd

from config import BQ_FULL, DATA_DIR, get_bq_client

PARQUET_DIR = DATA_DIR / "statcast"
PARQUET_DIR.mkdir(parents=True, exist_ok=True)


# =====================================================================
# Computed columns (shared by both projects)
# =====================================================================
def _add_computed_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Add derived columns useful for both projects."""
    if "home_score" in df.columns and "away_score" in df.columns:
        df["score_diff"] = df["home_score"] - df["away_score"]
    if "inning_topbot" in df.columns:
        df["is_bottom"] = (df["inning_topbot"] == "Bot").astype(int)
    if all(c in df.columns for c in ["on_1b", "on_2b", "on_3b"]):
        df["total_runners"] = (
            df["on_1b"].notna().astype(int)
            + df["on_2b"].notna().astype(int)
            + df["on_3b"].notna().astype(int)
        )
        df["scoring_position"] = (
            df["on_2b"].notna().astype(int)
            + df["on_3b"].notna().astype(int)
        )
    return df


def _convert_types(df: pd.DataFrame) -> pd.DataFrame:
    """Convert types for BQ/Arrow compatibility."""
    # String columns
    str_cols = [
        "sv_id", "game_date", "des", "description",
        "pitch_type", "pitch_name", "events", "bb_type", "type",
        "home_team", "away_team", "player_name", "stand", "p_throws",
        "inning_topbot", "game_type", "umpire",
        "if_fielding_alignment", "of_fielding_alignment",
    ]
    for col in str_cols:
        if col in df.columns:
            mask = df[col].notna()
            df[col] = df[col].astype(object)
            df.loc[mask, col] = df.loc[mask, col].astype(str)
            df.loc[~mask, col] = None

    # Numeric columns (nullable Int64 -> float64)
    numeric_cols = [
        "inning", "outs_when_up", "balls", "strikes",
        "home_score", "away_score", "bat_score", "fld_score",
        "post_home_score", "post_away_score",
        "launch_angle", "hit_distance_sc",
        "release_spin_rate", "spin_axis",
        "bat_speed", "swing_length", "attack_angle",
        "zone", "score_diff", "is_bottom",
        "total_runners", "scoring_position",
        "post_bat_score", "post_fld_score",
        "bat_score_diff", "hit_location",
        "age_bat", "age_pit",
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("float64")

    # Remaining nullable int types
    for col in df.columns:
        dtype_str = str(df[col].dtype)
        if dtype_str in ("Int8", "Int16", "Int32", "Int64",
                         "UInt8", "UInt16", "UInt32", "UInt64"):
            df[col] = df[col].astype("float64")

    return df


# =====================================================================
# Fetch
# =====================================================================
def fetch_statcast_year(year: int) -> Path:
    """Download full Statcast data for one season, save as parquet."""
    from pybaseball import statcast

    out_path = PARQUET_DIR / f"statcast_{year}.parquet"
    print(f"Fetching Statcast {year} ...")

    df = statcast(f"{year}-03-20", f"{year}-11-30")
    print(f"  {len(df):,} pitches, {len(df.columns)} columns")

    df.to_parquet(out_path, index=False)
    print(f"  Saved: {out_path}")
    return out_path


# =====================================================================
# Load to BQ
# =====================================================================
def load_to_bq(data_dir: Path, append: bool = False):
    """Load parquet files to BigQuery, one year at a time."""
    from google.cloud import bigquery

    client = get_bq_client()
    parquets = sorted(data_dir.glob("statcast_*.parquet"))
    if not parquets:
        print(f"ERROR: No parquet files in {data_dir}")
        return

    print(f"Found {len(parquets)} parquet files")
    table_ref = f"{BQ_FULL}.statcast_pitches"
    total_rows = 0

    for i, pf in enumerate(parquets):
        df = pd.read_parquet(pf)
        year_label = pf.stem.replace("statcast_", "")
        print(f"\n  {pf.name}: {len(df):,} rows, {len(df.columns)} cols")

        df = _add_computed_columns(df)
        df = _convert_types(df)

        if append:
            disposition = "WRITE_APPEND"
        else:
            disposition = "WRITE_TRUNCATE" if i == 0 else "WRITE_APPEND"

        job_config = bigquery.LoadJobConfig(
            write_disposition=disposition,
            autodetect=True,
        )
        if disposition == "WRITE_APPEND":
            job_config.schema_update_options = [
                bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION,
            ]

        print(f"  Loading to BQ ({disposition})...")
        job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
        job.result()
        total_rows += len(df)
        print(f"  Done: {len(df):,} rows")
        del df

    table = client.get_table(table_ref)
    print(f"\nBQ: {table_ref} -- {table.num_rows:,} rows, {len(table.schema)} cols, "
          f"{table.num_bytes / 1024**3:.2f} GB")


# =====================================================================
# Main
# =====================================================================
def main():
    parser = argparse.ArgumentParser(description="Fetch/load Statcast pitches")
    parser.add_argument("--years", type=str,
                        help="Year(s) to fetch: '2024' or '2015-2024'")
    parser.add_argument("--load-only", action="store_true",
                        help="Skip fetch, just load existing parquets to BQ")
    parser.add_argument("--data-dir", type=str, default=str(PARQUET_DIR),
                        help="Directory with parquet files")
    parser.add_argument("--append", action="store_true",
                        help="Append to BQ table instead of truncate")
    parser.add_argument("--no-bq", action="store_true")
    args = parser.parse_args()

    data_dir = Path(args.data_dir)

    if not args.load_only and args.years:
        if "-" in args.years:
            start, end = args.years.split("-")
            years = range(int(start), int(end) + 1)
        else:
            years = [int(args.years)]

        for year in years:
            fetch_statcast_year(year)

    if not args.no_bq:
        load_to_bq(data_dir, append=args.append)


if __name__ == "__main__":
    main()
