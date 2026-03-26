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

from config import BQ_FULL, DATA_DIR, get_bq_client, validate_bq_table

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
# Expected pitch counts per year (approximate, for validation)
EXPECTED_PITCHES = {
    2015: 700_000, 2016: 700_000, 2017: 700_000, 2018: 700_000,
    2019: 700_000, 2020: 250_000, 2021: 700_000, 2022: 700_000,
    2023: 720_000, 2024: 740_000, 2025: 740_000,
}


def fetch_statcast_year(year: int) -> Path:
    """Download full Statcast data for one season, save as parquet.

    Uses monthly chunks as fallback when Savant returns malformed CSV.
    """
    from pybaseball import statcast
    import time

    out_path = PARQUET_DIR / f"statcast_{year}.parquet"
    print(f"Fetching Statcast {year} ...")

    # Try full season first
    try:
        df = statcast(f"{year}-03-20", f"{year}-11-30")
    except Exception as e:
        print(f"  Full-season fetch failed: {e}")
        print(f"  Falling back to monthly chunks...")
        # Monthly chunks to work around Savant CSV parse errors
        chunks = []
        month_ranges = [
            (f"{year}-03-20", f"{year}-04-30"),
            (f"{year}-05-01", f"{year}-05-31"),
            (f"{year}-06-01", f"{year}-06-30"),
            (f"{year}-07-01", f"{year}-07-31"),
            (f"{year}-08-01", f"{year}-08-31"),
            (f"{year}-09-01", f"{year}-09-30"),
            (f"{year}-10-01", f"{year}-11-30"),
        ]
        for start, end in month_ranges:
            for attempt in range(1, 4):
                try:
                    chunk = statcast(start, end)
                    chunks.append(chunk)
                    print(f"    {start} to {end}: {len(chunk):,} rows")
                    break
                except Exception as e2:
                    if attempt < 3:
                        print(f"    {start} to {end}: retry {attempt}/3 ({e2})")
                        time.sleep(10 * attempt)
                    else:
                        print(f"    {start} to {end}: FAILED after 3 attempts")
            time.sleep(2)
        if not chunks:
            print(f"  ERROR: No data fetched for {year}")
            return out_path
        df = pd.concat(chunks, ignore_index=True)
        # Deduplicate (monthly chunks may overlap)
        if "game_pk" in df.columns and "at_bat_number" in df.columns and "pitch_number" in df.columns:
            before = len(df)
            df = df.drop_duplicates(subset=["game_pk", "at_bat_number", "pitch_number"])
            if len(df) < before:
                print(f"  Deduped: {before:,} -> {len(df):,}")

    n = len(df)
    print(f"  {n:,} pitches, {len(df.columns)} columns")

    # Validate row count
    expected = EXPECTED_PITCHES.get(year, 600_000)
    min_expected = int(expected * 0.5)
    if n < min_expected:
        print(f"  WARNING: only {n:,} rows (expected ~{expected:,}+)")
    elif year == 2020:
        print(f"  OK (shortened 2020 season)")
    else:
        print(f"  OK (expected ~{expected:,})")

    # Key column check
    key_cols = ["game_pk", "pitcher", "batter", "events", "game_year",
                "release_speed", "launch_speed", "home_team", "away_team"]
    missing = [c for c in key_cols if c not in df.columns]
    if missing:
        print(f"  WARNING: missing key columns: {missing}")
    else:
        print(f"  Key columns: all {len(key_cols)} present")

    # Null rate for critical columns
    for col in ["release_speed", "launch_speed", "events"]:
        if col in df.columns:
            null_pct = df[col].isna().mean() * 100
            print(f"  {col}: {null_pct:.1f}% null")

    # At-bat outcomes (events IS NOT NULL)
    if "events" in df.columns:
        ab_count = df["events"].notna().sum()
        print(f"  At-bat outcomes: {ab_count:,} ({ab_count/n*100:.1f}%)")

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

    # Post-load validation: year coverage
    q = f"""
        SELECT CAST(game_year AS INT64) AS yr, COUNT(*) AS n,
               COUNTIF(events IS NOT NULL) AS ab_outcomes
        FROM `{table_ref}`
        GROUP BY yr ORDER BY yr
    """
    print("\nYear coverage:")
    total, total_ab = 0, 0
    for row in client.query(q).result():
        total += row.n
        total_ab += row.ab_outcomes
        expected = EXPECTED_PITCHES.get(row.yr, 600_000)
        flag = " LOW" if row.n < expected * 0.5 else ""
        print(f"  {row.yr}: {row.n:>10,} pitches, {row.ab_outcomes:>8,} ABs{flag}")
    print(f"  TOTAL: {total:>10,} pitches, {total_ab:>8,} ABs")


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
            try:
                fetch_statcast_year(year)
            except Exception as e:
                print(f"  ERROR fetching {year}: {e}")
                print(f"  Continuing with next year...")

    if not args.no_bq:
        load_to_bq(data_dir, append=args.append)


if __name__ == "__main__":
    main()
