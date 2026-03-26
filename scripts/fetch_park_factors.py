"""
Fetch park factors from Baseball Savant via savant-extras.

Table produced:
  mlb_shared.park_factors

Usage:
  python scripts/fetch_park_factors.py
  python scripts/fetch_park_factors.py --no-bq
"""

from __future__ import annotations

import argparse

import pandas as pd

from config import (
    BQ_FULL,
    DATA_DIR,
    END_SEASON,
    START_SEASON,
    get_bq_client,
    sanitize_columns,
    validate_bq_table,
    validate_dataframe,
)


def fetch_park_factors(start=START_SEASON, end=END_SEASON) -> pd.DataFrame:
    from savant_extras import park_factors_range

    print(f"Park factors {start}-{end} ...")
    df = park_factors_range(start, end)
    out = DATA_DIR / "park_factors.csv"
    df.to_csv(out, index=False)
    print(f"Saved: {out} ({len(df)} rows, {df['season'].min()}-{df['season'].max()})")
    return df


def load_to_bq(df: pd.DataFrame):
    from google.cloud import bigquery

    df_bq = sanitize_columns(df.copy())
    client = get_bq_client()
    table_ref = f"{BQ_FULL}.park_factors"
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        autodetect=True,
    )
    job = client.load_table_from_dataframe(df_bq, table_ref, job_config=job_config)
    job.result()
    table = client.get_table(table_ref)
    print(f"BQ: {table_ref} -- {table.num_rows:,} rows")


def main():
    parser = argparse.ArgumentParser(description="Fetch park factors -> CSV + BQ")
    parser.add_argument("--start-year", type=int, default=START_SEASON)
    parser.add_argument("--end-year", type=int, default=END_SEASON)
    parser.add_argument("--no-bq", action="store_true")
    args = parser.parse_args()

    df = fetch_park_factors(args.start_year, args.end_year)

    if len(df) > 0:
        validate_dataframe(df, "park_factors",
                           expected_years=(args.start_year, args.end_year),
                           required_cols=["season", "team", "pf_5yr", "pf_hr"])

    if not args.no_bq and len(df) > 0:
        load_to_bq(df)
        validate_bq_table("park_factors")

    print("\nPark factors fetch complete.")


if __name__ == "__main__":
    main()
