"""
Migrate statcast_pitches from mlb_wp to mlb_shared via BQ-to-BQ copy.

This is a one-time migration script. It copies the full table using
CREATE TABLE ... AS SELECT (no network fetch, no pybaseball dependency).

Safety:
  - Checks source table exists before copying
  - Checks destination table does NOT exist (won't overwrite)
  - Use --force to overwrite existing destination

Usage:
  python scripts/migrate_statcast_pitches.py
  python scripts/migrate_statcast_pitches.py --force
  python scripts/migrate_statcast_pitches.py --dry-run
"""

from __future__ import annotations

import argparse
import sys

from config import BQ_PROJECT, BQ_DATASET, get_bq_client

SRC_DATASET = "mlb_wp"
TABLE_NAME = "statcast_pitches"
SRC_TABLE = f"{BQ_PROJECT}.{SRC_DATASET}.{TABLE_NAME}"
DST_TABLE = f"{BQ_PROJECT}.{BQ_DATASET}.{TABLE_NAME}"


def table_exists(client, table_ref: str) -> bool:
    try:
        client.get_table(table_ref)
        return True
    except Exception:
        return False


def get_row_count(client, table_ref: str) -> int:
    t = client.get_table(table_ref)
    return t.num_rows


def main():
    parser = argparse.ArgumentParser(description="Migrate statcast_pitches mlb_wp -> mlb_shared")
    parser.add_argument("--force", action="store_true", help="Overwrite destination if exists")
    parser.add_argument("--dry-run", action="store_true", help="Check only, don't copy")
    args = parser.parse_args()

    client = get_bq_client()

    # Check source
    if not table_exists(client, SRC_TABLE):
        print(f"ERROR: Source table {SRC_TABLE} does not exist")
        sys.exit(1)

    src_rows = get_row_count(client, SRC_TABLE)
    print(f"Source: {SRC_TABLE} -- {src_rows:,} rows")

    # Check destination
    dst_exists = table_exists(client, DST_TABLE)
    if dst_exists:
        dst_rows = get_row_count(client, DST_TABLE)
        print(f"Destination: {DST_TABLE} -- already exists ({dst_rows:,} rows)")
        if not args.force:
            print("Use --force to overwrite. Aborting.")
            sys.exit(0)
        print("--force specified, will overwrite")

    if args.dry_run:
        print("Dry run complete. Would copy {src_rows:,} rows.")
        sys.exit(0)

    # Execute BQ-to-BQ copy
    create_or_replace = "CREATE OR REPLACE" if args.force else "CREATE"
    sql = f"""
    {create_or_replace} TABLE `{DST_TABLE}` AS
    SELECT * FROM `{SRC_TABLE}`
    """
    print(f"Executing BQ copy...")
    job = client.query(sql)
    job.result()

    # Verify
    dst_rows = get_row_count(client, DST_TABLE)
    print(f"Done: {DST_TABLE} -- {dst_rows:,} rows")

    if dst_rows == src_rows:
        print(f"Verified: row count matches ({dst_rows:,} == {src_rows:,})")
    else:
        print(f"WARNING: row count mismatch! src={src_rows:,} dst={dst_rows:,}")
        sys.exit(1)


if __name__ == "__main__":
    main()
