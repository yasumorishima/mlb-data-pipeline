"""
Delete legacy BQ tables that have been migrated to mlb_shared.

Usage:
    python cleanup_legacy_tables.py [--dry-run]

Targets:
  - mlb_statcast dataset: all tables (entire dataset)
  - mlb_wp dataset: redundant tables only (play_states is kept)
"""

from __future__ import annotations

import argparse
import sys

from google.cloud import bigquery

PROJECT = "data-platform-490901"

# mlb_wp tables that are now served by mlb_shared
MLB_WP_REDUNDANT = [
    "fg_batting_stats",
    "fg_pitching_stats",
    "pitcher_plus_stats",
    "statcast_sprint_speed",
    "statcast_oaa",
    "statcast_team_oaa",
    "statcast_catcher",
]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true", help="List tables without deleting")
    args = parser.parse_args()

    client = bigquery.Client(project=PROJECT)

    # --- mlb_statcast: delete entire dataset ---
    print("=== mlb_statcast dataset ===")
    try:
        tables = list(client.list_tables(f"{PROJECT}.mlb_statcast"))
        if tables:
            for t in tables:
                tid = f"{PROJECT}.mlb_statcast.{t.table_id}"
                if args.dry_run:
                    print(f"  [DRY-RUN] would delete {tid}")
                else:
                    client.delete_table(tid)
                    print(f"  Deleted {tid}")
        else:
            print("  No tables found")
    except Exception as e:
        print(f"  Dataset not found or error: {e}")

    if not args.dry_run:
        try:
            client.delete_dataset(f"{PROJECT}.mlb_statcast", not_found_ok=True)
            print("  Deleted dataset mlb_statcast")
        except Exception as e:
            print(f"  Could not delete dataset: {e}")

    # --- mlb_wp: delete redundant tables only ---
    print("\n=== mlb_wp redundant tables ===")
    for table_name in MLB_WP_REDUNDANT:
        tid = f"{PROJECT}.mlb_wp.{table_name}"
        try:
            client.get_table(tid)
            if args.dry_run:
                print(f"  [DRY-RUN] would delete {tid}")
            else:
                client.delete_table(tid)
                print(f"  Deleted {tid}")
        except Exception:
            print(f"  {tid} — not found (already deleted?)")

    # --- Summary ---
    print("\n=== mlb_wp remaining tables ===")
    try:
        remaining = list(client.list_tables(f"{PROJECT}.mlb_wp"))
        for t in remaining:
            print(f"  {t.table_id}")
        if not remaining:
            print("  (none)")
    except Exception as e:
        print(f"  Error listing: {e}")

    if args.dry_run:
        print("\n[DRY-RUN] No changes made.")
    else:
        print("\nCleanup complete.")


if __name__ == "__main__":
    main()
