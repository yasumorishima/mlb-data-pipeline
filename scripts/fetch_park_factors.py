"""
Fetch park factors from Baseball Savant via savant-extras.

Table produced:
  mlb_shared.park_factors

Until savant-extras 0.5.0 this said "Baseball Savant" while the package
actually scraped FanGraphs Guts!, which now answers plain HTTP clients
with a Cloudflare challenge (403) from GitHub runners and a residential
line alike. Every season failed, the step was continue-on-error, and the
table never reached the dataset at all. 0.5.0 reads the Statcast park
factor leaderboard, so this step is expected to succeed and no longer
gets to fail quietly.

Columns changed with the source: pf_5yr and pf_fip are gone (Savant
publishes 1-year and 3-year windows, and no FIP factor), pf_1yr and
pf_3yr carry the runs factor, and venue_id / venue_name / n_pa_* / the
wOBA-family indices are new.

Usage:
  python scripts/fetch_park_factors.py
  python scripts/fetch_park_factors.py --no-bq
"""

from __future__ import annotations

import argparse
import time

import pandas as pd

from config import (
    DATA_DIR,
    DATA_TARGET,
    END_SEASON,
    START_SEASON,
    validate_bq_table,
    validate_dataframe,
    write_dataframe,
)

# Budget: park factors is one step within the 180-min job
BUDGET_MIN = 15


def _log_elapsed(label: str, start: float, budget_min: int = BUDGET_MIN):
    elapsed_min = (time.time() - start) / 60
    print(f"  [{label}] elapsed: {elapsed_min:.1f} min / {budget_min} min budget")
    if elapsed_min > budget_min * 0.8:
        print(f"  ⚠️ WARNING: {label} used {elapsed_min:.0f}/{budget_min} min "
              f"({elapsed_min / budget_min * 100:.0f}%) — timeout risk!")


def fetch_park_factors(start=START_SEASON, end=END_SEASON) -> pd.DataFrame:
    from savant_extras import park_factors_range

    print(f"Park factors {start}-{end} ...")
    df = park_factors_range(start, end)
    out = DATA_DIR / "park_factors.csv"
    df.to_csv(out, index=False)
    # A frame with no columns at all - what an all-seasons failure
    # returns - would raise KeyError on df["season"] here, before the
    # explanation written for that case in main() ever prints.
    span = (f"{df['season'].min()}-{df['season'].max()}"
            if "season" in df.columns and len(df) else "no seasons")
    print(f"Saved: {out} ({len(df)} rows, {span})")
    return df


def main():
    parser = argparse.ArgumentParser(description="Fetch park factors -> CSV + BQ")
    parser.add_argument("--start-year", type=int, default=START_SEASON)
    parser.add_argument("--end-year", type=int, default=END_SEASON)
    parser.add_argument("--no-bq", action="store_true")
    args = parser.parse_args()

    t0 = time.time()
    df = fetch_park_factors(args.start_year, args.end_year)
    _log_elapsed("park_factors fetch", t0)

    # An empty frame used to fall through both guards and exit 0, which is
    # how park_factors went missing from the dataset without the job ever
    # going red. Savant is reachable from the runner, so nothing here is
    # an expected empty.
    if len(df) == 0:
        print("ERROR: park_factors fetched 0 rows. Writing nothing would "
              "leave the previous copy in place and still exit 0.")
        raise SystemExit(1)

    # validate_dataframe returns a bool and prints its verdict. Calling
    # it bare - which every fetch script used to do - means a frame
    # missing a required column, or missing half the seasons asked for,
    # writes its parquet and exits 0.
    ok = validate_dataframe(df, "park_factors",
                            expected_years=(args.start_year, args.end_year),
                            required_cols=["season", "team", "pf_1yr", "pf_3yr"])
    if not ok:
        print("ERROR: park_factors failed validation (see the warnings "
              "above). Refusing to publish it over the previous copy.")
        raise SystemExit(1)

    if not args.no_bq:
        write_dataframe(df, "park_factors")
        if DATA_TARGET == "bq":
            validate_bq_table("park_factors")

    _log_elapsed("park_factors total", t0)
    print("\nPark factors fetch complete.")


if __name__ == "__main__":
    main()
