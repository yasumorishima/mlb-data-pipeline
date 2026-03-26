"""
Fetch fielding and baserunning metrics from Baseball Savant.

Sources (superset of both projects):
  - Sprint speed (2015+)
  - Outs Above Average by position (2016+)
  - Catcher pop time + framing (2015+)

Tables produced:
  mlb_shared.sprint_speed
  mlb_shared.oaa
  mlb_shared.oaa_team
  mlb_shared.catcher

Usage:
  python scripts/fetch_fielding_running.py
  python scripts/fetch_fielding_running.py --sprint-only --no-bq
"""

from __future__ import annotations

import argparse
import time
from pathlib import Path

import pandas as pd

from config import (
    BQ_FULL,
    DATA_DIR,
    END_SEASON,
    START_SEASON,
    fetch_with_retry,
    get_bq_client,
    sanitize_columns,
    validate_bq_table,
    validate_dataframe,
)

# Budget: fielding/running is one step within the 180-min job
BUDGET_MIN = 60


def _log_elapsed(label: str, start: float, budget_min: int = BUDGET_MIN):
    elapsed_min = (time.time() - start) / 60
    print(f"  [{label}] elapsed: {elapsed_min:.1f} min / {budget_min} min budget")
    if elapsed_min > budget_min * 0.8:
        print(f"  ⚠️ WARNING: {label} used {elapsed_min:.0f}/{budget_min} min "
              f"({elapsed_min / budget_min * 100:.0f}%) — timeout risk!")


MIN_YEAR_OAA = 2016
MIN_YEAR_SPRINT = 2015
MIN_YEAR_CATCHER = 2015

# Statcast abbreviations for team name mapping
TEAM_NAME_TO_ABBREV = {
    "Angels": "LAA", "Astros": "HOU", "Athletics": "OAK",
    "Blue Jays": "TOR", "Braves": "ATL", "Brewers": "MIL",
    "Cardinals": "STL", "Cubs": "CHC", "D-backs": "ARI",
    "Diamondbacks": "ARI", "Dodgers": "LAD", "Giants": "SF",
    "Guardians": "CLE", "Indians": "CLE", "Mariners": "SEA",
    "Marlins": "MIA", "Mets": "NYM", "Nationals": "WSH",
    "Orioles": "BAL", "Padres": "SD", "Phillies": "PHI",
    "Pirates": "PIT", "Rangers": "TEX", "Rays": "TB",
    "Red Sox": "BOS", "Reds": "CIN", "Rockies": "COL",
    "Royals": "KC", "Tigers": "DET", "Twins": "MIN",
    "White Sox": "CWS", "Yankees": "NYY",
}


def _ensure_player_id_int(df: pd.DataFrame) -> pd.DataFrame:
    """Ensure player_id is int, drop rows without it."""
    if "player_id" in df.columns:
        df["player_id"] = pd.to_numeric(df["player_id"], errors="coerce")
        df = df.dropna(subset=["player_id"])
        df["player_id"] = df["player_id"].astype(int)
    return df


# =====================================================================
# Sprint speed
# =====================================================================
def fetch_sprint_speed(start=START_SEASON, end=END_SEASON) -> pd.DataFrame:
    from pybaseball import statcast_sprint_speed

    print(f"Sprint speed {max(start, MIN_YEAR_SPRINT)}-{end} ...")
    frames = []
    for year in range(max(start, MIN_YEAR_SPRINT), end + 1):
        try:
            df = fetch_with_retry(statcast_sprint_speed, year, min_opp=10)
            df["season"] = year
            frames.append(df)
            print(f"  [{year}] {len(df)} players")
            time.sleep(3)
        except Exception as e:
            print(f"  [{year}] skipped: {e}")

    if not frames:
        return pd.DataFrame()

    combined = pd.concat(frames, ignore_index=True)
    combined = _ensure_player_id_int(combined)

    out = DATA_DIR / "sprint_speed.csv"
    combined.to_csv(out, index=False)
    print(f"Saved: {out} ({len(combined):,} rows)")
    return combined


# =====================================================================
# OAA (by position + team aggregate)
# =====================================================================
def fetch_oaa(start=START_SEASON, end=END_SEASON) -> pd.DataFrame:
    from pybaseball import statcast_outs_above_average

    positions = {3: "1B", 4: "2B", 5: "3B", 6: "SS", 7: "LF", 8: "CF", 9: "RF"}

    print(f"OAA {max(start, MIN_YEAR_OAA)}-{end} ...")
    frames = []
    for year in range(max(start, MIN_YEAR_OAA), end + 1):
        for pos, pos_name in positions.items():
            try:
                df = fetch_with_retry(
                    statcast_outs_above_average, year, pos, min_att="q",
                )
                if "year" in df.columns:
                    df = df.rename(columns={"year": "season"})
                else:
                    df["season"] = year
                df["position"] = pos
                frames.append(df)
                print(f"  [{year}] {pos_name}: {len(df)} players")
                time.sleep(2)
            except Exception as e:
                print(f"  [{year}] {pos_name} skipped: {e}")

    if not frames:
        return pd.DataFrame()

    combined = pd.concat(frames, ignore_index=True)
    combined = _ensure_player_id_int(combined)

    out = DATA_DIR / "oaa.csv"
    combined.to_csv(out, index=False)
    print(f"Saved: {out} ({len(combined):,} rows)")

    # Team aggregate
    team_oaa = _aggregate_team_oaa(combined)
    if len(team_oaa) > 0:
        team_out = DATA_DIR / "oaa_team.csv"
        team_oaa.to_csv(team_out, index=False)
        print(f"Saved: {team_out} ({len(team_oaa)} team-seasons)")

    return combined


def _aggregate_team_oaa(df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate OAA by team + season."""
    oaa_col = next((c for c in ["outs_above_average", "oaa", "OAA"] if c in df.columns), None)
    team_col = next((c for c in ["display_team_name", "team", "team_name"] if c in df.columns), None)

    if oaa_col is None or team_col is None:
        return pd.DataFrame()

    df_filtered = df[df[team_col] != "---"].copy()
    team_oaa = df_filtered.groupby([team_col, "season"]).agg(
        total_oaa=(oaa_col, "sum"),
        avg_oaa=(oaa_col, "mean"),
        n_qualified_fielders=(oaa_col, "count"),
    ).reset_index()
    team_oaa = team_oaa.rename(columns={team_col: "team_name"})
    team_oaa["team_abbrev"] = team_oaa["team_name"].map(TEAM_NAME_TO_ABBREV)
    return team_oaa


# =====================================================================
# Catcher
# =====================================================================
def fetch_catcher(start=START_SEASON, end=END_SEASON) -> pd.DataFrame:
    from pybaseball import statcast_catcher_poptime

    print(f"Catcher stats {max(start, MIN_YEAR_CATCHER)}-{end} ...")
    poptime_frames = []
    framing_frames = []

    for year in range(max(start, MIN_YEAR_CATCHER), end + 1):
        # Pop time (primary)
        try:
            pt = fetch_with_retry(statcast_catcher_poptime, year, min_2b_att=5)
            pt["season"] = year
            if "entity_id" in pt.columns and "player_id" not in pt.columns:
                pt = pt.rename(columns={"entity_id": "player_id"})
            poptime_frames.append(pt)
            print(f"  [{year}] pop time: {len(pt)} catchers")
        except Exception as e:
            print(f"  [{year}] pop time skipped: {e}")
        time.sleep(2)

        # Framing (best-effort)
        try:
            from pybaseball import statcast_catcher_framing
            fr = statcast_catcher_framing(year, min_called_p="q")
            if fr is not None and len(fr) > 0:
                fr["season"] = year
                framing_frames.append(fr)
                print(f"  [{year}] framing: {len(fr)} catchers")
        except Exception as e:
            print(f"  [{year}] framing skipped (known issue): {type(e).__name__}")
        time.sleep(2)

    # Merge
    poptime_df = pd.concat(poptime_frames, ignore_index=True) if poptime_frames else pd.DataFrame()
    framing_df = pd.concat(framing_frames, ignore_index=True) if framing_frames else pd.DataFrame()

    if len(poptime_df) > 0:
        poptime_df = _ensure_player_id_int(poptime_df)
    if len(framing_df) > 0:
        framing_df = _ensure_player_id_int(framing_df)

    if len(poptime_df) > 0 and len(framing_df) > 0:
        fr_rename = {c: f"fr_{c}" for c in framing_df.columns if c not in ("player_id", "season")}
        framing_df = framing_df.rename(columns=fr_rename)
        combined = poptime_df.merge(framing_df, on=["player_id", "season"], how="outer")
    elif len(poptime_df) > 0:
        combined = poptime_df
    elif len(framing_df) > 0:
        combined = framing_df
    else:
        return pd.DataFrame()

    out = DATA_DIR / "catcher.csv"
    combined.to_csv(out, index=False)
    print(f"Saved: {out} ({len(combined):,} rows)")
    return combined


# =====================================================================
# BQ upload
# =====================================================================
def load_all_to_bq():
    from google.cloud import bigquery

    table_map = {
        "sprint_speed.csv": "sprint_speed",
        "oaa.csv": "oaa",
        "oaa_team.csv": "oaa_team",
        "catcher.csv": "catcher",
    }
    client = get_bq_client()
    for csv_name, table_name in table_map.items():
        path = DATA_DIR / csv_name
        if not path.exists():
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
    parser = argparse.ArgumentParser(description="Fetch fielding/running -> CSV + BQ")
    parser.add_argument("--start-year", type=int, default=START_SEASON)
    parser.add_argument("--end-year", type=int, default=END_SEASON)
    parser.add_argument("--no-bq", action="store_true")
    parser.add_argument("--sprint-only", action="store_true")
    parser.add_argument("--oaa-only", action="store_true")
    parser.add_argument("--catcher-only", action="store_true")
    args = parser.parse_args()

    run_all = not (args.sprint_only or args.oaa_only or args.catcher_only)

    t0 = time.time()
    if run_all or args.sprint_only:
        fetch_sprint_speed(args.start_year, args.end_year)
        _log_elapsed("sprint_speed", t0)
    if run_all or args.oaa_only:
        fetch_oaa(args.start_year, args.end_year)
        _log_elapsed("oaa", t0)
    if run_all or args.catcher_only:
        fetch_catcher(args.start_year, args.end_year)
        _log_elapsed("catcher", t0)

    # Validate before upload
    yr_range = (args.start_year, args.end_year)
    if run_all or args.sprint_only:
        sprint_csv = DATA_DIR / "sprint_speed.csv"
        if sprint_csv.exists():
            _df = pd.read_csv(sprint_csv)
            validate_dataframe(_df, "sprint_speed",
                               expected_years=(max(2015, yr_range[0]), yr_range[1]),
                               required_cols=["player_id", "season", "sprint_speed"])
    if run_all or args.oaa_only:
        oaa_csv = DATA_DIR / "oaa.csv"
        if oaa_csv.exists():
            _df = pd.read_csv(oaa_csv)
            validate_dataframe(_df, "oaa",
                               expected_years=(max(2016, yr_range[0]), yr_range[1]),
                               required_cols=["player_id", "season"])
        team_csv = DATA_DIR / "oaa_team.csv"
        if team_csv.exists():
            _df = pd.read_csv(team_csv)
            validate_dataframe(_df, "oaa_team",
                               expected_years=(max(2016, yr_range[0]), yr_range[1]),
                               required_cols=["team_name", "season", "total_oaa"])
    if run_all or args.catcher_only:
        catch_csv = DATA_DIR / "catcher.csv"
        if catch_csv.exists():
            _df = pd.read_csv(catch_csv)
            validate_dataframe(_df, "catcher",
                               expected_years=(max(2015, yr_range[0]), yr_range[1]),
                               required_cols=["player_id", "season"])

    if not args.no_bq:
        load_all_to_bq()
        for tn in ["sprint_speed", "oaa", "oaa_team", "catcher"]:
            try:
                validate_bq_table(tn)
            except Exception:
                pass

    _log_elapsed("fielding/running total", t0)
    print("\nFielding/running fetch complete.")


if __name__ == "__main__":
    main()
