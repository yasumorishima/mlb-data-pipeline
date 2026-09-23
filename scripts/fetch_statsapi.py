"""
Fetch season batting / pitching tables from the MLB Stats API.

Tables produced:
  mlb_shared.statsapi_batting    -- stats=season + stats=sabermetrics, group=hitting
  mlb_shared.statsapi_pitching   -- stats=season + stats=sabermetrics, group=pitching

Why this exists
---------------
fg_batting / fg_pitching come from FanGraphs, which answers the GitHub
runner with 403 (run 35565978836, 2026-09-21), so they are frozen at a
2026-04 snapshot that ends with 2025. The Stats API is keyless and reachable
from the runner, and its ``sabermetrics`` stat type carries wOBA, wRAA, wRC,
wRC+, WAR, FIP and xFIP.

Measured 2026-09-23 against that frozen snapshot, joined on MLBAM id: for
2015 / 2019 / 2020 the two agree to rounding (wOBA max |diff| 0.0021, wRC+
0.50 - FanGraphs stores integers - WAR 0.05-0.15, FIP / xFIP 0.005). For 2025
wOBA / wRAA still agree but wRC+ differs by up to 5.6 with the difference
lined up by club (ATH +5.06, CIN -3.05): park factors revised after the
snapshot. So past seasons can change and every run refetches all of them.

These tables are NOT FanGraphs tables and are not named as such. MLB does not
document how its sabermetrics feed is computed or where it comes from.

One row per (player_id, season). A player who changed clubs has one row with
his season total; ``last_team_id`` is the club he finished with and
``num_teams`` how many he played for.

Usage:
  python scripts/fetch_statsapi.py --start-year 2015 --end-year 2026
  python scripts/fetch_statsapi.py --no-bq
"""

from __future__ import annotations

import argparse
import datetime as dt
import http.client
import io
import json
import time
import urllib.error
import urllib.request

import pandas as pd

from config import (
    DATA_TARGET,
    END_SEASON,
    START_SEASON,
    validate_bq_table,
    validate_dataframe,
    write_dataframe,
)

API = "https://statsapi.mlb.com/api/v1/stats"
HF_BASE = "https://huggingface.co/datasets/yasumorishima/mlb-stats/resolve/main/"
USER_AGENT = "mlb-data-pipeline (+https://github.com/yasumorishima/mlb-data-pipeline)"
LIMIT = 5000
BUDGET_MIN = 10

# Kept as its own flat map so tests/test_check_outputs.py, which reads the
# fetch scripts for table-name dicts, can see what this script writes.
TABLES = {
    "hitting": "statsapi_batting",
    "pitching": "statsapi_pitching",
}

GROUPS = {
    "hitting": {
        "season_required": ["plateAppearances", "atBats", "hits", "homeRuns",
                            "baseOnBalls", "strikeOuts"],
        # Counting values every player carries.
        "saber_required": ["wRaa", "wRc", "war"],
        # Rates the API omits when the denominator is zero. Measured
        # 2026-09-23: every hitter without woba / wRcPlus had 0 plate
        # appearances (288 in 2015, 92 in 2025), and the one 2015 pitcher
        # without fip / xfip had recorded 0 outs.
        "rate_required": ["woba", "wRcPlus"],
        "denominator": "plateAppearances",
    },
    "pitching": {
        "season_required": ["inningsPitched", "outs", "battersFaced",
                            "strikeOuts", "baseOnBalls", "homeRuns"],
        "saber_required": ["war"],
        "rate_required": ["fip", "xfip"],
        "denominator": "outs",
    },
}

# Rate stats the API sends as strings (".287", "3.45"), sometimes as ".---"
# or "-.--" when the denominator is zero. inningsPitched is deliberately NOT
# here: "5.1" means five and one third innings, so it stays a string and
# ``outs`` carries the exact count.
_STRING_RATES = {
    "avg", "obp", "slg", "ops", "babip", "era", "whip", "winPercentage",
    "stolenBasePercentage", "caughtStealingPercentage", "groundOutsToAirouts",
    "atBatsPerHomeRun", "strikeoutWalkRatio", "strikeoutsPer9Inn",
    "walksPer9Inn", "hitsPer9Inn", "homeRunsPer9", "runsScoredPer9",
    "pitchesPerInning", "strikePercentage",
}


class FetchError(RuntimeError):
    """The API answered with something this script must not publish."""


def _get(url: str, retries: int = 3) -> dict:
    """GET and decode JSON, retrying only what a retry can fix.

    Transient: 5xx / 429, a dropped or reset connection, a truncated body,
    a timeout, and a body that is not JSON (an error page from a proxy).
    Of those, only URLError is wrapped by urllib; RemoteDisconnected,
    ConnectionResetError and IncompleteRead arrive bare. Any other 4xx is
    permanent and fails at once. Whatever gives up raises FetchError.
    """
    last: Exception | None = None
    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
            with urllib.request.urlopen(req, timeout=60) as resp:
                return json.load(resp)
        except urllib.error.HTTPError as e:
            if e.code != 429 and e.code < 500:
                raise FetchError(f"{url}: HTTP {e.code}") from e
            last = e
        except (urllib.error.URLError, http.client.HTTPException,
                ConnectionError, TimeoutError, json.JSONDecodeError) as e:
            last = e
        if attempt + 1 < retries:
            time.sleep(2 * (attempt + 1))
    raise FetchError(f"{url}: gave up after {retries} attempts: {last!r}")


def _url(stat_type: str, group: str, season: int) -> str:
    return (f"{API}?stats={stat_type}&group={group}&season={season}"
            f"&sportId=1&playerPool=ALL&limit={LIMIT}")


def splits_to_frame(payload: dict, season: int, required: list[str],
                    label: str) -> pd.DataFrame:
    """One row per split, after the checks that would otherwise pass silently.

    Raises FetchError when the page was truncated, answered for another
    season, lacks a required stat, or repeats a player.
    """
    stats = payload.get("stats") or []
    if len(stats) != 1:
        raise FetchError(f"{label} {season}: expected 1 stats block, got {len(stats)}")
    block = stats[0]
    splits = block.get("splits") or []
    total = block.get("totalSplits")
    if total is None or len(splits) != total:
        raise FetchError(f"{label} {season}: {len(splits)} splits but "
                         f"totalSplits={total} - the page was truncated")
    if not splits:
        raise FetchError(f"{label} {season}: no splits")

    rows = []
    for s in splits:
        if str(s.get("season")) != str(season):
            raise FetchError(f"{label} {season}: a split answered for season "
                             f"{s.get('season')!r}")
        stat = s.get("stat") or {}
        missing = [k for k in required if k not in stat]
        if missing:
            raise FetchError(f"{label} {season}: player {s.get('player', {}).get('id')} "
                             f"lacks {missing}")
        team = s.get("team") or {}
        rows.append({
            "player_id": s["player"]["id"],
            "season": season,
            "name": s["player"].get("fullName"),
            "last_team_id": team.get("id"),
            "num_teams": s.get("numTeams"),
            "position": (s.get("position") or {}).get("abbreviation"),
            **stat,
        })
    df = pd.DataFrame(rows)
    dups = int(df.duplicated(subset=["player_id", "season"]).sum())
    if dups:
        raise FetchError(f"{label} {season}: {dups} duplicate player rows")
    return df


def merge_season_saber(season_df: pd.DataFrame, saber_df: pd.DataFrame,
                       label: str) -> pd.DataFrame:
    """Outer-join the two stat types on (player_id, season).

    A player only the sabermetrics side knows means the key or an endpoint
    broke, so that fails. A player only the season side knows is allowed
    (2026 pitching had one mid-season) and leaves the sabermetric columns
    null; the count is printed.
    """
    saber = saber_df.drop(columns=["name", "last_team_id", "num_teams", "position"],
                          errors="ignore")
    clash = (set(saber.columns) & set(season_df.columns)) - {"player_id", "season"}
    saber = saber.rename(columns={c: f"saber_{c}" for c in clash})
    merged = season_df.merge(saber, on=["player_id", "season"], how="outer",
                             indicator=True)
    only_saber = int((merged["_merge"] == "right_only").sum())
    only_season = int((merged["_merge"] == "left_only").sum())
    if only_saber:
        raise FetchError(f"{label}: {only_saber} players exist only in the "
                         "sabermetrics response")
    if only_season:
        print(f"  {label}: {only_season} players have no sabermetrics row")
    return merged.drop(columns="_merge")


def check_rates(df: pd.DataFrame, rates: list[str], denominator: str,
                label: str) -> None:
    """Every player with a non-zero denominator must carry every rate.

    The API leaves a rate out when it cannot be computed, which is right for
    a hitter with no plate appearances and wrong for anyone else. A player
    with only a season row (no sabermetrics row at all) is exempt here;
    merge_season_saber already reported him.
    """
    if denominator not in df.columns:
        raise FetchError(f"{label}: no {denominator} column to judge rates by")
    has_saber = df["war"].notna() if "war" in df.columns else pd.Series(False, index=df.index)
    used = (pd.to_numeric(df[denominator], errors="coerce").fillna(0) > 0) & has_saber
    for rate in rates:
        if rate not in df.columns:
            raise FetchError(f"{label}: no player carries {rate}")
        bad = used & df[rate].isna()
        if bad.any():
            ids = df.loc[bad, "player_id"].head(5).tolist()
            raise FetchError(f"{label}: {int(bad.sum())} players with "
                             f"{denominator} > 0 lack {rate} (e.g. {ids})")
    skipped = int((~used & has_saber).sum())
    if skipped:
        print(f"    {label}: {skipped} players with {denominator} = 0 carry no rates")


def coerce_rates(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    for col in df.columns:
        if col in _STRING_RATES:
            before = df[col].notna().sum()
            df[col] = pd.to_numeric(df[col], errors="coerce")
            lost = int(before - df[col].notna().sum())
            if lost:
                print(f"    {col}: {lost} placeholder values (e.g. '.---') -> NaN")
    return df


def fetch_group(group: str, start: int, end: int, today: dt.date) -> pd.DataFrame:
    spec = GROUPS[group]
    frames = []
    for season in range(start, end + 1):
        a = splits_to_frame(_get(_url("season", group, season)), season,
                            spec["season_required"], f"{group}/season")
        b = splits_to_frame(_get(_url("sabermetrics", group, season)), season,
                            spec["saber_required"], f"{group}/sabermetrics")
        m = merge_season_saber(a, b, f"{group} {season}")
        check_rates(m, spec["rate_required"], spec["denominator"],
                    f"{group} {season}")
        print(f"  {group} {season}: {len(m)} players")
        frames.append(m)
        time.sleep(0.5)
    df = coerce_rates(pd.concat(frames, ignore_index=True))
    df["is_partial"] = df["season"] >= today.year
    df["fetched_at"] = pd.Timestamp.now(tz="UTC").floor("s")
    return df


def rows_per_season_vs_published(df: pd.DataFrame, published: pd.DataFrame | None,
                                  today: dt.date) -> list[str]:
    """Completed seasons that now have fewer players than the published copy.

    The player set of a finished season should not shrink from one week to the
    next; values can be revised, rows should not vanish. This compares with
    what is on Hugging Face rather than with a hardcoded floor - the honest
    count moved from ~1,250 hitters to ~770 when the universal DH arrived in
    2022, so any fixed floor is wrong for half the seasons.
    """
    if published is None or published.empty:
        return []
    now = df[df["season"] < today.year].groupby("season").size()
    before = published[published["season"] < today.year].groupby("season").size()
    problems = []
    for season, n_before in before.items():
        n_now = int(now.get(season, 0))
        if n_now < n_before:
            problems.append(f"{season}: {n_now} rows, published copy has {n_before}")
    return problems


def _published(table: str) -> pd.DataFrame | None:
    """The season column of the copy on Hugging Face, or None if there is none.

    Only a 404 means "nothing published yet". Any other failure raises: if
    the comparison could quietly switch itself off on a network error, it
    would guard nothing on exactly the weeks something else is going wrong.
    """
    req = urllib.request.Request(HF_BASE + f"{table}.parquet",
                                 headers={"User-Agent": USER_AGENT})
    try:
        with urllib.request.urlopen(req, timeout=120) as resp:
            data = resp.read()
    except urllib.error.HTTPError as e:
        if e.code == 404:
            print(f"  {table} is not published yet - nothing to compare with")
            return None
        raise FetchError(f"reading the published {table}: HTTP {e.code}") from e
    except (urllib.error.URLError, TimeoutError) as e:
        raise FetchError(f"reading the published {table}: {e}") from e
    try:
        return pd.read_parquet(io.BytesIO(data), columns=["season"])
    except Exception as e:  # an HTML error page, a truncated body
        raise FetchError(f"the published {table} is not a parquet with a "
                         f"season column ({type(e).__name__}: {e})") from e


def _today() -> dt.date:
    """UTC date; a function so tests can pin the season boundary."""
    return dt.datetime.now(dt.timezone.utc).date()


def main() -> int:
    parser = argparse.ArgumentParser(description="Fetch MLB Stats API tables -> Parquet")
    parser.add_argument("--start-year", type=int, default=START_SEASON)
    parser.add_argument("--end-year", type=int, default=END_SEASON)
    parser.add_argument("--no-bq", action="store_true")
    parser.add_argument(
        "--allow-shrink", action="store_true",
        help="publish even if a completed season has fewer players than the "
             "published copy. For a checked, deliberate run only: without it "
             "a real removal upstream would keep every weekly run red, since "
             "the copy it is compared with never updates. It does not let a "
             "whole season disappear; scripts/check_outputs.py still refuses "
             "that.")
    args = parser.parse_args()

    t0 = time.time()
    today = _today()
    for group, spec in GROUPS.items():
        table = TABLES[group]
        print(f"{table} {args.start_year}-{args.end_year} ...")
        try:
            df = fetch_group(group, args.start_year, args.end_year, today)
        except FetchError as e:
            print(f"ERROR: {table}: {e}")
            raise SystemExit(1)

        ok = validate_dataframe(
            df, table, expected_years=(args.start_year, args.end_year),
            required_cols=["player_id", "season"] + spec["saber_required"]
            + spec["rate_required"])
        if not ok:
            print(f"ERROR: {table} failed validation (see above). "
                  "Refusing to publish it over the previous copy.")
            raise SystemExit(1)

        try:
            published = _published(table)
        except FetchError as e:
            print(f"ERROR: {table}: {e}")
            raise SystemExit(1)
        shrunk = rows_per_season_vs_published(df, published, today)
        if shrunk and args.allow_shrink:
            print(f"WARNING: {table} has fewer players than the published copy "
                  "in completed seasons; publishing anyway (--allow-shrink):")
            for line in shrunk:
                print(f"  {line}")
        elif shrunk:
            print(f"ERROR: {table} lost players in completed seasons:")
            for line in shrunk:
                print(f"  {line}")
            print("If MLB really did remove them (e.g. merged a duplicate "
                  "player id), check the ids, then run the workflow by hand "
                  "with allow_statsapi_shrink set to true.")
            raise SystemExit(1)

        if not args.no_bq:
            write_dataframe(df, table)
            if DATA_TARGET == "bq":
                validate_bq_table(table)

    print(f"Stats API fetch complete ({(time.time() - t0) / 60:.1f} min / "
          f"{BUDGET_MIN} min budget).")
    return 0


if __name__ == "__main__":
    main()
