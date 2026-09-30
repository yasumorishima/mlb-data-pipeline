"""Fetch what the team-wins forecast needs from the MLB Stats API (keyless).

- opening_rosters.parquet: each team's 40-man roster on the first day of the
  regular season (known before the season starts).
- standings.parquet: final regular-season wins, losses, runs scored and
  allowed, for seasons passed with --standings.

Responses are cached under cache/ so a rerun does not hit the API again.

Usage: python fetch.py --rosters 2016-2026 --standings 2015-2025
"""
import argparse
import json
import time
import urllib.request
from pathlib import Path

import pandas as pd

API = "https://statsapi.mlb.com/api/v1"
HERE = Path(__file__).resolve().parent
CACHE = HERE / "cache"


def get(path: str) -> dict:
    CACHE.mkdir(exist_ok=True)
    f = CACHE / (path.replace("/", "_").replace("?", "_").replace("&", "_").replace("=", "-").replace(",", "-") + ".json")
    if f.exists():
        return json.loads(f.read_text())
    for attempt in range(4):
        try:
            with urllib.request.urlopen(f"{API}/{path}", timeout=30) as r:
                d = json.load(r)
            break
        except Exception:
            if attempt == 3:
                raise
            time.sleep(2 ** attempt)
    f.write_text(json.dumps(d))
    time.sleep(0.3)
    return d


def years(s: str) -> list[int]:
    a, b = (int(x) for x in s.split("-"))
    return list(range(a, b + 1))


def opening_day(season: int) -> str:
    d = get(f"schedule?sportId=1&season={season}&gameType=R")
    return d["dates"][0]["date"]


def rosters(season: int) -> list[dict]:
    day = opening_day(season)
    teams = get(f"teams?sportId=1&season={season}")["teams"]
    assert len(teams) == 30, (season, len(teams))
    rows = []
    for t in teams:
        r = get(f"teams/{t['id']}/roster?rosterType=40Man&date={day}")["roster"]
        # 60-day IL players can sit on top of the 40.
        assert 30 <= len(r) <= 46, (season, t["id"], len(r))
        for p in r:
            rows.append({"season": season, "opening_day": day, "team_id": t["id"],
                         "team": t["abbreviation"],
                         "league_id": t["league"]["id"], "player_id": p["person"]["id"],
                         "position": p["position"]["abbreviation"],
                         "status": p.get("status", {}).get("code")})
    return rows


def standings(season: int) -> list[dict]:
    d = get(f"standings?leagueId=103,104&season={season}&standingsTypes=regularSeason")
    rows = []
    for rec in d["records"]:
        for t in rec["teamRecords"]:
            rows.append({"season": season, "team_id": t["team"]["id"],
                         "wins": t["wins"], "losses": t["losses"],
                         "runs_scored": t["runsScored"], "runs_allowed": t["runsAllowed"]})
    assert len(rows) == 30, (season, len(rows))
    return rows


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--rosters")
    ap.add_argument("--standings")
    a = ap.parse_args()
    if a.rosters:
        df = pd.DataFrame([r for y in years(a.rosters) for r in rosters(y)])
        df.to_parquet(HERE / "opening_rosters.parquet", index=False)
        print(df.groupby("season").agg(day=("opening_day", "first"), n=("player_id", "size")))
    if a.standings:
        df = pd.DataFrame([r for y in years(a.standings) for r in standings(y)])
        df.to_parquet(HERE / "standings.parquet", index=False)
        print(df.groupby("season")[["wins", "runs_scored", "runs_allowed"]].sum())
