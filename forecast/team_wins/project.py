"""Team wins from player projections (Marcel), before the season starts.

For target season Y, using only seasons Y-1..Y-3 and Y's opening-day 40-man
roster (both known before Y begins):

Batters (roster position not P): wOBA relative to the league, weighted
5/4/3 by PA over the three seasons and regressed with 1200 weighted PA of
league average (the Marcel of Tom Tango); age factor 1 + 0.006 (29 - age)
below 29, 1 + 0.003 (29 - age) above. Playing time 0.5 PA(Y-1) +
0.1 PA(Y-2) + 200, and only for players with MLB PA in the three seasons
(no history, no projection: rookies are a known gap).

Pitchers (P or TWP): FIP relative to the league, weighted 3/2/1 by IP,
regressed with 134 IP of league average, same age factor (inverted).
Innings 0.5 IP(Y-1) + 0.1 IP(Y-2) + 60 for starters (GS/G >= 0.5 in the
last season pitched) or 25 for relievers, again only with MLB history.

Team: playing time is rescaled so each team has the league's per-game PA
and IP of Y-1 over 162 games. Runs scored = PA x league R/PA + sum of
PA x relative wOBA / wOBA scale; runs allowed = IP x league RA9 / 9 + sum
of IP x relative FIP / 9. Nothing here models fielding or parks. Through
2021 (not 2020), NL teams also get the NL pitchers' batting runs per team of
the last full season. Runs allowed are scaled so the league totals match.
Win share = Pythagenpat, exponent ((RS + RA) / 162) ^ 0.287.

Every step for season Y sees only rows with season < Y.

Writes projections_<first>_<last>.csv.
Usage: python project.py 2016 2026 [raw parquet dir]
"""
import sys
from pathlib import Path

import numpy as np
import pandas as pd

HERE = Path(__file__).resolve().parent
HF = "https://huggingface.co/datasets/yasumorishima/mlb-stats/resolve/main"
G = 162
NL = 104
TEAMS = 30


def age_factor(age):
    return np.where(age < 29, 1 + 0.006 * (29 - age), 1 + 0.003 * (29 - age))


def load(raw_dir=None):
    src = raw_dir or HF
    bat = pd.read_parquet(f"{src}/statsapi_batting.parquet",
                          columns=["player_id", "season", "position", "age", "plateAppearances",
                                   "woba", "runs", "wRaa", "gamesPlayed", "last_team_id"])
    pit = pd.read_parquet(f"{src}/statsapi_pitching.parquet",
                          columns=["player_id", "season", "age", "outs", "fip", "runs",
                                   "gamesPitched", "gamesStarted"])
    bat = bat.rename(columns={"plateAppearances": "pa"})
    pit["ip"] = pit["outs"] / 3
    return bat, pit


def league(bat, pit, y):
    b = bat[(bat.season == y) & (bat.pa > 0) & bat.woba.notna()]
    p = pit[(pit.season == y) & (pit.ip > 0) & pit.fip.notna()]
    lg_woba = np.average(b.woba, weights=b.pa)
    # wRAA = (wOBA - lg) / scale x PA, so wRAA / PA is linear in wOBA with
    # slope 1 / scale; fit it weighted by PA (a ratio of sums is unstable
    # because both sums are near zero).
    slope = np.polyfit(b.woba, b.wRaa / b.pa, 1, w=np.sqrt(b.pa))[0]
    scale = 1 / slope
    games = 60 if y == 2020 else G   # a player traded mid-season can show 163
    return {"woba": lg_woba, "scale": scale, "games": games,
            "r_pa": bat.loc[bat.season == y, "runs"].sum() / bat.loc[bat.season == y, "pa"].sum(),
            "fip": np.average(p.fip, weights=p.ip),
            "ra9": 9 * pit.loc[pit.season == y, "runs"].sum() / pit.loc[pit.season == y, "ip"].sum(),
            "pa_g": bat.loc[bat.season == y, "pa"].sum() / TEAMS / games,
            "ip_g": pit.loc[pit.season == y, "ip"].sum() / TEAMS / games}


def _history(df, ids, y, value, weight_col, lgs, lg_key, weights):
    h = df[df.player_id.isin(ids) & df.season.between(y - 3, y - 1)
           & (df[weight_col] > 0) & df[value].notna()].copy()
    h["rel"] = h[value] - h.season.map({s: lgs[s][lg_key] for s in lgs})
    h["w"] = h.season.map(weights)
    last = h.sort_values("season").groupby("player_id").tail(1).set_index("player_id")
    return h, last


def project_batters(bat, lgs, y, roster):
    ids = roster.loc[roster.position != "P", "player_id"].unique()
    h, last = _history(bat, ids, y, "woba", "pa", lgs, "woba", {y - 1: 5, y - 2: 4, y - 3: 3})
    num = (h.w * h.pa * h.rel).groupby(h.player_id).sum()
    den = (h.w * h.pa).groupby(h.player_id).sum()
    rel = num / (den + 1200)
    age = (last.age + (y - last.season)).loc[rel.index]
    rel = rel + lgs[y - 1]["woba"] * (age_factor(age) - 1)
    pa1 = h[h.season == y - 1].set_index("player_id").pa.reindex(rel.index).fillna(0)
    pa2 = h[h.season == y - 2].set_index("player_id").pa.reindex(rel.index).fillna(0)
    return pd.DataFrame({"rel": rel, "pa": 0.5 * pa1 + 0.1 * pa2 + 200})


def project_pitchers(pit, lgs, y, roster):
    ids = roster.loc[roster.position.isin(["P", "TWP"]), "player_id"].unique()
    h, last = _history(pit, ids, y, "fip", "ip", lgs, "fip", {y - 1: 3, y - 2: 2, y - 3: 1})
    num = (h.w * h.ip * h.rel).groupby(h.player_id).sum()
    den = (h.w * h.ip).groupby(h.player_id).sum()
    rel = num / (den + 134)
    age = (last.age + (y - last.season)).loc[rel.index]
    rel = rel - lgs[y - 1]["fip"] * (age_factor(age) - 1)
    starter = (last.gamesStarted / last.gamesPitched.clip(lower=1) >= 0.5).loc[rel.index]
    ip1 = h[h.season == y - 1].set_index("player_id").ip.reindex(rel.index).fillna(0)
    ip2 = h[h.season == y - 2].set_index("player_id").ip.reindex(rel.index).fillna(0)
    return pd.DataFrame({"rel": rel, "ip": 0.5 * ip1 + 0.1 * ip2 + np.where(starter, 60, 25)})


def nl_pitcher_batting(bat, lgs, rosters, s):
    """NL pitchers' batting runs above league average per NL team over 162 games, season s."""
    lg = lgs[s]
    b = bat[(bat.season == s) & (bat.position == "P") & (bat.pa > 0) & bat.woba.notna()]
    runs = (b.woba - lg["woba"]) * b.pa / lg["scale"]
    # Rosters start in 2016; teams did not change leagues over these seasons.
    nl_teams = rosters.loc[rosters.league_id == NL, "team_id"].unique()
    return runs[b.last_team_id.isin(nl_teams)].sum() / len(nl_teams) / lg["games"] * G


def pythag(rs, ra):
    x = ((rs + ra) / G) ** 0.287
    return rs ** x / (rs ** x + ra ** x)


def project(y, bat, pit, rosters, lgs):
    roster = rosters[rosters.season == y]
    lg = lgs[y - 1]
    pb = project_batters(bat, lgs, y, roster)
    pp = project_pitchers(pit, lgs, y, roster)
    # NL pitchers batted through 2021 except 2020; use the last season they did.
    pnl = 0.0
    if y <= 2021 and y != 2020:
        pnl = nl_pitcher_batting(bat, lgs, rosters, 2019 if y == 2021 else y - 1)
    out = []
    for tid, r in roster.groupby("team_id"):
        ids = r.player_id.unique()
        b = pb.loc[pb.index.intersection(ids)]
        p = pp.loc[pp.index.intersection(ids)]
        pa = b.pa * (lg["pa_g"] * G / b.pa.sum())
        ip = p.ip * (lg["ip_g"] * G / p.ip.sum())
        rs = pa.sum() * lg["r_pa"] + (pa * b.rel).sum() / lg["scale"]
        if r.league_id.iloc[0] == NL:
            rs += pnl
        ra = ip.sum() * lg["ra9"] / 9 + (ip * p.rel).sum() / 9
        out.append({"season": y, "team_id": tid, "team": r.team.iloc[0],
                    "league_id": r.league_id.iloc[0], "n_batters": len(b),
                    "n_pitchers": len(p), "rs": rs, "ra": ra})
    df = pd.DataFrame(out)
    df["ra"] *= df.rs.sum() / df.ra.sum()
    df["pct"] = pythag(df.rs, df.ra)
    return df


if __name__ == "__main__":
    first, last = int(sys.argv[1]), int(sys.argv[2])
    bat, pit = load(sys.argv[3] if len(sys.argv) > 3 else None)
    rosters = pd.read_parquet(HERE / "opening_rosters.parquet")
    res = []
    for y in range(first, last + 1):
        b, p = bat[bat.season < y], pit[pit.season < y]
        lgs = {s: league(b, p, s) for s in range(max(2015, y - 3), y)}
        # 2019 is needed for NL pitcher batting when projecting 2021.
        if y == 2021:
            lgs[2019] = league(b, p, 2019)
        res.append(project(y, b, p, rosters, lgs))
    df = pd.concat(res)
    df.to_csv(HERE / f"projections_{first}_{last}.csv", index=False, float_format="%.4f")
    print(df.groupby("season")[["rs", "n_batters", "n_pitchers"]].agg(["mean", "min", "max"]).round(1))
