"""Player-level: projected vs actual for MIL 2026; backtest: miss vs roster age."""
import os
import numpy as np
import pandas as pd

import project as P
import score as S

RAW = os.environ.get("RAW", P.HF)   # local copy of the raw parquet files, or Hugging Face
bat, pit = P.load(RAW)
names = pd.concat([pd.read_parquet(f"{RAW}/statsapi_batting.parquet", columns=["player_id", "name"]),
                   pd.read_parquet(f"{RAW}/statsapi_pitching.parquet", columns=["player_id", "name"])]).drop_duplicates("player_id").set_index("player_id").name
rosters = pd.read_parquet("opening_rosters.parquet")

def projections(y):
    b, p = bat[bat.season < y], pit[pit.season < y]
    lgs = {s: P.league(b, p, s) for s in range(max(2015, y - 3), y)}
    ros = rosters[rosters.season == y]
    return P.project_batters(b, lgs, y, ros), P.project_pitchers(p, lgs, y, ros), lgs

# MIL 2026 player level: runs above average, projected vs actual
pb, pp, lgs = projections(2026)
lg25 = lgs[2025]
lg26 = P.league(bat, pit, 2026)
mil = rosters[(rosters.season == 2026) & (rosters.team == "MIL")].player_id
a_b = bat[(bat.season == 2026) & bat.player_id.isin(mil) & (bat.pa > 0)].set_index("player_id")
a_p = pit[(pit.season == 2026) & pit.player_id.isin(mil) & (pit.ip > 0)].set_index("player_id")
# The team projection rescales playing time so the roster fills 162 games;
# use the same scaling here so the players add up to the team figure.
kb = lg25["pa_g"] * P.G / pb.pa.reindex(mil).dropna().sum()
kp = lg25["ip_g"] * P.G / pp.ip.reindex(mil).dropna().sum()
rb = pd.DataFrame({"proj_pa": kb * pb.pa, "proj_runs": kb * pb.pa * pb.rel / lg25["scale"]}).reindex(mil).dropna()
rb["act_pa"] = a_b.pa.reindex(rb.index).fillna(0)
rb["act_runs"] = ((a_b.woba - lg26["woba"]) * a_b.pa / lg26["scale"]).reindex(rb.index).fillna(0)
rp = pd.DataFrame({"proj_ip": kp * pp.ip, "proj_runs": -kp * pp.ip * pp.rel / 9}).reindex(mil).dropna()
rp["act_ip"] = a_p.ip.reindex(rp.index).fillna(0)
rp["act_runs"] = (-(a_p.fip - lg26["fip"]) * a_p.ip / 9).reindex(rp.index).fillna(0)
for nm, r in [("MIL batters", rb), ("MIL pitchers (runs saved, FIP)", rp)]:
    r["gap"] = r.act_runs - r.proj_runs
    r.index = names.reindex(r.index).values
    print(nm, "sum proj", r.proj_runs.sum().round(1), "sum act", r.act_runs.sum().round(1))
    print(r.sort_values("gap", ascending=False).head(6).round(1).to_string())
    r.to_csv("mil_bat.csv" if "batters" in nm else "mil_pit.csv", float_format="%.3f")

# backtest + 2026: miss vs PA/IP-weighted age of the projected roster
rows = []
for y in [2016, 2017, 2018, 2019, 2021, 2022, 2023, 2024, 2025, 2026]:
    b, p = bat[bat.season < y], pit[pit.season < y]
    last_age_b = b.sort_values("season").groupby("player_id").tail(1).set_index("player_id")
    ros = rosters[rosters.season == y]
    pb, pp, _ = projections(y)
    for tid, r in ros.groupby("team_id"):
        ids = r.player_id.unique()
        bb = pb.loc[pb.index.intersection(ids)]
        age = (last_age_b.age + (y - last_age_b.season)).reindex(bb.index)
        rows.append({"season": y, "team_id": tid, "age": np.average(age, weights=bb.pa)})
ages = pd.DataFrame(rows)
st = pd.read_parquet("standings.parquet")
proj = pd.read_csv("projections_2016_2026.csv")
d = proj.merge(st, on=["season", "team_id"]).merge(ages, on=["season", "team_id"])
d = d[d.season != 2020]
d["miss"] = d.pct * (d.wins + d.losses) - d.wins
d["age_c"] = d.age - d.groupby("season").age.transform("mean")
print("corr(miss, batter age centred by season)", np.corrcoef(d.miss, d.age_c)[0, 1].round(3), "n", len(d))
d["q"] = pd.qcut(d.age_c, 4, labels=["youngest", "2", "3", "oldest"])
print(d.groupby("q", observed=True).miss.agg(["mean", "count"]).round(2))
d.to_csv("age_miss.csv", index=False, float_format="%.3f")
