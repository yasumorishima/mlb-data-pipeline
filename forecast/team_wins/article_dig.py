"""Why the 2026 team-win projections missed: runs scored vs allowed vs luck,
and how much playing time went to players the projection had nothing on."""
import os
import numpy as np
import pandas as pd

import project as P
import score as S

RAW = os.environ.get("RAW", P.HF)   # local copy of the raw parquet files, or Hugging Face
bat, pit = P.load(RAW)
bat2 = pd.read_parquet(f"{RAW}/statsapi_batting.parquet", columns=["player_id", "season", "name", "war", "plateAppearances"])
pit2 = pd.read_parquet(f"{RAW}/statsapi_pitching.parquet", columns=["player_id", "season", "name", "war", "last_team_id"])
rosters = pd.read_parquet("opening_rosters.parquet")
proj = pd.read_csv("projections_2016_2026.csv")
st = pd.read_parquet("standings.parquet")

# 1. decomposition of each 2026 miss
d = proj[proj.season == 2026].merge(st[st.season == 2026], on=["season", "team_id"])
d["g"] = d.wins + d.losses
d["w_proj"] = d.pct * d.g
d["w_pyth_actual"] = P.pythag(d.runs_scored * 162 / d.g, d.runs_allowed * 162 / d.g) * d.g
d["rs_err"] = d.rs * d.g / 162 - d.runs_scored
d["ra_err"] = d.ra * d.g / 162 - d.runs_allowed
d["miss"] = d.w_proj - d.wins
d["miss_runs"] = d.w_proj - d.w_pyth_actual          # projection vs what the runs were worth
d["miss_luck"] = d.w_pyth_actual - d.wins            # runs vs record (sequencing, one-run games)
print("MAE total", d.miss.abs().mean().round(2), "| MAE if runs were known (luck only)", d.miss_luck.abs().mean().round(2),
      "| MAE projection vs pythag of actual runs", d.miss_runs.abs().mean().round(2))
print("sd rs_err", d.rs_err.std().round(1), "sd ra_err", d.ra_err.std().round(1))
cols = ["team", "wins", "w_proj", "w_pyth_actual", "miss", "miss_runs", "miss_luck", "rs", "runs_scored", "ra", "runs_allowed"]
print(d.reindex(d.miss.abs().sort_values(ascending=False).index)[cols].head(8).round(1).to_string(index=False))

# 2. share of 2026 playing time by players the projection had (on opening roster with history)
ros26 = rosters[rosters.season == 2026]
hist_b = set(bat[(bat.season.between(2023, 2025)) & (bat.pa > 0)].player_id)
hist_p = set(pit[(pit.season.between(2023, 2025)) & (pit.ip > 0)].player_id)
b26 = bat[(bat.season == 2026) & (bat.position != "P")].copy()
p26 = pit[pit.season == 2026].copy()
p26 = p26.merge(pit2[pit2.season == 2026][["player_id", "last_team_id", "war"]], on="player_id")
b26 = b26.merge(bat2[bat2.season == 2026][["player_id", "war", "name"]], on="player_id")
on_ros = ros26.groupby("team_id").player_id.apply(set).to_dict()
b26["projected"] = [pid in on_ros.get(t, set()) and pid in hist_b for pid, t in zip(b26.player_id, b26.last_team_id)]
p26["projected"] = [pid in on_ros.get(t, set()) and pid in hist_p for pid, t in zip(p26.player_id, p26.last_team_id)]
b26["no_hist"] = ~b26.player_id.isin(hist_b)
p26["no_hist"] = ~p26.player_id.isin(hist_p)
share = pd.DataFrame({
    "pa_unproj": b26.groupby("last_team_id").apply(lambda x: x.pa[~x.projected].sum() / x.pa.sum()),
    "ip_unproj": p26.groupby("last_team_id").apply(lambda x: x.ip[~x.projected].sum() / x.ip.sum()),
    "war_unproj": b26[~b26.projected].groupby("last_team_id").war.sum() + p26[~p26.projected].groupby("last_team_id").war.sum(),
    "war_nohist": b26[b26.no_hist].groupby("last_team_id").war.sum() + p26[p26.no_hist].groupby("last_team_id").war.sum(),
}).fillna(0)
d = d.merge(share, left_on="team_id", right_index=True)
pd.DataFrame({"pa": [b26.pa[~b26.projected].sum() / b26.pa.sum()],
              "ip": [p26.ip[~p26.projected].sum() / p26.ip.sum()]}).to_csv("share_2026.csv", index=False)
print("league: PA by unprojected", round(b26.pa[~b26.projected].sum() / b26.pa.sum(), 3),
      "IP by unprojected", round(p26.ip[~p26.projected].sum() / p26.ip.sum(), 3))
print("corr(miss, war_unproj)", np.corrcoef(d.miss, d.war_unproj)[0, 1].round(3),
      "corr(miss, war_nohist)", np.corrcoef(d.miss, d.war_nohist)[0, 1].round(3))
print(d.reindex(d.miss.abs().sort_values(ascending=False).index)[["team", "miss", "pa_unproj", "ip_unproj", "war_unproj", "war_nohist"]].head(8).round(2).to_string(index=False))
for t in ["MIL", "TB", "SF", "ATH"]:
    tid = d.loc[d.team == t, "team_id"].iloc[0]
    x = pd.concat([b26[(b26.last_team_id == tid)][["name", "war", "projected", "no_hist"]].assign(k="bat"),
                   p26[(p26.last_team_id == tid)].merge(pit2[pit2.season == 2026][["player_id", "name"]], on="player_id")[["name", "war", "projected", "no_hist"]].assign(k="pit")])
    print(t, x.sort_values("war", ascending=False).head(6).round(1).to_string(index=False))
d.to_csv("dig_2026.csv", index=False, float_format="%.3f")
