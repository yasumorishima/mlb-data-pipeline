"""Rebuild the c*n/(n+k) fit from the pitch carryover article and report r at fixed pitch counts.

Pairs: same pitcher and pitch type in consecutive seasons, 100+ pitches in both,
second season finished; value minus the season x pitch-type mean (pitches >= 100).
Fit: 12 equal-size groups by the smaller pitch count; r_pred = c * mean(sqrt(s1*s2)),
s = n/(n+k); least squares weighted by group size; c in (0, 1].
Interval: resample pitchers with replacement.
"""
import json

import numpy as np
import pandas as pd
from scipy.optimize import minimize

HF = "https://huggingface.co/datasets/yasumorishima/mlb-stats/resolve/main"
a = pd.read_parquet(f"{HF}/marts/mart_pitch_arsenal_scouting.parquet")
partial = set(pd.read_parquet(f"{HF}/marts/mart_pitcher_season.parquet").query("is_partial").season)
a = a[a.pitches >= 100].copy()
METRICS = ["whiff_rate", "xwoba_allowed", "run_value_per_100"]
for m in METRICS:
    a[m + "_w"] = a[m] - a.groupby(["season", "pitch_type"])[m].transform("mean")
x = a.merge(a, on=["player_id", "pitch_type"], suffixes=("1", "2"))
x = x[(x.season2 == x.season1 + 1) & ~x.season2.isin(partial)].reset_index(drop=True)
print("pairs", len(x))


def fit(d, m):
    d = d.dropna(subset=[m + "_w1", m + "_w2"]).copy()
    d["mn"] = np.minimum(d.pitches1, d.pitches2)
    d = d.sort_values("mn").reset_index(drop=True)
    groups = np.array_split(np.arange(len(d)), 12)
    obs, n1s, n2s, ws = [], [], [], []
    for g in groups:
        s = d.iloc[g]
        obs.append(np.corrcoef(s[m + "_w1"], s[m + "_w2"])[0, 1])
        n1s.append(s.pitches1.to_numpy()); n2s.append(s.pitches2.to_numpy()); ws.append(len(g))
    obs, ws = np.array(obs), np.array(ws)

    def loss(p):
        c, lk = p
        k = np.exp(lk)
        pred = np.array([c * np.mean(np.sqrt(a1 / (a1 + k) * a2 / (a2 + k))) for a1, a2 in zip(n1s, n2s)])
        return np.sum(ws * (obs - pred) ** 2)
    best = None
    for c0 in (0.3, 0.6, 0.9):
        for k0 in (50, 300, 2000):
            r = minimize(loss, [c0, np.log(k0)], bounds=[(1e-3, 1.0), (np.log(5), np.log(1e5))])
            if best is None or r.fun < best.fun:
                best = r
    c, k = best.x[0], np.exp(best.x[1])
    return c, k


def r_at(c, k, n):
    return c * n / (n + k)


# A starter's main pitch over one season: median of the most-used pitch of the
# 316 pitcher-seasons with 2,400+ pitches in 2021-2025 (from the article).
FULL = 1086
out = {}
rng = np.random.default_rng(0)
pitchers = x.player_id.unique()
for m in METRICS:
    c, k = fit(x, m)
    boots = []
    for _ in range(200):
        pick = rng.choice(pitchers, size=len(pitchers), replace=True)
        cnt = pd.Series(pick).value_counts()
        d = x.merge(cnt.rename("w").rename_axis("player_id").reset_index(), on="player_id")
        d = d.loc[d.index.repeat(d.w)]
        cb, kb = fit(d, m)
        boots.append((cb, kb, r_at(cb, kb, 500), r_at(cb, kb, FULL)))
    b = np.array(boots)
    q = lambda v: [float(x) for x in np.percentile(v, [2.5, 97.5])]
    out[m] = {"c": c, "k": k, "k95": q(b[:, 1]), "r500": r_at(c, k, 500), "r500_95": q(b[:, 2]),
              "rfull": r_at(c, k, FULL), "rfull_95": q(b[:, 3]), "c_at_cap": int((b[:, 0] > 0.999).sum()), "boots": len(b)}
    print(m, {kk: (round(v, 3) if isinstance(v, float) else v) for kk, v in out[m].items()})
json.dump(out, open("pitch_carryover_fit.json", "w"), indent=1)
