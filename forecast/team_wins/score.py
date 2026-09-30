"""Score projected team wins against the finished seasons, next to three floors.

Floors (as in npb-prediction): every team at .500; last season's win share;
last season's Pythagenpat win share. All are turned into wins with the
target season's games played, as is the projection. The 60-game 2020
season is not scored (it still counts as "last season" for 2021).

Primary statistic: mean absolute error of wins over team-seasons. The
comparison with a floor is the difference in MAE; its interval resamples
whole seasons (teams in one season share a league-wide error).

Usage: python score.py projections_2016_2026.csv 2016 2025
"""
import sys
from pathlib import Path

import numpy as np
import pandas as pd

from project import pythag

HERE = Path(__file__).resolve().parent


def table(proj_csv, first, last):
    proj = pd.read_csv(HERE / proj_csv)
    st = pd.read_parquet(HERE / "standings.parquet")
    st["g"] = st.wins + st.losses
    st["pct_actual"] = st.wins / st.g
    st["pct_pyth"] = pythag(st.runs_scored * 162 / st.g, st.runs_allowed * 162 / st.g)
    prev = st[["season", "team_id", "pct_actual", "pct_pyth"]].copy()
    prev["season"] += 1
    prev = prev.rename(columns={"pct_actual": "pct_prev", "pct_pyth": "pct_prev_pyth"})
    d = (proj.merge(st, on=["season", "team_id"], how="inner")
             .merge(prev, on=["season", "team_id"], how="left"))
    d = d[d.season.between(first, last) & (d.season != 2020)]
    assert d.pct_prev.notna().all() and (d.groupby("season").size() == 30).all()
    for name, col in [("model", "pct"), ("floor_500", None), ("floor_prev", "pct_prev"),
                      ("floor_prev_pyth", "pct_prev_pyth")]:
        d[name] = (0.5 if col is None else d[col]) * d.g
    return d


ARMS = ["model", "floor_500", "floor_prev", "floor_prev_pyth"]


def summarise(d, seed=0, reps=20000):
    err = {a: (d[a] - d.wins).abs() for a in ARMS}
    by_season = pd.DataFrame({a: err[a].groupby(d.season).mean() for a in ARMS})
    rng = np.random.default_rng(seed)
    seasons = by_season.index.to_numpy()
    idx = rng.integers(0, len(seasons), size=(reps, len(seasons)))
    rows = []
    for f in ARMS[1:]:
        diff = (by_season["model"] - by_season[f]).to_numpy()
        boot = diff[idx].mean(axis=1)
        rows.append({"floor": f, "mae_model": err["model"].mean(), "mae_floor": err[f].mean(),
                     "diff": diff.mean(), "lo95": np.quantile(boot, 0.025),
                     "hi95": np.quantile(boot, 0.975),
                     "seasons_model_better": int((diff < 0).sum()), "seasons": len(diff)})
    return by_season, pd.DataFrame(rows)


def one_season(d, arms, seed=0, reps=20000):
    """One season: differences in MAE with 95 % intervals from resampling teams."""
    rng = np.random.default_rng(seed)
    idx = rng.integers(0, len(d), size=(reps, len(d)))
    err = {a: (d[a] - d.wins).abs().to_numpy() for a in arms}
    rows = []
    for a, b in [(x, y) for x in arms for y in arms if x != y and x in ("model", "stretched")]:
        diff = err[a] - err[b]
        boot = diff[idx].mean(axis=1)
        lo, hi = np.quantile(boot, [0.025, 0.975])
        rows.append({"arm": a, "vs": b, "mae_arm": err[a].mean(), "mae_vs": err[b].mean(),
                     "diff": diff.mean(), "lo95": lo, "hi95": hi,
                     "verdict": "better" if hi < 0 else "worse" if lo > 0 else "indistinguishable"})
    return pd.DataFrame(rows)


if __name__ == "__main__":
    if sys.argv[1] == "--2026":
        # Pre-registered scoring (PREREG.md). Uses the frozen pred_2026.csv.
        pred = pd.read_csv(HERE / "pred_2026.csv")
        d = table("projections_2016_2026.csv", 2026, 2026)
        d = d.merge(pred[["team_id", "stretched_wins"]], on="team_id")
        assert np.allclose(d.pct * 162, d.merge(pred, on="team_id").model_wins, atol=1e-3)
        d["stretched"] = d.stretched_wins / 162 * d.g
        pd.set_option("display.width", 200)
        print(one_season(d, ["model", "stretched", "floor_500", "floor_prev", "floor_prev_pyth"])
              .round(3).to_string(index=False))
        print("corr(model, wins) =", round(np.corrcoef(d.model, d.wins)[0, 1], 3),
              " games played:", d.g.min(), "-", d.g.max())
        d["miss"] = d.model - d.wins
        print(d.reindex(d.miss.abs().sort_values(ascending=False).index)
              [["team", "wins", "model", "stretched", "miss"]].head(5).round(1).to_string(index=False))
        sys.exit(0)
    d = table(sys.argv[1], int(sys.argv[2]), int(sys.argv[3]))
    by_season, s = summarise(d)
    pd.set_option("display.width", 200)
    print(by_season.round(2))
    print(s.round(3).to_string(index=False))
    print("corr(model, wins) =", round(np.corrcoef(d.model, d.wins)[0, 1], 3),
          " n =", len(d))
