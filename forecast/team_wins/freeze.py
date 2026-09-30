"""Freeze the 2026 team-win predictions before the 2026 standings are read.

Writes pred_2026.csv with two arms:
- model: the Marcel projection of project.py, win share x 162.
- stretched: 0.5 + k (share - 0.5), k fit by least squares through .500 on
  the scored backtest seasons 2016-2025 (2020 left out). Declared secondary.

Usage: python freeze.py projections_2016_2026.csv
"""
import sys
from pathlib import Path

import pandas as pd

import score

HERE = Path(__file__).resolve().parent

if __name__ == "__main__":
    proj = pd.read_csv(HERE / sys.argv[1])
    bt = score.table(sys.argv[1], 2016, 2025)
    x, y = bt.pct - 0.5, bt.wins / bt.g - 0.5
    k = float((x * y).sum() / (x * x).sum())
    p = proj[proj.season == 2026].copy()
    assert len(p) == 30
    p["model_wins"] = p.pct * 162
    p["stretched_wins"] = (0.5 + k * (p.pct - 0.5)) * 162
    p["k"] = k
    cols = ["season", "team_id", "team", "league_id", "rs", "ra", "pct",
            "model_wins", "stretched_wins", "k"]
    p[cols].sort_values("model_wins", ascending=False).to_csv(
        HERE / "pred_2026.csv", index=False, float_format="%.4f")
    print(f"k = {k:.4f}")
    print(p[cols].sort_values("model_wins", ascending=False).round(1).to_string(index=False))
