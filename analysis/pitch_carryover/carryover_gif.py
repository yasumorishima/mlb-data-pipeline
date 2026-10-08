"""Animated year-to-year carry-over of two pitch metrics, whiff rate and run
value per 100 pitches, for the same pitcher and pitch type with 400+ pitches
in both seasons.

Each season pair is drawn in turn. Every pitch starts on the diagonal, where it
would sit if next season repeated this one exactly, and slides to where next
season actually put it. Values are within pitch type (the season x pitch type
mean removed), as in mart_scouting_reliability.

Before drawing, the correlations and pair counts of the two sample-size bins
it covers (400-800 and 800+ pitches) are recomputed and must equal the
published mart_scouting_reliability.

Usage: python carryover_gif.py <sc_pitcher_arsenal.parquet> <statsapi_pitching.parquet>
                               <mart_scouting_reliability.parquet> <out gif>
"""
import io
import json
import pathlib
import sys

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from PIL import Image

MIN_PITCHES = 400
METRICS = [("whiff_rate", "Whiff rate", "percentage points", 100.0),
           ("run_value_per_100", "Run value per 100 pitches", "runs", 1.0)]
NOW, PAST, DIAG = "#3b5b92", "#c9ced8", "#999999"


def pairs(raw, partial, m):
    d = pd.DataFrame({"pid": raw["player_id"], "season": raw["season"].astype(int), "pt": raw["pitch_type"],
                      "n": raw["pitches"]})
    d[m] = raw["whiff_percent"] / 100.0 if m == "whiff_rate" else raw["run_value_per_100"]
    d = d[d["n"] >= 100].dropna(subset=[m])
    if m == "whiff_rate" and not (d[m].between(0, 1).all() and d[m].median() > 0.05):
        raise SystemExit("whiff rate is not a share between 0 and 1")
    if d.duplicated(["pid", "season", "pt"]).any():
        raise SystemExit("duplicate (pitcher, season, pitch type) rows")
    d["w"] = d[m] - d.groupby(["season", "pt"])[m].transform("mean")
    nxt = d.copy()
    nxt["season"] -= 1
    p = d.merge(nxt, on=["pid", "season", "pt"], suffixes=("1", "2"))
    p = p[~(p["season"] + 1).isin(partial)]
    p["mn"] = np.minimum(p["n1"], p["n2"])
    return p


def main(arsenal, statsapi, mart, out_gif):
    raw = pd.read_parquet(arsenal)
    sp = pd.read_parquet(statsapi)
    partial = set(sp.loc[sp["is_partial"].astype(bool), "season"].astype(int))
    rel = pd.read_parquet(mart)
    data = {}
    for m, *_ in METRICS:
        p = pairs(raw, partial, m)
        for lo, hi in ((400, 800), (800, 10 ** 9)):
            q = p[(p["mn"] >= lo) & (p["mn"] < hi)]
            k = rel[(rel["metric"] == m) & (rel["min_pitches_lo"] == lo)]
            if len(k) != 1 or int(k["n_pairs"].iloc[0]) != len(q) \
                    or abs(float(k["yoy_corr_within_type"].iloc[0]) - np.corrcoef(q["w1"], q["w2"])[0, 1]) > 1e-9:
                raise SystemExit(f"{m} {lo}+: recomputed bin does not match mart_scouting_reliability")
        data[m] = p[p["mn"] >= MIN_PITCHES]
    seasons = sorted(set(data[METRICS[0][0]]["season"]))
    if any(sorted(set(data[m]["season"])) != seasons for m, *_ in METRICS):
        raise SystemExit("metrics cover different season pairs")
    lim = {m: float(np.abs(np.r_[data[m]["w1"], data[m]["w2"]]).max()) * s * 1.05
           for m, _, _, s in METRICS}   # every point inside the axes
    r_all = {m: float(np.corrcoef(data[m]["w1"], data[m]["w2"])[0, 1]) for m, *_ in METRICS}

    ease = [0.0, 0.15, 0.4, 0.7, 0.9, 1.0]
    steps, durations = [], []
    for s in seasons:
        for t in ease:
            steps.append((s, t))
            durations.append(70)
        durations[-1] = 900
    durations[-1] = 5500
    frames = []
    for s, t in steps:
        fig, axes = plt.subplots(1, 2, figsize=(10, 5.6), dpi=80)
        fig.patch.set_facecolor("white")
        for ax, (m, name, unit, scale) in zip(axes, METRICS):
            ax.set_facecolor("white")
            L = lim[m]
            ax.plot([-L, L], [-L, L], color=DIAG, lw=1, ls="--")
            ax.axhline(0, color="#dddddd", lw=0.8)
            ax.axvline(0, color="#dddddd", lw=0.8)
            p = data[m]
            old = p[p["season"] < s]
            cur = p[p["season"] == s]
            ax.scatter(old["w1"] * scale, old["w2"] * scale, s=9, color=PAST, lw=0)
            x = cur["w1"].to_numpy() * scale
            y = x + t * (cur["w2"].to_numpy() * scale - x)
            ax.scatter(x, y, s=14, color=NOW, alpha=0.85, lw=0)
            seen = p[p["season"] <= s] if t == 1.0 else old
            if len(seen) > 2:
                r = np.corrcoef(seen["w1"], seen["w2"])[0, 1]
                ax.text(0.04, 0.95, f"r = {r:.2f}", transform=ax.transAxes, fontsize=20, weight="bold",
                        va="top", color="#333333")
                ax.text(0.04, 0.84, "pairs so far", transform=ax.transAxes, fontsize=10, va="top",
                        color="#555555")
            ax.set_xlim(-L, L)
            ax.set_ylim(-L, L)
            ax.set_aspect("equal")
            ax.set_title(name, fontsize=14, loc="left")
            ax.set_xlabel(f"This season ({unit})", fontsize=12)
            ax.set_ylabel(f"Next season ({unit})", fontsize=12)
            ax.tick_params(labelsize=10)
            for side in ("top", "right"):
                ax.spines[side].set_visible(False)
        last = (s == seasons[-1] and t == 1.0)
        head = (f"Whiff rate carries over (r {r_all['whiff_rate']:.2f}); run value mostly does not "
                f"(r {r_all['run_value_per_100']:.2f})" if last
                else "Does a pitch's season carry over to the next?")
        fig.suptitle(head, fontsize=16, x=0.02, ha="left", y=0.975)
        fig.text(0.98, 0.93, f"{s} → {s + 1}", fontsize=22, ha="right", va="top", weight="bold",
                 color="#444444")
        fig.text(0.02, 0.01, "\n".join([
            f"Same pitcher and pitch type, {MIN_PITCHES}+ pitches in both seasons. Run value: + is good for the pitcher.",
            "Pitch-type average removed (over every pitch with 100+ in the season, so these sit above zero).",
            "Dashed line: next season = this season. Data: Statcast via Baseball Savant."]),
                 fontsize=9.5, color="#555555")
        fig.subplots_adjust(left=0.08, right=0.98, top=0.8, bottom=0.23, wspace=0.3)  # fixed: no jitter
        buf = io.BytesIO()
        fig.savefig(buf, format="png", facecolor="white")
        plt.close(fig)
        frames.append(Image.open(buf).convert("RGB"))

    pal = frames[-1].quantize(colors=64, method=Image.Quantize.MEDIANCUT)  # last frame holds every colour
    q = [fr.quantize(palette=pal, dither=Image.Dither.NONE) for fr in frames]
    q[0].save(out_gif, save_all=True, append_images=q[1:], duration=durations, loop=0, optimize=True)
    print(json.dumps({"frames": len(q), "seconds": sum(durations) / 1000, "bytes": pathlib.Path(out_gif).stat().st_size,
                      "pairs": {m: len(data[m]) for m, *_ in METRICS}, "r": r_all,
                      "seasons": [int(seasons[0]), int(seasons[-1]) + 1]}))


if __name__ == "__main__":
    if len(sys.argv) != 5:
        raise SystemExit(__doc__)
    main(*sys.argv[1:])
