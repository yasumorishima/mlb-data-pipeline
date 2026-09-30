# Figure 8 of the pitch carryover article: fitted year-to-year correlation at a fixed
# pitch count (500, and 1,086 = a starter's main pitch over one season), from
# pitch_carryover_fit.json written by pitch_carryover_fit.py (the fit). Style as figs3.py.
import json
import sys

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

BLUE, ORANGE, GRAY, INK, SUB = "#2a78d6", "#eb6834", "#b9b8b3", "#0b0b0b", "#52514e"
fit = json.load(open(sys.argv[1] if len(sys.argv) > 1 else "pitch_carryover_fit.json"))
OUT = sys.argv[2] if len(sys.argv) > 2 else "."
M = ["whiff_rate", "xwoba_allowed", "run_value_per_100"]
T = {
    "ja": dict(font="Noto Sans CJK JP",
               title="500 球投げたときの翌年との相関：\n空振り率 0.68、被 xwOBA 0.41、run value 0.20",
               lab=["空振り率", "被 xwOBA", "run value"],
               k500="500 球", kfull="約 1,100 球（先発の主な球種 1 年分）",
               x="翌年との相関（式から計算した値）",
               note="横線は投手単位で選び直して 200 回計算した 95% 区間。2 シーズンとも同じ球数を投げたとした値"),
    "en": dict(font="DejaVu Sans",
               title="Correlation with next season at 500 pitches:\nwhiff rate 0.68, xwOBA allowed 0.41, run value 0.20",
               lab=["Whiff rate", "xwOBA allowed", "Run value"],
               k500="500 pitches", kfull="~1,100 pitches (a starter's main pitch, one season)",
               x="Correlation with next season (from the fitted curve)",
               note="Lines: 95% interval from 200 resamples of pitchers. Same pitch count assumed in both seasons"),
}


def base(ax):
    for s in ("top", "right", "left"):
        ax.spines[s].set_visible(False)
    ax.spines["bottom"].set_color(GRAY)
    ax.tick_params(colors=SUB, length=0, labelsize=14)


for lang, t in T.items():
    plt.rcParams["font.family"] = t["font"]
    f, ax = plt.subplots(figsize=(11, 5.8), dpi=150)
    ys = [2, 1, 0]
    for y, m, lab in zip(ys, M, t["lab"]):
        d = fit[m]
        col = ORANGE if m == "run_value_per_100" else BLUE
        # 500 pitches: solid marker; ~1,100: hollow marker, slightly lower
        ax.plot(d["r500_95"], [y + 0.12] * 2, color=col, lw=2)
        ax.scatter([d["r500"]], [y + 0.12], s=110, color=col, zorder=3)
        ax.text(d["r500_95"][1] + 0.015, y + 0.12, f"{d['r500']:.2f}", va="center", fontsize=15, color=col)
        ax.plot(d["rfull_95"], [y - 0.18] * 2, color=GRAY, lw=2)
        ax.scatter([d["rfull"]], [y - 0.18], s=90, facecolor="white", edgecolor=SUB, lw=1.8, zorder=3)
        ax.text(d["rfull_95"][1] + 0.015, y - 0.18, f"{d['rfull']:.2f}", va="center", fontsize=13, color=SUB)
    ax.set_yticks(ys, t["lab"], fontsize=15)
    ax.set_xlim(0, 1); ax.set_xticks([0, 0.25, 0.5, 0.75, 1.0], ["0", "0.25", "0.50", "0.75", "1"])
    ax.set_ylim(-0.6, 2.6)
    ax.set_xlabel(t["x"], fontsize=14, color=SUB)
    base(ax)
    f.text(0.03, 0.97, t["title"], fontsize=19, weight="bold", color=INK, va="top")
    f.text(0.03, 0.80, "● " + t["k500"], fontsize=13, color=INK, va="top")
    f.text(0.22 if lang == "ja" else 0.26, 0.80, "○ " + t["kfull"], fontsize=13, color=SUB, va="top")
    f.text(0.03, 0.02, t["note"], fontsize=12, color=SUB)
    f.tight_layout(rect=(0, 0.05, 1, 0.76))
    f.savefig(f"{OUT}/pitch_carryover_8_{lang}.png")
    plt.close(f)
print("ok")
