# Charts for the write-up of decision_pairs and decision_pairs_batters, in the
# style of forecast/team_wins/article_figs_tw.py: one message per chart, the
# message is the title, one accent colour, direct labels, no gridlines.
#
#     python article_dig.py && python article_figs.py
import json
from pathlib import Path

import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

BLUE, GRAY, LIGHT, INK, SUB = "#2a78d6", "#b9b8b3", "#dcdbd6", "#0b0b0b", "#52514e"
HERE = Path(__file__).resolve().parent
OUT = HERE.parent.parent / "docs" / "images"

PP = json.loads((HERE / "primary.json").read_text())
PD = json.loads((HERE / "dev.json").read_text())
BP = json.loads((HERE.parent / "decision_pairs_batters" / "primary.json").read_text())
BD = json.loads((HERE.parent / "decision_pairs_batters" / "dev.json").read_text())
DIG = json.loads((HERE / "article_dig.json").read_text())
HIT = pd.read_json(HERE / "article_hitters_2025.json")

T = {
 "ja": dict(font="Noto Sans CJK JP",
   bl=dict(woba="wOBA", xwoba="xwOBA", wrc_plus="wRC+", blend="混合"),
   pl=dict(era="ERA", fip="FIP", xera="xERA", kbb="K−BB%", blend="混合"),
   f1="打者：前年の xwOBA で選ぶと 65%、wOBA だと 61% 当たった",
   f1n="2025 年の数字で 2 人のうち 1 人を選び、2026 年の wOBA が高いほうなら正解（62,126 組）",
   f2="打者は xwOBA と混合が wOBA より当たり、投手はどれも ERA と区別できなかった",
   f2n="2026 年、前年の wOBA（打者）・ERA（投手）で選んだときとの正解率の差と 95% 区間（ポイント）",
   f2a="打者（基準：wOBA）", f2b="投手（基準：ERA）",
   f3="2026 年の結果は、作り方を決めた期間とおおむね同じ並びだった",
   f3n="正解率。白丸＝2015〜2024 年（2019・2020 年を除く。混合はこの期間で式を決めた）、青丸＝2026 年",
   f3a="打者", f3b="投手",
   f4="2 つの数字が別の選手を指した組では、打者は xwOBA が 59% 当たった",
   f4n="2026 年、2 つの数字が割れた組だけでの正解率（点線＝五分五分）。投手の 54% は 1 シーズンでは区別できない",
   f4l=["wOBA と xwOBA が\n割れた 12,195 組", "ERA と K−BB% が\n割れた 11,028 組"],
   f4a=["wOBA", "xwOBA"], f4b=["ERA", "K−BB%"],
   f5="xwOBA で選んだ打者の翌年 wOBA は、全部の組の平均で .019 高かった",
   f5n="2026 年、選んだ選手と選ばなかった選手の翌年の成績の差（外れた組はマイナスとして全組で平均）",
   f5a="打者：翌年の wOBA の差", f5b="投手：翌年の ERA の差（点／9 回）",
   f6="前年の xwOBA のほうが、翌年の wOBA と強く結びつく",
   f6n="2025 年と 2026 年の両方で 100 打席以上の 353 人",
   f6xa="2025 年の wOBA", f6xb="2025 年の xwOBA", f6y="2026 年の wOBA",
   f7="同じ年の数字で当てても、8 割前後だった（参考値）",
   f7n="2026 年の正解率。「同じ年」は 2026 年の数字で 2026 年の結果を当てた参考値",
   f7l=dict(coin="五分五分", prev_a="前年の\nwOBA", prev_b="前年の\nxwOBA", same_b="同じ年の\nxwOBA",
            prev_c="前年の\nERA", prev_d="前年の\nK−BB%", same_d="同じ年の\nFIP"),
   f7a="打者（翌年の wOBA）", f7b="投手（翌年の ERA）",
   f8="打席数で分けると、どの帯でも xwOBA の正解率が wOBA より上だった（報告のみ）",
   f8n="2 人の 2025 年の打席数の少ないほうで分けた正解率（2026 年）",
   f8x=["100〜299 打席", "300〜499 打席", "500 打席以上"]),
 "en": dict(font="DejaVu Sans",
   bl=dict(woba="wOBA", xwoba="xwOBA", wrc_plus="wRC+", blend="Blend"),
   pl=dict(era="ERA", fip="FIP", xera="xERA", kbb="K−BB%", blend="Blend"),
   f1="Hitters: last season's xwOBA picked right 65% of the time, wOBA 61%",
   f1n="Pick one of two hitters on 2025 numbers; right if the pick had the higher 2026 wOBA (62,126 pairs)",
   f2="Hitters: xwOBA and the blend beat wOBA. Pitchers: none separable from ERA",
   f2n="2026, share picked right minus the wOBA (hitters) or ERA (pitchers) rule, 95% intervals (points)",
   f2a="Hitters (vs wOBA)", f2b="Pitchers (vs ERA)",
   f3="2026 came out in roughly the same order as the development seasons",
   f3n="Share picked right. White = 2015-2024 (without 2019 and 2020; the blend was fitted on it), blue = 2026",
   f3a="Hitters", f3b="Pitchers",
   f4="When wOBA and xwOBA disagree, xwOBA was right 59% of the time",
   f4n="2026, only pairs where the two rules pick different players (dotted = coin flip). Pitchers' 54% is not separable in one season",
   f4l=["wOBA vs xwOBA\n12,195 pairs", "ERA vs K−BB%\n11,028 pairs"],
   f4a=["wOBA", "xwOBA"], f4b=["ERA", "K−BB%"],
   f5="Hitters picked on xwOBA had a next-season wOBA .019 higher, over all pairs",
   f5n="2026, next-season gap between the player picked and the one left (wrong picks negative), mean over all pairs",
   f5a="Hitters: next-season wOBA gap", f5b="Pitchers: next-season ERA gap (runs per 9)",
   f6="Last season's xwOBA tracks next season's wOBA more closely",
   f6n="353 hitters with 100+ PA in both 2025 and 2026",
   f6xa="2025 wOBA", f6xb="2025 xwOBA", f6y="2026 wOBA",
   f7="Same-season numbers picked right about 80% of the time (reference)",
   f7n="2026 share picked right. Same season = 2026 numbers used to pick the 2026 result (reference)",
   f7l=dict(coin="Coin flip", prev_a="Last\nwOBA", prev_b="Last\nxwOBA", same_b="Same-season\nxwOBA",
            prev_c="Last\nERA", prev_d="Last\nK−BB%", same_d="Same-season\nFIP"),
   f7a="Hitters (next-season wOBA)", f7b="Pitchers (next-season ERA)",
   f8="In every PA band, xwOBA had the higher share picked right (reported only)",
   f8n="2026 share picked right, by the smaller of the two hitters' 2025 PA",
   f8x=["100-299 PA", "300-499 PA", "500+ PA"]),
}


def base(ax, left=False):
    for s in ("top", "right") + (() if left else ("left",)):
        ax.spines[s].set_visible(False)
    ax.spines["bottom"].set_color(GRAY)
    if left:
        ax.spines["left"].set_color(GRAY)
    ax.tick_params(colors=SUB, length=0, labelsize=14)


def title(fig, text, note=None):
    fig.text(0.03, 0.96, text, fontsize=18, color=INK, ha="left", va="top", weight="bold")
    if note:
        fig.text(0.03, 0.885, note, fontsize=13, color=SUB, ha="left", va="top")


def save(fig, n, lang):
    fig.savefig(OUT / f"decision_pairs_{n}_{lang}.png", dpi=110, facecolor="white")
    plt.close(fig)


def pct(x):
    return f"{100 * x:.1f}%"


def three(x):  # .019 style for wOBA
    return f"{x:.3f}".replace("0.", ".", 1)


for lang, t in T.items():
    plt.rcParams["font.family"] = t["font"]

    # 1 hitters, 2026 share right
    order = ["woba", "wrc_plus", "xwoba", "blend"]
    acc = [BP["all"][k]["acc"] for k in order]
    fig, ax = plt.subplots(figsize=(12, 6))
    fig.subplots_adjust(left=0.12, right=0.95, top=0.76, bottom=0.1)
    title(fig, t["f1"], t["f1n"]); base(ax)
    ax.barh(range(4), acc, color=[GRAY, GRAY, BLUE, BLUE], height=0.6)
    for i, a in enumerate(acc):
        ax.text(a + 0.002, i, pct(a), va="center", fontsize=15, color=INK)
    ax.set_yticks(range(4), [t["bl"][k] for k in order]); ax.invert_yaxis()
    ax.set_xlim(0.5, 0.68); ax.set_xticks([0.5, 0.55, 0.6, 0.65], ["50%", "55%", "60%", "65%"])
    save(fig, 1, lang)

    # 2 difference from the base rule, with 95% intervals
    fig, axs = plt.subplots(1, 2, figsize=(13, 6), gridspec_kw=dict(width_ratios=[3, 4]))
    fig.subplots_adjust(left=0.08, right=0.97, top=0.72, bottom=0.1, wspace=0.35)
    title(fig, t["f2"], t["f2n"])
    for ax, J, ks, lab, nm, b in [(axs[0], BP, ["xwoba", "wrc_plus", "blend"], t["f2a"], t["bl"], "woba"),
                                  (axs[1], PP, ["fip", "xera", "kbb", "blend"], t["f2b"], t["pl"], "era")]:
        base(ax); ax.axvline(0, color=SUB, lw=1)
        for i, k in enumerate(ks):
            r = J["all"][k]; lo, hi = r["ci95"]; m = r["acc_minus_" + b]
            c = BLUE if lo > 0 else GRAY
            ax.plot([100 * lo, 100 * hi], [i, i], color=c, lw=3, solid_capstyle="round")
            ax.plot(100 * m, i, "o", color=c, ms=10)
            ax.text(100 * hi + 0.4, i, f"{100 * m:+.1f}", va="center", fontsize=14, color=INK)
        ax.set_yticks(range(len(ks)), [nm[k] for k in ks]); ax.invert_yaxis()
        ax.set_xlim(-3, 9); ax.set_title(lab, fontsize=15, color=INK, loc="left")
    save(fig, 2, lang)

    # 3 development seasons vs 2026
    fig, axs = plt.subplots(1, 2, figsize=(13, 6), gridspec_kw=dict(width_ratios=[4, 5]))
    fig.subplots_adjust(left=0.08, right=0.97, top=0.74, bottom=0.1, wspace=0.3)
    title(fig, t["f3"], t["f3n"])
    for ax, P_, D_, ks, lab, nm, lo in [(axs[0], BP, BD, ["woba", "wrc_plus", "xwoba", "blend"], t["f3a"], t["bl"], 0.600),
                                        (axs[1], PP, PD, ["era", "fip", "xera", "kbb", "blend"], t["f3b"], t["pl"], 0.550)]:
        base(ax)
        for i, k in enumerate(ks):
            a0, a1 = D_["all"][k]["acc"], P_["all"][k]["acc"]
            ax.plot([a0, a1], [i, i], color=LIGHT, lw=2)
            ax.plot(a0, i, "o", mfc="white", mec=SUB, ms=10, mew=1.5)
            ax.plot(a1, i, "o", color=BLUE, ms=10)
        ax.set_yticks(range(len(ks)), [nm[k] for k in ks]); ax.invert_yaxis()
        span = 0.075 if P_ is BP else 0.05
        ticks = list(np.arange(lo, lo + span + 1e-9, 0.025))
        ax.set_xlim(lo - 0.005, lo + span + 0.005); ax.set_xticks(ticks, [f"{100 * v:.1f}%" for v in ticks])
        ax.set_title(lab, fontsize=15, color=INK, loc="left")
    save(fig, 3, lang)

    # 4 pairs where the two rules disagree
    rows = [(BP["disagree_woba_xwoba"], "woba", "xwoba", t["f4a"]), (PP["disagree_era_kbb"], "era", "kbb", t["f4b"])]
    fig, ax = plt.subplots(figsize=(12, 5.5))
    fig.subplots_adjust(left=0.2, right=0.95, top=0.74, bottom=0.08)
    title(fig, t["f4"], t["f4n"]); base(ax); ax.spines["bottom"].set_visible(False)
    for i, (r, a, b, labs) in enumerate(rows):
        va, vb = r[a]["acc"], r[b]["acc"]
        ax.barh(i, va, color=GRAY, height=0.55); ax.barh(i, vb, left=va, color=BLUE, height=0.55)
        ax.text(va / 2, i, f"{labs[0]} {100 * va:.0f}%", ha="center", va="center", fontsize=15, color=INK)
        ax.text(va + vb / 2, i, f"{labs[1]} {100 * vb:.0f}%", ha="center", va="center", fontsize=15, color="white")
    ax.axvline(0.5, color=INK, lw=1, ls=":")
    ax.set_yticks(range(2), t["f4l"]); ax.invert_yaxis(); ax.set_xlim(0, 1); ax.set_xticks([])
    save(fig, 4, lang)

    # 5 what a pick is worth
    fig, axs = plt.subplots(1, 2, figsize=(13, 5.5))
    fig.subplots_adjust(left=0.08, right=0.97, top=0.72, bottom=0.08, wspace=0.35)
    title(fig, t["f5"], t["f5n"])
    for ax, J, ks, key, lab, nm, fmt in [(axs[0], BP, ["woba", "xwoba"], "woba_gain", t["f5a"], t["bl"], three),
                                         (axs[1], PP, ["era", "kbb"], "era_gain", t["f5b"], t["pl"], lambda x: f"{x:.2f}")]:
        base(ax); ax.spines["bottom"].set_visible(False)
        v = [J["all"][k][key] for k in ks]
        ax.barh(range(2), v, color=[GRAY, BLUE], height=0.55)
        for i, x in enumerate(v):
            ax.text(x * 1.02, i, fmt(x), va="center", fontsize=15, color=INK)
        ax.set_yticks(range(2), [nm[k] for k in ks]); ax.invert_yaxis(); ax.set_xticks([])
        ax.set_xlim(0, max(v) * 1.3); ax.set_title(lab, fontsize=15, color=INK, loc="left")
    save(fig, 5, lang)

    # 6 scatter, hitters
    fig, axs = plt.subplots(1, 2, figsize=(13, 6.5), sharey=True)
    fig.subplots_adjust(left=0.08, right=0.97, top=0.76, bottom=0.13, wspace=0.12)
    title(fig, t["f6"], t["f6n"])
    for ax, c, xl, col in [(axs[0], "woba", t["f6xa"], GRAY), (axs[1], "xwoba", t["f6xb"], BLUE)]:
        base(ax, left=True)
        ax.scatter(HIT[c], HIT.woba_next, s=16, color=col, alpha=0.7, lw=0)
        r = np.corrcoef(HIT[c], HIT.woba_next)[0, 1]
        ax.text(0.03, 0.95, f"r = {r:.2f}", transform=ax.transAxes, fontsize=16, color=INK, va="top")
        ax.set_xlabel(xl, fontsize=14, color=SUB)
    axs[0].set_ylabel(t["f6y"], fontsize=14, color=SUB)
    save(fig, 6, lang)

    # 7 same-season reference
    L = t["f7l"]; hp, pp = DIG["hitters"], DIG["pitchers"]
    groups = [(t["f7a"], [("coin", 0.5, LIGHT), ("prev_a", hp["acc_2026"]["woba"], GRAY),
                          ("prev_b", hp["acc_2026"]["xwoba"], BLUE), ("same_b", hp["same_season_2026"]["xwoba"], LIGHT)]),
              (t["f7b"], [("coin", 0.5, LIGHT), ("prev_c", pp["acc_2026"]["era"], GRAY),
                          ("prev_d", pp["acc_2026"]["kbb"], BLUE), ("same_d", pp["same_season_2026"]["fip"], LIGHT)])]
    fig, axs = plt.subplots(1, 2, figsize=(13, 6.5), sharey=True)
    fig.subplots_adjust(left=0.04, right=0.97, top=0.74, bottom=0.17, wspace=0.12)
    title(fig, t["f7"], t["f7n"])
    for ax, (lab, g) in zip(axs, groups):
        base(ax)
        ax.bar(range(4), [v for _, v, _ in g], color=[c for *_, c in g], width=0.65)
        for i, (_, v, _) in enumerate(g):
            ax.text(i, v + 0.01, f"{100 * v:.0f}%", ha="center", fontsize=15, color=INK)
        ax.set_xticks(range(4), [L[k] for k, _, _ in g], fontsize=13); ax.set_yticks([])
        ax.set_ylim(0, 0.95); ax.set_title(lab, fontsize=15, color=INK, loc="left")
    save(fig, 7, lang)

    # 8 PA bands, hitters (reported only)
    bands = ["band_100_300", "band_300_500", "band_500_1000000"]
    fig, ax = plt.subplots(figsize=(12, 6))
    fig.subplots_adjust(left=0.1, right=0.86, top=0.76, bottom=0.1)
    title(fig, t["f8"], t["f8n"]); base(ax, left=True)
    for k, col in [("woba", GRAY), ("xwoba", BLUE)]:
        v = [BP[b][k]["acc"] for b in bands]
        ax.plot(range(3), v, "-o", color=col, lw=2.5, ms=9)
        ax.text(2.1, v[-1], t["bl"][k], va="center", fontsize=15, color=col)
    ax.set_xticks(range(3), t["f8x"]); ax.set_xlim(-0.2, 2.45)
    ax.set_ylim(0.55, 0.7); ax.set_yticks([0.55, 0.6, 0.65, 0.7], ["55%", "60%", "65%", "70%"])
    save(fig, 8, lang)
print("ok")
