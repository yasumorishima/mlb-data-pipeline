# Charts for the team-wins article, in the style of zennchk/figs2.py:
# one message per chart, the message is the title, one accent colour, direct labels, no gridlines.
import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

import score as S

BLUE, ORANGE, GRAY, INK, SUB = "#2a78d6", "#eb6834", "#b9b8b3", "#0b0b0b", "#52514e"
OUT = "../../docs/images/"

bt = S.table("projections_2016_2026.csv", 2016, 2025)
by_season, _ = S.summarise(bt)
d26 = S.table("projections_2016_2026.csv", 2026, 2026)
dig = pd.read_csv("dig_2026.csv")
mb = pd.read_csv("mil_bat.csv", index_col=0)
mp = pd.read_csv("mil_pit.csv", index_col=0)

T = {
 "ja": dict(font="Noto Sans CJK JP",
   f1="2026 年：予測の誤差は床より小さいが、差は区別できない",
   f1n="30 球団の勝数の平均誤差（勝）",
   f1l=["選手の見込みを\n足した予測", "全球団\n5 割", "前年の\n勝率", "前年の\nピタゴラス勝率"],
   f2="9 シーズン中 7 シーズンで、前年のピタゴラス勝率より誤差が小さかった", f2n="平均誤差（勝）",
   f2a="選手の見込みを足した予測", f2b="前年のピタゴラス勝率（床）",
   f3="当たった球団と外れた球団", f3x="予測した勝数", f3y="実際の勝数",
   f4="予測の散らばりは実際の半分しかない", f4a="予測", f4b="実際", f4x="2026 年の勝数", f4n="標準偏差：予測 {p:.1f} 勝、実際 {a:.1f} 勝",
   f5="得失点が分かっていても 4 勝は外れる", f5n="2026 年・30 球団の平均誤差（勝）",
   f5l=["実際の得失点から\nピタゴラス式で", "選手の見込みから\n（今回の予測）"],
   f6="MIL：見込みの対象だった選手が上振れした", f6x="見込みとの差（点・失点を防いだ分も含む）",
   f6n="打者は wOBA、投手は FIP から換算した、リーグ平均を上回った点の差",
   f7="出場の 2〜3 割は、見込みの無い選手だった", f7n="2026 年、開幕時の 40 人枠に過去の成績がある選手以外が占めた割合",
   f7l=["打席", "投球回"]),
 "en": dict(font="DejaVu Sans",
   f1="2026: lower error than the floors, but not distinguishably",
   f1n="Mean absolute error of wins over 30 teams",
   f1l=["Projection from\nplayer forecasts", "Every team\n.500", "Last season's\nrecord", "Last season's\nPythagenpat"],
   f2="Lower error than last season's Pythagenpat in 7 of 9 seasons", f2n="Mean absolute error (wins)",
   f2a="Projection from player forecasts", f2b="Last season's Pythagenpat (floor)",
   f3="Hits and misses", f3x="Projected wins", f3y="Actual wins",
   f4="The projections spread half as wide as the results", f4a="Projected", f4b="Actual", f4x="2026 wins", f4n="Standard deviation: projected {p:.1f}, actual {a:.1f} wins",
   f5="Even with the actual runs, a 4-win miss remains", f5n="2026, mean absolute error over 30 teams (wins)",
   f5l=["Pythagenpat from\nactual runs", "From player\nforecasts (this one)"],
   f6="MIL: players it did project played above their forecasts", f6x="Actual minus projected runs",
   f6n="Runs above league average from wOBA (batters) or FIP (pitchers)",
   f7="20-30% of playing time went to players with no forecast", f7n="2026: share not by opening-day 40-man players with MLB history",
   f7l=["Plate appearances", "Innings"]),
}


def base(ax):
    for s in ("top", "right", "left"):
        ax.spines[s].set_visible(False)
    ax.spines["bottom"].set_color(GRAY)
    ax.tick_params(colors=SUB, length=0, labelsize=13)


def title(fig, text, note=None):
    fig.text(0.04, 0.95, text, fontsize=19, color=INK, ha="left", va="top", weight="bold")
    if note:
        fig.text(0.04, 0.875, note, fontsize=13, color=SUB, ha="left", va="top")


def save(fig, name, lang):
    fig.savefig(f"{OUT}{name}_{lang}.png", dpi=110, facecolor="white")
    plt.close(fig)


mae26 = [(d26[a] - d26.wins).abs().mean() for a in S.ARMS]
dig["w_proj"] = dig.pct * dig.g
for lang, t in T.items():
    plt.rcParams["font.family"] = t["font"]

    # 1. 2026 MAE
    fig = plt.figure(figsize=(10, 5.6)); title(fig, t["f1"], t["f1n"])
    ax = fig.add_axes([0.06, 0.12, 0.9, 0.62]); base(ax)
    x = np.arange(4)
    ax.bar(x, mae26, color=[BLUE, GRAY, GRAY, GRAY], width=0.55)
    for i, v in enumerate(mae26):
        ax.text(i, v + 0.15, f"{v:.2f}", ha="center", fontsize=15, color=BLUE if i == 0 else SUB)
    ax.set_xticks(x, t["f1l"]); ax.set_yticks([]); ax.set_ylim(0, 10.5)
    save(fig, "team_wins_1", lang)

    # 2. backtest by season
    fig = plt.figure(figsize=(10, 5.6)); title(fig, t["f2"], t["f2n"])
    ax = fig.add_axes([0.08, 0.12, 0.6, 0.62]); base(ax)
    s = by_season.index.astype(str)
    ax.plot(s, by_season.floor_prev_pyth, color=GRAY, lw=2.5, marker="o")
    ax.plot(s, by_season.model, color=BLUE, lw=3, marker="o")
    ax.text(len(s) - 0.7, by_season.model.iloc[-1], t["f2a"], color=BLUE, fontsize=13, va="center")
    ax.text(len(s) - 0.7, by_season.floor_prev_pyth.iloc[-1] - 0.5, t["f2b"], color=SUB, fontsize=13, va="center")
    ax.set_ylim(5, 12.5); ax.set_yticks([6, 8, 10, 12])
    save(fig, "team_wins_2", lang)

    # 3. scatter
    fig = plt.figure(figsize=(8, 7.4)); title(fig, t["f3"])
    ax = fig.add_axes([0.12, 0.1, 0.82, 0.75]); base(ax); ax.spines["left"].set_visible(True); ax.spines["left"].set_color(GRAY)
    ax.plot([55, 106], [55, 106], color=GRAY, lw=1, ls="--")
    big = dig.set_index("team").miss.abs().sort_values(ascending=False).index[:4]
    for _, r in dig.iterrows():
        hi = r.team in big
        ax.scatter(r.w_proj, r.wins, s=70 if hi else 40, color=ORANGE if hi else GRAY, zorder=3)
        if hi:
            left = r.team == "ATH"
            ax.text(r.w_proj + (-0.8 if left else 0.8), r.wins, f"{r.team} {r.wins:.0f}", fontsize=13,
                    color=ORANGE, va="center", ha="right" if left else "left")
    ax.set_xlim(55, 106); ax.set_ylim(55, 106)
    ax.set_xlabel(t["f3x"], fontsize=14, color=SUB); ax.set_ylabel(t["f3y"], fontsize=14, color=SUB)
    save(fig, "team_wins_3", lang)

    # 4. spread
    fig = plt.figure(figsize=(10, 4.8)); title(fig, t["f4"], t["f4n"].format(p=dig.w_proj.std(), a=dig.wins.std()))
    ax = fig.add_axes([0.14, 0.18, 0.82, 0.55]); base(ax)
    ax.scatter(dig.w_proj, np.full(30, 1), s=60, color=BLUE, alpha=0.8)
    ax.scatter(dig.wins, np.full(30, 0), s=60, color=GRAY, alpha=0.9)
    ax.set_yticks([1, 0], [t["f4a"], t["f4b"]]); ax.set_ylim(-0.6, 1.6)
    ax.get_yticklabels()[0].set_color(BLUE)
    ax.set_xlabel(t["f4x"], fontsize=14, color=SUB)
    save(fig, "team_wins_4", lang)

    # 5. runs known vs projection
    fig = plt.figure(figsize=(10, 5.2)); title(fig, t["f5"], t["f5n"])
    ax = fig.add_axes([0.06, 0.12, 0.9, 0.6]); base(ax)
    v = [dig.miss_luck.abs().mean(), dig.miss.abs().mean()]
    ax.bar([0, 1], v, color=[GRAY, BLUE], width=0.45)
    for i, y in enumerate(v):
        ax.text(i, y + 0.15, f"{y:.2f}", ha="center", fontsize=15, color=BLUE if i else SUB)
    ax.set_xticks([0, 1], t["f5l"]); ax.set_yticks([]); ax.set_ylim(0, 9.5)
    save(fig, "team_wins_5", lang)

    # 6. MIL player gaps
    g = pd.concat([mb.gap, mp.gap]).sort_values(ascending=False).head(8)[::-1]
    fig = plt.figure(figsize=(10, 6)); title(fig, t["f6"], t["f6n"])
    ax = fig.add_axes([0.28, 0.1, 0.66, 0.66]); base(ax)
    ax.barh(range(len(g)), g.values, color=[ORANGE if v > 25 else GRAY for v in g.values], height=0.6)
    for i, v in enumerate(g.values):
        ax.text(v + 0.5, i, f"+{v:.0f}", va="center", fontsize=13, color=ORANGE if v > 25 else SUB)
    ax.set_yticks(range(len(g)), g.index); ax.set_xticks([])
    ax.set_xlabel(t["f6x"], fontsize=13, color=SUB)
    save(fig, "team_wins_6", lang)

    # 7. unprojected playing time
    fig = plt.figure(figsize=(10, 4.6)); title(fig, t["f7"], t["f7n"])
    ax = fig.add_axes([0.2, 0.12, 0.74, 0.55]); base(ax)
    v = [0.198, 0.271]
    ax.barh([1, 0], v, color=BLUE, height=0.5)
    for i, y in zip([1, 0], v):
        ax.text(y + 0.005, i, f"{y:.0%}", va="center", fontsize=15, color=BLUE)
    ax.set_yticks([1, 0], t["f7l"]); ax.set_xticks([]); ax.set_xlim(0, 0.35)
    save(fig, "team_wins_7", lang)
print("ok", mae26)
