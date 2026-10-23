# Draws docs/images/pitcher_process_{1,2,3}_{ja,en}.png (dbt/README.md).
# Committed as it was run on 2026-09-29 (it had lived only in a scratch
# directory). Reads 'pproc.duckdb' (a dbt DuckDB build of that day) and writes
# to ./pfig/. Needs the "Noto Sans CJK JP" font for the ja figures.
# NOTE: same caveat as pitcher_heldout.py - a build from today includes the
# 2025 -> 2026 pairs (2026 is finished), so the held-out panel and the
# reliability lines will differ from the committed PNGs.
import duckdb, numpy as np, matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib import font_manager as fm
plt.rcParams["font.family"]="Noto Sans CJK JP"
BLUE,ORANGE,GRAY,INK,SUB="#2a78d6","#e8693a","#b9b9b2","#111111","#555555"
c=duckdb.connect('pproc.duckdb',read_only=True)
P=c.sql("select metric,min_bf_lo lo,yoy_corr r from main.mart_pitcher_process_reliability").df()
B=c.sql("select metric,min_pa_lo lo,yoy_corr r from main.mart_batter_process_reliability").df()
L={"ja":{"x":"2 シーズンのうち少ないほうの対戦打者数","y":"翌年との相関","bins":["100〜199","200〜399","400〜599","600 以上"],
 "t1":"防御率は翌年に残りにくく、K−BB% は残る","era":"防御率","fip":"FIP","xera":"xERA","kbb":"K−BB%",
 "t2":"レギュラー級の打者の「xwOBA との差」は翌年も残り、\n投手の「xERA との差」はほとんど残らない","x2":"2 シーズンのうち少ないほうの打席数（投手は対戦打者数）",
 "bat":"打者 wOBA−xwOBA","pit":"投手 防御率−xERA",
 "t3":"翌年の防御率は、今年の防御率より\nFIP・xERA・K−BB% のどれを使っても当たる","x3":"全員に同じ値を置いたときより、誤差がどれだけ減ったか（防御率）",
 "n3":"2015〜2022 年で当てはめ、2023→2024 と 2024→2025 の 696 組で採点。\n横線は投手単位で取り直した 95% 区間。3 つの間の差は区別できない"},
 "en":{"x":"Batters faced, smaller of the two seasons","y":"Correlation with next season","bins":["100-199","200-399","400-599","600+"],
 "t1":"ERA barely carries over; K−BB% does","era":"ERA","fip":"FIP","xera":"xERA","kbb":"K−BB%",
 "t2":"Regular batters who beat their xwOBA keep beating it;\npitchers who beat their xERA mostly do not","x2":"PA (batters) or batters faced (pitchers), smaller of the two seasons",
 "bat":"Batters  wOBA−xwOBA","pit":"Pitchers  ERA−xERA",
 "t3":"FIP, xERA or K−BB% each predict next season's\nERA better than ERA itself","x3":"Error reduction vs. the same guess for everyone (runs of ERA)",
 "n3":"Fit on 2015-2022, scored on 696 pairs (2023→2024, 2024→2025).\nLines: 95% interval, resampling pitchers. The three are not distinguishable"}}
def style(ax):
    for s in ["top","right","left"]: ax.spines[s].set_visible(False)
    ax.spines["bottom"].set_color("#bbbbbb"); ax.tick_params(length=0,labelsize=14,colors=SUB)
def lines(ax,series,lang,xl):
    xs=np.arange(4)
    for lab,ys,col,w in series:
        ax.plot(xs,ys,color=col,lw=3.2,marker="o",ms=9,zorder=3 if col!=GRAY else 2)
        ax.text(3.15,ys[-1]+w,f"{lab}  {ys[-1]:.2f}",color=col if col!=GRAY else SUB,fontsize=15,va="center")
    ax.set_xticks(xs,L[lang]["bins"]); ax.set_xlim(-0.15,3.9); ax.set_xlabel(xl,fontsize=15,color=SUB)
    ax.set_ylabel(L[lang]["y"],fontsize=15,color=SUB); style(ax)
def get(D,m): return D[D.metric==m].sort_values("lo").r.values
# holdout
d=c.sql("""select x.player_id pid,x.season s,y.bf bf2,x.era,x.fip,x.xera,x.k_minus_bb_rate kbb,y.era nera
 from main.mart_pitcher_season x join main.mart_pitcher_season y on y.player_id=x.player_id and y.season=x.season+1
 where x.bf>=100 and y.bf>=100 and x.xera is not null and y.xera is not null and not y.is_partial""").df()
tr,te=d[d.s<=2022],d[d.s>=2023]
def err(cols):
    X=np.c_[np.ones(len(tr)),tr[cols].values]; w=np.sqrt(tr.bf2.values)
    b=np.linalg.lstsq(X*w[:,None],tr.nera.values*w,rcond=None)[0]
    return np.abs(np.c_[np.ones(len(te)),te[cols].values]@b-te.nera.values)
E0=err([]); w=te.bf2.values; pids=te.pid.values; up=np.unique(pids); idx=[np.where(pids==p)[0] for p in up]
rng=np.random.default_rng(0); draws=[np.concatenate([idx[j] for j in rng.integers(0,len(up),len(up))]) for _ in range(4000)]
H={}
for k,cols in [("era",["era"]),("fip",["fip"]),("xera",["xera"]),("kbb",["kbb"])]:
    dd=E0-err(cols); pt=np.average(dd,weights=w); bs=[np.average(dd[i],weights=w[i]) for i in draws]
    H[k]=(pt,*np.quantile(bs,[.025,.975]))
print(len(te),len(up),{k:tuple(round(v,4) for v in t) for k,t in H.items()})
for lang in ["ja","en"]:
    T=L[lang]
    f,ax=plt.subplots(figsize=(10,6),dpi=150)
    lines(ax,[(T["fip"],get(P,"fip"),GRAY,0.03),(T["xera"],get(P,"xera"),GRAY,-0.03),(T["kbb"],get(P,"k_minus_bb_rate"),BLUE,0),(T["era"],get(P,"era"),ORANGE,0)],lang,T["x"])
    ax.set_ylim(0,1); ax.set_yticks([0,.5,1],["0.0","0.5","1.0"])
    f.suptitle(T["t1"],x=0.04,ha="left",fontsize=20,fontweight="bold",color=INK); f.tight_layout(rect=(0,0,1,.93))
    f.savefig(f"pfig/pitcher_process_1_{lang}.png"); plt.close(f)
    f,ax=plt.subplots(figsize=(10,6),dpi=150)
    lines(ax,[(T["bat"],get(B,"woba_minus_xwoba"),BLUE,0),(T["pit"],get(P,"era_minus_xera"),ORANGE,0)],lang,T["x2"])
    ax.axhline(0,color="#bbbbbb",lw=1); ax.set_ylim(-0.1,0.5); ax.set_yticks([0,.25,.5],["0.0","0.25","0.5"])
    f.suptitle(T["t2"],x=0.04,ha="left",fontsize=19,fontweight="bold",color=INK); f.tight_layout(rect=(0,0,1,.9))
    f.savefig(f"pfig/pitcher_process_2_{lang}.png"); plt.close(f)
    f,ax=plt.subplots(figsize=(10,5.6),dpi=150)
    order=["era","fip","xera","kbb"]; ys=np.arange(4)[::-1]
    for y,k in zip(ys,order):
        pt,lo,hi=H[k]; col=ORANGE if k=="era" else BLUE
        ax.barh(y,pt,color=col,height=.55); ax.plot([lo,hi],[y,y],color=INK,lw=1.6)
        ax.text(max(hi,pt)+0.002,y,f"{pt:.3f}",va="center",fontsize=15,color=col)
    ax.set_yticks(ys,[T[k] for k in order],fontsize=15); ax.axvline(0,color="#bbbbbb",lw=1)
    ax.set_xlim(-0.01,0.1); ax.set_xlabel(T["x3"],fontsize=14,color=SUB); style(ax)
    f.suptitle(T["t3"],x=0.04,ha="left",fontsize=19,fontweight="bold",color=INK)
    f.text(0.04,0.02,T["n3"],fontsize=12,color=SUB); f.tight_layout(rect=(0,.09,1,.88))
    f.savefig(f"pfig/pitcher_process_3_{lang}.png"); plt.close(f)
