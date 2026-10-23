# Pitcher held-out check behind dbt/README.md "Result vs process for pitchers"
# (constant 0.914 / ERA 0.897 / FIP 0.871 / xERA 0.864 / K-BB% 0.857, 696 pairs,
# 457 pitchers). Committed as it was run on 2026-09-29 (it had lived only in a
# scratch directory). Reads 'pproc.duckdb', a dbt DuckDB build
# (MLB_MARTS_DB=pproc.duckdb dbt build --profiles-dir dbt ...) of that day.
# NOTE: the test set is "second season >= 2023 and not partial". Since PR #46
# made 2026 a finished season, a build from today also includes the
# 2025 -> 2026 pairs, so it will NOT reproduce the README figures. To reproduce
# them, keep te to x.season in (2023, 2024). Scoring 2025 -> 2026 is a new,
# unseen test: write its rule down before running it.
import duckdb, numpy as np
c=duckdb.connect('pproc.duckdb',read_only=True)
d=c.sql("""select x.player_id pid, x.season s, least(x.bf,y.bf) mb, y.bf bf2, x.era, x.fip, x.xera, x.k_minus_bb_rate kbb, y.era nera
 from main.mart_pitcher_season x join main.mart_pitcher_season y on y.player_id=x.player_id and y.season=x.season+1
 where x.bf>=100 and y.bf>=100 and x.xera is not null and y.xera is not null and not y.is_partial""").df()
print(len(d), d.s.min(), d.s.max())
tr=d[d.s<=2022]; te=d[d.s>=2023]; print(len(tr),len(te), sorted(te.s.unique()))
def fit(cols):
    X=np.c_[np.ones(len(tr)),tr[cols].values]; w=tr.bf2.values
    b=np.linalg.lstsq(X*np.sqrt(w)[:,None], tr.nera.values*np.sqrt(w),rcond=None)[0]
    p=np.c_[np.ones(len(te)),te[cols].values]@b
    e=np.abs(p-te.nera.values); k=te.mb.values>=400
    return b, np.average(e,weights=te.bf2), e.mean(), np.average(e[k],weights=te.bf2.values[k])
for cols in [[],['era'],['fip'],['xera'],['kbb'],['era','fip'],['era','xera'],['fip','xera'],['era','fip','xera']]:
    b,mw,m,m400=fit(cols); print(f"{'+'.join(cols) or 'const':15s} wMAE {mw:.4f}  MAE {m:.4f}  wMAE400 {m400:.4f}  coef {np.round(b,3)}")
rng=np.random.default_rng(0)
def pred(cols):
    X=np.c_[np.ones(len(tr)),tr[cols].values]; w=tr.bf2.values
    b=np.linalg.lstsq(X*np.sqrt(w)[:,None], tr.nera.values*np.sqrt(w),rcond=None)[0]
    return np.abs(np.c_[np.ones(len(te)),te[cols].values]@b-te.nera.values)
E={k:pred(v) for k,v in {'const':[],'era':['era'],'fip':['fip'],'xera':['xera'],'kbb':['kbb']}.items()}
w=te.bf2.values; n=len(te)
for a in ['era','fip','xera','kbb']:
  for bse in ['const','era']:
    if a==bse: continue
    ds=[]
    for _ in range(4000):
        i=rng.integers(0,n,n); ds.append(np.average(E[a][i]-E[bse][i],weights=w[i]))
    ds=np.array(ds); print(f"{a}-{bse}: {np.average(E[a]-E[bse],weights=w):+.4f}  95% [{np.quantile(ds,.025):+.4f},{np.quantile(ds,.975):+.4f}]")
print("-- cluster bootstrap by pitcher")
pids=te.pid.values; up=np.unique(pids); idx={p:np.where(pids==p)[0] for p in up}
print("pitchers",len(up))
for a,bse in [('era','const'),('fip','era'),('xera','era'),('kbb','era'),('kbb','xera')]:
    ds=[]
    for _ in range(4000):
        i=np.concatenate([idx[p] for p in rng.choice(up,len(up))]); ds.append(np.average(E[a][i]-E[bse][i],weights=w[i]))
    print(f"{a}-{bse}: {np.average(E[a]-E[bse],weights=w):+.4f}  95% [{np.quantile(ds,.025):+.4f},{np.quantile(ds,.975):+.4f}]")
