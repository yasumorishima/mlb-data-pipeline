"""Extra numbers for the write-up of decision_pairs and decision_pairs_batters.

Not part of either pre-registration: everything here was computed after the
2026 results were read, and the article says so. It reuses the frozen code of
both analyses (same pairs, same revisions) and adds
  - a reproduction of the 2026 point estimates (must match primary.json),
  - the same-season reference: next season's FIP / xERA / K-BB% (pitchers) or
    xwOBA (hitters) used to pick next season's ERA / wOBA,
  - correlations of each season-t number with the t+1 outcome,
  - concrete pairs where the two rules disagree strongly.

    python article_dig.py   # writes article_dig.json and article_hitters_2025.json
"""
import json
import sys
from pathlib import Path

import duckdb
import numpy as np

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import pairs as P  # noqa: E402
del sys.modules["pairs"]
sys.path[0] = str(HERE.parent / "decision_pairs_batters")
import pairs as B  # noqa: E402

TEST_REVISION = "e006cbe12da75aac0cb96892ac8b43b9a27e5261"  # the primary run of both


def point(mod, cs):
    res, n = mod.stats(cs, lambda c: np.ones(len(c["pid"])))
    return {k: v[0] for k, v in res.items()}, int(n)


def names(table):
    url = f"https://huggingface.co/datasets/{P.REPO}/resolve/{TEST_REVISION}/marts/{table}.parquet"
    df = duckdb.sql(f"select player_id, any_value(player_name) as name from '{url}' group by 1").df()
    return df.set_index("player_id")["name"].to_dict()


def pair_row(d, ida, idb, cols):
    a = d[d.player_id == ida].iloc[0]
    b = d[d.player_id == idb].iloc[0]
    return {c: [float(a[c]), float(b[c])] for c in cols}


out = {}

# ---------------- pitchers (the primary run: 2025 numbers from DEV_REVISION, 2026 from the test revision)
dev_df = P.load(P.DEV_REVISION)
test_df = P.load(TEST_REVISION)
d = P.build_rows(dev_df, [P.PRIMARY_SEASON], test_df)
sc = {k: f(d).to_numpy(float) for k, f in P.RULES.items()}
sc["blend"] = P.blend_score(d, np.array(P.FROZEN_BETA))
rep, n = point(P, P.cells(d, sc))
prim = json.loads((HERE / "primary.json").read_text())["all"]
for k, v in rep.items():
    assert abs(v - prim[k]["acc"]) < 1e-12, (k, v, prim[k]["acc"])
assert n == prim["pairs"]
nx = test_df[test_df.season == P.PRIMARY_SEASON + 1][["player_id", "era", "fip", "xera", "k_minus_bb_rate"]]
d2 = d.merge(nx.add_suffix("_26").rename(columns={"player_id_26": "player_id"}), on="player_id", how="left")
assert len(d2) == len(d) and np.allclose(d2.era_26, d2.era_next)
same, _ = point(P, P.cells(d2, {"fip": d2.fip_26.to_numpy(float), "xera": d2.xera_26.to_numpy(float),
                                 "kbb": -d2.k_minus_bb_rate_26.to_numpy(float)}))
out["pitchers"] = {
    "pairs": n, "pitchers": int(len(d)),
    "acc_2026": rep,
    "same_season_2026": same,
    "corr_t_with_next_era": {c: float(np.corrcoef(d[c], d.era_next)[0, 1]) for c in P.FEATURES},
}
# development seasons, same-season reference
dd = P.build_rows(dev_df, P.DEV_SEASONS)
nxt = dev_df[["player_id", "season", "fip", "xera", "k_minus_bb_rate"]].assign(season=lambda x: x.season - 1)
dd2 = dd.merge(nxt.rename(columns={"fip": "fip_n", "xera": "xera_n", "k_minus_bb_rate": "kbb_n"}),
               on=["player_id", "season"], how="left")
assert len(dd2) == len(dd) and dd2[["fip_n", "xera_n", "kbb_n"]].notna().all().all()
dsame, dn = point(P, P.cells(dd2, {"fip": dd2.fip_n.to_numpy(float), "xera": dd2.xera_n.to_numpy(float),
                                    "kbb": -dd2.kbb_n.to_numpy(float)}))
out["pitchers"]["same_season_dev"] = dsame
out["pitchers"]["dev_pairs"] = dn

# strong disagreements: same role, both 500+ BF in 2025, ERA lower by 1.00+ while K-BB% lower by 6+ points
pn = names("mart_pitcher_season")
big = []
for _, g in d.groupby("role"):
    g = g[g.bf >= 500]
    for _, a in g.iterrows():
        for _, b in g.iterrows():
            if a.era < b.era - 1.0 and a.k_minus_bb_rate < b.k_minus_bb_rate - 0.06:
                big.append(b.era_next < a.era_next)
out["pitchers"]["strong_disagree"] = {"pairs": len(big), "kbb_right": float(np.mean(big))}
pcols = ["era", "k_minus_bb_rate", "fip", "xera", "bf", "era_next"]
ids = {pn[i]: i for i in d.player_id}  # names among the pitchers in the decision set
assert len(ids) == len(d)
out["pitchers"]["examples"] = {
    f"{a} / {b}": pair_row(d, ids[a], ids[b], pcols)
    for a, b in [("Brayan Bello", "Dylan Cease"), ("Clay Holmes", "Jack Flaherty")]
}

# ---------------- hitters
bdf = B.load()
db = B.build_rows(bdf, [B.PRIMARY_SEASON] if hasattr(B, "PRIMARY_SEASON") else [2025])
bsc = {k: f(db).to_numpy(float) for k, f in B.RULES.items()}
brep, bn_ = point(B, B.cells(db, bsc))
bprim = json.loads((HERE.parent / "decision_pairs_batters" / "primary.json").read_text())["all"]
for k, v in brep.items():
    assert abs(v - bprim[k]["acc"]) < 1e-12, (k, v, bprim[k]["acc"])
assert bn_ == bprim["pairs"]
nb = bdf[bdf.season == 2026][["player_id", "woba", "xwoba"]].rename(columns={"woba": "woba_26", "xwoba": "xwoba_26"})
db2 = db.merge(nb, on="player_id", how="left")
assert len(db2) == len(db) and np.allclose(db2.woba_26, db2.woba_next)
bsame, _ = point(B, B.cells(db2, {"xwoba": -db2.xwoba_26.to_numpy(float)}))
out["hitters"] = {
    "pairs": bn_, "hitters": int(len(db)),
    "acc_2026": brep,
    "same_season_2026": bsame,
    "corr_t_with_next_woba": {c: float(np.corrcoef(db[c].astype(float), db.woba_next)[0, 1])
                              for c in ["woba", "xwoba", "wrc_plus"]},
}
bnames = names("mart_batter_season")
big = []
g = db[db.pa >= 500]
for _, a in g.iterrows():
    for _, b in g.iterrows():
        if a.woba > b.woba + 0.025 and a.xwoba < b.xwoba - 0.025:
            big.append(b.woba_next > a.woba_next)
out["hitters"]["strong_disagree"] = {"pairs": len(big), "xwoba_right": float(np.mean(big))}
bids = {bnames[i]: i for i in db.player_id}  # names among the hitters in the decision set
dup = db.player_id.map(bnames).value_counts()
dup = set(dup[dup > 1].index)  # two hitters share a name (e.g. Max Muncy); none of the examples may be one
bcols = ["woba", "xwoba", "pa", "woba_next"]
out["hitters"]["examples"] = {
    f"{a} / {b}": pair_row(db, bids[a], bids[b], bcols)
    for a, b in [("Harrison Bader", "Salvador Perez"), ("Jacob Wilson", "Salvador Perez")]
    if not ({a, b} & dup) or sys.exit(f"ambiguous name {a} / {b}")
}
db[["player_id", "woba", "xwoba", "pa", "woba_next"]].to_json(HERE / "article_hitters_2025.json", orient="records", indent=0)

(HERE / "article_dig.json").write_text(json.dumps(out, indent=1, ensure_ascii=False) + "\n")
print(json.dumps(out, indent=1, ensure_ascii=False))
