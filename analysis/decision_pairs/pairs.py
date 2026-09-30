"""Two pitchers, pick one: which of last season's numbers picks the one
with the lower ERA next season?

Every pair of pitchers in the same role (starter / reliever) and the same
season t is a decision. A rule picks one of the two from season-t numbers;
the pick is right when that pitcher's season t+1 ERA is lower. See PREREG.md
for what is frozen and how the result is read.

    python pairs.py --revision <HF sha> --mode dev      # t = 2015..2024
    python pairs.py --revision <HF sha> --mode primary  # t = 2025 (2026 outcome)
"""
import argparse
import json
import sys
import urllib.parse
import urllib.request

import duckdb
import numpy as np
import pandas as pd

REPO = "yasumorishima/mlb-stats"
MIN_BF = 100
SENS_MIN_BF_NEXT = 50  # reported sensitivity: a lower bar for being scored in t+1
# `is_partial` in the tables is a calendar flag (season >= the fetch year), so
# it cannot say that 2026 is over. The test instead needs a statsapi fetch
# made after the last regular-season game (2026-09-27) and a 2026 schedule
# with no regular-season game left to play.
COMPLETE_AFTER = "2026-09-29T00:00:00"
BANDS = [(100, 300), (300, 600), (600, 10**6)]
DEV_SEASONS = [t for t in range(2015, 2025) if t not in (2019, 2020)]  # t or t+1 = 2020 dropped
PRIMARY_SEASON = 2025
N_BOOT = 2000
SEED = 0
# The development rows and the blend are fixed to this revision in both modes;
# only the t = 2025 rows and their 2026 outcome come from --revision.
DEV_REVISION = "07a4d4f991b8553443173d8e119e12740a22ae3a"
FROZEN_BETA = [4.105933962076798, -0.04720754632831724, 0.025367990352207232,
               0.22199917638064393, -4.826348234202022]
# lower score = pick; K-BB% is sign-flipped so that lower is better everywhere
RULES = {
    "era": lambda d: d["era"],
    "fip": lambda d: d["fip"],
    "xera": lambda d: d["xera"],
    "kbb": lambda d: -d["k_minus_bb_rate"],
}
FEATURES = ["era", "fip", "xera", "k_minus_bb_rate"]


def load(revision):
    url = f"https://huggingface.co/datasets/{REPO}/resolve/{revision}/marts/mart_pitcher_season.parquet"
    df = duckdb.sql(f"select * from '{url}'").df()
    return df


def get_json(url, data=None):
    req = urllib.request.Request(url, data=data)
    with urllib.request.urlopen(req, timeout=60) as r:
        return json.load(r)


def check_complete(revision):
    """Stop unless this mart revision was built from a fetch made after 2026 ended."""
    man = get_json(f"https://huggingface.co/datasets/{REPO}/resolve/{revision}/marts/_manifest.json")
    inp = man["input_dataset_revision"]
    info = get_json(f"https://huggingface.co/api/datasets/{REPO}/paths-info/{inp}",
                    urllib.parse.urlencode({"paths": "statsapi_pitching.parquet", "expand": "true"}).encode())
    fetched = info[0]["lastCommit"]["date"]
    sched = get_json("https://statsapi.mlb.com/api/v1/schedule?sportId=1&season=2026&gameType=R")
    open_games = [g["gamePk"] for d in sched["dates"] for g in d["games"]
                  if g["status"]["abstractGameState"] != "Final"]
    print(f"input {inp} statsapi_pitching fetched {fetched}; unplayed 2026 games {len(open_games)}")
    if fetched[:19] < COMPLETE_AFTER or open_games:
        sys.exit("2026 is not complete in this revision: refresh first")
    return {"input_dataset_revision": inp, "statsapi_pitching_commit_date": fetched}


def build_rows(df, seasons, nxt_df=None, min_bf_next=MIN_BF):
    """Season-t rows from df; their t+1 ERA and BF from nxt_df (df when None)."""
    nxt_df = df if nxt_df is None else nxt_df
    cur = df[df["season"].isin(seasons)]
    nxt = nxt_df[nxt_df["season"].isin([t + 1 for t in seasons])][["player_id", "season", "era", "bf", "is_partial"]]
    nxt = nxt.assign(season=nxt["season"] - 1).rename(
        columns={"era": "era_next", "bf": "bf_next", "is_partial": "partial_next"})
    d = cur.merge(nxt, on=["player_id", "season"], how="inner")
    d = d[(d["bf"] >= MIN_BF) & (d["bf_next"] >= min_bf_next)]
    d = d.dropna(subset=FEATURES + ["era_next"])
    d = d.assign(role=np.where(d["games_started"] / d["games"] >= 0.5, "SP", "RP"))
    return d.reset_index(drop=True)


def fit_blend(dev):
    X = np.column_stack([np.ones(len(dev))] + [dev[c].to_numpy(float) for c in FEATURES])
    y = dev["era_next"].to_numpy(float)
    beta, *_ = np.linalg.lstsq(X, y, rcond=None)
    return beta


def blend_score(d, beta):
    X = np.column_stack([np.ones(len(d))] + [d[c].to_numpy(float) for c in FEATURES])
    return X @ beta


def cells(d, scores):
    """Per (season, role) cell: the pair matrices every statistic is built from."""
    out = []
    for (_, _), g in d.groupby(["season", "role"]):
        idx = g.index.to_numpy()
        y = g["era_next"].to_numpy(float)
        bf = g["bf"].to_numpy(float)
        dy = y[:, None] - y[None, :]                  # >0: i was worse next year
        valid = (dy != 0)
        np.fill_diagonal(valid, False)
        minbf = np.minimum(bf[:, None], bf[None, :])
        rule = {}
        for name, s in scores.items():
            s = s[idx]
            ds = s[:, None] - s[None, :]              # >0: the rule picks j
            # right when the rule picks the one with the lower next-year ERA;
            # a tie in the rule counts one half
            right = np.where(ds == 0, 0.5, ((ds > 0) == (dy > 0)).astype(float))
            gain = np.where(ds == 0, 0.0, np.sign(ds) * dy)   # ERA of the one left minus the one picked
            rule[name] = (right, gain, np.sign(ds))
        out.append(dict(pid=g["player_id"].to_numpy(), valid=valid, minbf=minbf, rule=rule))
    return out


def stats(cs, weights_of, mask_of=None):
    """Accuracy and mean ERA gain for every rule over the valid pairs of all cells."""
    num = {k: 0.0 for k in cs[0]["rule"]}
    gain = {k: 0.0 for k in cs[0]["rule"]}
    den = 0.0
    for c in cs:
        m = weights_of(c)
        W = m[:, None] * m[None, :] * c["valid"]
        if mask_of is not None:
            W = W * mask_of(c)
        den += W.sum()
        for k, (right, g, _) in c["rule"].items():
            num[k] += (W * right).sum()
            gain[k] += (W * g).sum()
    return {k: (num[k] / den, gain[k] / den) for k in num}, den / 2


def boot(cs, pids, mask_of=None):
    rng = np.random.default_rng(SEED)
    upid = np.unique(pids)
    draws = []
    for _ in range(N_BOOT):
        cnt = dict(zip(upid, rng.multinomial(len(upid), np.full(len(upid), 1 / len(upid)))))
        res, _ = stats(cs, lambda c: np.array([cnt[p] for p in c["pid"]], float), mask_of)
        draws.append(res)
    return draws


def summarise(cs, pids, mask_of=None):
    point, npairs = stats(cs, lambda c: np.ones(len(c["pid"])), mask_of)
    draws = boot(cs, pids, mask_of)
    out = {"pairs": int(npairs)}
    for k, (acc, g) in point.items():
        out[k] = {"acc": acc, "era_gain": g}
    for k in point:
        if k == "era":
            continue
        diff = np.array([d[k][0] - d["era"][0] for d in draws])
        lo, hi = np.percentile(diff, [2.5, 97.5])
        out[k]["acc_minus_era"] = point[k][0] - point["era"][0]
        out[k]["ci95"] = [lo, hi]
        out[k]["reading"] = "better" if lo > 0 else ("worse" if hi < 0 else "indistinguishable")
    return out


def run(df, seasons, beta, nxt_df=None, min_bf_next=MIN_BF):
    d = build_rows(df, seasons, nxt_df, min_bf_next)
    scores = {k: f(d).to_numpy(float) for k, f in RULES.items()}
    scores["blend"] = blend_score(d, beta)
    cs = cells(d, scores)
    res = {"pitcher_seasons": len(d), "all": summarise(cs, d["player_id"].to_numpy())}
    for lo, hi in BANDS:
        res[f"band_{lo}_{hi}"] = summarise(
            cs, d["player_id"].to_numpy(), lambda c, lo=lo, hi=hi: (c["minbf"] >= lo) & (c["minbf"] < hi))
    # pairs where ERA and the other rule disagree: the only pairs where the choice of rule matters
    for k in ["fip", "xera", "kbb", "blend"]:
        res[f"disagree_era_{k}"] = summarise(
            cs, d["player_id"].to_numpy(),
            lambda c, k=k: c["rule"]["era"][2] * c["rule"][k][2] < 0)
    return d, res


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--revision", required=True)
    ap.add_argument("--mode", choices=["dev", "primary"], required=True)
    ap.add_argument("--out", required=True)
    a = ap.parse_args()
    dev_df = load(DEV_REVISION)
    dev = build_rows(dev_df, DEV_SEASONS)
    assert not dev["partial_next"].any() and not dev["is_partial"].any()
    beta = fit_blend(dev)
    assert np.max(np.abs(beta - np.array(FROZEN_BETA))) < 1e-9, beta
    if a.mode == "dev":
        d, res = run(dev_df, DEV_SEASONS, beta)
        sens_seasons, sens_nxt = DEV_SEASONS, None
    else:
        complete = check_complete(a.revision)
        # season-t numbers come from the frozen revision; only the 2026 outcome is new
        nxt_df = load(a.revision)
        d, res = run(dev_df, [PRIMARY_SEASON], beta, nxt_df)
        res["complete"] = complete
        sens_seasons, sens_nxt = [PRIMARY_SEASON], nxt_df
    _, sens = run(dev_df, sens_seasons, beta, sens_nxt, SENS_MIN_BF_NEXT)
    res[f"sens_min_bf_next_{SENS_MIN_BF_NEXT}"] = sens["all"]
    res["blend_beta"] = dict(zip(["const"] + FEATURES, beta.tolist()))
    res["revision"] = a.revision
    res["mode"] = a.mode
    json.dump(res, open(a.out, "w"), indent=1)
    print(json.dumps({k: res[k] for k in ["pitcher_seasons", "blend_beta"]}, indent=1))
    for key in ["all"] + [f"band_{lo}_{hi}" for lo, hi in BANDS]:
        r = res[key]
        line = f"{key:16s} pairs {r['pairs']:>8d}  era {r['era']['acc']:.4f}"
        for k in ["fip", "xera", "kbb", "blend"]:
            line += f"  {k} {r[k]['acc']:.4f} ({r[k]['ci95'][0]:+.4f},{r[k]['ci95'][1]:+.4f})"
        print(line)


if __name__ == "__main__":
    main()
