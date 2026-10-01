"""Two hitters, pick one: which of last season's numbers picks the one
with the higher wOBA next season?

The hitter version of ../decision_pairs. Every pair of hitters with at
least 100 plate appearances in season t and in t+1 is a decision. A rule
picks one of the two from season-t numbers; the pick is right when that
hitter's season t+1 wOBA is higher. See PREREG.md.

    python pairs.py --mode dev      # t = 2015..2024
    python pairs.py --mode primary  # t = 2025 (2026 outcome)
"""
import argparse
import json
import sys
import urllib.parse
import urllib.request

import duckdb
import numpy as np

REPO = "yasumorishima/mlb-stats"
# One revision for both modes: it already holds the finished 2026 season
# (built 2026-10-01 from a fetch made after the last game). The development
# run reads t <= 2024 only; the test reads t = 2025 and its 2026 outcome.
REVISION = "e006cbe12da75aac0cb96892ac8b43b9a27e5261"
MIN_PA = 100
SENS_MIN_PA_NEXT = 50
BANDS = [(100, 300), (300, 500), (500, 10**6)]
DEV_SEASONS = [t for t in range(2015, 2025) if t not in (2019, 2020)]
PRIMARY_SEASON = 2025
N_BOOT = 2000
SEED = 0
FEATURES = ["woba", "xwoba", "k_rate", "bb_rate", "iso"]
# Every score is "lower = pick", so the higher-is-better stats are negated.
RULES = {
    "woba": lambda d: -d["woba"],
    "xwoba": lambda d: -d["xwoba"],
    "wrc_plus": lambda d: -d["wrc_plus"].astype(float),
}
FROZEN_BETA = [0.17855467794058655, -0.0013454231231165847, 0.40142434492570034, -0.04748106827297323, 0.056232063955009196, 0.07990656739580318]  # const, woba, xwoba, k_rate, bb_rate, iso (development fit)


def load():
    url = f"https://huggingface.co/datasets/{REPO}/resolve/{REVISION}/marts/mart_batter_season.parquet"
    return duckdb.sql(f"select * from '{url}'").df()


def get_json(url):
    with urllib.request.urlopen(url, timeout=60) as r:
        return json.load(r)


def check_complete():
    man = get_json(f"https://huggingface.co/datasets/{REPO}/resolve/{REVISION}/marts/_manifest.json")
    inp = man["input_dataset_revision"]
    req = urllib.request.Request(
        f"https://huggingface.co/api/datasets/{REPO}/paths-info/{inp}",
        data=urllib.parse.urlencode({"paths": "statsapi_batting.parquet", "expand": "true"}).encode())
    with urllib.request.urlopen(req, timeout=60) as r:
        fetched = json.load(r)[0]["lastCommit"]["date"]
    sched = get_json("https://statsapi.mlb.com/api/v1/schedule?sportId=1&season=2026&gameType=R")
    open_games = [g["gamePk"] for day in sched["dates"] for g in day["games"]
                  if g["status"]["abstractGameState"] != "Final"]
    print(f"input {inp} statsapi_batting fetched {fetched}; unplayed 2026 games {len(open_games)}")
    if fetched[:19] < "2026-09-29T00:00:00" or open_games:
        sys.exit("2026 is not complete in this revision")
    return {"input_dataset_revision": inp, "statsapi_batting_commit_date": fetched}


def build_rows(df, seasons, min_pa_next=MIN_PA):
    cur = df[df["season"].isin(seasons)]
    nxt = df[df["season"].isin([t + 1 for t in seasons])][["player_id", "season", "woba", "pa", "is_partial"]]
    nxt = nxt.assign(season=nxt["season"] - 1).rename(
        columns={"woba": "woba_next", "pa": "pa_next", "is_partial": "partial_next"})
    d = cur.merge(nxt, on=["player_id", "season"], how="inner")
    d = d[(d["pa"] >= MIN_PA) & (d["pa_next"] >= min_pa_next)]
    d = d.dropna(subset=FEATURES + ["wrc_plus", "woba_next"])
    return d.reset_index(drop=True)


def design(d):
    return np.column_stack([np.ones(len(d))] + [d[c].to_numpy(float) for c in FEATURES])


def fit_blend(dev):
    beta, *_ = np.linalg.lstsq(design(dev), dev["woba_next"].to_numpy(float), rcond=None)
    return beta


def cells(d, scores):
    out = []
    for _, g in d.groupby("season"):
        idx = g.index.to_numpy()
        y = -g["woba_next"].to_numpy(float)          # lower = better, as the scores
        pa = g["pa"].to_numpy(float)
        dy = y[:, None] - y[None, :]                  # >0: i did worse next year
        valid = dy != 0
        np.fill_diagonal(valid, False)
        minpa = np.minimum(pa[:, None], pa[None, :])
        rule = {}
        for name, s in scores.items():
            s = s[idx]
            ds = s[:, None] - s[None, :]              # >0: the rule picks j
            right = np.where(ds == 0, 0.5, ((ds > 0) == (dy > 0)).astype(float))
            # wOBA of the one picked minus the one left (a tie adds 0)
            gain = np.where(ds == 0, 0.0, np.sign(ds) * dy)
            rule[name] = (right, gain, np.sign(ds))
        out.append(dict(pid=g["player_id"].to_numpy(), valid=valid, minpa=minpa, rule=rule))
    return out


def stats(cs, weights_of, mask_of=None):
    names = list(cs[0]["rule"])
    num = dict.fromkeys(names, 0.0)
    gain = dict.fromkeys(names, 0.0)
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
    return {k: (num[k] / den, gain[k] / den) for k in names}, den / 2


def summarise(cs, pids, mask_of=None):
    point, npairs = stats(cs, lambda c: np.ones(len(c["pid"])), mask_of)
    rng = np.random.default_rng(SEED)
    upid = np.unique(pids)
    draws = []
    for _ in range(N_BOOT):
        cnt = dict(zip(upid, rng.multinomial(len(upid), np.full(len(upid), 1 / len(upid)))))
        res, _ = stats(cs, lambda c: np.array([cnt[p] for p in c["pid"]], float), mask_of)
        draws.append(res)
    out = {"pairs": int(npairs)}
    for k, (acc, g) in point.items():
        out[k] = {"acc": acc, "woba_gain": g}
    for k in point:
        if k == "woba":
            continue
        diff = np.array([d[k][0] - d["woba"][0] for d in draws])
        lo, hi = np.percentile(diff, [2.5, 97.5])
        out[k]["acc_minus_woba"] = point[k][0] - point["woba"][0]
        out[k]["ci95"] = [lo, hi]
        out[k]["reading"] = "better" if lo > 0 else ("worse" if hi < 0 else "indistinguishable")
    return out


def run(df, seasons, beta, min_pa_next=MIN_PA, full=True):
    d = build_rows(df, seasons, min_pa_next)
    scores = {k: f(d).to_numpy(float) for k, f in RULES.items()}
    scores["blend"] = -(design(d) @ beta)
    cs = cells(d, scores)
    pids = d["player_id"].to_numpy()
    res = {"hitter_seasons": len(d), "all": summarise(cs, pids)}
    if not full:
        return d, res
    for lo, hi in BANDS:
        res[f"band_{lo}_{hi}"] = summarise(
            cs, pids, lambda c, lo=lo, hi=hi: (c["minpa"] >= lo) & (c["minpa"] < hi))
    for k in ["xwoba", "wrc_plus", "blend"]:
        res[f"disagree_woba_{k}"] = summarise(
            cs, pids, lambda c, k=k: c["rule"]["woba"][2] * c["rule"][k][2] < 0)
    return d, res


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=["dev", "primary"], required=True)
    ap.add_argument("--out", required=True)
    a = ap.parse_args()
    df = load()
    dev = build_rows(df, DEV_SEASONS)
    assert not dev["partial_next"].any() and not dev["is_partial"].any()
    beta = fit_blend(dev)
    if FROZEN_BETA is not None:
        assert np.max(np.abs(beta - np.array(FROZEN_BETA))) < 1e-9, beta
    elif a.mode == "primary":
        sys.exit("the blend is not frozen yet")
    seasons = DEV_SEASONS if a.mode == "dev" else [PRIMARY_SEASON]
    res = {}
    if a.mode == "primary":
        res["complete"] = check_complete()
    d, main_res = run(df, seasons, beta)
    if a.mode == "primary" and d["partial_next"].any():
        sys.exit("2026 rows are still marked partial")
    res.update(main_res)
    _, sens = run(df, seasons, beta, SENS_MIN_PA_NEXT, full=False)
    res[f"sens_min_pa_next_{SENS_MIN_PA_NEXT}"] = sens["all"]
    res["blend_beta"] = dict(zip(["const"] + FEATURES, beta.tolist()))
    res["revision"] = REVISION
    res["mode"] = a.mode
    json.dump(res, open(a.out, "w"), indent=1)
    print(json.dumps({"hitter_seasons": res["hitter_seasons"], "blend_beta": res["blend_beta"]}, indent=1))
    for key in ["all"] + [f"band_{lo}_{hi}" for lo, hi in BANDS]:
        r = res[key]
        line = f"{key:14s} pairs {r['pairs']:>8d}  woba {r['woba']['acc']:.4f}"
        for k in ["xwoba", "wrc_plus", "blend"]:
            line += f"  {k} {r[k]['acc']:.4f} ({r[k]['ci95'][0]:+.4f},{r[k]['ci95'][1]:+.4f})"
        print(line)


if __name__ == "__main__":
    main()
