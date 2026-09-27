"""The fetch scripts must act on validate_dataframe's verdict.

validate_dataframe returns a bool and prints its report. Called bare, a
table that lost seasons or columns is written anyway and the script exits 0,
so the weekly job publishes it over the good copy. These tests drive each
script's main() with stubbed fetches and check that:

  * a table that fails validation is not written, the tables that passed
    are, and the script exits non-zero;
  * the per-leaderboard first seasons are right (Savant has pitch arsenal
    stats from 2017 and bat tracking from 2024; asking for 2015 would fail
    every run);
  * every fetch step in the workflow runs under !cancelled(), so one failed
    fetch does not skip the fetches after it.

Runs under pytest and standalone (`python tests/test_fetch_validation.py`);
the standalone runner counts SystemExit as a failure, like
tests/test_check_outputs.py.
"""

from __future__ import annotations

import os
import re
import shutil
import sys
import tempfile
import types
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))

_TMP = Path(tempfile.mkdtemp(prefix="fetch_validation_"))
os.environ["MLB_DATA_TARGET"] = "parquet"
os.environ["MLB_PARQUET_ROOT"] = str(_TMP / "pq")

# The scripts import pybaseball at module level. The fetches are stubbed
# below, so a stand-in is enough where the package is not installed.
if "pybaseball" not in sys.modules:
    try:
        import pybaseball  # noqa: F401
    except ImportError:
        _pb = types.ModuleType("pybaseball")
        _pb.cache = types.SimpleNamespace(enable=lambda: None)
        sys.modules["pybaseball"] = _pb

import config  # noqa: E402
import fetch_fangraphs as fg  # noqa: E402
import fetch_fielding_running as fr  # noqa: E402
import fetch_savant_leaderboards as sv  # noqa: E402

DATA = _TMP / "data"
WORKFLOW = ROOT / ".github" / "workflows" / "weekly_refresh.yml"


def _frame(y0: int, y1: int, extra: dict | None = None, per: int = 12) -> pd.DataFrame:
    rows, pid = [], 0
    for y in range(y0, y1 + 1):
        for _ in range(per):
            pid += 1
            rows.append({"player_id": pid, "season": y, **(extra or {})})
    return pd.DataFrame(rows)


def _reset() -> None:
    for d in (_TMP / "pq", DATA):
        shutil.rmtree(d, ignore_errors=True)
    DATA.mkdir(parents=True)
    for mod in (config, sv, fr, fg):
        if hasattr(mod, "DATA_DIR"):
            mod.DATA_DIR = DATA


def _written() -> list[str]:
    root = _TMP / "pq"
    # Tables only: the failed-validation markers live in a dot directory.
    return (sorted(p.name for p in root.iterdir() if not p.name.startswith("."))
            if root.exists() else [])


def _run(mod, argv: list[str]):
    old = sys.argv
    sys.argv = ["x"] + argv
    try:
        mod.main()
        return 0
    except SystemExit as e:
        return e.code
    finally:
        sys.argv = old


# ---------------------------------------------------------------- Savant
_SV_FETCHES = {
    "fetch_batter_exitvelo": "sc_batter_exitvelo",
    "fetch_batter_expected": "sc_batter_expected",
    "fetch_pitcher_exitvelo": "sc_pitcher_exitvelo",
    "fetch_pitcher_expected": "sc_pitcher_expected",
    "fetch_pitcher_arsenal": "sc_pitcher_arsenal",
    "fetch_bat_tracking": "sc_bat_tracking",
    "fetch_batted_ball": "sc_batted_ball",
}
# What Savant actually returns: arsenal from 2017, bat tracking from 2024.
_SV_REAL_FIRST = {"sc_pitcher_arsenal": 2017, "sc_bat_tracking": 2024}


def _run_savant(first: dict[str, int]):
    _reset()
    csv_of = {table: csv for csv, table in sv.TABLE_MAP.items()}
    saved = {fn: getattr(sv, fn) for fn in _SV_FETCHES}

    def make(table):
        def f(start, end):
            df = _frame(max(first.get(table, start), start), end)
            df.to_csv(DATA / csv_of[table], index=False)
            return df
        return f

    try:
        for fn, table in _SV_FETCHES.items():
            setattr(sv, fn, make(table))
        return _run(sv, ["--start-year", "2015", "--end-year", "2026"])
    finally:
        for fn, f in saved.items():
            setattr(sv, fn, f)


def test_savant_real_first_seasons_pass():
    rc = _run_savant(dict(_SV_REAL_FIRST))
    assert rc in (0, None), rc
    assert len(_written()) == len(_SV_FETCHES), _written()


def test_savant_failed_table_is_not_written_others_are():
    rc = _run_savant({**_SV_REAL_FIRST, "sc_batter_expected": 2018})
    w = _written()
    assert rc == 1, rc
    assert "sc_batter_expected" not in w, w
    assert len(w) == len(_SV_FETCHES) - 1, w


def test_savant_arsenal_losing_its_own_seasons_still_fails():
    rc = _run_savant({**_SV_REAL_FIRST, "sc_pitcher_arsenal": 2019})
    assert rc == 1, rc
    assert "sc_pitcher_arsenal" not in _written()


# ---------------------------------------------------------------- Fielding
def _run_fielding(first: dict[str, int]):
    _reset()
    saved = (fr.fetch_sprint_speed, fr.fetch_oaa, fr.fetch_catcher)

    def sprint(start, end):
        _frame(max(first.get("sprint_speed", start), start), end,
               {"sprint_speed": 27.0}).to_csv(DATA / "sprint_speed.csv", index=False)

    def oaa(start, end):
        _frame(max(first.get("oaa", 2016), start), end).to_csv(DATA / "oaa.csv", index=False)
        (_frame(max(2016, start), end, {"team_name": "X", "total_oaa": 1})
         .drop(columns="player_id").to_csv(DATA / "oaa_team.csv", index=False))

    def catcher(start, end):
        _frame(max(first.get("catcher", start), start), end).to_csv(DATA / "catcher.csv", index=False)

    try:
        fr.fetch_sprint_speed, fr.fetch_oaa, fr.fetch_catcher = sprint, oaa, catcher
        return _run(fr, ["--start-year", "2015", "--end-year", "2026"])
    finally:
        fr.fetch_sprint_speed, fr.fetch_oaa, fr.fetch_catcher = saved


def test_fielding_all_valid():
    rc = _run_fielding({})
    assert rc in (0, None), rc
    assert _written() == ["catcher", "oaa", "oaa_team", "sprint_speed"], _written()


def test_fielding_failed_table_is_not_written_others_are():
    rc = _run_fielding({"catcher": 2020})
    assert rc == 1, rc
    assert _written() == ["oaa", "oaa_team", "sprint_speed"], _written()


# ---------------------------------------------------------------- FanGraphs
def _run_fangraphs(bat_first: int | None):
    _reset()
    saved = (fg.fetch_batting, fg.fetch_pitching, fg.fetch_pitcher_plus)
    try:
        fg.fetch_batting = lambda s, e: _frame(bat_first or s, e, {"wOBA": .3, "OPS": .7, "WAR": 1})
        fg.fetch_pitching = lambda s, e: _frame(s, e, {"ERA": 4, "FIP": 4, "WAR": 1})
        fg.fetch_pitcher_plus = lambda s, e: _frame(s, e, {"Stuff+": 100, "Location+": 100})
        return _run(fg, ["--start-year", "2015", "--end-year", "2025"])
    finally:
        fg.fetch_batting, fg.fetch_pitching, fg.fetch_pitcher_plus = saved


def test_fangraphs_all_valid():
    rc = _run_fangraphs(None)
    assert rc in (0, None), rc
    assert len(_written()) == 3, _written()


def test_fangraphs_failed_table_is_not_written_others_are():
    rc = _run_fangraphs(2019)
    w = _written()
    assert rc == 1, rc
    assert "fg_batting" not in w and len(w) == 2, w


# ---------------------------------------------------------------- Workflow
def test_every_fetch_step_runs_unless_cancelled():
    text = WORKFLOW.read_text(encoding="utf-8")
    steps = re.findall(r"- name: (Fetch [^\n]+)\n((?:        [^\n]*\n)+)", text)
    assert len(steps) >= 6, [s[0] for s in steps]
    for name, body in steps:
        m = re.search(r"^        if: (.+)$", body, re.M)
        assert m, f"{name}: no if:"
        assert "!cancelled()" in m.group(1), f"{name}: {m.group(1)}"



# ---------------------------------------------------------------- Markers + audit
def _marked() -> list[str]:
    d = _TMP / "pq" / config.FAILED_VALIDATION_DIRNAME
    return sorted(p.name for p in d.iterdir()) if d.exists() else []


def test_failed_tables_leave_a_marker_and_passing_ones_do_not():
    _run_fangraphs(2019)
    assert _marked() == ["fg_batting"], _marked()
    _run_fielding({"catcher": 2020})
    assert _marked() == ["catcher"], _marked()
    _run_savant({**_SV_REAL_FIRST, "sc_batter_expected": 2018})
    assert _marked() == ["sc_batter_expected"], _marked()
    _run_savant(dict(_SV_REAL_FIRST))
    assert _marked() == [], _marked()


def _audit(root: Path, steps: str):
    import check_outputs as C
    original = C.PARQUET_ROOT
    argv = sys.argv
    ok_list = root.parent / "ok_tables.txt"
    C.PARQUET_ROOT = root
    sys.argv = ["check_outputs.py", "--steps", steps, "--ok-list", str(ok_list)]
    try:
        rc = C.main()
    finally:
        sys.argv = argv
        C.PARQUET_ROOT = original
    return rc, ok_list.read_text(encoding="utf-8").split()


def test_audit_does_not_excuse_an_unreachable_table_that_failed_validation():
    # FanGraphs tables are declared unreachable: "no file" is excused.
    root = _TMP / "audit" / "pq"
    shutil.rmtree(root.parent, ignore_errors=True)
    root.mkdir(parents=True)
    rc, ok = _audit(root, "fangraphs")
    assert rc == 0 and ok == [], (rc, ok)
    # The same "no file", but because fetch_fangraphs refused it.
    (root / config.FAILED_VALIDATION_DIRNAME).mkdir()
    (root / config.FAILED_VALIDATION_DIRNAME / "fg_batting").write_text("x")
    rc, ok = _audit(root, "fangraphs")
    assert rc == 1 and ok == [], (rc, ok)


def test_fielding_only_flag_does_not_write_a_stale_csv():
    _reset()
    # Left over from an earlier local run; this run never validates it.
    _frame(2015, 2016, {"sprint_speed": 27.0}).to_csv(DATA / "sprint_speed.csv", index=False)
    saved = fr.fetch_oaa

    def oaa(start, end):
        _frame(max(2016, start), end).to_csv(DATA / "oaa.csv", index=False)
        (_frame(max(2016, start), end, {"team_name": "X", "total_oaa": 1})
         .drop(columns="player_id").to_csv(DATA / "oaa_team.csv", index=False))

    try:
        fr.fetch_oaa = oaa
        rc = _run(fr, ["--start-year", "2015", "--end-year", "2026", "--oaa-only"])
    finally:
        fr.fetch_oaa = saved
    assert rc in (0, None), rc
    assert _written() == ["oaa", "oaa_team"], _written()



def test_a_good_write_clears_a_stale_marker():
    # A persistent root (run_backfill) keeps markers between runs.
    _reset()
    config.mark_failed_validation("oaa")
    config.mark_failed_validation("catcher")
    config.write_dataframe(_frame(2016, 2017), "oaa")
    assert _marked() == ["catcher"], _marked()



def test_marker_survives_a_write_that_raises():
    # FanGraphs is continue-on-error and declared unreachable: if a write
    # raised before the marker was left, the refused table would be excused.
    saved = fg.write_dataframe

    def boom(df, table, *a, **k):
        raise RuntimeError("disk full")

    try:
        fg.write_dataframe = boom
        try:
            _run_fangraphs(2019)
        except RuntimeError:
            pass
    finally:
        fg.write_dataframe = saved
    assert _marked() == ["fg_batting"], _marked()


if __name__ == "__main__":
    tests = [(n, f) for n, f in sorted(globals().items())
             if n.startswith("test_") and callable(f)]
    failed = 0
    for name, fn in tests:
        try:
            fn()
            print(f"PASS {name}")
        except BaseException as e:  # SystemExit included on purpose
            failed += 1
            print(f"FAIL {name}: {type(e).__name__}: {e}")
    shutil.rmtree(_TMP, ignore_errors=True)
    print(f"\n{len(tests) - failed}/{len(tests)} passed")
    sys.exit(1 if failed else 0)
