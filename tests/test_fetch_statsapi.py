"""Tests for scripts/fetch_statsapi.py. No network.

Runs under pytest and standalone (`python tests/test_fetch_statsapi.py`),
because the workflow has no pytest installed. The workflow runs this file
right before the fetch, so a gate that stopped gating cannot publish.

Fixtures copy the shape the API actually returns (measured 2026-09-23):
`season` is a string, `numTeams` sits on the split, `team` and `position`
are nested objects, rates arrive as strings, `inningsPitched` is "5.1".

The standalone runner counts SystemExit as a failure on purpose: SystemExit
inherits from BaseException, so a bare `except Exception` lets it escape the
loop and the runner exits 0 while tests are failing.
"""

from __future__ import annotations

import copy
import datetime as dt
import io
import os
import sys
import tempfile
import urllib.error
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))

os.environ.setdefault("MLB_DATA_TARGET", "parquet")

import config  # noqa: E402
import fetch_statsapi as F  # noqa: E402

WORKFLOW = ROOT / ".github" / "workflows" / "weekly_refresh.yml"
TODAY = dt.date(2026, 9, 23)


# ------------------------------------------------------------------ fixtures


def _split(pid, season, stat, team_id=113, num_teams=1, pos="1B"):
    return {
        "season": str(season),
        "numTeams": num_teams,
        "stat": stat,
        "team": {"id": team_id, "name": "Team"},
        "player": {"id": pid, "fullName": f"Player {pid}"},
        "position": {"abbreviation": pos},
    }


def _payload(splits, total=None):
    return {"copyright": "Copyright MLB Advanced Media, L.P.",
            "stats": [{"totalSplits": len(splits) if total is None else total,
                       "splits": splits}]}


def _hit_season(pid, season, pa=100, **extra):
    stat = {"plateAppearances": pa, "atBats": pa - 10, "hits": 25,
            "homeRuns": 4, "baseOnBalls": 8, "strikeOuts": 20,
            "avg": ".278" if pa else ".---", "obp": ".340"}
    stat.update(extra)
    return _split(pid, season, stat)


def _hit_saber(pid, season, pa=100):
    stat = {"wRaa": 1.2, "wRc": 12.0, "war": 0.4, "rar": 3.0}
    if pa:
        stat.update({"woba": 0.330, "wRcPlus": 104.0})
    return _split(pid, season, stat)


def _pit_season(pid, season, outs=30):
    return _split(pid, season, {
        "inningsPitched": f"{outs // 3}.{outs % 3}", "outs": outs,
        "battersFaced": outs + 12, "strikeOuts": 11, "baseOnBalls": 4,
        "homeRuns": 1, "era": "3.60" if outs else "-.--"}, pos="P")


def _pit_saber(pid, season, outs=30):
    stat = {"war": 0.3, "rar": 2.0}
    if outs:
        stat.update({"fip": 3.9, "xfip": 4.1})
    return _split(pid, season, stat, pos="P")


def _fake_api(seasons, *, pa0=(), mutate=None, n_players=12):
    """A `_get` replacement answering like the API for the given seasons."""
    def get(url):
        q = dict(part.split("=", 1) for part in url.split("?", 1)[1].split("&"))
        season, group, kind = int(q["season"]), q["group"], q["stats"]
        assert season in seasons, url
        # 12, not 3: validate_dataframe rejects a season under 10 rows.
        ids = list(range(1, n_players + 1))
        if group == "hitting":
            make = _hit_season if kind == "season" else _hit_saber
            splits = [make(i, season, pa=0 if i in pa0 else 100) for i in ids]
        else:
            make = _pit_season if kind == "season" else _pit_saber
            splits = [make(i, season, outs=0 if i in pa0 else 30) for i in ids]
        payload = _payload(splits)
        if mutate:
            payload = mutate(copy.deepcopy(payload), season, group, kind)
        return payload
    return get


class _Patched:
    """Swap module attributes for one test and put them back."""

    def __init__(self, **attrs):
        self.attrs = attrs
        self.saved = {}

    def __enter__(self):
        for dotted, value in self.attrs.items():
            mod_name, attr = dotted.split("__", 1)
            mod = {"F": F, "config": config}[mod_name]
            self.saved[dotted] = getattr(mod, attr)
            setattr(mod, attr, value)
        return self

    def __exit__(self, *exc):
        for dotted, value in self.saved.items():
            mod_name, attr = dotted.split("__", 1)
            setattr({"F": F, "config": config}[mod_name], attr, value)


def _expect_fetch_error(fn, *args, contains=""):
    try:
        fn(*args)
    except F.FetchError as e:
        assert contains in str(e), f"wrong reason: {e}"
        return
    raise AssertionError("no FetchError raised")


# ----------------------------------------------------------- splits_to_frame


def test_a_clean_page_becomes_one_row_per_player():
    df = F.splits_to_frame(_payload([_hit_season(1, 2025), _hit_season(2, 2025)]),
                           2025, ["plateAppearances"], "t")
    assert list(df["player_id"]) == [1, 2]
    assert set(df["season"]) == {2025}
    assert list(df["last_team_id"]) == [113, 113]
    assert "team" not in df.columns, "a `team` column reads as per-club stats"


def test_a_truncated_page_fails():
    p = _payload([_hit_season(1, 2025)], total=2)
    _expect_fetch_error(F.splits_to_frame, p, 2025, [], "t", contains="truncated")


def test_a_page_without_totalsplits_fails():
    p = _payload([_hit_season(1, 2025)])
    del p["stats"][0]["totalSplits"]
    _expect_fetch_error(F.splits_to_frame, p, 2025, [], "t", contains="truncated")


def test_an_empty_page_fails():
    _expect_fetch_error(F.splits_to_frame, _payload([]), 2025, [], "t",
                        contains="no splits")


def test_an_answer_for_another_season_fails():
    p = _payload([_hit_season(1, 2025), _hit_season(2, 2024)])
    _expect_fetch_error(F.splits_to_frame, p, 2025, [], "t", contains="season")


def test_a_missing_required_stat_fails():
    s = _hit_season(1, 2025)
    del s["stat"]["homeRuns"]
    _expect_fetch_error(F.splits_to_frame, _payload([s]), 2025, ["homeRuns"], "t",
                        contains="homeRuns")


def test_a_repeated_player_fails():
    p = _payload([_hit_season(1, 2025), _hit_season(1, 2025)])
    _expect_fetch_error(F.splits_to_frame, p, 2025, [], "t", contains="duplicate")


def test_a_stats_block_count_other_than_one_fails():
    p = _payload([_hit_season(1, 2025)])
    p["stats"].append(p["stats"][0])
    _expect_fetch_error(F.splits_to_frame, p, 2025, [], "t", contains="stats block")


# -------------------------------------------------------- merge_season_saber


def _frames(season_ids, saber_ids):
    a = F.splits_to_frame(_payload([_hit_season(i, 2025) for i in season_ids]),
                          2025, [], "a")
    b = F.splits_to_frame(_payload([_hit_saber(i, 2025) for i in saber_ids]),
                          2025, [], "b")
    return a, b


def test_a_player_only_sabermetrics_knows_fails():
    a, b = _frames([1, 2], [1, 2, 3])
    _expect_fetch_error(F.merge_season_saber, a, b, "t", contains="only in the")


def test_a_player_only_the_season_side_knows_is_kept_with_null_saber():
    a, b = _frames([1, 2, 3], [1, 2])
    m = F.merge_season_saber(a, b, "t")
    assert len(m) == 3
    row = m[m["player_id"] == 3].iloc[0]
    assert pd.isna(row["war"]) and row["plateAppearances"] == 100


def test_clashing_columns_are_prefixed_not_suffixed():
    a, b = _frames([1], [1])
    b["hits"] = 99
    m = F.merge_season_saber(a, b, "t")
    assert "saber_hits" in m.columns and "hits" in m.columns
    assert not [c for c in m.columns if c.endswith(("_x", "_y"))]
    assert int(m["hits"].iloc[0]) == 25, "the season value was overwritten"


def test_the_merge_keeps_one_name_and_team():
    a, b = _frames([1], [1])
    m = F.merge_season_saber(a, b, "t")
    assert "saber_name" not in m.columns and "saber_last_team_id" not in m.columns


# --------------------------------------------------------------- check_rates


def _merged(pa_by_id, drop_rate_for=()):
    a = F.splits_to_frame(_payload([_hit_season(i, 2025, pa=pa)
                                    for i, pa in pa_by_id.items()]), 2025, [], "a")
    saber = []
    for i, pa in pa_by_id.items():
        s = _hit_saber(i, 2025, pa=pa)
        if i in drop_rate_for:
            s["stat"].pop("woba", None)
        saber.append(s)
    b = F.splits_to_frame(_payload(saber), 2025, [], "b")
    return F.merge_season_saber(a, b, "t")


def test_a_player_with_plate_appearances_must_have_woba():
    m = _merged({1: 100, 2: 50}, drop_rate_for={2})
    _expect_fetch_error(F.check_rates, m, ["woba", "wRcPlus"], "plateAppearances",
                        "t", contains="lack woba")


def test_a_player_with_no_plate_appearances_may_lack_rates():
    m = _merged({1: 100, 2: 0})
    F.check_rates(m, ["woba", "wRcPlus"], "plateAppearances", "t")


def test_a_rate_nobody_carries_fails():
    m = _merged({1: 100}).drop(columns=["woba"])
    _expect_fetch_error(F.check_rates, m, ["woba"], "plateAppearances", "t",
                        contains="no player carries")


def test_a_missing_denominator_fails():
    m = _merged({1: 100}).drop(columns=["plateAppearances"])
    _expect_fetch_error(F.check_rates, m, ["woba"], "plateAppearances", "t",
                        contains="no plateAppearances")


# -------------------------------------------------------------- coerce_rates


def test_placeholders_become_null_and_innings_stay_strings():
    df = pd.DataFrame({"avg": [".278", ".---"], "era": ["3.60", "-.--"],
                       "inningsPitched": ["5.1", "0.0"]})
    out = F.coerce_rates(df)
    assert out["avg"].iloc[0] == 0.278 and pd.isna(out["avg"].iloc[1])
    assert pd.isna(out["era"].iloc[1])
    assert list(out["inningsPitched"]) == ["5.1", "0.0"], (
        "5.1 innings is 5 and 1/3, it must not become a float")


# ---------------------------------------------- the published-copy comparison


def test_a_completed_season_that_lost_players_is_reported():
    now = pd.DataFrame({"season": [2024] * 9 + [2026] * 2})
    before = pd.DataFrame({"season": [2024] * 10 + [2026] * 5})
    problems = F.rows_per_season_vs_published(now, before, TODAY)
    assert len(problems) == 1 and problems[0].startswith("2024"), problems


def test_a_season_missing_entirely_is_reported():
    now = pd.DataFrame({"season": [2025] * 10})
    before = pd.DataFrame({"season": [2024] * 10 + [2025] * 10})
    assert F.rows_per_season_vs_published(now, before, TODAY) == [
        "2024: 0 rows, published copy has 10"]


def test_growth_and_the_current_season_are_not_reported():
    now = pd.DataFrame({"season": [2024] * 11 + [2026] * 1})
    before = pd.DataFrame({"season": [2024] * 10 + [2026] * 5})
    assert F.rows_per_season_vs_published(now, before, TODAY) == []


def test_nothing_published_yet_is_not_a_problem():
    now = pd.DataFrame({"season": [2024]})
    assert F.rows_per_season_vs_published(now, None, TODAY) == []


class _Resp(io.BytesIO):
    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()


def _urlopen_raising(exc):
    def urlopen(req, timeout=None):
        raise exc
    return urlopen


def test_published_404_means_nothing_published():
    err = urllib.error.HTTPError("u", 404, "Not Found", {}, None)
    with _Patched(**{"F__urllib": _FakeUrllib(_urlopen_raising(err))}):
        assert F._published("statsapi_batting") is None


def test_published_other_http_error_fails_rather_than_skipping_the_check():
    err = urllib.error.HTTPError("u", 503, "Unavailable", {}, None)
    with _Patched(**{"F__urllib": _FakeUrllib(_urlopen_raising(err))}):
        _expect_fetch_error(F._published, "statsapi_batting", contains="503")


def test_published_network_error_fails_rather_than_skipping_the_check():
    err = urllib.error.URLError("down")
    with _Patched(**{"F__urllib": _FakeUrllib(_urlopen_raising(err))}):
        _expect_fetch_error(F._published, "statsapi_batting", contains="down")


def test_published_reads_the_season_column():
    buf = io.BytesIO()
    pd.DataFrame({"season": [2024, 2025], "x": [1, 2]}).to_parquet(buf)
    data = buf.getvalue()
    with _Patched(**{"F__urllib": _FakeUrllib(lambda req, timeout=None: _Resp(data))}):
        out = F._published("statsapi_batting")
    assert list(out.columns) == ["season"] and list(out["season"]) == [2024, 2025]


class _FakeUrllib:
    """Stands in for the urllib package inside fetch_statsapi only."""

    def __init__(self, urlopen):
        self.error = urllib.error
        self.request = _FakeRequestModule(urlopen)


class _FakeRequestModule:
    def __init__(self, urlopen):
        import urllib.request as real
        self.Request = real.Request
        self.urlopen = urlopen


# --------------------------------------------------------- main(), end to end


def _run_main(get, published=None, start=2024, end=2026, extra=(), today=TODAY):
    """Drive main() with the network replaced; return (exit code, out root)."""
    root = Path(tempfile.mkdtemp(prefix="statsapi_test_"))
    assert str(root).startswith(tempfile.gettempdir()), root
    argv = sys.argv
    sys.argv = ["fetch_statsapi.py", "--start-year", str(start),
                "--end-year", str(end), *extra]

    def pub(table):
        return None if published is None else published.get(table)

    try:
        with _Patched(F___get=get, F___published=pub, F__time=_NoSleep(),
                      F___today=lambda: today,
                      config__PARQUET_ROOT=root, F__DATA_TARGET="parquet",
                      config__DATA_TARGET="parquet"):
            assert config.PARQUET_ROOT == root, "the output root was not substituted"
            try:
                code = F.main()
            except SystemExit as e:
                code = e.code
    finally:
        sys.argv = argv
    return code, root


class _NoSleep:
    @staticmethod
    def sleep(_):
        return None

    @staticmethod
    def time():
        import time as real
        return real.time()


def _written(root: Path) -> dict[str, pd.DataFrame]:
    out = {}
    for table in F.TABLES.values():
        path = root / table / f"{table}.parquet"
        if path.exists():
            out[table] = pd.read_parquet(path)
    return out


def test_main_writes_both_tables_when_everything_is_clean():
    code, root = _run_main(_fake_api({2024, 2025, 2026}, pa0={3}))
    assert code in (0, None), code
    w = _written(root)
    assert set(w) == set(F.TABLES.values()), sorted(w)
    bat = w["statsapi_batting"]
    assert len(bat) == 36 and sorted(bat["season"].unique()) == [2024, 2025, 2026]
    assert "wRcPlus" in bat.columns and "fip" not in bat.columns, (
        "the batting table must be the hitting group")
    assert "fip" in w["statsapi_pitching"].columns
    assert list(bat.groupby("season")["is_partial"].first()) == [False, False, True]
    ip = w["statsapi_pitching"]["inningsPitched"]
    assert ip.iloc[0] == "10.0" and isinstance(ip.iloc[0], str), (
        "5.1 innings is 5 and 1/3; the string must survive the parquet round trip")


def test_main_writes_nothing_when_a_page_is_truncated():
    def mutate(p, season, group, kind):
        if season == 2025 and kind == "sabermetrics":
            p["stats"][0]["totalSplits"] += 1
        return p
    code, root = _run_main(_fake_api({2024, 2025, 2026}, mutate=mutate))
    assert code == 1, code
    assert _written(root) == {}, "a table was written despite a truncated page"


def test_main_writes_nothing_when_validation_fails():
    # validate_dataframe returns a bool; calling it bare would publish anyway.
    code, root = _run_main(_fake_api({2024, 2025, 2026}, n_players=5))
    assert code == 1, code
    assert _written(root) == {}


def test_main_refuses_a_table_that_lost_players_in_a_completed_season():
    # The fake API answers 12 players a season; the published copy had 13.
    published = {"statsapi_batting": pd.DataFrame({"season": [2024] * 13})}
    code, root = _run_main(_fake_api({2024, 2025, 2026}), published=published)
    assert code == 1, code
    assert "statsapi_batting" not in _written(root)


def test_main_fails_when_the_published_copy_cannot_be_read():
    def pub(table):
        raise F.FetchError("HTTP 503")
    root = Path(tempfile.mkdtemp(prefix="statsapi_test_"))
    argv = sys.argv
    sys.argv = ["fetch_statsapi.py", "--start-year", "2024", "--end-year", "2026"]
    try:
        with _Patched(F___get=_fake_api({2024, 2025, 2026}), F___published=pub,
                      F__time=_NoSleep(), F___today=lambda: TODAY, config__PARQUET_ROOT=root,
                      F__DATA_TARGET="parquet", config__DATA_TARGET="parquet"):
            try:
                code = F.main()
            except SystemExit as e:
                code = e.code
    finally:
        sys.argv = argv
    assert code == 1, code
    assert _written(root) == {}


def test_main_fails_when_a_hitter_with_plate_appearances_lacks_woba():
    def mutate(p, season, group, kind):
        if group == "hitting" and kind == "sabermetrics" and season == 2024:
            p["stats"][0]["splits"][0]["stat"].pop("woba")
        return p
    code, root = _run_main(_fake_api({2024, 2025, 2026}, mutate=mutate))
    assert code == 1, code
    assert _written(root) == {}


# ---------------------------------------------------------- workflow wiring


def _step_block(name: str) -> str:
    text = WORKFLOW.read_text(encoding="utf-8")
    start = text.index("- name: " + name)
    nxt = text.find("\n      - name: ", start + 1)
    return text[start:nxt if nxt != -1 else None]


def test_the_workflow_fetches_statsapi_on_all_and_by_name():
    block = _step_block("Fetch MLB Stats API")
    assert "fetch_statsapi.py" in block
    assert "'all'" in block and "'statsapi'" in block


def test_the_statsapi_step_is_not_excused():
    assert "continue-on-error" not in _step_block("Fetch MLB Stats API")


def test_these_tests_run_before_the_fetch():
    block = _step_block("Fetch MLB Stats API")
    assert "test_fetch_statsapi.py" in block, "the gate's tests are not run"
    assert block.index("test_fetch_statsapi.py") < block.index("python fetch_statsapi.py")


def _fetch_steps_selected_by(steps_value: str) -> set[str]:
    """Which fetch steps the workflow's `if:` would run for this value.

    Evaluates the real conditions: each is `contains(steps..., 'x')` joined
    by `||`, and contains() on a string is a substring match.
    """
    import re
    text = WORKFLOW.read_text(encoding="utf-8")
    selected = set()
    for m in re.finditer(r"- name: (Fetch [^\n]+)\n\s+if: ([^\n]+)", text):
        name, cond = m.group(1), m.group(2)
        needles = re.findall(r"contains\(steps\.steps\.outputs\.steps, '([^']+)'\)", cond)
        assert needles, f"{name}: condition not understood: {cond}"
        if any(n in steps_value for n in needles):
            selected.add(name)
    return selected


def test_the_step_name_does_not_select_statcast():
    # contains() is a substring match; a step called "stats" would also run
    # the heavy manual-only statcast pull.
    import check_outputs as C
    assert _fetch_steps_selected_by("statsapi") == {"Fetch MLB Stats API"}
    assert "Fetch MLB Stats API" not in _fetch_steps_selected_by("statcast")
    assert "Fetch MLB Stats API" in _fetch_steps_selected_by("all")
    assert "Fetch Statcast pitches" not in _fetch_steps_selected_by("all")
    assert "statsapi" in C.ALL_STEPS
    assert set(C.STEP_TABLES["statsapi"]) == set(F.TABLES.values())


def test_no_step_hardcodes_the_end_year():
    # A literal year froze every table at that season once the calendar
    # moved on, with the job still green.
    import re
    text = WORKFLOW.read_text(encoding="utf-8")
    for line in text.splitlines():
        if "--end-year" in line:
            assert "steps.steps.outputs.end_year" in line, line
            assert not re.search(r"'20\d\d'", line), line
    block = _step_block("Determine steps")
    assert "regularSeasonStartDate" in block, "the default ignores opening day"
    assert "end_year=" in block


def _run_determine_steps(today: str, curl_body: str | None, end_input: str = ""):
    """Execute the real `Determine steps` script with curl and date stubbed.

    Returns (exit code, the lines it wrote to GITHUB_OUTPUT).
    """
    import shutil
    import subprocess
    import textwrap
    assert shutil.which("bash") and shutil.which("jq"), "bash and jq are required"
    block = _step_block("Determine steps")
    script = textwrap.dedent(block.split("run: |", 1)[1])
    script = script.replace("${{ inputs.steps || 'all' }}", "all")
    work = Path(tempfile.mkdtemp(prefix="determine_"))
    stubs = work / "bin"
    stubs.mkdir()
    year = today[:4]
    (stubs / "date").write_text(
        "#!/bin/bash\n"
        f'case "$*" in *%Y*) echo {year};; *%F*) echo {today};; esac\n',
        encoding="utf-8")
    if curl_body is None:
        curl = "#!/bin/bash\necho 'curl: (22) 404' >&2\nexit 22\n"
    else:
        curl = "#!/bin/bash\ncat <<'JSON'\n" + curl_body + "\nJSON\n"
    (stubs / "curl").write_text(curl, encoding="utf-8")
    for f in stubs.iterdir():
        f.chmod(0o755)
    (work / "run.sh").write_text(script, encoding="utf-8")
    out = work / "out.txt"
    out.write_text("", encoding="utf-8")
    env = dict(os.environ, PATH=f"{stubs}:{os.environ['PATH']}",
               GITHUB_OUTPUT=str(out), END_YEAR_INPUT=end_input)
    proc = subprocess.run(["bash", "-eo", "pipefail", str(work / "run.sh")],
                          env=env, capture_output=True, text=True)
    return proc.returncode, out.read_text(encoding="utf-8").split()


_SEASON_2027 = '{"seasons":[{"seasonId":"2027","regularSeasonStartDate":"2027-03-25"}]}'


def test_before_opening_day_the_end_year_is_last_season():
    code, out = _run_determine_steps("2027-01-10", _SEASON_2027)
    assert code == 0 and "end_year=2026" in out, (code, out)
    code, out = _run_determine_steps("2027-03-24", _SEASON_2027)
    assert code == 0 and "end_year=2026" in out, (code, out)


def test_from_opening_day_the_end_year_is_this_season():
    code, out = _run_determine_steps("2027-03-25", _SEASON_2027)
    assert code == 0 and "end_year=2027" in out, (code, out)


def test_an_unreadable_opening_day_stops_the_run():
    code, out = _run_determine_steps("2027-05-01", None)
    assert code != 0 and not any(o.startswith("end_year=") for o in out), (code, out)
    code, out = _run_determine_steps("2027-05-01", '{"seasons":[]}')
    assert code != 0 and not any(o.startswith("end_year=") for o in out), (code, out)


def test_an_explicit_end_year_wins_and_must_be_a_year():
    code, out = _run_determine_steps("2027-01-10", None, end_input="2024")
    assert code == 0 and "end_year=2024" in out, (code, out)
    code, out = _run_determine_steps("2027-01-10", _SEASON_2027, end_input="abc")
    assert code != 0, (code, out)


def test_allow_shrink_is_passed_only_when_asked():
    block = _step_block("Fetch MLB Stats API")
    assert '"$ALLOW_SHRINK" = "true"' in block
    assert "--allow-shrink" in block
    assert "inputs.allow_statsapi_shrink" in block


# ------------------------------------------------------------------ _get


class _Seq:
    """urlopen stand-in that plays a list of outcomes in order."""

    def __init__(self, outcomes):
        self.outcomes = list(outcomes)
        self.calls = 0

    def __call__(self, req, timeout=None):
        self.calls += 1
        out = self.outcomes.pop(0)
        if isinstance(out, BaseException):
            raise out
        return _Resp(out)


def _get_with(outcomes, retries=3):
    seq = _Seq(outcomes)
    with _Patched(F__urllib=_FakeUrllib(seq), F__time=_NoSleep()):
        try:
            return F._get("https://example.invalid/x", retries=retries), seq.calls
        except F.FetchError as e:
            return e, seq.calls


def test_get_retries_a_connection_dropped_without_urllib_wrapping_it():
    import http.client
    out, calls = _get_with([http.client.RemoteDisconnected("gone"),
                            ConnectionResetError("reset"), b'{"ok": 1}'])
    assert out == {"ok": 1} and calls == 3


def test_get_retries_a_truncated_body_and_a_non_json_page():
    import http.client
    out, calls = _get_with([http.client.IncompleteRead(b"x"), b"<html>busy</html>",
                            b'{"ok": 2}'])
    assert out == {"ok": 2} and calls == 3


def test_get_retries_5xx_and_429_but_not_other_4xx():
    e503 = urllib.error.HTTPError("u", 503, "x", {}, None)
    e429 = urllib.error.HTTPError("u", 429, "x", {}, None)
    out, calls = _get_with([e503, e429, b'{"ok": 3}'])
    assert out == {"ok": 3} and calls == 3
    e400 = urllib.error.HTTPError("u", 400, "x", {}, None)
    out, calls = _get_with([e400, b'{"ok": 4}'])
    assert isinstance(out, F.FetchError) and calls == 1, (out, calls)


def test_get_gives_up_with_a_fetch_error():
    out, calls = _get_with([urllib.error.URLError("down")] * 3)
    assert isinstance(out, F.FetchError) and calls == 3, (out, calls)


def test_published_html_body_is_a_fetch_error():
    with _Patched(**{"F__urllib": _FakeUrllib(
            lambda req, timeout=None: _Resp(b"<html>rate limited</html>"))}):
        _expect_fetch_error(F._published, "statsapi_batting", contains="not a parquet")


# ------------------------------------------------ season boundary, escape hatch


def test_in_january_the_finished_season_is_not_partial():
    code, root = _run_main(_fake_api({2024, 2025, 2026}),
                           today=dt.date(2027, 1, 10))
    assert code in (0, None), code
    bat = _written(root)["statsapi_batting"]
    assert not bat["is_partial"].any(), "2026 is over in January 2027"


def test_a_narrower_end_year_is_refused_for_completed_seasons():
    published = {"statsapi_batting": pd.DataFrame({"season": [2024] * 12 + [2025] * 12})}
    code, root = _run_main(_fake_api({2024}), published=published, end=2024)
    assert code == 1, code
    assert _written(root) == {}


def test_allow_shrink_publishes_a_checked_shrink():
    published = {"statsapi_batting": pd.DataFrame({"season": [2024] * 13})}
    code, root = _run_main(_fake_api({2024, 2025, 2026}), published=published,
                           extra=["--allow-shrink"])
    assert code in (0, None), code
    assert "statsapi_batting" in _written(root)


# --------------------------------------------------------------------- runner


def _standalone() -> int:
    tests = [(name, obj) for name, obj in sorted(globals().items())
             if name.startswith("test_") and callable(obj)]
    failed = 0
    for name, fn in tests:
        try:
            fn()
            print(f"PASS {name}")
        except (Exception, SystemExit) as exc:
            failed += 1
            print(f"FAIL {name}: {type(exc).__name__}: {exc}")
    print(f"\n{len(tests) - failed}/{len(tests)} passed")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(_standalone())
