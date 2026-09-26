"""Tests for scripts/check_outputs.py and for the workflow wiring it needs.

Runs under pytest and standalone (`python tests/test_check_outputs.py`),
because the workflow has no pytest installed - requirements.txt carries the
fetch dependencies only.

The standalone runner counts SystemExit as a failure on purpose: SystemExit
inherits from BaseException, so a bare `except Exception` lets it escape the
loop and the runner exits 0 while tests are failing.
"""

from __future__ import annotations

import ast
import os
import re
import sys
import tempfile
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))

os.environ.setdefault("MLB_DATA_TARGET", "parquet")

import check_outputs as C  # noqa: E402

WORKFLOW = ROOT / ".github" / "workflows" / "weekly_refresh.yml"


def _write(root: Path, table: str, rows: int, suffix: str = "") -> Path:
    d = root / table
    d.mkdir(parents=True, exist_ok=True)
    path = d / f"{table}{suffix}.parquet"
    pd.DataFrame({"season": [2026] * rows}).to_parquet(path)
    return path


def _run(steps: str, root: Path, published=None, **kwargs) -> int:
    """Run the audit against `root`.

    `published` stands in for the copies on Hugging Face: a dict of
    table -> set of seasons, or a callable. Absent tables read as "not
    published yet", so no test touches the network.
    """
    original = C.PARQUET_ROOT
    original_pub = C._published_seasons
    if callable(published):
        C._published_seasons = published
    else:
        C._published_seasons = lambda table: (published or {}).get(table)
    C.PARQUET_ROOT = root
    assert C.PARQUET_ROOT != original, "PARQUET_ROOT was not substituted"
    assert str(C.PARQUET_ROOT).startswith(tempfile.gettempdir()), C.PARQUET_ROOT
    argv = sys.argv
    sys.argv = ["check_outputs.py", "--steps", steps]
    for key, value in kwargs.items():
        sys.argv += [f"--{key.replace('_', '-')}", str(value)]
    try:
        return C.main()
    finally:
        sys.argv = argv
        C.PARQUET_ROOT = original
        C._published_seasons = original_pub


def _populate(root: Path, steps: list[str], skip=frozenset()) -> None:
    for step in steps:
        for table in C.STEP_TABLES[step]:
            if table in skip:
                continue
            _write(root, table, 10)


def _all_steps() -> list[str]:
    steps = C._selected_steps("all")
    assert steps is not None
    return steps


# ------------------------------------------------- the table map stays true


def _tables_written_by(script: Path) -> set[str]:
    """Table names a fetch script writes, string literals and dict values.

    A plain regex over `write_dataframe(df, "name")` only sees 5 of the 16
    tables: fetch_savant_leaderboards.py and fetch_fielding_running.py both
    call `write_dataframe(df, table_name)` out of a dict literal.
    """
    tree = ast.parse(script.read_text(encoding="utf-8"))
    names: set[str] = set()
    literal_arg = False
    for node in ast.walk(tree):
        if (isinstance(node, ast.Call)
                and isinstance(node.func, ast.Name)
                and node.func.id == "write_dataframe"
                and len(node.args) >= 2
                and isinstance(node.args[1], ast.Constant)
                and isinstance(node.args[1].value, str)):
            names.add(node.args[1].value)
            literal_arg = True
        if isinstance(node, ast.Dict):
            values = [v.value for v in node.values
                      if isinstance(v, ast.Constant) and isinstance(v.value, str)]
            # A table map: every value is a bare table name, and at least one
            # of them is a name this script could plausibly write.
            if values and all(
                    v and not v.endswith(".csv") and v.replace("_", "").isalnum()
                    and v.islower()
                    for v in values):
                names |= set(values)
    del literal_arg
    return names


def test_step_tables_covers_every_table_the_fetch_scripts_write():
    known = {t for tables in C.STEP_TABLES.values() for t in tables}
    written: set[str] = set()
    for script in sorted((ROOT / "scripts").glob("fetch_*.py")):
        written |= _tables_written_by(script) & known | (
            _tables_written_by(script) - known)
    # Only judge names that look like our tables; the dict scan can pick up
    # unrelated lowercase maps, so intersect with "mentioned in a fetch
    # script AND shaped like a table name we manage".
    candidates = {n for n in written if n.startswith(
        ("fg_", "sc_", "oaa", "sprint_", "catcher", "park_", "statcast_"))}
    assert candidates, "no table name found - did the extractor rot?"
    assert candidates <= known, f"tables written but never checked: {sorted(candidates - known)}"


def test_the_extractor_sees_the_tables_written_through_a_variable():
    """Guards the guard: the regex version saw only 5 of 16."""
    found: set[str] = set()
    for script in sorted((ROOT / "scripts").glob("fetch_*.py")):
        found |= _tables_written_by(script)
    for table in ("sc_bat_tracking", "sprint_speed", "oaa_team", "catcher"):
        assert table in found, f"{table} is written through a variable and was missed"


def test_every_unreachable_table_is_a_real_table():
    known = {t for tables in C.STEP_TABLES.values() for t in tables}
    assert set(C.KNOWN_UNREACHABLE) <= known, sorted(set(C.KNOWN_UNREACHABLE) - known)


def test_every_unreachable_entry_states_a_reason():
    for table, reason in C.KNOWN_UNREACHABLE.items():
        assert str(reason).strip(), f"{table} is excused with no reason"
        assert "measured" in str(reason).lower(), f"{table}: no measurement cited"


def test_a_bad_declaration_fails_before_any_data_is_read():
    """Every table is present, so a bad declaration is the only reason to fail.

    An earlier version of this test used an empty directory, where every
    table is missing anyway - it passed no matter what the declaration
    check did.
    """
    original = dict(C.KNOWN_UNREACHABLE)
    try:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _populate(root, _all_steps())
            assert _run("all", root) == 0, "baseline: a full root must pass"

            C.KNOWN_UNREACHABLE["not_a_table"] = "measured nowhere"
            assert _run("all", root) == 1, "an unreachable entry for no real table"

            C.KNOWN_UNREACHABLE.clear()
            C.KNOWN_UNREACHABLE.update(original)
            C.KNOWN_UNREACHABLE["fg_batting"] = "   "
            assert _run("all", root) == 1, "an unreachable entry with no reason"
    finally:
        C.KNOWN_UNREACHABLE.clear()
        C.KNOWN_UNREACHABLE.update(original)


# ------------------------------------------------------------- step parsing


def test_all_excludes_the_heavy_statcast_step():
    assert C._selected_steps("all") == C.ALL_STEPS
    assert "statcast" not in C.ALL_STEPS
    assert C._selected_steps("statcast") == ["statcast"]


def test_all_plus_statcast_checks_statcast_too():
    steps = C._selected_steps("all,statcast")
    assert steps is not None
    assert "statcast" in steps, "a dispatched full run would skip statcast_pitches"


def test_unknown_steps_is_a_failure_not_a_pass():
    # GHA's contains() is a substring test, so 'fall' and 'install' both run
    # every fetch step. The gate must not report success on a run whose
    # contents it cannot name.
    for raw in ("", "nonsense", "fall", "install", "all,bogus"):
        assert C._selected_steps(raw) is None, raw
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        _populate(root, _all_steps())
        assert _run("nonsense", root) == 1
        assert _run("", root) == 1


def test_case_and_whitespace_are_tolerated():
    assert C._selected_steps(" ALL ") == C.ALL_STEPS
    assert C._selected_steps("all, park") == C.ALL_STEPS


# ------------------------------------------------------------- the verdict


def test_everything_present_passes():
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        _populate(root, _all_steps())
        assert _run("all", root) == 0


def test_missing_table_fails():
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        _populate(root, _all_steps(), skip={"park_factors"})
        assert _run("all", root) == 1, "a missing park_factors was tolerated"


def test_empty_table_fails():
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        _populate(root, _all_steps(), skip={"oaa"})
        _write(root, "oaa", 0)
        assert _run("all", root) == 1, "a 0-row table was tolerated"


def test_unreadable_table_fails():
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        _populate(root, _all_steps(), skip={"catcher"})
        path = _write(root, "catcher", 10)
        path.write_bytes(b"not a parquet file at all")
        assert _run("all", root) == 1, "a corrupt parquet was tolerated"


def test_declared_unreachable_table_does_not_fail_the_run():
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        _populate(root, _all_steps(), skip=set(C.KNOWN_UNREACHABLE))
        assert _run("all", root) == 0


def test_the_excuse_covers_a_missing_file_not_a_broken_one():
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        _populate(root, _all_steps(), skip={"fg_batting"})
        _write(root, "fg_batting", 0)
        assert _run("all", root) == 1, "an empty excused table was tolerated"
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        _populate(root, _all_steps(), skip={"fg_batting"})
        _write(root, "fg_batting", 5).write_bytes(b"junk")
        assert _run("all", root) == 1, "a corrupt excused table was tolerated"


def test_the_unreachable_declaration_expires():
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        _populate(root, _all_steps(), skip=set(C.KNOWN_UNREACHABLE))
        day_before = C.RECHECK_AFTER.isoformat()
        assert _run("all", root, today=day_before) == 0
        after = C.RECHECK_AFTER.replace(year=C.RECHECK_AFTER.year + 1)
        assert _run("all", root, today=after.isoformat()) == 1


def test_only_the_selected_step_is_checked():
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        _populate(root, ["fielding"])
        assert _run("fielding", root) == 0
        assert _run("fielding,park", root) == 1


def test_partitioned_table_counted_across_files():
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        _write(root, "statcast_pitches", 5, suffix="_2024")
        _write(root, "statcast_pitches", 7, suffix="_2025")
        assert _run("statcast", root) == 0


def test_partitioned_table_all_empty_fails():
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        _write(root, "statcast_pitches", 0, suffix="_2024")
        assert _run("statcast", root) == 1


def test_a_stray_parquet_does_not_stand_in_for_the_table():
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        _populate(root, ["fielding"], skip={"oaa"})
        (root / "oaa").mkdir(parents=True, exist_ok=True)
        pd.DataFrame({"x": [1]}).to_parquet(root / "oaa" / "something_else.parquet")
        assert _run("fielding", root) == 1, "a foreign parquet was counted as oaa"


# -------------------------------------------------------------- the outputs


def test_ok_list_names_only_the_good_tables():
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        _populate(root, ["fielding"], skip={"oaa"})
        _write(root, "oaa", 0)
        listing = root / "ok.txt"
        assert _run("fielding", root, ok_list=listing) == 1
        names = listing.read_text(encoding="utf-8").split()
        assert "oaa" not in names, "an empty table would have been uploaded"
        assert set(names) == {"sprint_speed", "oaa_team", "catcher"}


def test_ok_list_excludes_an_unreachable_table_with_no_file():
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        _populate(root, _all_steps(), skip=set(C.KNOWN_UNREACHABLE))
        listing = root / "ok.txt"
        assert _run("all", root, ok_list=listing) == 0
        names = set(listing.read_text(encoding="utf-8").split())
        assert not (names & set(C.KNOWN_UNREACHABLE))
        assert "park_factors" in names


def test_summary_names_the_missing_tables():
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        summary = root / "summary.md"
        _populate(root, ["fielding"], skip={"oaa"})
        os.environ["GITHUB_STEP_SUMMARY"] = str(summary)
        try:
            assert _run("fielding", root) == 1
        finally:
            del os.environ["GITHUB_STEP_SUMMARY"]
        text = summary.read_text(encoding="utf-8")
        assert "sprint_speed" in text
        assert "10 rows" in text
        assert "MISSING" in text, "the summary did not flag the gap"
        # The per-table row already contains "oaa", so look for the closing
        # line that names what did not arrive - that is what a human reads.
        closing = [line for line in text.splitlines()
                   if "Missing, empty or unreadable" in line]
        assert closing, "the summary never says which tables did not arrive"
        named = closing[0].replace("*", " ").replace(",", " ").split()
        assert "oaa" in named, f"the closing line does not name oaa: {closing[0]}"
        assert "not uploaded" in text


# ------------------------------------------- seasons the published copy has


def _tmp() -> Path:
    return Path(tempfile.mkdtemp(prefix="check_outputs_"))


def test_a_season_the_published_copy_has_and_this_run_lacks_fails():
    root = _tmp()
    _populate(root, ["park"])  # writes season 2026 only
    ok = root / "ok.txt"
    code = _run("park", root, published={"park_factors": {2025, 2026}}, ok_list=ok)
    assert code == 1
    assert "park_factors" not in ok.read_text(encoding="utf-8").split(), (
        "a table that lost seasons would replace the full copy on Hugging Face")


def test_the_same_or_more_seasons_than_published_passes():
    root = _tmp()
    _populate(root, ["park"])
    assert _run("park", root, published={"park_factors": {2026}}) == 0


def test_nothing_published_yet_passes():
    root = _tmp()
    _populate(root, ["park"])
    assert _run("park", root, published={}) == 0


def test_an_unreadable_published_copy_fails_rather_than_skipping():
    def broken(table):
        raise C.SeasonCheckError("published " + table + ": HTTP 503")
    root = _tmp()
    _populate(root, ["park"])
    ok = root / "ok.txt"
    assert _run("park", root, published=broken, ok_list=ok) == 1
    assert ok.read_text(encoding="utf-8").strip() == ""


def test_a_file_without_a_season_column_fails():
    root = _tmp()
    d = root / "park_factors"
    d.mkdir(parents=True)
    pd.DataFrame({"year": [2026] * 5}).to_parquet(d / "park_factors.parquet")
    assert _run("park", root, published={"park_factors": {2026}}) == 1


def test_the_partitioned_table_is_not_compared():
    root = _tmp()
    _write(root, "statcast_pitches", 10, "_2026")
    calls = []

    def pub(table):
        calls.append(table)
        return {2015, 2026}
    assert _run("statcast", root, published=pub) == 0
    assert "statcast_pitches" not in calls


def test_published_seasons_404_is_none_and_other_errors_raise():
    import urllib.error
    import urllib.request

    def raising(exc):
        def urlopen(req, timeout=None):
            raise exc
        return urlopen
    real = urllib.request.urlopen
    real_sleep = C._sleep
    C._sleep = lambda s: None
    try:
        urllib.request.urlopen = raising(
            urllib.error.HTTPError("u", 404, "nf", {}, None))
        assert C._published_seasons("park_factors") is None
        for exc in (urllib.error.HTTPError("u", 503, "x", {}, None),
                    urllib.error.URLError("down"), ConnectionResetError("reset")):
            urllib.request.urlopen = raising(exc)
            try:
                C._published_seasons("park_factors")
            except C.SeasonCheckError:
                continue
            raise AssertionError(f"{exc!r} did not raise")
    finally:
        urllib.request.urlopen = real
        C._sleep = real_sleep


def test_published_seasons_rejects_a_body_that_is_not_parquet():
    import io
    import urllib.request

    class R(io.BytesIO):
        def __enter__(self):
            return self

        def __exit__(self, *a):
            return None
    real = urllib.request.urlopen
    real_sleep = C._sleep
    C._sleep = lambda s: None
    try:
        urllib.request.urlopen = lambda req, timeout=None: R(b"<html>busy</html>")
        try:
            C._published_seasons("park_factors")
        except C.SeasonCheckError:
            return
        raise AssertionError("an HTML body was accepted")
    finally:
        urllib.request.urlopen = real
        C._sleep = real_sleep


def _parquet_body(seasons):
    import io
    buf = io.BytesIO()
    pd.DataFrame({"season": list(seasons)}).to_parquet(buf)
    return buf.getvalue()


def _play(outcomes):
    """urlopen stand-in that plays outcomes in order; counts calls."""
    import io

    class R(io.BytesIO):
        def __enter__(self):
            return self

        def __exit__(self, *a):
            return None
    state = {"calls": 0}

    def urlopen(req, timeout=None):
        state["calls"] += 1
        out = outcomes.pop(0)
        if isinstance(out, BaseException):
            raise out
        return R(out)
    return urlopen, state


def test_published_seasons_retries_a_blip_then_reads():
    import http.client
    import urllib.error
    import urllib.request
    urlopen, state = _play([urllib.error.HTTPError("u", 502, "x", {}, None),
                            http.client.IncompleteRead(b""),
                            _parquet_body([2025, 2026])])
    real, real_sleep = urllib.request.urlopen, C._sleep
    urllib.request.urlopen, C._sleep = urlopen, (lambda s: None)
    try:
        assert C._published_seasons("park_factors") == {2025, 2026}
    finally:
        urllib.request.urlopen, C._sleep = real, real_sleep
    assert state["calls"] == 3


def test_published_seasons_does_not_retry_a_permanent_4xx():
    import urllib.error
    import urllib.request
    urlopen, state = _play([urllib.error.HTTPError("u", 401, "x", {}, None),
                            _parquet_body([2026])])
    real, real_sleep = urllib.request.urlopen, C._sleep
    urllib.request.urlopen, C._sleep = urlopen, (lambda s: None)
    try:
        try:
            C._published_seasons("park_factors")
        except C.SeasonCheckError:
            pass
        else:
            raise AssertionError("a 401 was read as a copy")
    finally:
        urllib.request.urlopen, C._sleep = real, real_sleep
    assert state["calls"] == 1


def test_allow_lost_seasons_lets_only_the_named_table_through():
    root = _tmp()
    _populate(root, ["park", "fielding"])  # every file has season 2026 only
    ok = root / "ok.txt"
    published = {"park_factors": {2025, 2026}, "oaa": {2025, 2026}}
    code = _run("park,fielding", root, published=published, ok_list=ok,
                allow_lost_seasons="park_factors")
    listed = ok.read_text(encoding="utf-8").split()
    assert "park_factors" in listed, "the named table was still refused"
    assert "oaa" not in listed, "a table nobody named was let through"
    assert code == 1, "oaa still lost a season; the run must stay red"


def test_allow_lost_seasons_alone_passes_when_it_is_the_only_problem():
    root = _tmp()
    _populate(root, ["park"])
    code = _run("park", root, published={"park_factors": {2025, 2026}},
                allow_lost_seasons="park_factors")
    assert code == 0


def test_allow_lost_seasons_rejects_an_unknown_table():
    root = _tmp()
    _populate(root, ["park"])
    assert _run("park", root, allow_lost_seasons="park_factor") == 1


def test_the_workflow_passes_allow_lost_seasons_to_the_audit():
    text = _workflow_text()
    block = text[text.index("- name: Audit outputs"):][:900]
    assert "ALLOW_LOST_SEASONS: ${{ inputs.allow_lost_seasons }}" in block, (
        "the input never reaches the step environment")
    assert '--allow-lost-seasons "$ALLOW_LOST_SEASONS"' in block, (
        "the environment value is not what the audit receives")


# ------------------------------------------------------- the workflow wiring


def _workflow_text() -> str:
    return WORKFLOW.read_text(encoding="utf-8")


def test_the_workflow_runs_this_gate():
    text = _workflow_text()
    assert "scripts/check_outputs.py" in text, "the gate is not wired in"
    assert "tests/test_check_outputs.py" in text, "the gate's own tests do not run"


def test_the_gate_runs_before_the_upload():
    text = _workflow_text()
    gate = text.index("scripts/check_outputs.py")
    upload = text.index("Upload to Hugging Face")
    assert gate < upload, "the audit must decide before anything is published"


def test_the_upload_uses_the_ok_list():
    text = _workflow_text()
    assert "--ok-list" in text, "the gate is not asked which tables are safe"
    upload_block = text[text.index("Upload to Hugging Face"):]
    assert "done < ok_tables.txt" in upload_block, (
        "the upload loop does not read the ok-list; naming the file in a "
        "guard above the loop is not the same thing")


def test_the_upload_survives_a_failed_fetch_step():
    """A step with no `if:` carries the implicit success().

    So a fetch step exiting non-zero - which park factors now can, it is no
    longer continue-on-error - would skip both upload steps and cost the
    week for every table that did arrive. That is the trade this gate
    exists to avoid.
    """
    text = _workflow_text()
    for step in ("Install huggingface_hub", "Upload to Hugging Face"):
        block = text[text.index("- name: " + step):][:500]
        assert "if:" in block, step + " carries the implicit success()"
        assert "steps.audit.outcome" in block, step + " does not consult the audit"


def test_a_broken_gate_blocks_publishing():
    text = _workflow_text()
    for step in ("Install huggingface_hub", "Upload to Hugging Face"):
        block = text[text.index("- name: " + step):][:500]
        assert "steps.audit_tests.outcome == " in block, (
            step + " would publish even when the gate own tests fail")


def test_a_failed_audit_still_fails_the_job():
    # Scoped to the step that does it: steps.audit.outcome also appears in
    # the upload conditions now, so a bare "in text" would pass with this
    # step gutted.
    text = _workflow_text()
    block = text[text.index("- name: Fail if the audit found a problem"):][:400]
    assert "steps.audit.outcome == " in block, (
        "the audit is continue-on-error; this step must still fail the job")
    assert "failure" in block
    assert "if: false" not in block


def test_the_park_step_is_not_excused():
    text = _workflow_text()
    park = text.index("Fetch park factors")
    following = text[park:park + 400]
    assert "continue-on-error" not in following, (
        "park factors comes from Savant now; a failure there is a real failure")


def test_the_dataset_card_exists_and_is_published():
    card = ROOT / "docs" / "hf_dataset_card.md"
    assert card.exists(), "the dataset card is gone"
    text = _workflow_text()
    assert "docs/hf_dataset_card.md" in text, "the card is never uploaded"
    upload = text[text.index("- name: Upload to Hugging Face"):]
    assert "docs/hf_dataset_card.md" in upload, "the card ships from some other step"


def test_the_dataset_card_names_every_table():
    """A card that stops listing a table is a card that lies by omission."""
    card = (ROOT / "docs" / "hf_dataset_card.md").read_text(encoding="utf-8")
    for step, tables in C.STEP_TABLES.items():
        for table in tables:
            assert table in card, step + "/" + table + " is missing from the dataset card"


def test_the_dataset_card_says_which_tables_are_stale():
    card = (ROOT / "docs" / "hf_dataset_card.md").read_text(encoding="utf-8")
    for table in C.KNOWN_UNREACHABLE:
        line = [l for l in card.splitlines() if table in l and "|" in l]
        assert line, table + " has no row in the dataset card table"
        assert "frozen" in line[0].lower(), (
            table + " is declared unreachable but the card does not call it frozen")


def _card_configs() -> dict:
    """config_name -> data_files, parsed without pyyaml.

    pyyaml reaches the runner only as a transitive dependency of wandb, and
    a test that skips itself when an import fails is a test that can stop
    checking without anyone noticing.
    """
    text = (ROOT / "docs" / "hf_dataset_card.md").read_text(encoding="utf-8")
    assert text.startswith("---"), "the card has no YAML front matter"
    front = text.split("---")[1].splitlines()
    configs, name = {}, None
    inside = False
    for line in front:
        if line.rstrip() == "configs:":
            inside = True
            continue
        if inside and line and not line.startswith(" "):
            break
        if not inside:
            continue
        stripped = line.strip()
        if stripped.startswith("- config_name:"):
            name = stripped.split(":", 1)[1].strip()
        elif stripped.startswith("data_files:") and name is not None:
            configs[name] = stripped.split(":", 1)[1].strip()
            name = None
    return configs


def test_the_card_declares_one_configuration_per_table():
    """A single config over *.parquet groups tables with different schemas.

    The Hub then tries to read them as one dataset and the viewer breaks.
    """
    declared = _card_configs()
    assert declared, "no configs parsed out of the card front matter"
    expected = {t for step, tables in C.STEP_TABLES.items() if step != "statcast"
                for t in tables}
    # dbt marts are published under marts/ by .github/workflows/dbt_marts.yml.
    marts = {p.stem for p in (ROOT / "dbt" / "models" / "marts").glob("*.sql")}
    assert marts, "no dbt marts found"
    assert set(declared) == expected | marts, (
        "card configs do not match the tables and marts: "
        + str(sorted(set(declared) ^ (expected | marts))))
    for name, files in declared.items():
        want = ("marts/" if name in marts else "") + name + ".parquet"
        assert files == want, name + " points at " + str(files)
        assert "*" not in files, name + " uses a glob"


def _standalone() -> int:
    tests = [(name, obj) for name, obj in sorted(globals().items())
             if name.startswith("test_") and callable(obj)]
    failed = 0
    for name, fn in tests:
        try:
            fn()
            print(f"PASS {name}")
        except (Exception, SystemExit) as exc:
            # SystemExit is a BaseException: `except Exception` would let it
            # escape this loop, skip sys.exit below and report rc=0 while
            # tests are failing.
            failed += 1
            print(f"FAIL {name}: {type(exc).__name__}: {exc}")
    print(f"\n{len(tests) - failed}/{len(tests)} passed")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(_standalone())
