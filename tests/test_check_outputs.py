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


def _run(steps: str, root: Path, **kwargs) -> int:
    original = C.PARQUET_ROOT
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
