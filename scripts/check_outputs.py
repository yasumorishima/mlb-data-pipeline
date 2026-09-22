"""Say what this run actually produced, and fail when a table went missing.

Why this exists
---------------
Every fetch step writes Parquet under ``MLB_PARQUET_ROOT``; the upload step
then pushes whatever files exist. A table that fetched nothing simply has no
file, so the upload skips it and Hugging Face keeps the previous copy. The
job stays green and nothing in the log says which table stopped arriving.

Measured 2026-09-21 (run 35565978836, conclusion "success"): the weekly job
was green while ``fg_batting`` / ``fg_pitching`` / ``fg_pitcher_plus``
fetched nothing for all 12 seasons and ``park_factors`` fetched nothing at
all - park_factors had never reached the dataset, and the FanGraphs tables
on Hugging Face were frozen at a 2026-04 snapshot ending with 2025.

So: print one line per expected table, put the same thing in the job
summary, and exit non-zero when an expected table is absent or empty unless
it is declared unreachable below.

``--ok-list`` writes the tables that are fine, one per line. The upload step
reads it and pushes only those, so one bad table does not cost the week's
refresh of every other table while the job still goes red.

Usage:
  python scripts/check_outputs.py --steps all --ok-list ok_tables.txt
"""

from __future__ import annotations

import argparse
import datetime as dt
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from config import PARQUET_ROOT  # noqa: E402

# Which fetch step is responsible for which table. tests/test_check_outputs.py
# reads the fetch scripts and fails if one writes a table missing from here.
STEP_TABLES: dict[str, list[str]] = {
    "fangraphs": ["fg_batting", "fg_pitching", "fg_pitcher_plus"],
    "savant": [
        "sc_batter_exitvelo",
        "sc_pitcher_exitvelo",
        "sc_batter_expected",
        "sc_pitcher_expected",
        "sc_pitcher_arsenal",
        "sc_batted_ball",
        "sc_bat_tracking",
    ],
    "fielding": ["sprint_speed", "oaa", "oaa_team", "catcher"],
    "park": ["park_factors"],
    "statcast": ["statcast_pitches"],
}

# The steps "all" runs. Mirrors the `if:` conditions in weekly_refresh.yml,
# which run everything except the heavy statcast pull on "all".
ALL_STEPS = ["fangraphs", "savant", "fielding", "park"]

# Tables whose source refuses this runner. Reported, but they do not fail
# the job, because nothing this run can do would change it.
#
# Each entry states what was measured and when. RECHECK_AFTER makes the
# exemption expire: past that date the audit fails until someone re-measures
# and either lifts the entry or moves the date. A permanent green exemption
# is the same disease this script exists to cure.
_FG_REASON = (
    "FanGraphs blocks datacenter addresses. Measured on GitHub Actions run "
    "35565978836 (2026-09-21): pybaseball, which sets no User-Agent and so "
    "sends the honest python-requests default, got 403 for all 12 seasons. "
    "The same request from a residential line returns 200 (measured "
    "2026-09-23), so this is the network, not the client."
)
KNOWN_UNREACHABLE: dict[str, str] = {
    "fg_batting": _FG_REASON,
    "fg_pitching": _FG_REASON,
    "fg_pitcher_plus": _FG_REASON,
}
RECHECK_AFTER = dt.date(2027, 3, 31)


def _selected_steps(raw: str) -> list[str] | None:
    """The steps this run fetched, or None if the string is not understood.

    None means "the gate does not know what this run did", which is a reason
    to fail rather than to pass quietly.
    """
    wanted = [s.strip().lower() for s in (raw or "").split(",") if s.strip()]
    if not wanted:
        return None
    steps: list[str] = []
    for token in wanted:
        if token == "all":
            steps.extend(s for s in ALL_STEPS if s not in steps)
        elif token in STEP_TABLES:
            if token not in steps:
                steps.append(token)
        else:
            return None
    return steps


def _files_for(table: str) -> list[Path]:
    """Every parquet a table wrote.

    ``write_dataframe`` puts them at PARQUET_ROOT/<table>/<table><suffix>.parquet,
    and statcast_pitches uses the suffix to partition by year, so match the
    whole directory rather than one exact filename.
    """
    return sorted((PARQUET_ROOT / table).glob(f"{table}*.parquet"))


def _row_count(path: Path) -> int | None:
    """Rows in one parquet file, read from the footer.

    ``pd.read_parquet(path, columns=[])`` looks like a cheap way to do this
    and is not: it hands back a frame with no columns and no rows, so every
    table reads as empty. There is deliberately no full-read fallback -
    statcast_pitches is 6.8M rows and materialising it to count would turn
    this check into the slowest step in the job. A file whose footer cannot
    be read is reported as unreadable, which fails.
    """
    try:
        import pyarrow.parquet as pq

        return pq.ParquetFile(path).metadata.num_rows
    except Exception:
        return None


def _check_declarations() -> list[str]:
    """Problems with this file's own tables, before looking at any data."""
    problems = []
    known = {t for tables in STEP_TABLES.values() for t in tables}
    for table, reason in KNOWN_UNREACHABLE.items():
        if table not in known:
            problems.append(f"KNOWN_UNREACHABLE names {table!r}, which no step produces")
        if not str(reason).strip():
            problems.append(f"KNOWN_UNREACHABLE[{table!r}] has no reason")
    for step in ALL_STEPS:
        if step not in STEP_TABLES:
            problems.append(f"ALL_STEPS names {step!r}, which is not a step")
    return problems


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--steps", default="all",
                    help="comma separated, same value the workflow passes")
    ap.add_argument("--ok-list",
                    help="write the tables that are fine here, one per line")
    ap.add_argument("--today", help="override the date (tests only)")
    args = ap.parse_args()

    today = (dt.date.fromisoformat(args.today) if args.today
             else dt.date.today())

    problems = _check_declarations()
    if problems:
        for line in problems:
            print(f"ERROR: {line}")
        return 1

    steps = _selected_steps(args.steps)
    if steps is None:
        print(f"ERROR: --steps {args.steps!r} names no fetch step this script "
              f"knows. Known: {', '.join(sorted(STEP_TABLES))}, or 'all'.")
        print("Refusing to report success on a run whose contents are unknown.")
        return 1

    lines: list[str] = []
    missing: list[str] = []
    excused: list[str] = []
    ok: list[str] = []

    for step in steps:
        for table in STEP_TABLES[step]:
            files = _files_for(table)
            reason = KNOWN_UNREACHABLE.get(table)

            if not files:
                if reason:
                    excused.append(table)
                    lines.append(f"| {table} | not produced | declared unreachable |")
                else:
                    missing.append(table)
                    lines.append(f"| {table} | **MISSING** | step `{step}` produced no file |")
                continue

            counts = [_row_count(f) for f in files]
            if any(c is None for c in counts):
                # Unreadable is a failure even for a declared-unreachable
                # table: the excuse covers "no file", not "a broken file".
                missing.append(table)
                lines.append(f"| {table} | **UNREADABLE** | "
                             f"{len(files)} file(s), at least one footer could not be read |")
                continue

            rows = sum(counts)
            if rows == 0:
                missing.append(table)
                lines.append(f"| {table} | **EMPTY** | "
                             f"{len(files)} file(s) written with 0 rows |")
                continue

            ok.append(table)
            size_mb = sum(f.stat().st_size for f in files) / 1e6
            note = f"{size_mb:.1f} MB"
            if len(files) > 1:
                note += f", {len(files)} files"
            lines.append(f"| {table} | {rows:,} rows | {note} |")

    expired = excused and today > RECHECK_AFTER

    header = ["| table | this run | note |", "|---|---|---|"]
    body = "\n".join(header + lines)
    print("\nOutputs produced by this run")
    print(body)

    if args.ok_list:
        Path(args.ok_list).write_text(
            "".join(f"{t}\n" for t in ok), encoding="utf-8")
        print(f"\nWrote {len(ok)} uploadable table(s) to {args.ok_list}")

    summary_path = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary_path:
        with open(summary_path, "a", encoding="utf-8") as fh:
            fh.write("## Tables produced by this run\n\n")
            fh.write(body + "\n\n")
            if excused:
                fh.write(f"Declared unreachable, so the copy already on Hugging Face "
                         f"stays as it is: {', '.join(sorted(excused))}. "
                         f"Re-measure by {RECHECK_AFTER.isoformat()}.\n\n")
            if missing:
                fh.write(f"**Missing, empty or unreadable: "
                         f"{', '.join(sorted(missing))}** - not uploaded.\n\n")
            if expired:
                fh.write(f"**The unreachable declaration expired on "
                         f"{RECHECK_AFTER.isoformat()}.**\n\n")

    if excused:
        print(f"\nDeclared unreachable ({', '.join(sorted(excused))}); the copy "
              f"already on Hugging Face stays as it is.")
        print(f"  {_FG_REASON}")
        print(f"  Re-measure by {RECHECK_AFTER.isoformat()}.")

    if expired:
        print(f"\nERROR: the unreachable declaration for "
              f"{', '.join(sorted(excused))} expired on "
              f"{RECHECK_AFTER.isoformat()}.")
        print("Re-measure whether the source is still blocked, then lift the "
              "entry or move RECHECK_AFTER in scripts/check_outputs.py.")
        return 1

    if missing:
        print(f"\nERROR: expected table(s) missing, empty or unreadable: "
              f"{', '.join(sorted(missing))}")
        print("Those are not uploaded; the rest of this run still is. "
              "The job fails so the gap is visible instead of silently "
              "leaving the previous copy in place.")
        return 1

    print("\nEvery expected table was produced.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
