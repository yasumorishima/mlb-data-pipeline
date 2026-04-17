"""
Backfill orchestrator for mlb-data-pipeline (RPi5 + local Parquet).

Runs one work unit per invocation. Tracks progress via JSON checkpoint.
Designed for a systemd timer (3h cadence): each firing picks the next pending
(or previously-failed) unit and executes the underlying fetcher as a subprocess.

Work units:
  - statcast_<YYYY>    : per-year statcast pitches (11 units, 2015-2025)
  - fg_batting         : FanGraphs batting
  - fg_pitching        : FanGraphs pitching
  - fg_pitcher_plus    : FanGraphs Stuff+/Location+/Pitching+
  - fielding_running   : sprint_speed / oaa / oaa_team / catcher (bundled)
  - park_factors       : savant park factors
  - savant_leaderboards: 7 savant leaderboard tables (bundled)

Expected env (via systemd service or manual export):
  MLB_DATA_TARGET=parquet
  MLB_PARQUET_ROOT=/mnt/ssd/mlb_shared
  MLB_VENV_PYTHON=/mnt/ssd/venv/bin/python   (optional; defaults to sys.executable)
  DISCORD_WEBHOOK_URL=<webhook>              (optional; failure + completion notifications)

Usage:
  python scripts/run_backfill.py                 # run next pending unit
  python scripts/run_backfill.py --status        # print progress table and exit
  python scripts/run_backfill.py --dry-run       # show next unit without running
  python scripts/run_backfill.py --force-unit statcast_2024  # re-run specific unit
"""

from __future__ import annotations

import argparse
import fcntl
import json
import os
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable

SCRIPT_DIR = Path(__file__).parent
REPO_DIR = SCRIPT_DIR.parent
VENV_PYTHON = os.environ.get("MLB_VENV_PYTHON", sys.executable)

PARQUET_ROOT = Path(
    os.environ.get("MLB_PARQUET_ROOT", str(REPO_DIR / "data" / "parquet_out"))
)
STATE_PATH = PARQUET_ROOT / ".state.json"

START_YEAR = 2015
END_YEAR = 2025
STATCAST_YEARS = list(range(START_YEAR, END_YEAR + 1))

# Unit definition: (unit_id, fetcher_script, [extra_args])
WORK_UNITS: list[tuple[str, str, list[str]]] = [
    *[
        (f"statcast_{year}", "fetch_statcast_pitches.py", ["--years", str(year)])
        for year in STATCAST_YEARS
    ],
    ("fg_batting", "fetch_fangraphs.py", ["--batting-only"]),
    ("fg_pitching", "fetch_fangraphs.py", ["--pitching-only"]),
    ("fg_pitcher_plus", "fetch_fangraphs.py", ["--plus-only"]),
    ("fielding_running", "fetch_fielding_running.py", []),
    ("park_factors", "fetch_park_factors.py", []),
    ("savant_leaderboards", "fetch_savant_leaderboards.py", []),
]

# Per-unit timeout. Statcast full-year can occasionally take >1h on Savant slowness.
UNIT_TIMEOUT_SEC = 3 * 3600


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def load_state() -> dict:
    if STATE_PATH.exists():
        return json.loads(STATE_PATH.read_text())
    return {
        "mode": "backfill",
        "units": {uid: {"status": "pending"} for uid, _, _ in WORK_UNITS},
        "last_run": None,
    }


def update_state(mutator: Callable[[dict], None]) -> dict:
    """Atomic read-modify-write on state.json under exclusive file lock.

    Prevents races when multiple orchestrator processes run concurrently
    (e.g. systemd timer fire overlapping with a manual --force-unit run).
    `mutator` mutates the state dict in-place and returns None.
    Returns the post-mutation state.
    """
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    lock_path = STATE_PATH.with_suffix(".lock")
    with open(lock_path, "w") as lock_file:
        fcntl.flock(lock_file, fcntl.LOCK_EX)
        state = load_state()
        mutator(state)
        state["last_run"] = _now()
        tmp = STATE_PATH.with_suffix(".tmp")
        tmp.write_text(json.dumps(state, indent=2))
        tmp.replace(STATE_PATH)
    return state


def pick_next_unit(state: dict, force: str | None = None):
    if force:
        for uid, script, args in WORK_UNITS:
            if uid == force:
                return (uid, script, args)
        return None

    # Priority: pending first, then failed (retry)
    for status_pref in ("pending", "failed"):
        for uid, script, args in WORK_UNITS:
            if state["units"].get(uid, {}).get("status") == status_pref:
                return (uid, script, args)
    return None


def run_unit(uid: str, script: str, args: list[str]):
    cmd = [VENV_PYTHON, str(SCRIPT_DIR / script), *args]
    print(f"[{uid}] Running: {' '.join(cmd)}")
    start = time.time()
    try:
        result = subprocess.run(
            cmd,
            cwd=str(SCRIPT_DIR),
            capture_output=True,
            text=True,
            timeout=UNIT_TIMEOUT_SEC,
            check=False,
        )
    except subprocess.TimeoutExpired:
        return False, f"timeout after {UNIT_TIMEOUT_SEC}s"
    except Exception as e:
        return False, f"{type(e).__name__}: {e}"

    elapsed = time.time() - start
    if result.returncode == 0:
        print(f"[{uid}] OK in {elapsed:.0f}s")
        return True, None

    err = (
        f"exit={result.returncode} ({elapsed:.0f}s)\n"
        f"stderr tail:\n{result.stderr[-1500:]}"
    )
    print(f"[{uid}] FAIL in {elapsed:.0f}s: exit={result.returncode}")
    return False, err


def notify_discord(msg: str) -> None:
    webhook = os.environ.get("DISCORD_WEBHOOK_URL")
    if not webhook:
        return
    try:
        import urllib.request

        req = urllib.request.Request(
            webhook,
            data=json.dumps({"content": f"[MLB] {msg}"[:1900]}).encode("utf-8"),
            headers={"Content-Type": "application/json"},
        )
        with urllib.request.urlopen(req, timeout=10):
            pass
    except Exception as e:
        print(f"Discord notify failed: {e}")


def progress_counts(state: dict) -> tuple[int, int, int]:
    units = state["units"]
    n_total = len(WORK_UNITS)
    n_done = sum(1 for uid, _, _ in WORK_UNITS if units.get(uid, {}).get("status") == "completed")
    n_failed = sum(1 for uid, _, _ in WORK_UNITS if units.get(uid, {}).get("status") == "failed")
    return n_done, n_failed, n_total


def print_status(state: dict) -> None:
    n_done, n_failed, n_total = progress_counts(state)
    print(f"Mode: {state.get('mode')}")
    print(f"Progress: {n_done}/{n_total} completed, {n_failed} failed")
    print(f"Last run: {state.get('last_run')}")
    print()
    icons = {
        "completed": "✓",
        "pending": "·",
        "failed": "✗",
        "in_progress": "→",
    }
    for uid, _, _ in WORK_UNITS:
        unit = state["units"].get(uid, {})
        status = unit.get("status", "?")
        icon = icons.get(status, "?")
        extra = ""
        if status == "failed":
            extra = f"  (error: {(unit.get('error') or '')[:80]})"
        print(f"  {icon} {uid}: {status}{extra}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[1])
    parser.add_argument("--status", action="store_true", help="Print progress and exit")
    parser.add_argument("--dry-run", action="store_true", help="Show next unit without running")
    parser.add_argument("--force-unit", type=str, help="Force specific unit_id to run")
    args = parser.parse_args()

    state = load_state()

    if args.status:
        print_status(state)
        return 0

    unit = pick_next_unit(state, force=args.force_unit)
    if unit is None:
        # All units done
        if state.get("mode") == "backfill":
            def _to_complete(s: dict) -> None:
                s["mode"] = "complete"

            state = update_state(_to_complete)
            n_done, _, n_total = progress_counts(state)
            notify_discord(f"🎉 Backfill complete ({n_done}/{n_total} units)")
            print(f"Backfill complete: {n_done}/{n_total} units. Mode -> complete.")
        else:
            print("No units to run (mode=complete).")
        return 0

    uid, script, extra_args = unit

    if args.dry_run:
        print(f"[dry-run] next unit: {uid} -> {script} {extra_args}")
        return 0

    def _mark_in_progress(s: dict) -> None:
        s["units"][uid] = {"status": "in_progress", "last_attempt": _now()}

    update_state(_mark_in_progress)

    ok, err = run_unit(uid, script, extra_args)

    if ok:
        def _mark_done(s: dict) -> None:
            s["units"][uid] = {"status": "completed", "last_attempt": _now()}

        state = update_state(_mark_done)
    else:
        err_msg = err or ""

        def _mark_failed(s: dict) -> None:
            s["units"][uid] = {
                "status": "failed",
                "last_attempt": _now(),
                "error": err_msg,
            }

        state = update_state(_mark_failed)
        notify_discord(f"Unit {uid} failed: {err_msg[:400]}")

    n_done, n_failed, n_total = progress_counts(state)
    print(f"Progress: {n_done}/{n_total} completed, {n_failed} failed")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
