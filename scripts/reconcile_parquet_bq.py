"""
Compare row counts between local Parquet and legacy BQ mlb_shared.

Run this after the Phase 3 backfill reaches 17/17 and before deleting
the BQ dataset (`bq rm -r -f -d data-platform-490901:mlb_shared`).

Layout assumption (matches `config.write_dataframe`):
  <PARQUET_ROOT>/<table>/<table>[<suffix>].parquet

`statcast_pitches` is partitioned as `statcast_pitches_<YYYY>.parquet`;
all other tables are a single file per table.

The script has three modes so Parquet (RPi5, no BQ auth) and BQ (host with
SA key) can be reconciled from different machines:

  dump         — read Parquet only, write manifest JSON. Needs no BQ auth.
  compare      — read BQ + manifest JSON, print the reconciliation table.
  reconcile    — read both Parquet and BQ in one pass (default; requires both).

Usage:
  # On RPi5 (Parquet host, no BQ auth)
  MLB_PARQUET_ROOT=/mnt/ssd/mlb_shared \\
    python scripts/reconcile_parquet_bq.py dump --out /tmp/parquet_manifest.json

  # Copy manifest to a host with BQ credentials, then
  python scripts/reconcile_parquet_bq.py compare --manifest parquet_manifest.json

  # One-shot on a host that has both (local dev with SSHFS mount, etc.)
  MLB_PARQUET_ROOT=... python scripts/reconcile_parquet_bq.py reconcile

Exit codes (for `compare` and `reconcile`):
  0 = every BQ table has matching Parquet within tolerance
  1 = at least one BQ table has no Parquet or exceeds tolerance
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

import pyarrow.parquet as pq


def _collect_parquet(parquet_root: Path) -> dict[str, dict]:
    """Return {table: {rows, files, file_list}} by reading Parquet footers only."""
    out: dict[str, dict] = {}
    for table_dir in sorted(p for p in parquet_root.iterdir() if p.is_dir()):
        rows = 0
        files: list[dict] = []
        for p in sorted(table_dir.glob("*.parquet")):
            n = pq.read_metadata(p).num_rows
            rows += n
            files.append({"name": p.name, "rows": n})
        if files:
            out[table_dir.name] = {
                "rows": rows,
                "file_count": len(files),
                "files": files,
            }
    return out


def cmd_dump(args: argparse.Namespace) -> int:
    from config import PARQUET_ROOT

    parquet_root = Path(args.parquet_root) if args.parquet_root else PARQUET_ROOT
    if not parquet_root.is_dir():
        print(f"Parquet root not found: {parquet_root}")
        return 1

    manifest = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "parquet_root": str(parquet_root),
        "tables": _collect_parquet(parquet_root),
    }
    Path(args.out).write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    n_tables = len(manifest["tables"])
    n_rows = sum(t["rows"] for t in manifest["tables"].values())
    print(f"Wrote {args.out}: {n_tables} tables, {n_rows:,} total rows")
    return 0


def _print_report(
    bq_totals: dict[str, tuple[int, int]],  # table -> (rows, cols)
    parquet_tables: dict[str, dict],
    bq_full: str,
    parquet_root: str,
    tolerance_pct: float,
) -> int:
    print(f"\nReconcile: {bq_full}  vs  {parquet_root}")
    print(f"Tolerance: ±{tolerance_pct:.3f}%")
    print("=" * 82)
    print(
        f"  {'table':<26} {'BQ rows':>12} {'Parquet rows':>14} {'files':>5}"
        f"  {'diff':>10}  status"
    )
    print("-" * 82)

    missing: list[str] = []
    mismatches: list[str] = []
    extras: list[str] = []

    for table_id in sorted(bq_totals):
        bq_rows, _ = bq_totals[table_id]
        pq_info = parquet_tables.get(table_id, {"rows": 0, "file_count": 0})
        pq_rows = pq_info["rows"]
        n_files = pq_info["file_count"]
        diff = pq_rows - bq_rows

        if pq_rows == 0 and bq_rows == 0:
            status = "OK"  # both sides empty, agreement is trivial
        elif pq_rows == 0:
            status = "MISSING"
            missing.append(table_id)
        else:
            limit = bq_rows * tolerance_pct / 100.0
            if abs(diff) <= limit:
                status = "OK"
            else:
                status = "DIFF"
                mismatches.append(
                    f"{table_id}: BQ={bq_rows:,} Parquet={pq_rows:,} diff={diff:+,}"
                )

        print(
            f"  {table_id:<26} {bq_rows:>12,} {pq_rows:>14,} {n_files:>5}"
            f"  {diff:>+10,}  [{status}]"
        )

    # Tables that exist in Parquet but not in BQ (useful for surfacing drift)
    for table_id in sorted(parquet_tables):
        if table_id not in bq_totals:
            extras.append(table_id)
            pq_info = parquet_tables[table_id]
            print(
                f"  {table_id:<26} {'—':>12} {pq_info['rows']:>14,}"
                f" {pq_info['file_count']:>5}  {'':>10}  [PARQUET-ONLY]"
            )

    print("=" * 82)
    if missing:
        print(f"MISSING Parquet ({len(missing)}): {', '.join(missing)}")
    if mismatches:
        print(f"ROW MISMATCH ({len(mismatches)}):")
        for m in mismatches:
            print(f"  {m}")
    if extras:
        print(f"PARQUET-ONLY ({len(extras)}): {', '.join(extras)}  (informational)")
    if not missing and not mismatches:
        print("All BQ tables reconcile with Parquet. Safe to delete BQ dataset.")
        return 0
    return 1


def _get_bq_totals(table_filter: str | None) -> tuple[dict[str, tuple[int, int]], str]:
    from config import BQ_FULL, get_bq_client

    client = get_bq_client()
    bq_tables = list(client.list_tables(BQ_FULL))
    totals: dict[str, tuple[int, int]] = {}
    for t in bq_tables:
        if table_filter and t.table_id != table_filter:
            continue
        tbl = client.get_table(t.reference)
        totals[t.table_id] = (tbl.num_rows, len(tbl.schema))
    return totals, BQ_FULL


def cmd_compare(args: argparse.Namespace) -> int:
    manifest = json.loads(Path(args.manifest).read_text(encoding="utf-8"))
    parquet_tables = manifest["tables"]
    parquet_root = manifest["parquet_root"]
    bq_totals, bq_full = _get_bq_totals(args.table)

    if args.table:
        parquet_tables = {k: v for k, v in parquet_tables.items() if k == args.table}

    return _print_report(
        bq_totals, parquet_tables, bq_full, parquet_root, args.tolerance_pct
    )


def cmd_reconcile(args: argparse.Namespace) -> int:
    from config import PARQUET_ROOT

    parquet_root = Path(args.parquet_root) if args.parquet_root else PARQUET_ROOT
    parquet_tables = _collect_parquet(parquet_root) if parquet_root.is_dir() else {}
    bq_totals, bq_full = _get_bq_totals(args.table)

    if args.table:
        parquet_tables = {k: v for k, v in parquet_tables.items() if k == args.table}

    return _print_report(
        bq_totals, parquet_tables, bq_full, str(parquet_root), args.tolerance_pct
    )


def main() -> int:
    doc = (__doc__ or "").strip()
    summary = doc.splitlines()[0] if doc else ""
    parser = argparse.ArgumentParser(description=summary)
    sub = parser.add_subparsers(dest="mode", required=True)

    p_dump = sub.add_parser("dump", help="Write Parquet manifest (no BQ access)")
    p_dump.add_argument("--out", required=True, help="Manifest JSON output path")
    p_dump.add_argument("--parquet-root", help="Override MLB_PARQUET_ROOT")
    p_dump.set_defaults(func=cmd_dump)

    p_compare = sub.add_parser("compare", help="Compare BQ against a Parquet manifest")
    p_compare.add_argument("--manifest", required=True, help="Manifest JSON from `dump`")
    p_compare.add_argument("--table", help="Restrict to a single table")
    p_compare.add_argument("--tolerance-pct", type=float, default=0.0)
    p_compare.set_defaults(func=cmd_compare)

    p_recon = sub.add_parser(
        "reconcile", help="Read Parquet and BQ on the same host"
    )
    p_recon.add_argument("--parquet-root", help="Override MLB_PARQUET_ROOT")
    p_recon.add_argument("--table", help="Restrict to a single table")
    p_recon.add_argument("--tolerance-pct", type=float, default=0.0)
    p_recon.set_defaults(func=cmd_reconcile)

    args = parser.parse_args()
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
