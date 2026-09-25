"""Write every mart built by `dbt build` to parquet for publishing.

The set of marts is read from models/marts/*.sql, so a mart that failed to
build (or a table left over from an older build) cannot slip through: the
script fails unless the database holds exactly those tables, each non-empty.

Usage: python export_marts.py <duckdb file> <output dir>
"""
import pathlib
import sys

import duckdb

HERE = pathlib.Path(__file__).resolve().parent


def expected_marts() -> list[str]:
    return sorted(p.stem for p in (HERE / "models" / "marts").glob("*.sql"))


def main(db_path: str, out_dir: str) -> int:
    want = expected_marts()
    if not want:
        print("no mart models found under models/marts")
        return 1
    con = duckdb.connect(db_path, read_only=True)
    have = sorted(
        r[0]
        for r in con.execute(
            "select table_name from information_schema.tables "
            "where table_schema = 'main' and table_type = 'BASE TABLE' "
            "and starts_with(table_name, 'mart_')"
        ).fetchall()
    )
    if have != want:
        print(f"marts in the database {have} != mart models {want}")
        return 1
    out = pathlib.Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    for name in want:
        n = con.execute(f'select count(*) from "{name}"').fetchone()[0]
        if n == 0:
            print(f"{name} is empty; refusing to publish it")
            return 1
        target = out / f"{name}.parquet"
        con.execute(f"copy \"{name}\" to '{target.as_posix()}' (format parquet)")
        back = con.execute(f"select count(*) from '{target.as_posix()}'").fetchone()[0]
        if back != n:
            print(f"{target} has {back} rows, expected {n}")
            return 1
        print(f"{name}: {n} rows -> {target}")
    return 0


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print(__doc__)
        sys.exit(2)
    sys.exit(main(sys.argv[1], sys.argv[2]))
