"""Print summary of all tables in mlb_shared BQ dataset."""
from config import get_bq_client, BQ_PROJECT, BQ_DATASET

client = get_bq_client()
ds = f"{BQ_PROJECT}.{BQ_DATASET}"
tables = list(client.list_tables(ds))

sep = "=" * 60
print(f"\n{sep}")
print(f"BQ SUMMARY: {ds}")
print(sep)

total_rows, total_bytes = 0, 0
for t in sorted(tables, key=lambda x: x.table_id):
    tbl = client.get_table(t.reference)
    total_rows += tbl.num_rows
    total_bytes += tbl.num_bytes
    print(
        f"  {t.table_id:<25} {tbl.num_rows:>10,} rows"
        f"  {tbl.num_bytes / 1024**2:>8.1f} MB"
        f"  {len(tbl.schema):>3} cols"
    )

print("-" * 60)
print(
    f"  {'TOTAL':<25} {total_rows:>10,} rows"
    f"  {total_bytes / 1024**3:>8.2f} GB"
    f"  {len(tables)} tables"
)
