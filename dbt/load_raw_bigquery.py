"""Load the raw parquet tables that the dbt sources read into BigQuery.

DuckDB reads the Hugging Face parquet over HTTPS; BigQuery cannot, so this
copies each source table from one dataset revision into BigQuery with a batch
load job (free, and allowed in the BigQuery sandbox, unlike streaming or DML).
Each load replaces the table and is checked against the parquet row count.

Usage: python load_raw_bigquery.py <dataset revision sha>

Environment:
  BQ_PROJECT       GCP project (default mlb-marts-sandbox)
  BQ_RAW_DATASET   dataset for the raw tables (default mlb_raw)
  BQ_ACCESS_TOKEN  optional OAuth access token; otherwise Application Default
                   Credentials (what google-github-actions/auth sets up)
"""
import datetime
import os
import pathlib
import re
import sys
import tempfile
import urllib.request

import pyarrow.parquet as pq
import yaml
from google.api_core.exceptions import NotFound
from google.cloud import bigquery

HERE = pathlib.Path(__file__).resolve().parent
HF = "https://huggingface.co/datasets/yasumorishima/mlb-stats/resolve/{rev}/{name}.parquet"

# A sandbox table lives at most 60 days from its creation. A truncating load
# keeps the creation time, and asking for an expiry past creation + 60 days is
# refused with 403 "Billing has not been enabled ... Table expiration time
# must be less than 60 days while in sandbox mode" (from 2026-09-29 on, two
# days after the tables were created, every run failed this way). So the
# expiry is capped at that limit, and a table with less than RENEW left is
# dropped and loaded again, which starts a new 60 days. This job runs only
# after a successful weekly refresh or a push, so RENEW leaves room for
# several missed weeks.
LIFE = datetime.timedelta(days=60)
MARGIN = datetime.timedelta(hours=1)
RENEW = datetime.timedelta(days=30)


def expiry(created: datetime.datetime, now: datetime.datetime) -> datetime.datetime:
    """59 days from now, but never past what the sandbox allows."""
    e = min(now + datetime.timedelta(days=59), created + LIFE - MARGIN)
    return e.replace(microsecond=e.microsecond // 1000 * 1000)  # BigQuery keeps ms


def needs_renewal(created: datetime.datetime, now: datetime.datetime) -> bool:
    """True when the table can no longer be kept for RENEW more days."""
    return created + LIFE - MARGIN - now < RENEW


def source_tables() -> list[str]:
    doc = yaml.safe_load((HERE / "models" / "sources.yml").read_text(encoding="utf-8"))
    (raw,) = [s for s in doc["sources"] if s["name"] == "raw"]
    return [t["name"] for t in raw["tables"]]


def client(project: str) -> bigquery.Client:
    token = os.environ.get("BQ_ACCESS_TOKEN")
    if token:
        from google.oauth2.credentials import Credentials
        return bigquery.Client(project=project, credentials=Credentials(token))
    return bigquery.Client(project=project)


def main(rev: str) -> int:
    if not re.fullmatch(r"[0-9a-f]{40}", rev):
        print(f"not a revision sha: {rev!r}")
        return 1
    project = os.environ.get("BQ_PROJECT", "mlb-marts-sandbox")
    dataset = os.environ.get("BQ_RAW_DATASET", "mlb_raw")
    bq = client(project)
    config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    marker = f"yasumorishima/mlb-stats@{rev}"
    names = source_tables()
    # The sandbox's 10 GiB storage allowance is for life and is not given back
    # when data is deleted, so a revision already in place is not loaded again.
    now = datetime.datetime.now(datetime.timezone.utc)
    current, renew = [], set()
    for name in names:
        try:
            t = bq.get_table(f"{project}.{dataset}.{name}")
        except NotFound:
            current.append(False)
            continue
        if needs_renewal(t.created, now):
            renew.add(name)
        current.append(t.description == marker and name not in renew)
    if all(current):
        # Off-season refreshes can leave the dataset revision unchanged for
        # months; without this the raw tables would expire under the weekly
        # build (and the dashboard reading the marts). A metadata update, not
        # a load, so it costs no storage.
        for name in names:
            t = bq.get_table(f"{project}.{dataset}.{name}")
            t.expires = expires = expiry(t.created, now)
            if bq.update_table(t, ["expires"]).expires != expires:
                print(f"{name}: expiry not moved to {expires}")
                return 1
            print(f"{name}: created {t.created:%Y-%m-%d}, expires {expires:%Y-%m-%d}")
        print(f"{dataset} already holds {marker}; nothing loaded")
        return 0
    with tempfile.TemporaryDirectory() as tmp:
        for name in names:
            path = pathlib.Path(tmp) / f"{name}.parquet"
            with urllib.request.urlopen(HF.format(rev=rev, name=name), timeout=120) as r:
                path.write_bytes(r.read())
            want = pq.ParquetFile(path).metadata.num_rows
            table = f"{project}.{dataset}.{name}"
            if name in renew:
                # A new table gets a new 60 days; a truncating load would not.
                bq.delete_table(table, not_found_ok=True)
                print(f"{name}: dropped to renew its 60 days")
            with path.open("rb") as f:
                bq.load_table_from_file(f, table, job_config=config).result(timeout=600)
            t = bq.get_table(table)
            if t.num_rows != want:
                print(f"{table}: {t.num_rows} rows loaded, parquet has {want}")
                return 1
            got = t.num_rows
            # Marked only after the count checks, so a failed run is reloaded.
            t.description, t.expires = marker, expiry(t.created, now)
            expires = t.expires
            if bq.update_table(t, ["description", "expires"]).expires != expires:
                print(f"{table}: expiry not set to {expires}")
                return 1
            print(f"{name}: {got} rows -> {table}, created {t.created:%Y-%m-%d}, expires {expires:%Y-%m-%d}")
    return 0


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print(__doc__)
        sys.exit(2)
    sys.exit(main(sys.argv[1]))
