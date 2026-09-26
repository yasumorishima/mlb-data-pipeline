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
    current = []
    for name in names:
        try:
            current.append(bq.get_table(f"{project}.{dataset}.{name}").description == marker)
        except NotFound:
            current.append(False)
    if all(current):
        print(f"{dataset} already holds {marker}; nothing loaded")
        return 0
    # Sandbox tables expire after at most 60 days. Set it on every load rather
    # than rely on whether a truncating load keeps the old expiry.
    expires = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(days=59)
    with tempfile.TemporaryDirectory() as tmp:
        for name in names:
            path = pathlib.Path(tmp) / f"{name}.parquet"
            with urllib.request.urlopen(HF.format(rev=rev, name=name), timeout=120) as r:
                path.write_bytes(r.read())
            want = pq.ParquetFile(path).metadata.num_rows
            table = f"{project}.{dataset}.{name}"
            with path.open("rb") as f:
                bq.load_table_from_file(f, table, job_config=config).result(timeout=600)
            t = bq.get_table(table)
            if t.num_rows != want:
                print(f"{table}: {t.num_rows} rows loaded, parquet has {want}")
                return 1
            got = t.num_rows
            # Marked only after the count checks, so a failed run is reloaded.
            t.description, t.expires = marker, expires
            bq.update_table(t, ["description", "expires"])
            print(f"{name}: {got} rows -> {table}")
    return 0


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print(__doc__)
        sys.exit(2)
    sys.exit(main(sys.argv[1]))
