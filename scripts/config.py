"""
Shared configuration for mlb-data-pipeline.

Constants, BQ auth, column sanitization, retry logic.
All scripts import from here to ensure consistency.
"""

from __future__ import annotations

import os
import re
import time
from pathlib import Path

import pandas as pd

# =====================================================================
# BigQuery
# =====================================================================
BQ_PROJECT = "data-platform-490901"
BQ_DATASET = "mlb_shared"
BQ_FULL = f"{BQ_PROJECT}.{BQ_DATASET}"

# =====================================================================
# Data range
# =====================================================================
START_SEASON = 2015
END_SEASON = 2025

# =====================================================================
# Output
# =====================================================================
DATA_DIR = Path(__file__).parent.parent / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

# =====================================================================
# Retry
# =====================================================================
MAX_RETRIES = 3
RETRY_DELAY = 5  # seconds (multiplied by attempt number)


def fetch_with_retry(func, *args, **kwargs):
    """Execute API call with exponential backoff retry."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            if attempt < MAX_RETRIES:
                wait = RETRY_DELAY * attempt
                print(f"    retry {attempt}/{MAX_RETRIES} (wait {wait}s): {e}")
                time.sleep(wait)
            else:
                raise


# =====================================================================
# Column sanitization (unified rule for all tables)
# =====================================================================
def sanitize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Sanitize column names for BigQuery compatibility.

    Rules (unified across all projects):
      %  -> _pct
      /  -> _per_
      +  -> _plus
      other special chars -> _
      collapse consecutive underscores
      strip leading/trailing underscores
      prefix leading digit with _
    """
    rename = {}
    for col in df.columns:
        new = col
        new = new.replace("%", "_pct")
        new = new.replace("/", "_per_")
        new = new.replace("+", "_plus")
        new = re.sub(r"[^a-zA-Z0-9_]", "_", new)
        new = re.sub(r"_+", "_", new)
        new = new.strip("_")
        if new and new[0].isdigit():
            new = f"_{new}"
        if not new:
            new = f"col_{id(col)}"
        if new != col:
            rename[col] = new
    if rename:
        df = df.rename(columns=rename)

    # Deduplicate (BQ is case-insensitive)
    seen: dict[str, int] = {}
    new_cols = []
    for col in df.columns:
        key = col.lower()
        if key in seen:
            seen[key] += 1
            new_cols.append(f"{col}_{seen[key]}")
        else:
            seen[key] = 0
            new_cols.append(col)
    if new_cols != list(df.columns):
        df.columns = new_cols

    return df


# =====================================================================
# GCP Authentication
# =====================================================================
def get_bq_client():
    """Get authenticated BigQuery client.

    Priority:
      1. GCP_SA_KEY env var (GitHub Actions — base64 or raw JSON)
      2. GOOGLE_APPLICATION_CREDENTIALS env var
      3. Local key file (~/.claude/gcp-sa-key.json)
    """
    from google.cloud import bigquery

    sa_key = os.environ.get("GCP_SA_KEY")
    if sa_key and not os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"):
        key_path = Path("/tmp/gcp-sa-key.json")
        key_path.write_text(sa_key)
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(key_path)

    cred_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    if not cred_path:
        local_key = Path(r"C:\Users\fw_ya\.claude\gcp-sa-key.json")
        if local_key.exists():
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(local_key)

    client = bigquery.Client(project=BQ_PROJECT)

    # Ensure dataset exists (auto-create on first run)
    dataset_ref = bigquery.DatasetReference(BQ_PROJECT, BQ_DATASET)
    dataset = bigquery.Dataset(dataset_ref)
    dataset.location = "US"
    client.create_dataset(dataset, exists_ok=True)

    return client


# =====================================================================
# MLBAM ID mapping
# =====================================================================
def map_fg_to_mlbam(df: pd.DataFrame) -> pd.DataFrame:
    """Map FanGraphs IDfg to MLBAM player_id via Chadwick register.

    Expects df to have 'IDfg' column. Adds 'player_id' (MLBAM int).
    Renames 'IDfg' to 'fg_id' (kept for backward compatibility with
    baseball-mlops which uses FG ID as a join key).
    Drops rows without MLBAM mapping.
    """
    from pybaseball import chadwick_register

    print("  Mapping FanGraphs ID -> MLBAM ID via Chadwick register...")
    reg = chadwick_register()
    valid = reg[reg["key_fangraphs"].notna() & reg["key_mlbam"].notna()]
    fg_to_mlbam = dict(zip(
        valid["key_fangraphs"].astype(int),
        valid["key_mlbam"].astype(int),
    ))

    df = df.copy()
    df["player_id"] = df["IDfg"].astype(int).map(fg_to_mlbam)
    n_mapped = df["player_id"].notna().sum()
    n_total = len(df)
    print(f"  Mapped: {n_mapped}/{n_total} ({n_mapped / n_total * 100:.1f}%)")

    df = df.dropna(subset=["player_id"]).copy()
    df["player_id"] = df["player_id"].astype(int)
    # Keep FG ID as fg_id (baseball-mlops uses it as join key)
    df = df.rename(columns={"IDfg": "fg_id"})
    return df
