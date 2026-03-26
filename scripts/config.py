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
      trailing -  -> _minus  (e.g. ERA- -> ERA_minus, avoids collision with ERA)
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
        # Trailing minus (FanGraphs normalized stats: ERA-, FIP-, xFIP-)
        new = re.sub(r"-$", "_minus", new)
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
# Data quality validation
# =====================================================================
def validate_dataframe(
    df: pd.DataFrame,
    table_name: str,
    expected_years: tuple[int, int] | None = None,
    required_cols: list[str] | None = None,
    min_rows_per_year: int = 10,
    max_null_pct: float = 50.0,
) -> bool:
    """Validate DataFrame quality after fetch and before BQ upload.

    Prints detailed report and returns True if all checks pass.
    """
    print(f"\n{'─' * 60}")
    print(f"VALIDATION: {table_name}")
    print(f"{'─' * 60}")

    ok = True

    # --- Shape ---
    print(f"  Shape: {df.shape[0]:,} rows × {df.shape[1]} cols")
    if df.empty:
        print(f"  ❌ EMPTY DataFrame")
        return False

    # --- Year coverage ---
    year_col = next((c for c in ["season", "game_year"] if c in df.columns), None)
    if year_col and expected_years:
        years_present = sorted(df[year_col].dropna().unique())
        years_expected = list(range(expected_years[0], expected_years[1] + 1))
        missing_years = [y for y in years_expected if y not in years_present]
        extra_years = [y for y in years_present if y not in years_expected]

        print(f"  Years: {min(years_present)}-{max(years_present)} "
              f"({len(years_present)} present)")
        if missing_years:
            print(f"  ⚠ Missing years: {missing_years}")
            ok = False
        if extra_years:
            print(f"  ℹ Extra years: {extra_years}")

        # Per-year row counts
        print(f"  Per-year rows:")
        for y in sorted(years_present):
            n = (df[year_col] == y).sum()
            flag = " ⚠ LOW" if n < min_rows_per_year else ""
            print(f"    {int(y)}: {n:>6,}{flag}")
            if n < min_rows_per_year:
                ok = False

    # --- Required columns ---
    if required_cols:
        missing_cols = [c for c in required_cols if c not in df.columns]
        if missing_cols:
            print(f"  ❌ Missing required columns: {missing_cols}")
            ok = False
        else:
            print(f"  ✓ All {len(required_cols)} required columns present")

    # --- Null rates ---
    null_pcts = df.isnull().mean() * 100
    high_null = null_pcts[null_pcts > max_null_pct].sort_values(ascending=False)
    zero_null = (null_pcts == 0).sum()
    any_null = (null_pcts > 0).sum()

    print(f"  Nulls: {zero_null} cols 0% null, {any_null} cols with nulls")
    if len(high_null) > 0:
        print(f"  ⚠ Columns >{max_null_pct}% null:")
        for col, pct in high_null.head(10).items():
            print(f"    {col}: {pct:.1f}%")

    # --- Duplicate check ---
    key_cols = [c for c in ["player_id", year_col] if c and c in df.columns]
    if len(key_cols) >= 2:
        n_dups = df.duplicated(subset=key_cols).sum()
        if n_dups > 0:
            print(f"  ⚠ {n_dups} duplicate rows on {key_cols}")
        else:
            print(f"  ✓ No duplicates on {key_cols}")

    # --- Column name sanitization preview ---
    sanitized = sanitize_columns(df.head(0).copy())
    renamed = [(old, new) for old, new in zip(df.columns, sanitized.columns) if old != new]
    if renamed:
        print(f"  Sanitize preview ({len(renamed)} cols renamed):")
        for old, new in renamed[:5]:
            print(f"    {old} → {new}")
        if len(renamed) > 5:
            print(f"    ... and {len(renamed) - 5} more")

    status = "✓ PASS" if ok else "⚠ WARNINGS"
    print(f"  Result: {status}")
    return ok


def validate_bq_table(table_name: str) -> None:
    """Validate a BQ table after upload — row count and schema check."""
    from google.cloud import bigquery

    client = get_bq_client()
    table_ref = f"{BQ_FULL}.{table_name}"
    table = client.get_table(table_ref)

    print(f"  BQ verify: {table_ref}")
    print(f"    Rows: {table.num_rows:,}")
    print(f"    Size: {table.num_bytes / 1024**2:.1f} MB")
    print(f"    Cols: {len(table.schema)} ({', '.join(f.name for f in table.schema[:8])}...)")


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
