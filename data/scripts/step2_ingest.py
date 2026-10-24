"""Step 2: Bronze / raw ingestion for the Retail Banking Data Engineering Pipeline.

Ingests all eight source files (CSV + JSON) into a local DuckDB database.
Preserves original source files. Records metadata for every file.
Never silently repairs malformed JSON; logs errors and continues with valid files.
Safe to rerun: raw tables are replaced each run (full refresh), so no duplicates.
Uses DuckDB + Python standard library only.
"""

from __future__ import annotations

import csv
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import duckdb

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

# This file lives at <root>/data/scripts/step2_ingest.py, so three levels
# up is the repository root: <root>/hcl_hackathon.
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent

DAY1_DIR = PROJECT_ROOT / "data" / "source_day1"
DAY2_DIR = PROJECT_ROOT / "data" / "source_day2"
BRONZE_DIR = PROJECT_ROOT / "data" / "bronze"
BRONZE_DB = BRONZE_DIR / "retail_banking_bronze.duckdb"
ERRORS_REPORT = BRONZE_DIR / "ingestion_errors.txt"

# (source_filename, source_directory, day_label, target_table_name)
SOURCE_FILES = [
    # Day 1
    ("branches.csv", DAY1_DIR, "Day 1", "bronze_branches_day1"),
    ("customers.csv", DAY1_DIR, "Day 1", "bronze_customers_day1"),
    ("products.json", DAY1_DIR, "Day 1", "bronze_products_day1"),
    ("transactions.csv", DAY1_DIR, "Day 1", "bronze_transactions_day1"),
    # Day 2
    ("branches_day2.csv", DAY2_DIR, "Day 2", "bronze_branches_day2"),
    ("customer_updates_day2.csv", DAY2_DIR, "Day 2", "bronze_customer_updates_day2"),
    ("products_day2.json", DAY2_DIR, "Day 2", "bronze_products_day2"),
    ("transactions_day2.csv", DAY2_DIR, "Day 2", "bronze_transactions_day2"),
]

INGESTION_LOG_TABLE = "bronze_ingestion_log"
INGESTION_ERRORS_TABLE = "bronze_ingestion_errors"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def now_utc() -> str:
    """Current UTC timestamp as an ISO string."""
    return datetime.now(timezone.utc).isoformat()


def ensure_bronze_dir() -> None:
    """Create the bronze output folder if it doesn't exist."""
    BRONZE_DIR.mkdir(parents=True, exist_ok=True)


def init_metadata_tables(conn: duckdb.DuckDBPyConnection) -> None:
    """Create (or refresh) the audit tables that track every ingestion run.

    These tables are cleared at the start of each run so the log always
    reflects the most recent execution — this keeps reruns clean and free of
    duplicate metadata rows.
    """
    conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {INGESTION_LOG_TABLE} (
            run_timestamp VARCHAR,
            source_filename VARCHAR,
            source_day VARCHAR,
            target_table VARCHAR,
            source_rows BIGINT,
            ingestion_status VARCHAR,
            error_message VARCHAR
        )
    """)
    conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {INGESTION_ERRORS_TABLE} (
            run_timestamp VARCHAR,
            source_filename VARCHAR,
            source_day VARCHAR,
            error_type VARCHAR,
            error_message VARCHAR,
            error_line BIGINT,
            error_column BIGINT,
            note VARCHAR
        )
    """)
    # Refresh for this run: remove previous run's metadata rows.
    conn.execute(f"DELETE FROM {INGESTION_LOG_TABLE}")
    conn.execute(f"DELETE FROM {INGESTION_ERRORS_TABLE}")


def append_error_report(lines: list[str]) -> None:
    """Append human-readable error lines to the bronze error report file."""
    with ERRORS_REPORT.open("a", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")


def log_ingestion(conn, run_ts, filename, day, target_table, row_count, status, error_msg=""):
    """Insert one summary row into the ingestion log table."""
    conn.execute(
        f"""
        INSERT INTO {INGESTION_LOG_TABLE}
        (run_timestamp, source_filename, source_day, target_table,
         source_rows, ingestion_status, error_message)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        [run_ts, filename, day, target_table, row_count, status, error_msg],
    )


def log_error(conn, run_ts, filename, day, error_type, message, line, col, note):
    """Insert one detailed error row into the ingestion errors table."""
    conn.execute(
        f"""
        INSERT INTO {INGESTION_ERRORS_TABLE}
        (run_timestamp, source_filename, source_day, error_type,
         error_message, error_line, error_column, note)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [run_ts, filename, day, error_type, message, line, col, note],
    )


# ---------------------------------------------------------------------------
# CSV ingestion
# ---------------------------------------------------------------------------

def ingest_csv(conn, run_ts, filename, directory, day, target_table):
    """Read a CSV file into a DuckDB raw table with metadata columns.

    Returns the number of data rows ingested.
    """
    path = directory / filename
    if not path.is_file():
        log_ingestion(conn, run_ts, filename, day, target_table, 0, "MISSING", "File not found")
        append_error_report([
            f"[{run_ts}] MISSING: {filename} ({day}) expected at {path}",
            f"  Action: Place the file in {directory} and rerun Step 2.",
            "",
        ])
        return 0

    rows = []
    columns = []
    try:
        with path.open("r", encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f)
            columns = reader.fieldnames or []
            for row in reader:
                rows.append(row)
    except (OSError, UnicodeDecodeError, csv.Error) as error:
        log_ingestion(conn, run_ts, filename, day, target_table, 0, "READ_ERROR", str(error))
        append_error_report([
            f"[{run_ts}] READ_ERROR: {filename} ({day})",
            f"  Error: {error}",
            f"  Action: Preserve original file; investigate readability.",
            "",
        ])
        return 0

    col_defs = ", ".join([f'"{c}" VARCHAR' for c in columns]) if columns else ""
    metadata_cols = (
        "source_row_number BIGINT, "
        "source_filename VARCHAR, "
        "source_day VARCHAR, "
        "ingestion_timestamp VARCHAR, "
        "ingestion_status VARCHAR, "
    )
    full_defs = metadata_cols + col_defs

    conn.execute(f"CREATE OR REPLACE TABLE {target_table} ({full_defs})")

    insert_cols = [
        "source_row_number", "source_filename", "source_day",
        "ingestion_timestamp", "ingestion_status",
    ] + columns
    col_refs = ", ".join([f'"{c}"' for c in insert_cols])
    placeholders = ", ".join(["?"] * len(insert_cols))

    for i, row in enumerate(rows, start=1):
        values = [i, filename, day, run_ts, "LOADED"] + [row.get(c) for c in columns]
        conn.execute(f"INSERT INTO {target_table} ({col_refs}) VALUES ({placeholders})", values)

    log_ingestion(conn, run_ts, filename, day, target_table, len(rows), "LOADED")
    print(f"  Loaded {len(rows)} rows into {target_table}")
    return len(rows)


# ---------------------------------------------------------------------------
# JSON ingestion
# ---------------------------------------------------------------------------

def metadata_cols_for_json() -> str:
    """Shared metadata column definition for JSON raw tables."""
    return (
        "source_row_number BIGINT, "
        "source_filename VARCHAR, "
        "source_day VARCHAR, "
        "ingestion_timestamp VARCHAR, "
        "ingestion_status VARCHAR, "
    )


def ingest_json(conn, run_ts, filename, directory, day, target_table):
    """Read a JSON file into a DuckDB raw table with metadata columns.

    Returns the number of records ingested, or 0 if the file is missing or
    malformed. Malformed JSON is logged and reported but never silently
    repaired.
    """
    path = directory / filename
    if not path.is_file():
        log_ingestion(conn, run_ts, filename, day, target_table, 0, "MISSING", "File not found")
        append_error_report([
            f"[{run_ts}] MISSING: {filename} ({day}) expected at {path}",
            f"  Action: Place the file in {directory} and rerun Step 2.",
            "",
        ])
        return 0

    try:
        with path.open("r", encoding="utf-8-sig") as f:
            raw = f.read()
        data = json.loads(raw)
    except (OSError, UnicodeDecodeError) as error:
        log_ingestion(conn, run_ts, filename, day, target_table, 0, "READ_ERROR", str(error))
        append_error_report([
            f"[{run_ts}] READ_ERROR: {filename} ({day})",
            f"  Error: {error}",
            f"  Action: Preserve original file; investigate readability.",
            "",
        ])
        return 0
    except json.JSONDecodeError as error:
        log_ingestion(
            conn, run_ts, filename, day, target_table, 0,
            "FAILED_JSON", f"{error.msg} (line {error.lineno}, col {error.colno})",
        )
        log_error(
            conn, run_ts, filename, day,
            "JSON_PARSE_ERROR", error.msg, error.lineno, error.colno,
            "Preserve original file; do not edit. Investigate in cleaning step.",
        )
        append_error_report([
            f"[{run_ts}] JSON_PARSE_ERROR: {filename} ({day})",
            f"  Error: {error.msg}",
            f"  Line: {error.lineno}, Column: {error.colno}",
            f"  Action: Preserve original file; do not silently repair.",
            f"          Investigate and fix in a later cleaning step.",
            "",
        ])
        print(f"  JSON parse failed for {filename}: {error.msg} (line {error.lineno}, col {error.colno})")
        return 0

    if not isinstance(data, list):
        log_ingestion(
            conn, run_ts, filename, day, target_table, 0,
            "FAILED_STRUCTURE", "Expected a JSON array at the top level",
        )
        log_error(
            conn, run_ts, filename, day,
            "JSON_STRUCTURE_ERROR", "Expected a JSON array at the top level",
            None, None, "Preserve original file; investigate structure.",
        )
        append_error_report([
            f"[{run_ts}] JSON_STRUCTURE_ERROR: {filename} ({day})",
            f"  Error: Expected a JSON array at the top level.",
            f"  Action: Preserve original file; investigate structure.",
            "",
        ])
        print(f"  JSON structure failed for {filename}: expected a JSON array")
        return 0

    all_keys = []
    seen = set()
    for record in data:
        if isinstance(record, dict):
            for key in record.keys():
                if key not in seen:
                    seen.add(key)
                    all_keys.append(key)

    col_defs = ", ".join([f'"{k}" VARCHAR' for k in all_keys]) if all_keys else ""
    full_defs = metadata_cols_for_json() + col_defs

    conn.execute(f"CREATE OR REPLACE TABLE {target_table} ({full_defs})")

    insert_cols = [
        "source_row_number", "source_filename", "source_day",
        "ingestion_timestamp", "ingestion_status",
    ] + all_keys
    col_refs = ", ".join([f'"{c}"' for c in insert_cols])
    placeholders = ", ".join(["?"] * len(insert_cols))

    for i, record in enumerate(data, start=1):
        if isinstance(record, dict):
            values = [i, filename, day, run_ts, "LOADED"] + [record.get(k) for k in all_keys]
        else:
            values = [i, filename, day, run_ts, "LOADED"] + [None] * len(all_keys)
        conn.execute(f"INSERT INTO {target_table} ({col_refs}) VALUES ({placeholders})", values)

    log_ingestion(conn, run_ts, filename, day, target_table, len(data), "LOADED")
    print(f"  Loaded {len(data)} records into {target_table}")
    return len(data)


# ---------------------------------------------------------------------------
# Main orchestration
# ---------------------------------------------------------------------------

def print_verification(conn: duckdb.DuckDBPyConnection) -> None:
    """Run and print verification queries after ingestion."""
    print("\n" + "=" * 60)
    print("VERIFICATION QUERIES")
    print("=" * 60)

    # 1. Row counts per raw table
    print("\n-- Raw table row counts --")
    tables = conn.execute(
        "SELECT table_name FROM information_schema.tables "
        "WHERE table_schema='main' AND table_name LIKE 'bronze_%' "
        "AND table_name NOT IN ('bronze_ingestion_log','bronze_ingestion_errors') "
        "ORDER BY table_name"
    ).fetchall()
    for (table_name,) in tables:
        count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        print(f"  {table_name}: {count} rows")

    # 2. Ingestion log summary
    print("\n-- Ingestion log --")
    rows = conn.execute(
        f"SELECT source_filename, source_day, target_table, source_rows, "
        f"ingestion_status, error_message FROM {INGESTION_LOG_TABLE} "
        f"ORDER BY source_filename"
    ).fetchall()
    for filename, day, target_table, source_rows, status, error_message in rows:
        err = f" | error: {error_message}" if error_message else ""
        print(f"  {filename} ({day}) -> {target_table}: {source_rows} rows, status={status}{err}")

    # 3. Ingestion errors detail
    print("\n-- Ingestion errors --")
    err_rows = conn.execute(
        f"SELECT source_filename, error_type, error_message, error_line, error_column "
        f"FROM {INGESTION_ERRORS_TABLE} ORDER BY source_filename"
    ).fetchall()
    if err_rows:
        for filename, error_type, message, line, col in err_rows:
            loc = f" (line {line}, col {col})" if line is not None else ""
            print(f"  {filename}: {error_type}{loc} -> {message}")
    else:
        print("  None")


def main() -> int:
    ensure_bronze_dir()
    run_ts = now_utc()

    print("=" * 60)
    print("RETAIL BANK PIPELINE - STEP 2")
    print("BRONZE / RAW INGESTION")
    print("=" * 60)
    print(f"Project root : {PROJECT_ROOT}")
    print(f"Bronze DB    : {BRONZE_DB}")
    print(f"Run started  : {run_ts}")
    print()

    # Connect to DuckDB (creates the file if it does not exist).
    conn = duckdb.connect(str(BRONZE_DB))
    try:
        init_metadata_tables(conn)

        total_loaded = 0
        total_files = len(SOURCE_FILES)

        for filename, directory, day, target_table in SOURCE_FILES:
            print("-" * 60)
            print(f"File: {filename} ({day})")
            path = directory / filename

            if not path.is_file():
                print("  Status: MISSING - file not found")
                continue

            if filename.lower().endswith(".csv"):
                n = ingest_csv(conn, run_ts, filename, directory, day, target_table)
                total_loaded += n
            elif filename.lower().endswith(".json"):
                n = ingest_json(conn, run_ts, filename, directory, day, target_table)
                total_loaded += n
            else:
                print(f"  Status: SKIPPED - unsupported file type {path.suffix}")

        print()
        print("=" * 60)
        print(f"Files processed: {total_files}")
        print(f"Total rows/records loaded: {total_loaded}")
        print("=" * 60)

        print_verification(conn)

        # Check whether any file failed (missing, read error, or failed JSON).
        failed = conn.execute(
            f"SELECT COUNT(*) FROM {INGESTION_LOG_TABLE} WHERE ingestion_status != 'LOADED'"
        ).fetchone()[0]

        if failed:
            print(f"\nSTEP 2 STATUS: COMPLETED WITH {failed} FAILURE(S)")
            print("Valid files are in the raw layer. Failed file(s) are in the error log.")
            return 0
        else:
            print("\nSTEP 2 STATUS: ALL FILES INGESTED SUCCESSFULLY")
            return 0
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())