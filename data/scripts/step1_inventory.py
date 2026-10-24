"""Step 1: Source file inventory for the Retail Banking Data Engineering Pipeline.

Checks that all eight expected source files exist and are readable,
inspects CSV headers and data row counts, validates JSON syntax and counts
records when valid, and reports parse errors with line and column numbers.

This script only READS source files. It never modifies them.
It uses only Python's standard library (csv, json, pathlib, sys).
"""

from __future__ import annotations

import csv
import json
import sys
from pathlib import Path

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

# This file lives at <root>/data/scripts/step1_inventory.py, so three levels
# up is the repository root: <root>/hcl_hackathon.
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent

DAY1_DIR = PROJECT_ROOT / "data" / "source_day1"
DAY2_DIR = PROJECT_ROOT / "data" / "source_day2"

# Expected source files: filename -> directory.
EXPECTED_FILES = {
    # Day 1
    "branches.csv": DAY1_DIR,
    "customers.csv": DAY1_DIR,
    "products.json": DAY1_DIR,
    "transactions.csv": DAY1_DIR,
    # Day 2
    "branches_day2.csv": DAY2_DIR,
    "customer_updates_day2.csv": DAY2_DIR,
    "products_day2.json": DAY2_DIR,
    "transactions_day2.csv": DAY2_DIR,
}

CSV_FILES = {name: directory for name, directory in EXPECTED_FILES.items() if name.endswith(".csv")}
JSON_FILES = {name: directory for name, directory in EXPECTED_FILES.items() if name.endswith(".json")}

EXIT_SUCCESS = 0
EXIT_FAILURE = 1


# ---------------------------------------------------------------------------
# Inspection helpers
# ---------------------------------------------------------------------------

def inspect_csv(path: Path) -> dict:
    """Read a CSV file without changing its contents.

    Returns a dict with keys: status, columns, row_count, error.
    """
    result = {"status": "PASS", "columns": None, "row_count": 0, "error": None}
    try:
        # utf-8-sig strips a leading BOM if present; newline="" lets csv handle
        # line endings correctly across Windows/Linux/Mac files.
        with path.open("r", encoding="utf-8-sig", newline="") as file:
            reader = csv.DictReader(file)
            result["columns"] = reader.fieldnames
            result["row_count"] = sum(1 for _ in reader)
    except (OSError, UnicodeDecodeError, csv.Error) as error:
        result["status"] = "READ_ERROR"
        result["error"] = str(error)
    return result


def inspect_json(path: Path) -> dict:
    """Check JSON syntax and count records when valid.

    Returns a dict with keys: status, record_count, error, line, column.
    """
    result = {
        "status": "PASS",
        "record_count": None,
        "error": None,
        "line": None,
        "column": None,
    }
    try:
        with path.open("r", encoding="utf-8-sig") as file:
            raw = file.read()
        data = json.loads(raw)
    except (OSError, UnicodeDecodeError) as error:
        result["status"] = "READ_ERROR"
        result["error"] = str(error)
        return result
    except json.JSONDecodeError as error:
        # json.JSONDecodeError exposes lineno and colno (1-based).
        result["status"] = "JSON_PARSE_ERROR"
        result["error"] = error.msg
        result["line"] = error.lineno
        result["column"] = error.colno
        return result

    if not isinstance(data, list):
        result["status"] = "REVIEW"
        result["error"] = "Expected a JSON array at the top level."
        return result

    result["record_count"] = len(data)
    return result


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> int:
    print("=" * 60)
    print("RETAIL BANK PIPELINE - STEP 1")
    print("SOURCE FILE INVENTORY")
    print("=" * 60)
    print(f"Project root: {PROJECT_ROOT}")
    print()

    problems = 0
    checked = 0

    # Inspect in a stable, documented order: Day 1 then Day 2.
    ordered = [
        ("branches.csv", DAY1_DIR),
        ("customers.csv", DAY1_DIR),
        ("products.json", DAY1_DIR),
        ("transactions.csv", DAY1_DIR),
        ("branches_day2.csv", DAY2_DIR),
        ("customer_updates_day2.csv", DAY2_DIR),
        ("products_day2.json", DAY2_DIR),
        ("transactions_day2.csv", DAY2_DIR),
    ]

    for filename, directory in ordered:
        path = directory / filename
        checked += 1
        print("-" * 60)
        print(f"File: {path}")

        if not path.exists():
            print("  Status: MISSING")
            print(f"  Expected at: {path}")
            problems += 1
            continue

        if not path.is_file():
            print("  Status: NOT_A_FILE")
            print(f"  Path exists but is not a regular file: {path}")
            problems += 1
            continue

        try:
            if path.suffix.lower() == ".csv":
                result = inspect_csv(path)
                if result["status"] == "PASS":
                    print("  Status: PASS")
                    print(f"  Columns: {result['columns']}")
                    print(f"  Data rows: {result['row_count']}")
                else:
                    print(f"  Status: {result['status']}")
                    print(f"  Error: {result['error']}")
                    problems += 1

            elif path.suffix.lower() == ".json":
                result = inspect_json(path)
                if result["status"] == "PASS":
                    print("  Status: PASS")
                    print(f"  JSON records: {result['record_count']}")
                elif result["status"] == "JSON_PARSE_ERROR":
                    print("  Status: JSON_PARSE_ERROR")
                    print(f"  Error: {result['error']}")
                    print(f"  Line: {result['line']}")
                    print(f"  Column: {result['column']}")
                    print("  Action: Preserve original file; do not edit.")
                    print("          Investigate and fix in a later pipeline step.")
                    problems += 1
                elif result["status"] == "READ_ERROR":
                    print(f"  Status: READ_ERROR")
                    print(f"  Error: {result['error']}")
                    problems += 1
                else:
                    print(f"  Status: {result['status']}")
                    print(f"  Note: {result['error']}")
                    print("  Action: Preserve original file; investigate in a later step.")
                    problems += 1

        except Exception as error:  # noqa: BLE001 - report any surprise cleanly
            print(f"  Status: UNEXPECTED_ERROR")
            print(f"  Error: {error}")
            problems += 1

    print()
    print("=" * 60)
    print(f"Files checked: {checked} of {len(EXPECTED_FILES)}")
    print(f"Problems found: {problems}")
    print("=" * 60)

    if problems:
        print("STEP 1 STATUS: FAIL")
        print("Resolve missing or unreadable files before proceeding to Step 2.")
        return EXIT_FAILURE

    print("STEP 1 STATUS: PASS")
    print("All eight source files are present and readable.")
    print("Any JSON_PARSE_ERROR above is a documented finding for later steps.")
    return EXIT_SUCCESS


if __name__ == "__main__":
    sys.exit(main())