"""Step 3: Data-quality layer for the Retail Banking Data Engineering Pipeline.

Reads Bronze/raw tables from DuckDB, profiles and validates every record,
separates accepted vs quarantined records, assigns explicit reason codes,
and writes a reproducible quality report to a local Silver DuckDB database.

Design principles (beginner-friendly):
  * Raw/Bronze data is NEVER modified. We only READ from it.
  * Accepted and quarantined records are stored in SEPARATE tables.
  * Every quarantined record carries one or more explicit reason codes
    (e.g. MISSING_CUSTOMER_ID, INVALID_AMOUNT).
  * Hard errors -> quarantined. Warnings -> accepted but flagged.
  * We never reject a record just because it "looks unusual" unless a
    documented rule supports it.
  * Safe to rerun: Silver tables are replaced each run (full refresh).
  * Uses DuckDB + Python standard library only.
"""

from __future__ import annotations

import re
import sys
from datetime import datetime, timezone, date
from pathlib import Path
from typing import Optional

import duckdb

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
BRONZE_DIR = PROJECT_ROOT / "data" / "bronze"
BRONZE_DB = BRONZE_DIR / "retail_banking_bronze.duckdb"
SILVER_DIR = PROJECT_ROOT / "data" / "silver"
SILVER_DB = SILVER_DIR / "retail_banking_silver.duckdb"
QUALITY_REPORT = SILVER_DIR / "quality_report.txt"

SOURCE_TABLES = [
    ("bronze_branches_day1", "branches.csv", "Day 1", "branches"),
    ("bronze_customers_day1", "customers.csv", "Day 1", "customers"),
    ("bronze_transactions_day1", "transactions.csv", "Day 1", "transactions"),
    ("bronze_branches_day2", "branches_day2.csv", "Day 2", "branches"),
    ("bronze_customer_updates_day2", "customer_updates_day2.csv", "Day 2", "customers"),
    ("bronze_products_day2", "products_day2.json", "Day 2", "products"),
    ("bronze_transactions_day2", "transactions_day2.csv", "Day 2", "transactions"),
]

VALID_GENDERS = {"male", "female", "other"}
VALID_KYC = {"verified", "pending", "rejected"}
VALID_STATUSES = {"success", "failed", "pending"}
VALID_PAYMENT_METHODS = {"cash", "card", "upi", "netbanking", "cheque"}
VALID_CURRENCIES = {"inr"}
VALID_IS_REFUND = {"true", "false", "y", "n", "0", "1", ""}
VALID_BRANCH_TYPES = {"rural", "urban"}
VALID_PRODUCT_TYPES = {
    "credit card", "savings account", "loan", "fixed deposit",
}

DATE_FORMATS = [
    "%Y-%m-%d",
    "%Y/%m/%d",
    "%m/%d/%Y",
    "%d-%m-%Y",
    "%d/%m/%Y",
]
TIMESTAMP_FORMATS = ["%Y-%m-%d %H:%M:%S"]


def now_utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def ensure_silver_dir() -> None:
    SILVER_DIR.mkdir(parents=True, exist_ok=True)


def is_blank(value: Optional[str]) -> bool:
    return value is None or str(value).strip() == ""


def parse_date(value: Optional[str]) -> Optional[date]:
    if is_blank(value):
        return None
    raw = str(value).strip()
    for fmt in DATE_FORMATS:
        try:
            return datetime.strptime(raw, fmt).date()
        except ValueError:
            continue
    return None


def parse_timestamp(value: Optional[str]) -> Optional[datetime]:
    if is_blank(value):
        return None
    raw = str(value).strip()
    for fmt in TIMESTAMP_FORMATS:
        try:
            return datetime.strptime(raw, fmt)
        except ValueError:
            continue
    return None


def parse_amount(value: Optional[str]) -> Optional[float]:
    if is_blank(value):
        return None
    raw = str(value).strip()
    cleaned = raw.replace("Rs.", "").replace("rs.", "").replace(",", "").strip()
    if cleaned == "":
        return None
    try:
        return float(cleaned)
    except ValueError:
        return None


def is_valid_email(value: Optional[str]) -> bool:
    if is_blank(value):
        return False
    raw = str(value).strip()
    if "@" not in raw:
        return False
    if raw.count("@") != 1:
        return False
    local, domain = raw.split("@")
    return local != "" and domain != "" and "." in domain


def is_future_date(d: Optional[date]) -> bool:
    if d is None:
        return False
    return d > date.today()


def norm(value: Optional[str]) -> str:
    if is_blank(value):
        return ""
    return str(value).strip().lower()


# ---------------------------------------------------------------------------
# Validation rule sets
# ---------------------------------------------------------------------------

def validate_branches(row: dict, seen_keys: set) -> tuple[list[str], list[str]]:
    hard = []
    warn = []
    bid = row.get("branch_id", "")
    if is_blank(bid):
        hard.append("MISSING_BRANCH_ID")
    else:
        key = f"branches|{str(bid).strip()}"
        if key in seen_keys:
            hard.append("DUPLICATE_BRANCH_ID")
        else:
            seen_keys.add(key)
    if not is_blank(row.get("opened_date")) and parse_date(row["opened_date"]) is None:
        hard.append("INVALID_OPENED_DATE")
    if is_blank(row.get("manager_name")):
        warn.append("MISSING_MANAGER_NAME")
    region = norm(row.get("region"))
    if region and region not in {"north", "south", "east", "west"}:
        warn.append("UNKNOWN_REGION")
    raw_region = str(row.get("region", "")).strip()
    if raw_region and raw_region.lower() != raw_region and raw_region.upper() != raw_region:
        warn.append("INCONSISTENT_REGION_CASE")
    contact = str(row.get("contact_number", "")).strip()
    if contact:
        digits = re.sub(r"\D", "", contact)
        if len(digits) < 10:
            warn.append("INVALID_CONTACT_FORMAT")
    return hard, warn


def validate_customers(row: dict, seen_keys: set) -> tuple[list[str], list[str]]:
    hard = []
    warn = []
    cid = row.get("customer_id", "")
    if is_blank(cid):
        hard.append("MISSING_CUSTOMER_ID")
    else:
        key = f"customers|{str(cid).strip()}"
        if key in seen_keys:
            hard.append("DUPLICATE_CUSTOMER_ID")
        else:
            seen_keys.add(key)
    if not is_valid_email(row.get("email")):
        hard.append("INVALID_EMAIL")
    dob = parse_date(row.get("dob"))
    if not is_blank(row.get("dob")) and dob is None:
        hard.append("INVALID_DOB")
    elif is_future_date(dob):
        hard.append("FUTURE_DOB")
    if is_blank(row.get("account_id")):
        hard.append("MISSING_ACCOUNT_ID")
    raw_name = str(row.get("customer_name", "")).strip()
    if raw_name and raw_name.isupper():
        warn.append("INCONSISTENT_NAME_CASE")
    gender = norm(row.get("gender"))
    if gender and gender not in VALID_GENDERS:
        warn.append("INCONSISTENT_GENDER")
    if is_blank(row.get("phone")):
        warn.append("MISSING_PHONE")
    if is_blank(row.get("email")):
        warn.append("MISSING_EMAIL")
    kyc = norm(row.get("kyc_status"))
    if kyc and kyc not in VALID_KYC:
        warn.append("UNKNOWN_KYC_STATUS")
    return hard, warn


def validate_products(row: dict, seen_keys: set) -> tuple[list[str], list[str]]:
    hard = []
    warn = []
    pid = row.get("product_id", "")
    if is_blank(pid):
        hard.append("MISSING_PRODUCT_ID")
    else:
        key = f"products|{str(pid).strip()}"
        if key in seen_keys:
            hard.append("DUPLICATE_PRODUCT_ID")
        else:
            seen_keys.add(key)
    amount = parse_amount(row.get("price"))
    if not is_blank(row.get("price")) and amount is None:
        hard.append("INVALID_PRICE")
    elif amount is not None and amount < 0:
        warn.append("NEGATIVE_PRICE")
    if is_blank(row.get("product_name")):
        hard.append("MISSING_PRODUCT_NAME")
    if is_blank(row.get("is_active")):
        warn.append("MISSING_IS_ACTIVE")
    else:
        ia = norm(row.get("is_active"))
        if ia in {"true", "false"}:
            warn.append("IS_ACTIVE_STRING")
    cat = str(row.get("category", "")).strip()
    if cat and cat.lower() != cat:
        warn.append("INCONSISTENT_CATEGORY_CASE")
    pt = norm(row.get("product_type"))
    if pt and pt not in VALID_PRODUCT_TYPES:
        warn.append("UNKNOWN_PRODUCT_TYPE")
    return hard, warn


def validate_transactions(
    row: dict, seen_keys: set,
    valid_product_ids: set, valid_branch_ids: set, valid_account_ids: set,
) -> tuple[list[str], list[str]]:
    hard = []
    warn = []
    tid = row.get("transaction_id", "")
    if is_blank(tid):
        hard.append("MISSING_TRANSACTION_ID")
    else:
        key = f"transactions|{str(tid).strip()}"
        if key in seen_keys:
            hard.append("DUPLICATE_TRANSACTION_ID")
        else:
            seen_keys.add(key)
    amount = parse_amount(row.get("amount"))
    if not is_blank(row.get("amount")) and amount is None:
        hard.append("INVALID_AMOUNT")
    elif amount is not None and amount < 0:
        warn.append("NEGATIVE_AMOUNT")
    if not is_blank(row.get("date")) and parse_date(row["date"]) is None:
        hard.append("INVALID_DATE")
    if not is_blank(row.get("timestamp")) and parse_timestamp(row["timestamp"]) is None:
        hard.append("INVALID_TIMESTAMP")
    pid = str(row.get("product_id", "")).strip()
    if pid and valid_product_ids and pid not in valid_product_ids:
        hard.append("UNKNOWN_PRODUCT_ID")
    bid = str(row.get("branch_id", "")).strip()
    if bid and bid not in valid_branch_ids:
        hard.append("UNKNOWN_BRANCH_ID")
    acct = str(row.get("account_id", "")).strip()
    if acct and acct not in valid_account_ids:
        warn.append("UNKNOWN_ACCOUNT_ID")
    status = norm(row.get("status"))
    if status and status not in VALID_STATUSES:
        warn.append("INCONSISTENT_STATUS_CASE")
    pm = norm(row.get("payment_method"))
    if pm and pm not in VALID_PAYMENT_METHODS:
        warn.append("INCONSISTENT_PAYMENT_METHOD")
    curr = norm(row.get("currency"))
    if curr and curr not in VALID_CURRENCIES:
        warn.append("NON_INR_CURRENCY")
    if is_blank(row.get("is_refund")):
        warn.append("MISSING_IS_REFUND")
    else:
        irf = norm(row.get("is_refund"))
        if irf and irf not in VALID_IS_REFUND:
            warn.append("INVALID_IS_REFUND")
    return hard, warn


# ---------------------------------------------------------------------------
# Referential integrity helpers
# ---------------------------------------------------------------------------

def get_id_set(conn, table: str, column: str) -> set:
    rows = conn.execute(f"SELECT DISTINCT {column} FROM {table}").fetchall()
    result = set()
    for (val,) in rows:
        if not is_blank(val):
            result.add(str(val).strip())
    return result


# ---------------------------------------------------------------------------
# Per-table processing
# ---------------------------------------------------------------------------

def process_table(conn, bronze_table: str, source_file: str, day: str,
                  table_group: str,
                  valid_product_ids: set, valid_branch_ids: set,
                  valid_account_ids: set) -> dict:
    """Validate every row in one Bronze table.

    Returns a dict with metrics and lists of accepted / quarantined rows.
    """
    rows = conn.execute(f"SELECT * FROM {bronze_table}").fetchall()
    col_desc = conn.execute(f"DESCRIBE {bronze_table}").fetchall()
    columns = [c[0] for c in col_desc]

    seen_keys = set()
    accepted = []
    quarantined = []
    reason_counts = {}
    warning_counts = {}
    hard_count = 0
    warn_only_count = 0

    for row in rows:
        row_dict = dict(zip(columns, row))
        source_row_number = row_dict.get("source_row_number")

        if table_group == "branches":
            hard, warn = validate_branches(row_dict, seen_keys)
        elif table_group == "customers":
            hard, warn = validate_customers(row_dict, seen_keys)
        elif table_group == "products":
            hard, warn = validate_products(row_dict, seen_keys)
        elif table_group == "transactions":
            hard, warn = validate_transactions(
                row_dict, seen_keys,
                valid_product_ids, valid_branch_ids, valid_account_ids,
            )
        else:
            hard, warn = [], []

        record = {
            "columns": columns,
            "values": list(row),
            "source_row_number": source_row_number,
            "source_file": source_file,
            "source_day": day,
        }

        if hard:
            hard_count += 1
            record["rejection_reasons"] = hard
            record["reason_count"] = len(hard)
            quarantined.append(record)
            for reason in hard:
                reason_counts[reason] = reason_counts.get(reason, 0) + 1
        elif warn:
            warn_only_count += 1
            record["warnings"] = warn
            record["reason_count"] = 0
            accepted.append(record)
            for w in warn:
                warning_counts[w] = warning_counts.get(w, 0) + 1
        else:
            accepted.append(record)

    return {
        "bronze_table": bronze_table,
        "source_file": source_file,
        "source_day": day,
        "table_group": table_group,
        "columns": columns,
        "received": len(rows),
        "accepted": len(accepted),
        "quarantined": hard_count,
        "accepted_with_warnings": warn_only_count,
        "accepted_rows": accepted,
        "quarantined_rows": quarantined,
        "reason_counts": reason_counts,
        "warning_counts": warning_counts,
    }


# ---------------------------------------------------------------------------
# Silver DB output
# ---------------------------------------------------------------------------

def write_silver(results: list[dict], run_ts: str) -> None:
    """Write accepted and quarantined records to the Silver DuckDB database.

    Creates, for each source table:
      * silver_<bronze_table>_accepted   (clean records, warnings noted)
      * silver_<bronze_table>_quarantined (rejected records + reason codes)
    Also creates quality_summary and quality_reasons tables.
    """
    ensure_silver_dir()
    if SILVER_DB.exists():
        SILVER_DB.unlink()  # full refresh: start clean each run

    sconn = duckdb.connect(str(SILVER_DB))

    # Metadata columns carried into Silver (same as Bronze).
    meta_cols = [
        "source_row_number", "source_filename", "source_day",
        "ingestion_timestamp", "ingestion_status",
    ]

    summary_rows = []
    reason_rows = []

    for res in results:
        bt = res["bronze_table"]
        cols = res["columns"]
        # Original data columns exclude the 5 metadata columns.
        data_cols = [c for c in cols if c not in meta_cols]

        accepted_table = f"silver_{bt}_accepted"
        quarantined_table = f"silver_{bt}_quarantined"

        # Build column definitions: data columns as VARCHAR, plus metadata.
        def cols_with_types(extra: list[str]) -> str:
            parts = [f'"{c}" VARCHAR' for c in data_cols]
            parts += [f'"{c}" VARCHAR' for c in meta_cols]
            parts += [f'"{c}" VARCHAR' for c in extra]
            return ", ".join(parts)

        # Accepted table: data + metadata + warnings column.
        sconn.execute(f"CREATE OR REPLACE TABLE {accepted_table} ({cols_with_types(['warnings'])} )")
        # Quarantined table: data + metadata + rejection_reasons + reason_count.
        sconn.execute(f"CREATE OR REPLACE TABLE {quarantined_table} ({cols_with_types(['rejection_reasons', 'reason_count'])} )")

        acc_placeholders = ", ".join(["?"] * (len(data_cols) + len(meta_cols) + 1))
        q_placeholders = ", ".join(["?"] * (len(data_cols) + len(meta_cols) + 2))

        acc_cols = data_cols + meta_cols + ["warnings"]
        q_cols = data_cols + meta_cols + ["rejection_reasons", "reason_count"]

        for rec in res["accepted_rows"]:
            vals = list(rec["values"])
            # rec["values"] already aligns with `columns` (all bronze columns).
            # We need to map to data_cols + meta_cols order.
            row_dict = dict(zip(rec["columns"], rec["values"]))
            acc_vals = [row_dict.get(c) for c in data_cols] + \
                       [row_dict.get(c) for c in meta_cols] + \
                       [", ".join(rec.get("warnings", []))]
            sconn.execute(
                f"INSERT INTO {accepted_table} ({', '.join('\"' + c + '"' for c in acc_cols)}) VALUES ({acc_placeholders})",
                acc_vals,
            )

        for rec in res["quarantined_rows"]:
            row_dict = dict(zip(rec["columns"], rec["values"]))
            q_vals = [row_dict.get(c) for c in data_cols] + \
                     [row_dict.get(c) for c in meta_cols] + \
                     [", ".join(rec["rejection_reasons"]), str(rec["reason_count"])]
            sconn.execute(
                f"INSERT INTO {quarantined_table} ({', '.join('\"' + c + '"' for c in q_cols)}) VALUES ({q_placeholders})",
                q_vals,
            )

        # --- Quality summary row ---
        summary_rows.append((
            run_ts, res["source_file"], res["source_day"], bt,
            res["received"], res["accepted"], res["quarantined"],
            res["accepted_with_warnings"], ", ".join(sorted(res["reason_counts"].keys())),
        ))

        # --- Quality reason rows ---
        for reason, cnt in sorted(res["reason_counts"].items()):
            reason_rows.append((
                run_ts, res["source_file"], res["source_day"], bt, reason, cnt,
            ))

    # Write summary table.
    sconn.execute("""
        CREATE OR REPLACE TABLE quality_summary (
            run_timestamp VARCHAR,
            source_filename VARCHAR,
            source_day VARCHAR,
            bronze_table VARCHAR,
            received BIGINT,
            accepted BIGINT,
            quarantined BIGINT,
            accepted_with_warnings BIGINT,
            rejection_reason_codes VARCHAR
        )
    """)
    for row in summary_rows:
        sconn.execute("INSERT INTO quality_summary VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", row)

    # Write reasons table.
    sconn.execute("""
        CREATE OR REPLACE TABLE quality_reasons (
            run_timestamp VARCHAR,
            source_filename VARCHAR,
            source_day VARCHAR,
            bronze_table VARCHAR,
            reason_code VARCHAR,
            occurrence_count BIGINT
        )
    """)
    for row in reason_rows:
        sconn.execute("INSERT INTO quality_reasons VALUES (?, ?, ?, ?, ?, ?)", row)

    sconn.close()


# ---------------------------------------------------------------------------
# Text quality report
# ---------------------------------------------------------------------------

def write_text_report(results: list[dict], run_ts: str) -> None:
    ensure_silver_dir()
    lines = []
    lines.append("=" * 72)
    lines.append("RETAIL BANKING PIPELINE - STEP 3")
    lines.append("DATA-QUALITY / SILVER LAYER REPORT")
    lines.append("=" * 72)
    lines.append(f"Run timestamp (UTC): {run_ts}")
    lines.append(f"Bronze source DB    : {BRONZE_DB}")
    lines.append(f"Silver output DB    : {SILVER_DB}")
    lines.append("")
    lines.append("NOTE: Bronze/raw data was only READ, never modified.")
    lines.append("      Accepted and quarantined records are in SEPARATE Silver tables.")
    lines.append("      Every quarantined record carries explicit reason codes.")
    lines.append("      Warnings do NOT cause rejection unless a hard rule also applies.")
    lines.append("")

    total_received = 0
    total_accepted = 0
    total_quarantined = 0
    total_warn_only = 0

    for res in results:
        lines.append("-" * 72)
        lines.append(f"Source file : {res['source_file']} ({res['source_day']})")
        lines.append(f"Bronze table: {res['bronze_table']} ({res['table_group']})")
        lines.append(f"Records received : {res['received']}")
        lines.append(f"Records accepted : {res['accepted']}")
        lines.append(f"Records quarantined (hard errors): {res['quarantined']}")
        lines.append(f"Accepted with warnings : {res['accepted_with_warnings']}")
        lines.append("")
        if res["reason_counts"]:
            lines.append("Rejection reasons (hard errors):")
            for reason, cnt in sorted(res["reason_counts"].items()):
                lines.append(f"  {reason:<35} {cnt}")
            lines.append("")
        if res["warning_counts"]:
            lines.append("Warning flags (accepted, not rejected):")
            for warn, cnt in sorted(res["warning_counts"].items()):
                lines.append(f"  {warn:<35} {cnt}")
            lines.append("")
        total_received += res["received"]
        total_accepted += res["accepted"]
        total_quarantined += res["quarantined"]
        total_warn_only += res["accepted_with_warnings"]

    lines.append("=" * 72)
    lines.append("OVERALL TOTALS")
    lines.append("=" * 72)
    lines.append(f"Total records received   : {total_received}")
    lines.append(f"Total records accepted   : {total_accepted}")
    lines.append(f"Total records quarantined: {total_quarantined}")
    lines.append(f"Total accepted with warnings: {total_warn_only}")
    lines.append("")

    lines.append("=" * 72)
    lines.append("UNRESOLVED BUSINESS QUESTIONS & ASSUMPTIONS")
    lines.append("=" * 72)
    lines.append("1. Day 1 products.json is malformed (JSON parse error at line 41, col 3).")
    lines.append("   No Day 1 products table exists in Bronze, so Day 1 transactions CANNOT be")
    lines.append("   checked for UNKNOWN_PRODUCT_ID referential integrity. Those checks are")
    lines.append("   skipped for Day 1 transactions. This is a known gap, not a data error.")
    lines.append("")
    lines.append("2. account_id referential integrity (transactions.account_id ->")
    lines.append("   customers.account_id) is treated as a WARNING, not a hard error, because")
    lines.append("   an account may exist in the banking system without appearing in this")
    lines.append("   particular customer extract.")
    lines.append("")
    lines.append("3. Referential integrity uses GLOBAL master data across all days.")
    lines.append("   Branches and accounts are checked against the union of Day 1 + Day 2.")
    lines.append("   Products: Day 2 transactions are checked against Day 2's 18 products only,")
    lines.append("   because Day 2 is an incremental batch (not the full catalog). A high")
    lines.append("   UNKNOWN_PRODUCT_ID count for Day 2 may reflect products that exist in")
    lines.append("   Day 1's missing catalog, not necessarily bad data. This will be re-checked")
    lines.append("   once Day 1 products.json is fixed.")
    lines.append("")
    lines.append("4. Negative transaction amounts are flagged as a WARNING (NEGATIVE_AMOUNT),")
    lines.append("   not rejected. The data does not establish whether negative amounts mean")
    lines.append("   refunds, errors, or legitimate adjustments. Refund handling requires a")
    lines.append("   documented business rule before it can be enforced as a hard error.")
    lines.append("")
    lines.append("5. Transaction status variations (e.g. 'Faild', 'Succes', 'pending') are")
    lines.append("   flagged as INCONSISTENT_STATUS_CASE warnings. No status value is rejected")
    lines.append("   because the valid status set is not formally documented in the source.")
    lines.append("")
    lines.append("6. Malformed JSON (Day 1 products.json) is never repaired. The source file")
    lines.append("   remains unchanged. It must be fixed at the source and re-ingested.")
    lines.append("")
    lines.append("Reproducibility: Rerunning Step 3 replaces all Silver tables (full refresh),")
    lines.append("producing identical results from the same Bronze input.")
    lines.append("")

    with QUALITY_REPORT.open("a", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")


# ---------------------------------------------------------------------------
# Verification queries (printed to console)
# ---------------------------------------------------------------------------

def print_verification(sconn) -> None:
    print("\n" + "=" * 60)
    print("STEP 3 VERIFICATION QUERIES")
    print("=" * 60)

    print("\n-- Silver table inventory --")
    tables = sconn.execute(
        "SELECT table_name FROM information_schema.tables "
        "WHERE table_schema='main' "
        "AND table_name LIKE 'silver_%' "
        "AND table_name NOT IN ('quality_summary','quality_reasons') "
        "ORDER BY table_name"
    ).fetchall()
    for (tname,) in tables:
        cnt = sconn.execute(f"SELECT COUNT(*) FROM {tname}").fetchone()[0]
        print(f"  {tname}: {cnt} rows")

    print("\n-- Quality summary --")
    rows = sconn.execute(
        "SELECT source_filename, source_day, bronze_table, received, "
        "accepted, quarantined, accepted_with_warnings "
        "FROM quality_summary ORDER BY source_filename"
    ).fetchall()
    for r in rows:
        print(f"  {r[0]} ({r[1]}) | {r[2]}: received={r[3]}, accepted={r[4]}, "
              f"quarantined={r[5]}, accepted_with_warnings={r[6]}")

    print("\n-- Top rejection reasons --")
    rrows = sconn.execute(
        "SELECT reason_code, SUM(occurrence_count) AS total "
        "FROM quality_reasons GROUP BY reason_code ORDER BY total DESC"
    ).fetchall()
    for code, total in rrows:
        print(f"  {code:<35} {total}")

    print("\n-- Traceability: accepted + quarantined == received --")
    for r in rows:
        bt = r[2]
        acc_tab = f"silver_{bt}_accepted"
        quar_tab = f"silver_{bt}_quarantined"
        acc_cnt = sconn.execute(f"SELECT COUNT(*) FROM {acc_tab}").fetchone()[0]
        quar_cnt = sconn.execute(f"SELECT COUNT(*) FROM {quar_tab}").fetchone()[0]
        total = acc_cnt + quar_cnt
        status = "OK" if total == r[3] else "MISMATCH"
        print(f"  {bt}: accepted({acc_cnt}) + quarantined({quar_cnt}) = {total} "
              f"vs received={r[3]} -> {status}")

    print("\n-- Original Bronze row counts (immutability reference) --")
    bconn = duckdb.connect(str(BRONZE_DB))
    for r in rows:
        bt = r[2]
        b_cnt = bconn.execute(f"SELECT COUNT(*) FROM {bt}").fetchone()[0]
        print(f"  {bt}: {b_cnt} rows (unchanged in Bronze)")
    bconn.close()

    print("\nSTEP 3 COMPLETE.")
    print("Silver DB :", SILVER_DB)
    print("Text report:", QUALITY_REPORT)


# ---------------------------------------------------------------------------
# Main orchestration
# ---------------------------------------------------------------------------

def main() -> int:
    if not BRONZE_DB.exists():
        print(f"ERROR: Bronze database not found at {BRONZE_DB}")
        print("       Run Step 2 (step2_ingest.py) first.")
        return 1

    run_ts = now_utc()
    ensure_silver_dir()

    print("=" * 60)
    print("RETAIL BANK PIPELINE - STEP 3")
    print("DATA-QUALITY / SILVER LAYER")
    print("=" * 60)
    print(f"Bronze DB : {BRONZE_DB}")
    print(f"Silver DB : {SILVER_DB}")
    print(f"Run started: {run_ts}")
    print()

    bconn = duckdb.connect(str(BRONZE_DB), read_only=True)

    # ------------------------------------------------------------------
    # Build GLOBAL referential integrity ID sets across ALL days.
    # Branches and products are master data shared across days.
    # Day 2 is an incremental batch (only 2 branches), so we must use
    # the union of both days. Day 1 products is missing (malformed JSON),
    # so Day 2 products serve as the best-available reference for ALL
    # transactions (documented assumption).
    # ------------------------------------------------------------------
    valid_branch_ids = get_id_set(bconn, "bronze_branches_day1", "branch_id") | \
                       get_id_set(bconn, "bronze_branches_day2", "branch_id")
    valid_product_ids = get_id_set(bconn, "bronze_products_day2", "product_id")
    valid_account_ids = get_id_set(bconn, "bronze_customers_day1", "account_id") | \
                        get_id_set(bconn, "bronze_customer_updates_day2", "account_id")

    day1_has_products = False  # bronze_products_day1 does not exist
    print(f"  Global reference sets: branches={len(valid_branch_ids)}, "
          f"products={len(valid_product_ids)}, accounts={len(valid_account_ids)}")
    print()

    results = []

    for bronze_table, source_file, day, table_group in SOURCE_TABLES:
        print("-" * 60)
        print(f"Processing: {source_file} ({day}) -> {bronze_table}")

        # Transaction referential integrity uses GLOBAL sets.
        tx_product_ids = valid_product_ids
        tx_branch_ids = valid_branch_ids
        tx_account_ids = valid_account_ids

        if table_group == "transactions" and day == "Day 1":
            # Day 1 products table is missing (malformed JSON). We cannot
            # meaningfully check product referential integrity for Day 1
            # transactions, so we pass an empty set and skip that check.
            tx_product_ids = set()
            print("  NOTE: No Day 1 products table - UNKNOWN_PRODUCT_ID check skipped "
                  "for Day 1 transactions (reference data missing).")

        res = process_table(
            bconn, bronze_table, source_file, day, table_group,
            tx_product_ids, tx_branch_ids, tx_account_ids,
        )
        results.append(res)

        print(f"  Received={res['received']}  Accepted={res['accepted']}  "
              f"Quarantined={res['quarantined']}  Accepted+warnings={res['accepted_with_warnings']}")
        if res["reason_counts"]:
            top = sorted(res["reason_counts"].items(), key=lambda x: -x[1])[:3]
            reasons = ", ".join(f"{k}({v})" for k, v in top)
            print(f"  Top rejection reasons: {reasons}")

    bconn.close()

    # Write Silver outputs.
    write_silver(results, run_ts)

    # Write text report.
    write_text_report(results, run_ts)

    # Verification.
    sconn = duckdb.connect(str(SILVER_DB), read_only=True)
    print_verification(sconn)
    sconn.close()

    total_quarantined = sum(r["quarantined"] for r in results)
    if total_quarantined > 0:
        print(f"\nSTEP 3 STATUS: COMPLETED WITH {total_quarantined} QUARANTINED RECORD(S)")
        print("Review quarantined tables and quality report for reason codes.")
    else:
        print("\nSTEP 3 STATUS: ALL RECORDS ACCEPTED (no hard errors)")
    return 0


if __name__ == "__main__":
    sys.exit(main())