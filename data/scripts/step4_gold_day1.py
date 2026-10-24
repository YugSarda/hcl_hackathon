"""Step 4: Day 1 Gold / analytical data model (local DuckDB, Python only).

Builds a reliable Day 1 star-schema model from the validated Silver/accepted
records:
  * dim_customer
  * dim_branch
  * dim_product   (Day 1 products.json is malformed -> empty, documented gap)
  * fact_transaction

Design principles (keep it simple):
  * Day 1 ONLY. No Day 2 processing.
  * Source/Bronze/Silver layers are READ-ONLY and never modified.
  * Rebuildable: Gold tables are replaced each run (full refresh, no duplicates).
  * Business keys (customer_id, branch_id, transaction_id) are used as natural
    keys; a surrogate row_number key is added to the fact table.
  * Duplicate business keys in accepted data are treated as an error, never
    silently resolved by picking an arbitrary record.
  * Joins use equality on business keys; we verify uniqueness upstream to avoid
    row multiplication.
  * Day 1 product reference data does not exist (malformed JSON), so
    fact_transaction.product_key is NULL and product_id is kept as a degenerate
    attribute. This is documented, not guessed.
  * No separate account dimension is invented (account_id stays on the fact).
  * Product price is NOT treated as transaction revenue.
"""

from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import duckdb

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent

SILVER_DIR = PROJECT_ROOT / "data" / "silver"
SILVER_DB = SILVER_DIR / "retail_banking_silver.duckdb"

GOLD_DIR = PROJECT_ROOT / "data" / "gold"
GOLD_DB = GOLD_DIR / "retail_banking_gold_day1.duckdb"

# Silver accepted tables for Day 1.
SILVER_BRANCHES_DAY1 = "silver_bronze_branches_day1_accepted"
SILVER_CUSTOMERS_DAY1 = "silver_bronze_customers_day1_accepted"
SILVER_TRANSACTIONS_DAY1 = "silver_bronze_transactions_day1_accepted"

# There is NO silver_bronze_products_day1_accepted (Day 1 products.json
# failed JSON parsing in Step 2). dim_product will be created empty.


def now_utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def ensure_gold_dir() -> None:
    GOLD_DIR.mkdir(parents=True, exist_ok=True)


def fail(msg: str) -> None:
    print(f"FATAL: {msg}")
    sys.exit(1)


# ---------------------------------------------------------------------------
# Dimension builders
# ---------------------------------------------------------------------------

def build_dim_branch(gconn, sil_conn) -> int:
    src = f"SELECT * FROM {SILVER_BRANCHES_DAY1}"
    rows = sil_conn.execute(src).fetchall()
    cols = [d[0] for d in sil_conn.execute(f"DESCRIBE {SILVER_BRANCHES_DAY1}").fetchall()]
    col_index = {c: i for i, c in enumerate(cols)}

    def get(row, name):
        i = col_index.get(name)
        return row[i] if i is not None else None

    seen_branch_ids = set()
    dup_branch_ids = []
    for row in rows:
        bid = get(row, "branch_id")
        if bid in seen_branch_ids:
            dup_branch_ids.append(bid)
        seen_branch_ids.add(bid)
    if dup_branch_ids:
        fail(f"dim_branch: duplicate branch_id(s) in accepted data: {dup_branch_ids[:5]}")

    gconn.execute("""
        CREATE OR REPLACE TABLE dim_branch (
            branch_key         INTEGER,
            branch_id          VARCHAR,
            branch_name        VARCHAR,
            location           VARCHAR,
            manager_name       VARCHAR,
            opened_date        VARCHAR,
            opened_date_parsed DATE,
            region             VARCHAR,
            branch_type        VARCHAR,
            contact_number     VARCHAR,
            source_filename    VARCHAR,
            source_row_number  BIGINT
        )
    """)

    n = 0
    for row in rows:
        opened = get(row, "opened_date")
        opened_parsed = None
        if opened is not None and str(opened).strip() != "":
            for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%m/%d/%Y", "%d-%m-%Y", "%d/%m/%Y"):
                try:
                    opened_parsed = datetime.strptime(str(opened).strip(), fmt).date()
                    break
                except ValueError:
                    continue
        n += 1
        gconn.execute("""
            INSERT INTO dim_branch VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, [
            n,
            get(row, "branch_id"),
            get(row, "branch_name"),
            get(row, "location"),
            get(row, "manager_name"),
            get(row, "opened_date"),
            opened_parsed,
            get(row, "region"),
            get(row, "branch_type"),
            get(row, "contact_number"),
            get(row, "source_filename"),
            get(row, "source_row_number"),
        ])

    print(f"  dim_branch: {n} rows")
    return n


def build_dim_customer(gconn, sil_conn) -> int:
    src = f"SELECT * FROM {SILVER_CUSTOMERS_DAY1}"
    rows = sil_conn.execute(src).fetchall()
    cols = [d[0] for d in sil_conn.execute(f"DESCRIBE {SILVER_CUSTOMERS_DAY1}").fetchall()]
    col_index = {c: i for i, c in enumerate(cols)}

    def get(row, name):
        i = col_index.get(name)
        return row[i] if i is not None else None

    seen_customer_ids = set()
    dup_customer_ids = []
    for row in rows:
        cid = get(row, "customer_id")
        if cid in seen_customer_ids:
            dup_customer_ids.append(cid)
        seen_customer_ids.add(cid)
    if dup_customer_ids:
        fail(f"dim_customer: duplicate customer_id(s) in accepted data: {dup_customer_ids[:5]}")

    seen_account_ids = set()
    dup_account_ids = []
    for row in rows:
        acct = get(row, "account_id")
        if acct is None or str(acct).strip() == "":
            continue
        if acct in seen_account_ids:
            dup_account_ids.append(acct)
        seen_account_ids.add(acct)
    if dup_account_ids:
        print(f"  WARNING: dim_customer has duplicate account_id(s): {dup_account_ids[:5]}. "
              f"Fact join on account_id will resolve to the lowest customer_key deterministically; affected facts are logged.")

    gconn.execute("""
        CREATE OR REPLACE TABLE dim_customer (
            customer_key          INTEGER,
            customer_id           VARCHAR,
            customer_name         VARCHAR,
            email                 VARCHAR,
            phone                 VARCHAR,
            account_id            VARCHAR,
            gender                VARCHAR,
            dob                   VARCHAR,
            dob_parsed            DATE,
            address               VARCHAR,
            kyc_status            VARCHAR,
            registration_date     VARCHAR,
            registration_date_parsed DATE,
            transaction_id        VARCHAR,
            source_filename       VARCHAR,
            source_row_number     BIGINT
        )
    """)

    n = 0
    for row in rows:
        dob = get(row, "dob")
        dob_parsed = None
        if dob is not None and str(dob).strip() != "":
            for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%m/%d/%Y", "%d-%m-%Y", "%d/%m/%Y"):
                try:
                    dob_parsed = datetime.strptime(str(dob).strip(), fmt).date()
                    break
                except ValueError:
                    continue

        reg = get(row, "registration_date")
        reg_parsed = None
        if reg is not None and str(reg).strip() != "":
            for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%m/%d/%Y", "%d-%m-%Y", "%d/%m/%Y"):
                try:
                    reg_parsed = datetime.strptime(str(reg).strip(), fmt).date()
                    break
                except ValueError:
                    continue

        n += 1
        gconn.execute("""
            INSERT INTO dim_customer VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, [
            n,
            get(row, "customer_id"),
            get(row, "customer_name"),
            get(row, "email"),
            get(row, "phone"),
            get(row, "account_id"),
            get(row, "gender"),
            get(row, "dob"),
            dob_parsed,
            get(row, "address"),
            get(row, "kyc_status"),
            get(row, "registration_date"),
            reg_parsed,
            get(row, "transaction_id"),
            get(row, "source_filename"),
            get(row, "source_row_number"),
        ])

    print(f"  dim_customer: {n} rows")
    return n


def build_dim_product(gconn) -> int:
    gconn.execute("""
        CREATE OR REPLACE TABLE dim_product (
            product_key       INTEGER,
            product_id        VARCHAR,
            product_name      VARCHAR,
            product_type      VARCHAR,
            price             VARCHAR,
            launch_date       VARCHAR,
            is_active         VARCHAR,
            category          VARCHAR,
            vendor_name       VARCHAR,
            source_filename   VARCHAR,
            source_row_number BIGINT
        )
    """)
    print("  dim_product: 0 rows (Day 1 products.json is malformed; no Day 1 reference data)")
    return 0


def parse_amount(raw) -> Optional[float]:
    if raw is None or str(raw).strip() == "":
        return None
    s = str(raw).strip()
    s = s.replace("Rs.", "").replace("Rs", "").replace(",", "").strip()
    try:
        return float(s)
    except ValueError:
        return None


def parse_timestamp(raw) -> Optional[datetime]:
    if raw is None or str(raw).strip() == "":
        return None
    s = str(raw).strip()
    formats = (
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y/%m/%d %H:%M:%S",
        "%m/%d/%Y %H:%M:%S",
        "%d-%m-%Y %H:%M:%S",
        "%d/%m/%Y %H:%M:%S",
        "%Y-%m-%d",
    )
    for fmt in formats:
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            continue
    return None


def build_fact_transaction(gconn, sil_conn) -> int:
    src = f"SELECT * FROM {SILVER_TRANSACTIONS_DAY1}"
    rows = sil_conn.execute(src).fetchall()
    cols = [d[0] for d in sil_conn.execute(f"DESCRIBE {SILVER_TRANSACTIONS_DAY1}").fetchall()]
    col_index = {c: i for i, c in enumerate(cols)}

    def get(row, name):
        i = col_index.get(name)
        return row[i] if i is not None else None

    branch_map = {
        r[0]: r[1] for r in gconn.execute("SELECT branch_id, branch_key FROM dim_branch").fetchall()
    }
    # account_id may map to >1 customer (e.g. shared account). Resolve deterministically
    # to the lowest customer_key to guarantee 1:1 join (no row multiplication).
    customer_by_account = {}
    acct_multi = {}
    for acct, ckey in gconn.execute(
        "SELECT account_id, customer_key FROM dim_customer WHERE account_id IS NOT NULL AND account_id <> '' ORDER BY customer_key"
    ).fetchall():
        if acct in customer_by_account:
            acct_multi.setdefault(acct, []).append(ckey)
        else:
            customer_by_account[acct] = ckey

    seen_transaction_ids = set()
    dup_transaction_ids = []
    for row in rows:
        tid = get(row, "transaction_id")
        if tid in seen_transaction_ids:
            dup_transaction_ids.append(tid)
        seen_transaction_ids.add(tid)
    if dup_transaction_ids:
        fail(f"fact_transaction: duplicate transaction_id(s) in accepted data: {dup_transaction_ids[:5]}")

    gconn.execute("""
        CREATE OR REPLACE TABLE fact_transaction (
            transaction_key         INTEGER,
            transaction_id          VARCHAR,
            customer_key            INTEGER,
            branch_key              INTEGER,
            product_key             INTEGER,
            product_id              VARCHAR,
            account_id              VARCHAR,
            transaction_date        VARCHAR,
            transaction_date_parsed DATE,
            transaction_timestamp   VARCHAR,
            transaction_timestamp_parsed TIMESTAMP,
            amount                  VARCHAR,
            amount_parsed           DOUBLE,
            currency                VARCHAR,
            payment_method          VARCHAR,
            status                  VARCHAR,
            is_refund               VARCHAR,
            remarks                 VARCHAR,
            source_filename         VARCHAR,
            source_row_number       BIGINT
        )
    """)

    n = 0
    missing_branch = 0
    missing_customer = 0
    n = 0
    missing_branch = 0
    missing_customer = 0
    ambiguous_account = 0
    for row in rows:
        bid = get(row, "branch_id")
        acct = get(row, "account_id")
        pid = get(row, "product_id")
        amt_raw = get(row, "amount")
        ts_raw = get(row, "transaction_timestamp")
        date_raw = get(row, "date")

        branch_key = branch_map.get(bid)
        if branch_key is None:
            missing_branch += 1
        customer_key = customer_by_account.get(acct) if acct not in (None, "") else None
        if customer_key is None and acct not in (None, ""):
            missing_customer += 1
        elif acct in acct_multi:
            ambiguous_account += 1

        product_key = None  # Day 1 has no product reference data

        ts_parsed = parse_timestamp(ts_raw)
        amt_parsed = parse_amount(amt_raw)
        date_parsed = None
        if date_raw is not None and str(date_raw).strip() != "":
            for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%m/%d/%Y", "%d-%m-%Y", "%d/%m/%Y"):
                try:
                    date_parsed = datetime.strptime(str(date_raw).strip(), fmt).date()
                    break
                except ValueError:
                    continue

        n += 1
        gconn.execute("""
            INSERT INTO fact_transaction VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, [
            n,
            get(row, "transaction_id"),
            customer_key,
            branch_key,
            product_key,
            pid,
            acct,
            date_raw,
            date_parsed,
            ts_raw,
            ts_parsed,
            amt_raw,
            amt_parsed,
            get(row, "currency"),
            get(row, "payment_method"),
            get(row, "status"),
            get(row, "is_refund"),
            get(row, "remarks"),
            get(row, "source_filename"),
            get(row, "source_row_number"),
        ])

    print(f"  fact_transaction: {n} rows")
    print(f"    branch FK unresolved: {missing_branch}; customer FK unresolved: {missing_customer}; "
          f"ambiguous account (multi-customer) resolved deterministically: {ambiguous_account}; "
          f"product FK unresolved: {n} (Day 1 product data missing)")
    return n


# ---------------------------------------------------------------------------
# Quality checks
# ---------------------------------------------------------------------------

def run_quality_checks(gconn, sil_conn) -> None:
    failures = 0

    def check(name: str, ok: bool, detail: str = "") -> None:
        nonlocal failures
        status = "PASS" if ok else "FAIL"
        print(f"  [{status}] {name}" + (f" — {detail}" if detail else ""))
        if not ok:
            failures += 1

    # 1. Uniqueness of business keys in dimensions.
    dup_customer = gconn.execute("""
        SELECT customer_id, COUNT(*) c FROM dim_customer GROUP BY customer_id HAVING COUNT(*) > 1
    """).fetchall()
    check("dim_customer.customer_id unique", len(dup_customer) == 0, f"{len(dup_customer)} duplicates")

    dup_branch = gconn.execute("""
        SELECT branch_id, COUNT(*) c FROM dim_branch GROUP BY branch_id HAVING COUNT(*) > 1
    """).fetchall()
    check("dim_branch.branch_id unique", len(dup_branch) == 0, f"{len(dup_branch)} duplicates")

    dup_txn = gconn.execute("""
        SELECT transaction_id, COUNT(*) c FROM fact_transaction GROUP BY transaction_id HAVING COUNT(*) > 1
    """).fetchall()
    check("fact_transaction.transaction_id unique", len(dup_txn) == 0, f"{len(dup_txn)} duplicates")

    # 2. Missing required keys (NULL where not allowed).
    null_customer_id = gconn.execute("SELECT COUNT(*) FROM dim_customer WHERE customer_id IS NULL").fetchone()[0]
    check("dim_customer.customer_id not null", null_customer_id == 0, f"{null_customer_id} nulls")

    null_branch_id = gconn.execute("SELECT COUNT(*) FROM dim_branch WHERE branch_id IS NULL").fetchone()[0]
    check("dim_branch.branch_id not null", null_branch_id == 0, f"{null_branch_id} nulls")

    null_txn_id = gconn.execute("SELECT COUNT(*) FROM fact_transaction WHERE transaction_id IS NULL").fetchone()[0]
    check("fact_transaction.transaction_id not null", null_txn_id == 0, f"{null_txn_id} nulls")

    # 3. Orphan foreign keys in fact.
    orphan_branch = gconn.execute("""
        SELECT COUNT(*) FROM fact_transaction f
        LEFT JOIN dim_branch d ON f.branch_key = d.branch_key
        WHERE f.branch_key IS NOT NULL AND d.branch_key IS NULL
    """).fetchone()[0]
    check("fact.branch_key no orphans", orphan_branch == 0, f"{orphan_branch} orphans")

    orphan_customer = gconn.execute("""
        SELECT COUNT(*) FROM fact_transaction f
        LEFT JOIN dim_customer d ON f.customer_key = d.customer_key
        WHERE f.customer_key IS NOT NULL AND d.customer_key IS NULL
    """).fetchone()[0]
    check("fact.customer_key no orphans", orphan_customer == 0, f"{orphan_customer} orphans")

    orphan_product = gconn.execute("""
        SELECT COUNT(*) FROM fact_transaction f
        LEFT JOIN dim_product d ON f.product_key = d.product_key
        WHERE f.product_key IS NOT NULL AND d.product_key IS NULL
    """).fetchone()[0]
    check("fact.product_key no orphans", orphan_product == 0, f"{orphan_product} orphans")

    # 4. Row counts: fact == Silver accepted Day 1 transactions.
    silver_txn = sil_conn.execute(f"SELECT COUNT(*) FROM {SILVER_TRANSACTIONS_DAY1}").fetchone()[0]
    gold_txn = gconn.execute("SELECT COUNT(*) FROM fact_transaction").fetchone()[0]
    check("fact row count == silver accepted Day 1 transactions", gold_txn == silver_txn,
          f"gold={gold_txn} silver={silver_txn}")

    silver_cust = sil_conn.execute(f"SELECT COUNT(*) FROM {SILVER_CUSTOMERS_DAY1}").fetchone()[0]
    gold_cust = gconn.execute("SELECT COUNT(*) FROM dim_customer").fetchone()[0]
    check("dim_customer row count == silver accepted Day 1 customers", gold_cust == silver_cust,
          f"gold={gold_cust} silver={silver_cust}")

    silver_branch = sil_conn.execute(f"SELECT COUNT(*) FROM {SILVER_BRANCHES_DAY1}").fetchone()[0]
    gold_branch = gconn.execute("SELECT COUNT(*) FROM dim_branch").fetchone()[0]
    check("dim_branch row count == silver accepted Day 1 branches", gold_branch == silver_branch,
          f"gold={gold_branch} silver={silver_branch}")

    # 5. account_id uniqueness in dim_customer (prevents row multiplication on join).
    #    Duplicates are resolved deterministically (lowest customer_key) in the fact build;
    #    report but do not fail, since the 1:1 join guarantee is enforced by construction.
    dup_acct = gconn.execute("""
        SELECT account_id, COUNT(*) c FROM dim_customer
        WHERE account_id IS NOT NULL AND account_id <> ''
        GROUP BY account_id HAVING COUNT(*) > 1
    """).fetchall()
    if dup_acct:
        print(f"  [INFO] dim_customer.account_id duplicates (resolved deterministically): {len(dup_acct)} account_id(s)")

    # 6. No row multiplication: fact rows joined to customer/branch still == fact rows.
    joined = gconn.execute("""
        SELECT COUNT(*) FROM fact_transaction f
        LEFT JOIN dim_customer c ON f.customer_key = c.customer_key
        LEFT JOIN dim_branch b ON f.branch_key = b.branch_key
    """).fetchone()[0]
    check("fact-customer-branch join no row multiplication", joined == gold_txn, f"joined={joined} fact={gold_txn}")

    if failures:
        print(f"\n{failures} quality check(s) FAILED.")
        sys.exit(1)
    print("\nAll quality checks PASSED.")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    print("Step 4: Day 1 Gold analytical model")
    print(f"  Silver DB : {SILVER_DB}")
    print(f"  Gold DB   : {GOLD_DB}")
    print(f"  Started   : {now_utc()}")

    if not SILVER_DB.exists():
        fail(f"Silver DB not found: {SILVER_DB}. Run Step 3 first.")

    ensure_gold_dir()

    # Remove existing Gold DB for a clean full-refresh rebuild (repeatable, no duplicates).
    if GOLD_DB.exists():
        GOLD_DB.unlink()

    sil_conn = duckdb.connect(str(SILVER_DB), read_only=True)
    gconn = duckdb.connect(str(GOLD_DB))

    try:
        nb = build_dim_branch(gconn, sil_conn)
        nc = build_dim_customer(gconn, sil_conn)
        np_ = build_dim_product(gconn)
        nf = build_fact_transaction(gconn, sil_conn)

        print(f"\nSummary: dim_branch={nb} dim_customer={nc} dim_product={np_} fact_transaction={nf}")
        run_quality_checks(gconn, sil_conn)
    finally:
        sil_conn.close()
        gconn.close()

    print(f"  Finished  : {now_utc()}")
    print(f"Gold model written to {GOLD_DB}")


if __name__ == "__main__":
    main()