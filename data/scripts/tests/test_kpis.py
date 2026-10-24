"""Automated tests for Day 1 KPI logic.

Runs the same SQL logic used in step5_kpis.py against synthetic in-memory
DuckDB data so edge cases are deterministic: empty groups, ties, missing
values, zero denominators, refunds, and the 10-minute burst boundary.

Run: python data/scripts/tests/test_kpis.py
"""
from __future__ import annotations

import duckdb

PASS = 0
FAIL = 0


def check(cond, msg):
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"PASS: {msg}")
    else:
        FAIL += 1
        print(f"FAIL: {msg}")


def norm_status_sql():
    """Status normalization mirroring step5_kpis.py."""
    return """
        CASE
            WHEN upper(trim(coalesce(status,''))) IN ('SUCCESS','SUCCES','SUCESS') THEN 'Success'
            WHEN upper(trim(coalesce(status,''))) IN ('FAILED','FAILD') THEN 'Failed'
            WHEN upper(trim(coalesce(status,''))) = 'PENDING' THEN 'Pending'
            ELSE 'Unknown'
        END AS status_norm
    """


def build_conn(rows):
    con = duckdb.connect()
    con.execute("""
        CREATE TABLE norm_fact (
            transaction_key INTEGER, transaction_id VARCHAR, customer_key INTEGER,
            branch_key INTEGER, product_key INTEGER, product_id VARCHAR, account_id VARCHAR,
            transaction_date VARCHAR, transaction_date_parsed DATE,
            amount VARCHAR, amount_parsed DOUBLE, currency VARCHAR,
            is_refund VARCHAR, payment_method VARCHAR, status VARCHAR
        )
    """)
    con.execute("CREATE TABLE dim_customer (customer_key INTEGER, customer_id VARCHAR, customer_name VARCHAR)")
    con.execute("CREATE TABLE dim_branch (branch_key INTEGER, branch_id VARCHAR, branch_name VARCHAR, region VARCHAR)")
    con.executemany("INSERT INTO norm_fact VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", rows)
    return con


# ---------------------------------------------------------------------------
# KPI 1 — tie-breaking
# ---------------------------------------------------------------------------

def test_kpi1_tie_breaking():
    print("\n--- KPI 1 tie-breaking ---")
    con = build_conn([
        (1, 'T1', 10, 1, None, 'P1', 'A1', '2026-01-01', '2026-01-01', '100', 100.0, 'INR', '0', 'UPI', 'Success'),
        (2, 'T2', 10, 1, None, 'P1', 'A1', '2026-01-02', '2026-01-02', '100', 100.0, 'INR', '0', 'UPI', 'Success'),
        (3, 'T3', 20, 1, None, 'P1', 'A2', '2026-01-01', '2026-01-01', '200', 200.0, 'INR', '0', 'UPI', 'Success'),
        (4, 'T4', 30, 1, None, 'P1', 'A3', '2026-01-01', '2026-01-01', '100', 100.0, 'INR', '0', 'UPI', 'Success'),
        (5, 'T5', 30, 1, None, 'P1', 'A3', '2026-01-02', '2026-01-02', '100', 100.0, 'INR', '0', 'UPI', 'Success'),
    ])
    con.executemany("INSERT INTO dim_customer VALUES (?,?,?)", [
        (10, 'C_A', 'Customer A'), (20, 'C_B', 'Customer B'), (30, 'C_C', 'Customer C'),
    ])
    con.execute("""
        CREATE OR REPLACE TEMP VIEW ranked AS
        SELECT c.customer_id, COUNT(*) AS txn_count, SUM(nf.amount_parsed) AS net_value,
               RANK() OVER (ORDER BY SUM(nf.amount_parsed) DESC, COUNT(*) DESC, c.customer_id ASC) AS rnk
        FROM norm_fact nf JOIN dim_customer c ON nf.customer_key = c.customer_key
        WHERE nf.amount_parsed IS NOT NULL
          AND upper(trim(coalesce(nf.status,''))) IN ('SUCCESS','SUCCES','SUCESS')
        GROUP BY c.customer_id
    """)
    rows = con.execute("SELECT customer_id, txn_count, net_value, rnk FROM ranked ORDER BY rnk").fetchall()
    print(rows)
    order = [r[0] for r in rows]
    check(order == ['C_A', 'C_C', 'C_B'], f"KPI 1 tie-break order, got {order}")


# ---------------------------------------------------------------------------
# KPI 2 — MoM zero denominator / missing previous month
# ---------------------------------------------------------------------------

def test_kpi2_zero_denominator():
    print("\n--- KPI 2 MoM zero denominator ---")
    con = build_conn([
        (1, 'T1', 10, 1, None, 'P1', 'A1', '2026-01-10', '2026-01-10', '100', 100.0, 'INR', '0', 'UPI', 'Success'),
        (2, 'T2', 10, 1, None, 'P1', 'A1', '2026-03-10', '2026-03-10', '100', 100.0, 'INR', '0', 'UPI', 'Success'),
    ])
    con.executemany("INSERT INTO dim_branch VALUES (?,?,?,?)", [(1, 'B1', 'Branch 1', 'R1')])
    rows = con.execute("""
        SELECT ym, rev,
               LAG(rev) OVER (PARTITION BY branch_id ORDER BY ym) AS prev,
               CASE
                   WHEN LAG(rev) OVER (PARTITION BY branch_id ORDER BY ym) IS NULL THEN NULL
                   WHEN LAG(rev) OVER (PARTITION BY branch_id ORDER BY ym) = 0 THEN NULL
                   ELSE (rev - LAG(rev) OVER (PARTITION BY branch_id ORDER BY ym))
                        / LAG(rev) OVER (PARTITION BY branch_id ORDER BY ym)
               END AS growth
        FROM (
            SELECT strftime(nf.transaction_date_parsed, '%Y-%m') AS ym, b.branch_id,
                   SUM(CASE WHEN upper(trim(coalesce(nf.status,''))) IN ('SUCCESS','SUCCES','SUCESS') THEN nf.amount_parsed ELSE 0 END) AS rev
            FROM norm_fact nf JOIN dim_branch b ON nf.branch_key = b.branch_key
            WHERE nf.transaction_date_parsed IS NOT NULL
            GROUP BY 1, 2
        ) ORDER BY ym
    """).fetchall()
    print(rows)
    check(rows[0][2] is None, f"KPI 2 first month prev NULL, got {rows[0][2]}")
    check(rows[0][3] is None, f"KPI 2 first month growth NULL, got {rows[0][3]}")
    check(abs(rows[1][3] - 0.0) < 1e-9, f"KPI 2 Mar growth 0.0, got {rows[1][3]}")


# ---------------------------------------------------------------------------
# KPI 9 — refund rate zero denominator
# ---------------------------------------------------------------------------

def test_kpi9_zero_denominator():
    print("\n--- KPI 9 refund rate zero denominator ---")
    con = build_conn([
        (1, 'T1', None, 1, None, 'P1', 'A1', '2026-01-01', '2026-01-01', '100', 100.0, 'INR', '0', 'UPI', 'Success'),
    ])
    con.executemany("INSERT INTO dim_branch VALUES (?,?,?,?)", [(1, 'B1', 'Branch 1', 'R1')])
    rows = con.execute("""
        SELECT nf.product_id, b.branch_id, COUNT(*) AS eligible,
               SUM(CASE WHEN nf.amount_parsed < 0 THEN 1 ELSE 0 END) AS refunds,
               CASE WHEN COUNT(*) = 0 THEN NULL
                    ELSE ROUND(SUM(CASE WHEN nf.amount_parsed < 0 THEN 1 ELSE 0 END) * 1.0 / COUNT(*), 6)
               END AS rate
        FROM norm_fact nf LEFT JOIN dim_branch b ON nf.branch_key = b.branch_key
        WHERE nf.amount_parsed IS NOT NULL
        GROUP BY nf.product_id, b.branch_id
    """).fetchall()
    print(rows)
    check(len(rows) == 1, f"KPI 9 groups exclude zero-denominator, got {len(rows)}")
    check(rows[0][4] == 0.0, f"KPI 9 rate 0.0 when no refunds, got {rows[0][4]}")


# ---------------------------------------------------------------------------
# KPI 5 — 10-minute burst boundary (synthetic timestamps)
# ---------------------------------------------------------------------------

def test_kpi5_ten_minute_boundary():
    print("\n--- KPI 5 10-minute burst boundary ---")
    con = duckdb.connect()
    con.execute("CREATE TABLE txns (account_id VARCHAR, ts TIMESTAMP, amount_parsed DOUBLE, currency VARCHAR)")
    con.executemany("INSERT INTO txns VALUES (?,?,?,?)", [
        ('A1', '2026-01-01 09:50:00', 100.0, 'INR'),
        ('A1', '2026-01-01 09:55:00', 100.0, 'INR'),
        ('A1', '2026-01-01 10:00:00', 100.0, 'INR'),
        ('A2', '2026-01-01 09:50:00', 100.0, 'INR'),
        ('A2', '2026-01-01 10:00:00', 100.0, 'INR'),
        ('A3', '2026-01-01 09:50:00', 100.0, 'INR'),
        ('A3', '2026-01-01 09:54:00', 100.0, 'INR'),
        ('A3', '2026-01-01 09:58:00', 100.0, 'INR'),
        ('A3', '2026-01-01 10:02:00', 100.0, 'INR'),
    ])

    def max_window(acct):
        return con.execute("""
            SELECT MAX(cnt) FROM (
                SELECT COUNT(*) OVER (
                    PARTITION BY account_id ORDER BY ts
                    RANGE BETWEEN INTERVAL '10 minute' PRECEDING AND CURRENT ROW
                ) AS cnt FROM txns WHERE account_id = ?
            )
        """, [acct]).fetchone()[0]

    a1_max, a2_max, a3_max = max_window('A1'), max_window('A2'), max_window('A3')
    print("max window counts:", a1_max, a2_max, a3_max)
    check(a1_max == 3, f"KPI 5 A1 burst window=3, got {a1_max}")
    check(a2_max == 2, f"KPI 5 A2 boundary window=2, got {a2_max}")
    check(a3_max >= 3, f"KPI 5 A3 overlapping window>=3, got {a3_max}")


# ---------------------------------------------------------------------------
# KPI 8 — no double counting
# ---------------------------------------------------------------------------

def test_kpi8_no_double_count():
    print("\n--- KPI 8 no double counting ---")
    con = build_conn([
        (1, 'T1', 10, 1, None, 'P1', 'A1', '2026-01-01', '2026-01-01', '100', 100.0, 'INR', '0', 'UPI', 'Success'),
        (2, 'T2', 10, 1, None, 'P1', 'A1', '2026-01-02', '2026-01-02', '200', 200.0, 'INR', '0', 'UPI', 'Success'),
        (3, 'T3', 11, 1, None, 'P1', 'A2', '2026-01-01', '2026-01-01', '50', 50.0, 'INR', '0', 'UPI', 'Failed'),
    ])
    con.executemany("INSERT INTO dim_customer VALUES (?,?,?)", [
        (10, 'C_A', 'Customer A'), (11, 'C_B', 'Customer B'),
    ])
    con.execute("ALTER TABLE dim_customer ADD COLUMN kyc_status VARCHAR")
    con.execute("UPDATE dim_customer SET kyc_status = 'Verified' WHERE customer_key = 10")
    con.execute("UPDATE dim_customer SET kyc_status = NULL WHERE customer_key = 11")

    total = con.execute("SELECT COUNT(*) FROM norm_fact WHERE amount_parsed IS NOT NULL").fetchone()[0]
    notv = con.execute("""
        SELECT COUNT(*) FROM norm_fact nf
        LEFT JOIN dim_customer c ON nf.customer_key = c.customer_key
        WHERE nf.amount_parsed IS NOT NULL
          AND coalesce(upper(trim(c.kyc_status)), '') <> 'VERIFIED'
    """).fetchone()[0]
    print("total eligible:", total, "not_verified:", notv)
    check(total == 3, f"KPI 8 total eligible = 3, got {total}")
    check(notv == 1, f"KPI 8 not_verified = 1 (customer B only), got {notv}")


# ---------------------------------------------------------------------------
# KPI 6 — small dataset / ties (NTILE)
# ---------------------------------------------------------------------------

def test_kpi6_small_dataset():
    print("\n--- KPI 6 RFM-lite small dataset ---")
    con = build_conn([
        (1, 'T1', 10, 1, None, 'P1', 'A1', '2026-01-01', '2026-01-01', '100', 100.0, 'INR', '0', 'UPI', 'Success'),
    ])
    con.executemany("INSERT INTO dim_customer VALUES (?,?,?)", [(10, 'C1', 'Customer 1')])
    rows = con.execute("""
        SELECT c.customer_id, COUNT(*) AS frequency, SUM(nf.amount_parsed) AS monetary,
               NTILE(4) OVER (ORDER BY COUNT(*) ASC) AS f_score,
               NTILE(4) OVER (ORDER BY SUM(nf.amount_parsed) ASC) AS m_score
        FROM norm_fact nf JOIN dim_customer c ON nf.customer_key = c.customer_key
        WHERE nf.amount_parsed IS NOT NULL AND upper(trim(coalesce(nf.status,''))) IN ('SUCCESS','SUCCES','SUCESS')
        GROUP BY c.customer_id
    """).fetchall()
    print(rows)
    check(len(rows) == 1, f"KPI 6 handles single customer, got {len(rows)}")
    check(rows[0][3] == 1 and rows[0][4] == 1, f"KPI 6 single-customer scores are 1, got f={rows[0][3]} m={rows[0][4]}")


# ---------------------------------------------------------------------------
# KPI 4 — dormant window boundary
# ---------------------------------------------------------------------------

def test_kpi4_dormant_boundary():
    print("\n--- KPI 4 dormant window boundary ---")
    con = duckdb.connect()
    con.execute("CREATE TABLE norm_fact (account_id VARCHAR, amount_parsed DOUBLE, transaction_date_parsed DATE)")
    con.execute("CREATE TABLE accounts (account_id VARCHAR)")
    ref = '2026-06-01'
    con.executemany("INSERT INTO norm_fact VALUES (?,?,?)", [
        ('A1', 100.0, '2026-03-04'),
        ('A1', 100.0, '2026-03-03'),
        ('A2', 100.0, '2026-01-01'),
    ])
    con.executemany("INSERT INTO accounts VALUES (?)", [('A1',), ('A2',), ('A3',)])
    rows = con.execute(f"""
        SELECT a.account_id,
               CASE WHEN MAX(CASE WHEN nf.transaction_date_parsed > DATE '{ref}' - INTERVAL 90 DAY
                                      AND nf.transaction_date_parsed <= DATE '{ref}'
                                 THEN 1 ELSE 0 END) = 1 THEN 0 ELSE 1 END AS is_dormant
        FROM accounts a LEFT JOIN norm_fact nf ON a.account_id = nf.account_id AND nf.amount_parsed IS NOT NULL
        GROUP BY a.account_id
    """).fetchall()
    print(rows)
    d = {r[0]: r[1] for r in rows}
    check(d.get('A1') == 0, f"KPI 4 A1 active (txn inside window), got {d.get('A1')}")
    check(d.get('A2') == 1, f"KPI 4 A2 dormant, got {d.get('A2')}")
    check(d.get('A3') == 1, f"KPI 4 A3 dormant (no txns), got {d.get('A3')}")


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    test_kpi1_tie_breaking()
    test_kpi2_zero_denominator()
    test_kpi9_zero_denominator()
    test_kpi5_ten_minute_boundary()
    test_kpi8_no_double_count()
    test_kpi6_small_dataset()
    test_kpi4_dormant_boundary()
    print(f"\nResults: {PASS} passed, {FAIL} failed out of {PASS + FAIL}")
    if FAIL:
        raise SystemExit(1)