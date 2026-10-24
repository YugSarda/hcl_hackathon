"""Step 5: Day 1 KPI reporting pipeline (local DuckDB + Python only).

Computes 10 Day 1 analytical KPIs from the Gold Day 1 model:
  data/gold/retail_banking_gold_day1.duckdb

Outputs:
  data/reports/kpi_day1.duckdb   -> queryable DuckDB with one table per KPI
  data/reports/kpi_*.csv         -> human-readable CSV reports

Strict scope: Day 1 ONLY. No Day 2, no incremental logic, no SCD.

Key data realities (observed from the Gold model):
  * fact_transaction : 866 rows; transaction_timestamp is ALL NULL
                      -> KPI 5 time-window rules cannot be computed (documented)
  * is_refund        : inconsistent ('True/False/Y/N/1/0/'')
                      -> KPI 9 uses amount_parsed < 0 as primary refund signal
  * status           : many case/variant spellings -> normalized in pipeline
  * currency         : INR, inr, EUR, USD -> 'inr' normalized to INR
  * dim_product      : EMPTY (Day 1 products.json malformed)
                      -> product_id used as degenerate attribute from fact
  * account_id       : not unique in dim_customer (A0046 shared by 2 customers)
  * 90 fact rows have customer_key NULL (account not in customer master)
"""

from __future__ import annotations

import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path

import duckdb

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent

GOLD_DB = PROJECT_ROOT / "data" / "gold" / "retail_banking_gold_day1.duckdb"
SILVER_DB = PROJECT_ROOT / "data" / "silver" / "retail_banking_silver.duckdb"

REPORTS_DIR = PROJECT_ROOT / "data" / "reports"
KPI_DB = REPORTS_DIR / "kpi_day1.duckdb"

# Refund threshold for suspicious-amount rule (rupees).
SUSPICIOUS_AMOUNT_THRESHOLD = 100_000.0

# Dormancy window (days) relative to the latest transaction date.
DORMANT_DAYS = 90

# RFM-lite segment score boundaries (sum of R+F+M scores, each 1..4).
RFM_PLATINUM_MIN = 10
RFM_GOLD_MIN = 7
RFM_SILVER_MIN = 4


def now_utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def fail(msg: str) -> None:
    print(f"FATAL: {msg}")
    sys.exit(1)


def ensure_reports_dir() -> None:
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)


# ---------------------------------------------------------------------------
# Normalized fact table (shared by all KPIs)
# ---------------------------------------------------------------------------

def build_normalized_fact(out_conn) -> int:
    """Create norm_fact in the KPI output DB by reading Gold (read-only via ATTACH).

    Normalizes status, currency, and derives refund signals. Does NOT alter Gold.
    Returns row count of norm_fact.
    """
    out_conn.execute(f"ATTACH '{GOLD_DB}' AS gold (READ_ONLY)")

    out_conn.execute("""
        DROP TABLE IF EXISTS norm_fact;
        CREATE TABLE norm_fact AS
        SELECT
            transaction_key,
            transaction_id,
            customer_key,
            branch_key,
            product_key,
            product_id,
            account_id,
            transaction_date,
            transaction_date_parsed,
            transaction_timestamp,
            transaction_timestamp_parsed,
            amount,
            amount_parsed,
            -- currency normalization ('inr' -> INR)
            CASE
                WHEN lower(trim(coalesce(currency, ''))) = 'inr' THEN 'INR'
                WHEN upper(trim(coalesce(currency, ''))) = 'EUR' THEN 'EUR'
                WHEN upper(trim(coalesce(currency, ''))) = 'USD' THEN 'USD'
                WHEN coalesce(currency, '') = '' THEN NULL
                ELSE upper(trim(currency))
            END AS currency_norm,
            -- status normalization
            CASE
                WHEN upper(trim(coalesce(status, ''))) IN ('SUCCESS','SUCCES','SUCESS') THEN 'Success'
                WHEN upper(trim(coalesce(status, ''))) IN ('FAILED','FAILD') THEN 'Failed'
                WHEN upper(trim(coalesce(status, ''))) = 'PENDING' THEN 'Pending'
                ELSE 'Unknown'
            END AS status_norm,
            is_refund,
            -- Primary refund signal (defensible): negative amount reverses value
            CASE WHEN amount_parsed < 0 THEN 1 ELSE 0 END AS is_refund_by_amount,
            -- Secondary refund signal (unreliable, inconsistent source values)
            CASE
                WHEN upper(trim(coalesce(is_refund, ''))) IN ('TRUE','Y','1') THEN 1
                WHEN upper(trim(coalesce(is_refund, ''))) IN ('FALSE','N','0')
                     OR trim(coalesce(is_refund, '')) = '' THEN 0
                ELSE NULL
            END AS is_refund_by_flag,
            payment_method,
            remarks,
            source_filename,
            source_row_number
        FROM gold.fact_transaction
    """)

    n = out_conn.execute("SELECT COUNT(*) FROM norm_fact").fetchone()[0]
    print(f"  norm_fact: {n} rows (normalized from Gold fact_transaction)")
    return n


# ===========================================================================
# KPI 1 — Top five customers by net transaction value (refunds accounted)
# ===========================================================================

def kpi_top_customers(out_conn) -> int:
    """Rank customers by net transaction value.

    Eligibility: amount_parsed IS NOT NULL AND status_norm = 'Success'
                 (completed transactions only; realized revenue).
    Net value: SUM(amount_parsed). Refunds are negative amounts, so they
    naturally reduce net value. is_refund flag is NOT used here because it
    is inconsistent; amount sign is the defensible signal.
    Tie-break: transaction count DESC, then customer_id ASC (deterministic).
    Customers with customer_key NULL (90 rows) cannot be joined to dim_customer
    and are excluded; documented.
    """
    out_conn.execute("""
        DROP TABLE IF EXISTS kpi_1_top_customers;
        CREATE TABLE kpi_1_top_customers AS
        WITH ranked AS (
            SELECT
                c.customer_id,
                c.customer_name,
                COUNT(*) AS txn_count,
                SUM(nf.amount_parsed) AS net_transaction_value,
                SUM(CASE WHEN nf.is_refund_by_amount = 1 THEN 1 ELSE 0 END) AS refund_txns,
                RANK() OVER (
                    ORDER BY SUM(nf.amount_parsed) DESC, COUNT(*) DESC, c.customer_id ASC
                ) AS value_rank
            FROM norm_fact nf
            JOIN gold.dim_customer c ON nf.customer_key = c.customer_key
            WHERE nf.amount_parsed IS NOT NULL
              AND nf.status_norm = 'Success'
            GROUP BY c.customer_id, c.customer_name
        )
        SELECT
            customer_id,
            customer_name,
            txn_count,
            net_transaction_value,
            refund_txns,
            value_rank
        FROM ranked
        WHERE value_rank <= 5
        ORDER BY value_rank, net_transaction_value DESC, txn_count DESC, customer_id ASC
    """)
    n = out_conn.execute("SELECT COUNT(*) FROM kpi_1_top_customers").fetchone()[0]
    print(f"  kpi_1_top_customers: {n} rows")
    return n


# ===========================================================================
# KPI 2 — Monthly transaction trend by branch and region (+ MoM growth)
# ===========================================================================

def kpi_monthly_trend(out_conn) -> int:
    """Monthly metrics grouped by branch and region, with MoM growth.

    Eligibility: transaction_date_parsed IS NOT NULL.
    Revenue uses status_norm = 'Success'; refunds tracked separately.
    MoM growth = (curr_rev - prev_rev) / prev_rev using LAG over months that
    HAVE data (so a missing calendar month means growth compares to the last
    month with data — documented). prev_rev = 0 or NULL -> growth NULL
    (zero denominator handled explicitly).
    """
    out_conn.execute("""
        DROP TABLE IF EXISTS kpi_2_monthly_trend;
        CREATE TABLE kpi_2_monthly_trend AS
        WITH monthly AS (
            SELECT
                strftime(nf.transaction_date_parsed, '%Y-%m') AS year_month,
                b.branch_id,
                b.branch_name,
                upper(trim(coalesce(b.region, ''))) AS region,
                COUNT(*) AS txn_count,
                SUM(CASE WHEN nf.status_norm = 'Success' THEN nf.amount_parsed ELSE 0 END)
                    AS gross_revenue,
                SUM(CASE WHEN nf.is_refund_by_amount = 1 THEN nf.amount_parsed ELSE 0 END)
                    AS refund_amount
            FROM norm_fact nf
            JOIN gold.dim_branch b ON nf.branch_key = b.branch_key
            WHERE nf.transaction_date_parsed IS NOT NULL
            GROUP BY 1, 2, 3, 4
        ),
        with_lag AS (
            SELECT
                year_month,
                branch_id,
                branch_name,
                region,
                txn_count,
                gross_revenue,
                refund_amount,
                LAG(gross_revenue) OVER (
                    PARTITION BY branch_id ORDER BY year_month
                ) AS prev_month_revenue
            FROM monthly
        )
        SELECT
            year_month,
            branch_id,
            branch_name,
            region,
            txn_count,
            gross_revenue,
            refund_amount,
            prev_month_revenue,
            CASE
                WHEN prev_month_revenue IS NULL THEN NULL
                WHEN prev_month_revenue = 0 THEN NULL
                ELSE (gross_revenue - prev_month_revenue) / prev_month_revenue
            END AS mom_growth,
            CASE
                WHEN prev_month_revenue IS NULL THEN 'no_previous_month'
                WHEN prev_month_revenue = 0 THEN 'zero_denominator'
                ELSE 'computed'
            END AS mom_growth_basis
        FROM with_lag
        ORDER BY branch_id, year_month
    """)
    n = out_conn.execute("SELECT COUNT(*) FROM kpi_2_monthly_trend").fetchone()[0]
    print(f"  kpi_2_monthly_trend: {n} rows")
    return n


# ===========================================================================
# KPI 3 — Product-wise revenue contribution and period comparison
# ===========================================================================

def kpi_product_revenue(out_conn) -> int:
    out_conn.execute("""
        DROP TABLE IF EXISTS kpi_3_product_revenue;
        CREATE TABLE kpi_3_product_revenue AS
        WITH totals AS (
            SELECT SUM(CASE WHEN status_norm = 'Success' THEN amount_parsed ELSE 0 END) AS total_revenue
            FROM norm_fact
            WHERE amount_parsed IS NOT NULL
        ),
        bounds AS (
            SELECT MIN(transaction_date_parsed) AS min_date,
                   MAX(transaction_date_parsed) AS max_date
            FROM norm_fact
            WHERE transaction_date_parsed IS NOT NULL
        ),
        prod AS (
            SELECT nf.product_id,
                   COUNT(*) AS txn_count,
                   SUM(CASE WHEN nf.status_norm = 'Success' THEN nf.amount_parsed ELSE 0 END) AS revenue
            FROM norm_fact nf
            WHERE nf.amount_parsed IS NOT NULL AND nf.status_norm = 'Success'
            GROUP BY nf.product_id
        ),
        split AS (
            SELECT min_date, max_date,
                   min_date + CAST((max_date - min_date) / 2 AS INTEGER) AS mid_date
            FROM bounds
        ),
        prod_period AS (
            SELECT nf.product_id,
                   CASE WHEN nf.transaction_date_parsed <= s.mid_date
                        THEN 'period_A' ELSE 'period_B' END AS period,
                   SUM(CASE WHEN nf.status_norm = 'Success' THEN nf.amount_parsed ELSE 0 END) AS period_revenue
            FROM norm_fact nf
            CROSS JOIN split s
            WHERE nf.amount_parsed IS NOT NULL
              AND nf.transaction_date_parsed IS NOT NULL
            GROUP BY nf.product_id, period
        ),
        period_pivot AS (
            SELECT product_id,
                   MAX(CASE WHEN period = 'period_A' THEN period_revenue END) AS rev_period_a,
                   MAX(CASE WHEN period = 'period_B' THEN period_revenue END) AS rev_period_b
            FROM prod_period
            GROUP BY product_id
        )
        SELECT
            p.product_id,
            p.txn_count,
            p.revenue,
            ROUND(p.revenue / NULLIF(t.total_revenue, 0), 6) AS revenue_contribution,
            pp.rev_period_a,
            pp.rev_period_b,
            CASE
                WHEN pp.rev_period_a IS NULL OR pp.rev_period_b IS NULL THEN NULL
                WHEN pp.rev_period_a = 0 THEN NULL
                ELSE ROUND((pp.rev_period_b - pp.rev_period_a) / pp.rev_period_a, 6)
            END AS period_b_vs_a_change
        FROM prod p
        CROSS JOIN totals t
        LEFT JOIN period_pivot pp ON p.product_id = pp.product_id
        ORDER BY p.revenue DESC
    """)
    n = out_conn.execute("SELECT COUNT(*) FROM kpi_3_product_revenue").fetchone()[0]
    print(f"  kpi_3_product_revenue: {n} rows")
    return n


# ===========================================================================
# KPI 4 — Dormant accounts (no eligible txn in preceding 90 days)
# ===========================================================================

def kpi_dormant_accounts(out_conn) -> int:
    out_conn.execute("""
        DROP TABLE IF EXISTS kpi_4_dormant_accounts;
        CREATE TABLE kpi_4_dormant_accounts AS
        WITH ref AS (
            SELECT MAX(transaction_date_parsed) AS ref_date
            FROM norm_fact
            WHERE transaction_date_parsed IS NOT NULL
        ),
        accounts AS (
            SELECT DISTINCT account_id FROM gold.dim_customer
            WHERE account_id IS NOT NULL AND account_id <> ''
            UNION
            SELECT DISTINCT account_id FROM norm_fact
            WHERE account_id IS NOT NULL AND account_id <> ''
        ),
        account_activity AS (
            SELECT nf.account_id,
                   MAX(nf.transaction_date_parsed) AS last_txn_date,
                   COUNT(*) AS txn_count_in_window
            FROM norm_fact nf
            CROSS JOIN ref r
            WHERE nf.account_id IS NOT NULL AND nf.account_id <> ''
              AND nf.amount_parsed IS NOT NULL
              AND nf.transaction_date_parsed IS NOT NULL
              AND nf.transaction_date_parsed > (r.ref_date - INTERVAL 90 DAY)
              AND nf.transaction_date_parsed <= r.ref_date
            GROUP BY nf.account_id
        )
        SELECT
            a.account_id,
            r.ref_date AS reference_date,
            (r.ref_date - INTERVAL 90 DAY) AS window_start,
            r.ref_date AS window_end,
            aa.last_txn_date,
            aa.txn_count_in_window,
            CASE WHEN aa.account_id IS NULL THEN 1 ELSE 0 END AS is_dormant,
            NULL AS is_credit_card_or_loan,
            'no_product_account_relationship' AS higher_risk_reason
        FROM accounts a
        CROSS JOIN ref r
        LEFT JOIN account_activity aa ON a.account_id = aa.account_id
        ORDER BY is_dormant DESC, a.account_id
    """)
    n = out_conn.execute("SELECT COUNT(*) FROM kpi_4_dormant_accounts").fetchone()[0]
    dormant = out_conn.execute("SELECT COUNT(*) FROM kpi_4_dormant_accounts WHERE is_dormant = 1").fetchone()[0]
    print(f"  kpi_4_dormant_accounts: {n} rows ({dormant} dormant)")
    return n


# ===========================================================================
# KPI 5 — Suspicious transaction flags
# ===========================================================================

def kpi_suspicious_flags(out_conn) -> int:
    out_conn.execute("""
        DROP TABLE IF EXISTS kpi_5_suspicious_flags;
        CREATE TABLE kpi_5_suspicious_flags AS
        SELECT
            transaction_id,
            account_id,
            transaction_date_parsed,
            amount_parsed,
            currency_norm,
            status_norm,
            CASE
                WHEN amount_parsed IS NULL THEN 0
                WHEN currency_norm = 'INR' AND amount_parsed > 100000 THEN 1
                ELSE 0
            END AS flag_amount_large,
            CAST(NULL AS INTEGER) AS flag_10min_burst,
            CAST(NULL AS INTEGER) AS flag_night_txn,
            CASE
                WHEN currency_norm = 'INR' AND amount_parsed > 100000 THEN 1
                ELSE 0
            END AS overall_flag,
            'transaction_timestamp_all_null' AS time_window_not_computable_reason
        FROM norm_fact
    """)
    flagged = out_conn.execute("SELECT COUNT(*) FROM kpi_5_suspicious_flags WHERE overall_flag = 1").fetchone()[0]
    total = out_conn.execute("SELECT COUNT(*) FROM kpi_5_suspicious_flags").fetchone()[0]
    print(f"  kpi_5_suspicious_flags: {total} rows ({flagged} flagged, all by amount rule)")
    return total


# ===========================================================================
# KPI 6 — RFM-lite customer segmentation
# ===========================================================================

def kpi_rfm_lite(out_conn) -> int:
    out_conn.execute("""
        DROP TABLE IF EXISTS kpi_6_rfm_lite;
        CREATE TABLE kpi_6_rfm_lite AS
        WITH ref AS (
            SELECT MAX(transaction_date_parsed) AS ref_date
            FROM norm_fact
            WHERE transaction_date_parsed IS NOT NULL
        ),
        cust_rfm AS (
            SELECT c.customer_id, c.customer_name,
                   MAX(nf.transaction_date_parsed) AS last_txn_date,
                   COUNT(*) AS frequency,
                   SUM(nf.amount_parsed) AS monetary
            FROM norm_fact nf
            JOIN gold.dim_customer c ON nf.customer_key = c.customer_key
            WHERE nf.amount_parsed IS NOT NULL AND nf.status_norm = 'Success'
            GROUP BY c.customer_id, c.customer_name
        ),
        scored AS (
            SELECT customer_id, customer_name, last_txn_date, frequency, monetary,
                   DATEDIFF('day', last_txn_date, (SELECT ref_date FROM ref)) AS recency_days,
                   NTILE(4) OVER (ORDER BY DATEDIFF('day', last_txn_date, (SELECT ref_date FROM ref)) DESC) AS r_score,
                   NTILE(4) OVER (ORDER BY frequency ASC) AS f_score,
                   NTILE(4) OVER (ORDER BY monetary ASC) AS m_score
            FROM cust_rfm
        )
        SELECT customer_id, customer_name, recency_days, frequency, monetary,
               r_score, f_score, m_score,
               (r_score + f_score + m_score) AS rfm_score,
               CASE
                   WHEN (r_score + f_score + m_score) >= 10 THEN 'Platinum'
                   WHEN (r_score + f_score + m_score) >= 7 THEN 'Gold'
                   WHEN (r_score + f_score + m_score) >= 4 THEN 'Silver'
                   ELSE 'Bronze'
               END AS segment,
               'RFM-lite (not a formal banking standard)' AS segment_note
        FROM scored
        ORDER BY rfm_score DESC, customer_id
    """)
    n = out_conn.execute("SELECT COUNT(*) FROM kpi_6_rfm_lite").fetchone()[0]
    print(f"  kpi_6_rfm_lite: {n} rows")
    return n


# ===========================================================================
# KPI 7 — Branch ranking within each region
# ===========================================================================

def kpi_branch_ranking(out_conn) -> int:
    """Branch revenue ranked within its region.

    Revenue = SUM(amount_parsed) for eligible transactions
    (amount_parsed IS NOT NULL AND status_norm = 'Success').
    Branches ranked within region using RANK() so tied revenue branches
    receive the same rank (standard competition ranking, with gaps).
    Ordering is deterministic: revenue DESC, branch_id ASC.
    """
    out_conn.execute("""
        DROP TABLE IF EXISTS kpi_7_branch_ranking;
        CREATE TABLE kpi_7_branch_ranking AS
        WITH branch_rev AS (
            SELECT b.branch_id, b.branch_name,
                   upper(trim(coalesce(b.region, ''))) AS region,
                   COUNT(*) AS txn_count,
                   SUM(CASE WHEN nf.status_norm = 'Success' THEN nf.amount_parsed ELSE 0 END) AS revenue
            FROM norm_fact nf
            JOIN gold.dim_branch b ON nf.branch_key = b.branch_key
            WHERE nf.amount_parsed IS NOT NULL AND nf.status_norm = 'Success'
            GROUP BY b.branch_id, b.branch_name, upper(trim(coalesce(b.region, '')))
        )
        SELECT branch_id, branch_name, region, txn_count, revenue,
               RANK() OVER (PARTITION BY region ORDER BY revenue DESC, branch_id ASC) AS rank_within_region
        FROM branch_rev
        ORDER BY region, rank_within_region, branch_id
    """)
    n = out_conn.execute("SELECT COUNT(*) FROM kpi_7_branch_ranking").fetchone()[0]
    print(f"  kpi_7_branch_ranking: {n} rows")
    return n


# ===========================================================================
# KPI 8 — KYC risk exposure
# ===========================================================================

def kpi_kyc_risk(out_conn) -> int:
    """Percentage and monetary value of eligible transactions with non-Verified KYC.

    Eligible transaction = amount_parsed IS NOT NULL.
    Monetary value uses status_norm = 'Success' (realized).
    Join fact -> dim_customer on customer_key (1:1 because customer_key is the
    dim_customer primary key), so no double-counting of transactions.
    Customers with customer_key NULL (90 fact rows) cannot be KYC-assessed;
    counted separately as kyc_unassessed. Missing/unexpected KYC statuses are
    treated as 'Not Verified' (conservative) and reported in kyc_not_verified.
    """
    out_conn.execute("""
        DROP TABLE IF EXISTS kpi_8_kyc_risk;
        CREATE TABLE kpi_8_kyc_risk AS
        WITH cust_kyc AS (
            SELECT customer_key,
                   CASE
                       WHEN upper(trim(coalesce(kyc_status, ''))) = 'VERIFIED' THEN 'Verified'
                       ELSE 'Not Verified'
                   END AS kyc_bucket
            FROM gold.dim_customer
        ),
        eligible AS (
            SELECT nf.transaction_key, nf.amount_parsed, nf.status_norm, c.kyc_bucket
            FROM norm_fact nf
            LEFT JOIN cust_kyc c ON nf.customer_key = c.customer_key
            WHERE nf.amount_parsed IS NOT NULL
        )
        SELECT
            COUNT(*) AS total_eligible_txns,
            COUNT(CASE WHEN kyc_bucket = 'Not Verified' THEN 1 END) AS not_verified_txns,
            COUNT(CASE WHEN kyc_bucket IS NULL THEN 1 END) AS kyc_unassessed_txns,
            ROUND(
                COUNT(CASE WHEN kyc_bucket = 'Not Verified' THEN 1 END) * 100.0 / NULLIF(COUNT(*), 0), 4
            ) AS not_verified_pct,
            SUM(CASE WHEN kyc_bucket = 'Not Verified' AND status_norm = 'Success'
                     THEN amount_parsed ELSE 0 END) AS not_verified_revenue,
            SUM(CASE WHEN status_norm = 'Success' THEN amount_parsed ELSE 0 END) AS total_success_revenue,
            ROUND(
                SUM(CASE WHEN kyc_bucket = 'Not Verified' AND status_norm = 'Success'
                         THEN amount_parsed ELSE 0 END)
                * 100.0 / NULLIF(SUM(CASE WHEN status_norm = 'Success' THEN amount_parsed END), 0), 4
            ) AS not_verified_revenue_pct
        FROM eligible
    """)
    n = out_conn.execute("SELECT COUNT(*) FROM kpi_8_kyc_risk").fetchone()[0]
    print(f"  kpi_8_kyc_risk: {n} summary row")
    return n


# ===========================================================================
# KPI 9 — Refund rate by product and branch
# ===========================================================================

def kpi_refund_rate(out_conn) -> int:
    """Refund rate grouped by product and branch.

    LIMITATION (documented): the source is_refund field is inconsistent
    ('True/False/Y/N/1/0/'''), so it cannot be relied upon. The PRIMARY,
    defensible refund signal is amount_parsed < 0 (a negative amount reverses
    value). A secondary is_refund_by_flag-based rate is NOT shown as primary.

    Numerator   = refund transactions (amount_parsed < 0)
    Denominator = eligible transactions (amount_parsed IS NOT NULL)
    Refund rate = numerator / denominator; zero denominator -> NULL (N/A).
    Product dimension empty -> product_id used (no product names).
    """
    out_conn.execute("""
        DROP TABLE IF EXISTS kpi_9_refund_rate;
        CREATE TABLE kpi_9_refund_rate AS
        SELECT
            COALESCE(nf.product_id, '(no_product_id)') AS product_id,
            b.branch_id,
            b.branch_name,
            upper(trim(coalesce(b.region, ''))) AS region,
            COUNT(*) AS eligible_txns,
            SUM(CASE WHEN nf.amount_parsed < 0 THEN 1 ELSE 0 END) AS refund_txns,
            CASE
                WHEN COUNT(*) = 0 THEN NULL
                ELSE ROUND(SUM(CASE WHEN nf.amount_parsed < 0 THEN 1 ELSE 0 END) * 1.0 / COUNT(*), 6)
            END AS refund_rate,
            'refund_signal=amount_lt_0 (is_refund source unreliable)' AS refund_signal_note
        FROM norm_fact nf
        LEFT JOIN gold.dim_branch b ON nf.branch_key = b.branch_key
        WHERE nf.amount_parsed IS NOT NULL
        GROUP BY nf.product_id, b.branch_id, b.branch_name,
                 upper(trim(coalesce(b.region, '')))
        ORDER BY refund_rate DESC NULLS LAST, product_id
    """)
    n = out_conn.execute("SELECT COUNT(*) FROM kpi_9_refund_rate").fetchone()[0]
    print(f"  kpi_9_refund_rate: {n} rows (product x branch groups)")
    return n


# ===========================================================================
# KPI 10 — Data-quality scorecard (reuses Step 3 results from Silver)
# ===========================================================================

def kpi_quality_scorecard(out_conn) -> int:
    """Reuse Step 3 quality_summary + quality_reasons from the Silver DB.

    No duplicate quality rules are re-implemented. Reports per Day 1 file:
      received, accepted, quarantined, accepted_with_warnings,
      rejection_pct (quarantined / received), acceptance_pct.
    Top 3 rejection reasons per file by occurrence_count.
    """
    out_conn.execute(f"ATTACH '{SILVER_DB}' AS silver (READ_ONLY)")
    out_conn.execute("""
        DROP TABLE IF EXISTS kpi_10_quality_scorecard;
        CREATE TABLE kpi_10_quality_scorecard AS
        SELECT
            source_filename,
            source_day,
            bronze_table,
            received,
            accepted,
            quarantined,
            accepted_with_warnings,
            ROUND(quarantined * 100.0 / NULLIF(received, 0), 4) AS rejection_pct,
            ROUND(accepted * 100.0 / NULLIF(received, 0), 4) AS acceptance_pct,
            rejection_reason_codes
        FROM silver.quality_summary
        WHERE source_day = 'Day 1'
        ORDER BY source_filename
    """)
    out_conn.execute("""
        DROP TABLE IF EXISTS kpi_10_top_rejection_reasons;
        CREATE TABLE kpi_10_top_rejection_reasons AS
        SELECT
            source_filename,
            reason_code,
            occurrence_count,
            ROW_NUMBER() OVER (PARTITION BY source_filename ORDER BY occurrence_count DESC, reason_code) AS reason_rank
        FROM silver.quality_reasons
        WHERE source_day = 'Day 1'
    """)
    n = out_conn.execute("SELECT COUNT(*) FROM kpi_10_quality_scorecard").fetchone()[0]
    reasons = out_conn.execute("SELECT COUNT(*) FROM kpi_10_top_rejection_reasons WHERE reason_rank <= 3").fetchone()[0]
    print(f"  kpi_10_quality_scorecard: {n} Day-1 files; {reasons} top-3 reason rows")
    return n


# ===========================================================================
# Reporting — export KPI tables to CSV + keep queryable DuckDB
# ===========================================================================

KPI_TABLES = [
    "kpi_1_top_customers",
    "kpi_2_monthly_trend",
    "kpi_3_product_revenue",
    "kpi_4_dormant_accounts",
    "kpi_5_suspicious_flags",
    "kpi_6_rfm_lite",
    "kpi_7_branch_ranking",
    "kpi_8_kyc_risk",
    "kpi_9_refund_rate",
    "kpi_10_quality_scorecard",
    "kpi_10_top_rejection_reasons",
]


def export_reports(out_conn) -> None:
    ensure_reports_dir()
    for table in KPI_TABLES:
        exists = out_conn.execute(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?",
            [table],
        ).fetchone()[0]
        if not exists:
            print(f"  [skip] {table} not found")
            continue
        df = out_conn.execute(f"SELECT * FROM {table}").fetchdf()
        csv_path = REPORTS_DIR / f"{table}.csv"
        df.to_csv(csv_path, index=False)
        print(f"  wrote {csv_path} ({len(df)} rows)")
    # Also write a markdown index for interview-ready descriptions.
    write_index()


def write_index() -> None:
    index_path = REPORTS_DIR / "KPI_README.md"
    lines = [
        "# Day 1 KPI Reports",
        f"Generated: {now_utc()}",
        f"Source Gold DB: {GOLD_DB}",
        f"Source Silver DB: {SILVER_DB}",
        "",
        "## Scope",
        "Day 1 only. Derived from the validated Silver/accepted records via the Gold model.",
        "",
        "## Tables",
    ]
    for table in KPI_TABLES:
        lines.append(f"- {table} -> {table}.csv")
    lines += [
        "",
        "## Key conventions",
        "- Eligible transaction: amount_parsed IS NOT NULL.",
        "- Revenue uses status_norm = 'Success' (completed transactions).",
        "- Refunds: primary signal = amount_parsed < 0 (is_refund source unreliable).",
        "- ₹100,000 suspicious-amount rule applies to INR-normalized transactions only.",
        "- transaction_timestamp is ALL NULL in Day 1 -> KPI 5 time-window rules not computable.",
        "- dim_product is EMPTY (Day 1 products.json malformed) -> product_id used as degenerate attribute.",
    ]
    index_path.write_text("\n".join(lines), encoding="utf-8")
    print(f"  wrote {index_path}")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    print("Step 5: Day 1 KPI pipeline")
    print(f"  Gold DB : {GOLD_DB}")
    print(f"  Silver DB : {SILVER_DB}")
    print(f"  Reports : {REPORTS_DIR}")
    print(f"  Started : {now_utc()}")

    for p in (GOLD_DB, SILVER_DB):
        if not p.exists():
            fail(f"Database not found: {p}. Run Steps 3 and 4 first.")

    # Fresh KPI output DB (read-write) so rebuilds are clean.
    if KPI_DB.exists():
        KPI_DB.unlink()
    out_conn = duckdb.connect(str(KPI_DB))

    try:
        build_normalized_fact(out_conn)
        kpi_top_customers(out_conn)
        kpi_monthly_trend(out_conn)
        kpi_product_revenue(out_conn)
        kpi_dormant_accounts(out_conn)
        kpi_suspicious_flags(out_conn)
        kpi_rfm_lite(out_conn)
        kpi_branch_ranking(out_conn)
        kpi_kyc_risk(out_conn)
        kpi_refund_rate(out_conn)
        kpi_quality_scorecard(out_conn)
        export_reports(out_conn)
    finally:
        out_conn.close()

    print(f"  Finished : {now_utc()}")
    print(f"KPI outputs written to {REPORTS_DIR}")


if __name__ == "__main__":
    main()