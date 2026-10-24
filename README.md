# hcl_hackathon — Day 1 Retail Banking Analytics (Gold + KPIs)

## Scope (strict)
**Day 1 only.** No Day 2 ingestion/KPIs, no incremental upserts, no SCD Type 2, no Day-over-Day reconciliation.

## Data flow (local DuckDB + Python only)
```
Step 2 ingest  ->  Step 3 quality (Silver)  ->  Step 4 Gold star schema  ->  Step 5 KPI reporting
```
- Silver/Bronze/Gold source DBs are opened **read-only**; KPI outputs are written to a new file.
- Step 4 Gold DB: `data/gold/retail_banking_gold_day1.duckdb`
- Silver DB (quality results reused for KPI 10): `data/silver/retail_banking_silver.duckdb`

## Step 5 — KPI pipeline
- Script: `data/scripts/step5_kpis.py`
- Tests: `data/scripts/tests/test_kpis.py` (16 edge-case tests, all pass)
- Outputs: `data/reports/` — one CSV per KPI + queryable DuckDB `kpi_day1.duckdb` + `KPI_README.md`

### Run commands (from project root)
```powershell
# Generate all KPI reports
python data/scripts/step5_kpis.py

# Run automated edge-case tests
python data/scripts/tests/test_kpis.py
```

## Gold star schema (source for all KPIs)
| Table | Rows | Key | Notes |
|---|---|---|---|
| `dim_branch` | 99 | `branch_key` (surrogate), `branch_id` (business) | region normalized in KPI queries |
| `dim_customer` | 231 | `customer_key` (surrogate), `customer_id` (business) | `account_id` kept as attribute |
| `dim_product` | 0 | — | **EMPTY** — Day 1 `products.json` malformed (JSON parse error line 41, col 3). `product_id` survives as a degenerate attribute on `fact_transaction`. |
| `fact_transaction` | 866 | `transaction_key` (surrogate), `transaction_id` (business) | `customer_key` NULL on 90 rows; `product_key` NULL on all rows |

- Joins: fact→customer via `account_id` (deterministic lowest `customer_key` for duplicate `A0046`, 5 fact rows affected); fact→branch via `branch_id`.
- No row multiplication: fact–customer–branch returns exactly 866 rows.
- `transaction_timestamp` is **ALL NULL** in Day 1 → KPI 5 time-window rules are not computable (documented, not fabricated).
- `is_refund` is inconsistent (`True/False/Y/N/1/0/'`) → KPI 9 uses `amount_parsed < 0` as the defensible refund signal.
- Dates span `2025-03-09` to `2027-02-12` (~23 months).

## Shared transaction conventions
- **Eligible transaction**: `amount_parsed IS NOT NULL` (847 of 866; 19 null amounts excluded).
- **Revenue / realized value**: `status_norm = 'Success'` only (status variants normalized).
- **Refund**: primary signal = `amount_parsed < 0` (negative amount reverses value). `is_refund` flag treated as unreliable and reported as a caveat, not a primary input.
- **Currency**: normalized `INR/inr → INR`, `EUR`, `USD`. The ₹100,000 suspicious-amount rule applies to INR-only transactions (no FX rates for EUR/USD).
- **Reference date**: `MAX(transaction_date_parsed)` = `2027-02-12` (used for dormancy, RFM recency, period splits).
- No double counting: `fact → dim_customer` on `customer_key` (PK) is 1:1.

---

## KPI 1 — Top five customers by net transaction value
**Business logic:** Rank customers by net transaction value. Net value = `SUM(amount_parsed)` over eligible, successful transactions; refunds (negative amounts) reduce net automatically. Tie-break: transaction count DESC, then `customer_id` ASC (deterministic).
**Output:** `kpi_1_top_customers.csv` — 5 rows ranked.
**Assumptions/limitations:** Only `status = 'Success'` counted as realized. 90 fact rows with `customer_key = NULL` cannot be joined to a customer and are excluded.
**Example (verified):** C7249 (561,658), C035 (483,102), C152 (434,678), C054 (421,561), C059 (389,406).

## KPI 2 — Monthly transaction trend by branch and region
**Business logic:** Monthly txn count, gross revenue (Success), and refund amount grouped by branch + region. MoM growth = `(curr − prev) / prev` via `LAG` over months that HAVE data.
- Missing months: growth compares to the last month with data (flagged `no_previous_month`).
- Zero denominator (`prev = 0`): growth = `NULL` (flagged `zero_denominator`).
**Output:** `kpi_2_monthly_trend.csv` (666 branch-month rows). Growth basis: computed 204 / no_previous_month 102 / zero_denominator 360.

## KPI 3 — Product-wise revenue contribution and period comparison
**Business logic:** Product-level revenue (Success, eligible) and contribution to total = `revenue / total`. Period comparison splits the date range at its midpoint (period A vs B) and shows `period_b_vs_a_change`.
**Limitation:** `dim_product` is EMPTY → `product_id` only (no product names/types). Sparse per-product data over ~23 months, so period comparison is indicative. Contribution sums to ~1.0 (106 products).

## KPI 4 — Dormant accounts
**Business logic:** Reference date = `2027-02-12`. Dormant = account with NO eligible transaction in `(ref_date − 90 days, ref_date]`. Account universe = distinct `account_id` from `dim_customer` UNION `fact_transaction` (no separate account master). Does NOT assume one account per customer (`A0046` shared by 2 customers).
**Higher-risk (credit card/loan):** CANNOT be classified — no product/account-type relationship (`dim_product` empty). Flag left NULL with reason `no_product_account_relationship`.
**Output:** `kpi_4_dormant_accounts.csv` — 275 accounts, 270 dormant, 5 active in window.

## KPI 5 — Suspicious transaction flags
**Business logic:** Three rules, each a separate boolean column plus an overall flag.
1. `flag_amount_large`: amount > ₹100,000 (INR only).
2. `flag_10min_burst`: ≥3 txns same account in rolling 10-min window — **not computable** (`transaction_timestamp` all NULL).
3. `flag_night_txn`: time 00:00–05:00 — **not computable** (no time component).
Overlapping windows: N/A (only one rule computable). Overall flag = amount rule only (399 flagged).
**Output:** `kpi_5_suspicious_flags.csv` (866 rows). 10-minute boundary logic unit-tested with synthetic data in `tests/test_kpis.py`.

## KPI 6 — RFM-lite customer segmentation
**Business logic:** Reference date `2027-02-12`. Per customer: recency (days since last txn), frequency (txn count), monetary (sum of eligible Success amounts). Each of R/F/M scored 1–4 via `NTILE(4)` (recency inverted: fewer days = higher score). Segment by score sum: Platinum ≥10, Gold ≥7, Silver ≥4, Bronze <4. Labeled **RFM-lite** — not a formal banking standard.
**Output:** `kpi_6_rfm_lite.csv` (155 customers with transactions). Ties handled by consistent `NTILE` ordering; small datasets use the same quartile logic.

## KPI 7 — Branch ranking within each region
**Business logic:** Branch revenue (eligible, Success) ranked within its region using `RANK()` (tied revenue → same rank, gaps in sequence). Deterministic ordering: revenue DESC, `branch_id` ASC. Region normalized (`upper(trim(region))`) so `'West'`, `' West '`, `'WEST'` group together.
**Output:** `kpi_7_branch_ranking.csv` (93 branches with transactions, ranked within region).

## KPI 8 — KYC risk exposure
**Business logic:** % and monetary value of eligible transactions whose customer KYC is not Verified. Missing/unexpected KYC statuses treated as 'Not Verified' (conservative, reported separately). Join fact→customer on `customer_key` (PK) → 1:1, **no double counting**. 90 fact rows with `customer_key = NULL` counted separately as unassessed.
**Output:** `kpi_8_kyc_risk.csv` — 1 summary row: 847 eligible txns, 463 not-Verified (54.66%), 89 unassessed; not-Verified revenue 13,296,496 of 24,971,370 total Success revenue (53.25%).

## KPI 9 — Refund rate by product and branch
**Business logic:** Refund rate = refund txns (`amount_parsed < 0`) / eligible txns (`amount_parsed IS NOT NULL`), grouped by product × branch. Zero denominator → `NULL`.
**Limitation (verified):** source `is_refund` is inconsistent, so it is NOT the primary signal; amount-sign is the defensible alternative. `dim_product` empty → product_id only.
**Output:** `kpi_9_refund_rate.csv` (822 product×branch groups), each row annotated with the refund-signal note.

## KPI 10 — Data-quality scorecard
**Business logic:** Reuses Step 3 `quality_summary` / `quality_reasons` from Silver (no duplicate quality rules). Per Day-1 file: received, accepted, quarantined, accepted_with_warnings, rejection_pct (= quarantined / received), acceptance_pct. Top 3 rejection reasons per file by occurrence count.
**Output:** `kpi_10_quality_scorecard.csv` (3 Day-1 files) + `kpi_10_top_rejection_reasons.csv`.

| File | Received | Accepted | Quarantined | Rejection % |
|---|---|---|---|---|
| branches.csv | 102 | 99 | 3 | 2.94% |
| customers.csv | 257 | 231 | 26 | 10.12% |
| transactions.csv | 940 | 866 | 74 | 7.87% |

## Verification checklist (all ten Day 1 KPIs)
- [ ] KPI 1: Top-5 customers ranked by net value; ties broken by count then customer_id; matches independent recomputation.
- [ ] KPI 2: Monthly branch/region trend with MoM growth; NULL growth for no-previous-month and zero-denominator cases.
- [ ] KPI 3: Product revenue + contribution (~1.0 total); period A vs B change; product-catalogue gap documented.
- [ ] KPI 4: Dormant = no eligible txn in prior 90 days vs documented reference date (2027-02-12); credit-card/loan higher-risk limitation documented.
- [ ] KPI 5: Separate reason flags + overall flag; amount rule = 399 flagged; time-window rules documented as not computable; boundary logic unit-tested.
- [ ] KPI 6: RFM-lite Platinum/Gold/Silver/Bronze with explicit thresholds; labeled as non-standard.
- [ ] KPI 7: Branches ranked within region via RANK(); ties share rank; region normalized.
- [ ] KPI 8: KYC not-Verified % and revenue; missing KYC conservative; no double counting (1:1 join on customer_key).
- [ ] KPI 9: Refund rate numerator/denominator defined; zero denominator → NULL; unreliable `is_refund` caveat documented.
- [ ] KPI 10: Reuses Step 3 quality results; per-file counts + top-3 rejection reasons; percentages with denominator = received.

### Independent checks performed (all match)
- KPI 1 top-5 values recomputed from raw Gold: identical.
- KPI 8 eligible/not-Verified/unassessed = 847 / 463 / 89: matches.
- KPI 5 INR amount > 100,000 = 399: matches.
- KPI 4 dormant = 270 of 275 accounts: matches.
- KPI 3 revenue contribution sums to ~1.0: matches.