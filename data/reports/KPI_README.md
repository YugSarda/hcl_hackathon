# Day 1 KPI Reports
Generated: 2026-10-24T07:59:54.627019+00:00
Source Gold DB: C:\Users\yugsa\hcl_hackathon\data\gold\retail_banking_gold_day1.duckdb
Source Silver DB: C:\Users\yugsa\hcl_hackathon\data\silver\retail_banking_silver.duckdb

## Scope
Day 1 only. Derived from the validated Silver/accepted records via the Gold model.

## Tables
- kpi_1_top_customers -> kpi_1_top_customers.csv
- kpi_2_monthly_trend -> kpi_2_monthly_trend.csv
- kpi_3_product_revenue -> kpi_3_product_revenue.csv
- kpi_4_dormant_accounts -> kpi_4_dormant_accounts.csv
- kpi_5_suspicious_flags -> kpi_5_suspicious_flags.csv
- kpi_6_rfm_lite -> kpi_6_rfm_lite.csv
- kpi_7_branch_ranking -> kpi_7_branch_ranking.csv
- kpi_8_kyc_risk -> kpi_8_kyc_risk.csv
- kpi_9_refund_rate -> kpi_9_refund_rate.csv
- kpi_10_quality_scorecard -> kpi_10_quality_scorecard.csv
- kpi_10_top_rejection_reasons -> kpi_10_top_rejection_reasons.csv

## Key conventions
- Eligible transaction: amount_parsed IS NOT NULL.
- Revenue uses status_norm = 'Success' (completed transactions).
- Refunds: primary signal = amount_parsed < 0 (is_refund source unreliable).
- ₹100,000 suspicious-amount rule applies to INR-normalized transactions only.
- transaction_timestamp is ALL NULL in Day 1 -> KPI 5 time-window rules not computable.
- dim_product is EMPTY (Day 1 products.json malformed) -> product_id used as degenerate attribute.