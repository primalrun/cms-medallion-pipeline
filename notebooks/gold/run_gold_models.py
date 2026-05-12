# Databricks notebook source
# Gold Layer: Execute gold model SQL via Spark
# Equivalent to `dbt run` — reads from silver, writes to dbw_cms_medallion_dev.gold
# dbt is used for local development; this notebook runs the same logic in the workflow

# COMMAND ----------

catalog = "dbw_cms_medallion_dev"
silver  = f"{catalog}.default"
gold    = f"{catalog}.gold"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {gold}")

# COMMAND ----------
# DBTITLE 1,agg_provider_spending

spark.sql(f"""
CREATE OR REPLACE TABLE {gold}.agg_provider_spending
USING DELTA AS

WITH medicare AS (
    SELECT
        provider_npi,
        first(provider_specialty)           AS provider_specialty,
        first(provider_state)               AS provider_state,
        sum(total_services)                 AS medicare_total_services,
        sum(total_beneficiaries)            AS medicare_total_beneficiaries,
        avg(avg_medicare_payment_amt)       AS medicare_avg_payment_amt,
        avg(avg_submitted_charge)           AS medicare_avg_submitted_charge,
        count(DISTINCT hcpcs_code)          AS medicare_distinct_hcpcs_count
    FROM {silver}.medicare
    GROUP BY provider_npi
),

medicaid AS (
    SELECT
        provider_npi,
        sum(total_paid)                     AS medicaid_total_paid,
        sum(total_patients)                 AS medicaid_total_patients,
        sum(total_claim_lines)              AS medicaid_total_claim_lines,
        count(DISTINCT hcpcs_code)          AS medicaid_distinct_hcpcs_count,
        count(DISTINCT claim_month)         AS medicaid_months_active
    FROM {silver}.medicaid
    GROUP BY provider_npi
)

SELECT
    m.provider_npi,
    m.provider_specialty,
    m.provider_state,
    m.medicare_total_services,
    m.medicare_total_beneficiaries,
    m.medicare_avg_payment_amt,
    m.medicare_avg_submitted_charge,
    m.medicare_distinct_hcpcs_count,
    md.medicaid_total_paid,
    md.medicaid_total_patients,
    md.medicaid_total_claim_lines,
    md.medicaid_distinct_hcpcs_count,
    md.medicaid_months_active,
    CASE WHEN md.provider_npi IS NOT NULL THEN true ELSE false END AS bills_both_programs,
    2022 AS snapshot_year
FROM medicare m
LEFT JOIN medicaid md ON m.provider_npi = md.provider_npi
""")

print(f"agg_provider_spending: {spark.table(f'{gold}.agg_provider_spending').count():,} rows")

# COMMAND ----------
# DBTITLE 1,agg_specialty_utilization

spark.sql(f"""
CREATE OR REPLACE TABLE {gold}.agg_specialty_utilization
USING DELTA AS

SELECT
    provider_specialty,
    hcpcs_code,
    hcpcs_description,
    count(DISTINCT provider_npi)        AS provider_count,
    sum(total_beneficiaries)            AS total_beneficiaries,
    sum(total_services)                 AS total_services,
    avg(avg_medicare_payment_amt)       AS avg_medicare_payment_amt,
    avg(avg_submitted_charge)           AS avg_submitted_charge,
    avg(avg_submitted_charge)
        - avg(avg_medicare_payment_amt) AS avg_charge_to_payment_gap,
    2022                                AS snapshot_year
FROM {silver}.medicare
WHERE provider_specialty IS NOT NULL
  AND hcpcs_code IS NOT NULL
GROUP BY provider_specialty, hcpcs_code, hcpcs_description
""")

print(f"agg_specialty_utilization: {spark.table(f'{gold}.agg_specialty_utilization').count():,} rows")

# COMMAND ----------
# DBTITLE 1,fraud_risk_indicators

spark.sql(f"""
CREATE OR REPLACE TABLE {gold}.fraud_risk_indicators
USING DELTA AS

WITH overlap AS (
    SELECT
        provider_npi,
        hcpcs_code,
        hcpcs_description,
        provider_specialty,
        provider_state,
        medicare_total_services,
        medicare_total_beneficiaries,
        medicare_avg_submitted_charge,
        medicare_avg_payment_amt,
        medicaid_total_paid,
        medicaid_total_patients,
        medicaid_total_claim_lines,
        medicaid_months_billed
    FROM {silver}.provider_overlap
),

specialty_benchmarks AS (
    SELECT
        provider_specialty,
        hcpcs_code,
        avg(medicare_avg_submitted_charge)              AS specialty_avg_submitted_charge,
        stddev(medicare_avg_submitted_charge)           AS specialty_stddev_submitted_charge,
        avg(medicare_total_services)                    AS specialty_avg_services,
        percentile_approx(medicare_total_services, 0.95) AS specialty_p95_services
    FROM overlap
    GROUP BY provider_specialty, hcpcs_code
)

SELECT
    o.provider_npi,
    o.hcpcs_code,
    o.hcpcs_description,
    o.provider_specialty,
    o.provider_state,
    o.medicare_total_services,
    o.medicare_avg_submitted_charge,
    o.medicare_avg_payment_amt,
    o.medicaid_total_paid,
    o.medicaid_total_patients,
    o.medicaid_months_billed,
    sb.specialty_avg_submitted_charge,
    sb.specialty_avg_services,
    sb.specialty_p95_services,
    round(
        o.medicare_avg_submitted_charge / nullif(sb.specialty_avg_submitted_charge, 0), 2
    )                                                   AS charge_ratio_vs_specialty,
    CASE WHEN o.medicare_total_services > sb.specialty_p95_services
        THEN true ELSE false END                        AS high_volume_flag,
    CASE WHEN o.medicare_avg_submitted_charge > 2 * sb.specialty_avg_submitted_charge
        THEN true ELSE false END                        AS high_charge_flag,
    CASE WHEN o.medicaid_total_paid > 50000
        THEN true ELSE false END                        AS high_medicaid_spend_flag,
    (
        CASE WHEN o.medicare_total_services > sb.specialty_p95_services    THEN 1 ELSE 0 END
      + CASE WHEN o.medicare_avg_submitted_charge
                 > 2 * sb.specialty_avg_submitted_charge                   THEN 1 ELSE 0 END
      + CASE WHEN o.medicaid_total_paid > 50000                            THEN 1 ELSE 0 END
    )                                                   AS risk_score,
    2022                                                AS snapshot_year
FROM overlap o
LEFT JOIN specialty_benchmarks sb
    ON o.provider_specialty = sb.provider_specialty
   AND o.hcpcs_code = sb.hcpcs_code
""")

print(f"fraud_risk_indicators: {spark.table(f'{gold}.fraud_risk_indicators').count():,} rows")

# COMMAND ----------

print("Gold layer complete.")
