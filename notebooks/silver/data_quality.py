# Databricks notebook source
# Silver Layer: Data Quality Checks
# Validates silver tables after transformation.
# Raises an exception if any critical check fails — used by Databricks Workflow to halt the pipeline.

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

storage_account = "cmsdevadls"

silver_medicare_path = f"abfss://silver@{storage_account}.dfs.core.windows.net/medicare"
silver_medicaid_path = f"abfss://silver@{storage_account}.dfs.core.windows.net/medicaid"
silver_overlap_path  = f"abfss://silver@{storage_account}.dfs.core.windows.net/provider_overlap"

# COMMAND ----------

def run_dq_checks(name, df, checks):
    """
    Runs a list of (label, condition_col) checks against df.
    Prints PASS/FAIL for each and raises if any critical check fails.
    """
    print(f"\n── {name} ──────────────────────────────")
    failures = []
    for label, condition, critical in checks:
        fail_count = df.filter(~condition).count()
        status = "PASS" if fail_count == 0 else "FAIL"
        print(f"  [{status}] {label} — {fail_count:,} violations")
        if fail_count > 0 and critical:
            failures.append(label)
    if failures:
        raise ValueError(f"Critical DQ failures in {name}: {failures}")

# COMMAND ----------
# DBTITLE 1,Medicare silver checks

medicare = spark.read.format("delta").load(silver_medicare_path)
total = medicare.count()
print(f"Medicare row count: {total:,}")

run_dq_checks("silver/medicare", medicare, [
    ("provider_npi not null",        F.col("provider_npi").isNotNull(),              True),
    ("hcpcs_code not null",          F.col("hcpcs_code").isNotNull(),                True),
    ("total_services >= 0",          F.col("total_services") >= 0,                   True),
    ("avg_medicare_payment_amt >= 0",F.col("avg_medicare_payment_amt") >= 0,         True),
    ("avg_submitted_charge >= 0",    F.col("avg_submitted_charge") >= 0,             True),
    ("snapshot_year = 2022",         F.col("snapshot_year") == 2022,                 True),
    ("provider_npi is 10 digits",    F.length(F.col("provider_npi")) == 10,          True),
])

# COMMAND ----------
# DBTITLE 1,Medicaid silver checks

medicaid = spark.read.format("delta").load(silver_medicaid_path)
total = medicaid.count()
print(f"Medicaid row count: {total:,}")

run_dq_checks("silver/medicaid", medicaid, [
    ("provider_npi not null",   F.col("provider_npi").isNotNull(),      True),
    ("hcpcs_code not null",     F.col("hcpcs_code").isNotNull(),        True),
    ("total_paid >= 0",         F.col("total_paid") >= 0,               False),  # 520 legitimate claim reversals
    ("total_patients >= 0",     F.col("total_patients") >= 0,           True),
    ("claim_year = 2022",       F.col("claim_year") == 2022,            True),
    ("claim_month format valid",F.col("claim_month").rlike(r"^\d{4}-\d{2}$"), False),
])

# COMMAND ----------
# DBTITLE 1,Provider overlap checks

overlap = spark.read.format("delta").load(silver_overlap_path)
total = overlap.count()
print(f"Provider overlap row count: {total:,}")

run_dq_checks("silver/provider_overlap", overlap, [
    ("provider_npi not null",              F.col("provider_npi").isNotNull(),              True),
    ("hcpcs_code not null",                F.col("hcpcs_code").isNotNull(),                True),
    ("medicare_total_services >= 0",       F.col("medicare_total_services") >= 0,          True),
    ("medicaid_total_paid >= 0",           F.col("medicaid_total_paid") >= 0,              False),  # legitimate claim reversals
    ("medicare_avg_payment_amt >= 0",      F.col("medicare_avg_payment_amt") >= 0,         True),
    ("snapshot_year = 2022",               F.col("snapshot_year") == 2022,                 True),
])

# COMMAND ----------

print("\nAll DQ checks complete.")
