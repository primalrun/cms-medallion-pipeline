# Databricks notebook source
# Silver Layer: Provider Transformations
# - Cleans and casts Medicare and Medicaid bronze tables
# - Filters Medicaid to 2022 to align with Medicare snapshot year
# - Joins on NPI to produce dual-billing provider overlap table

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, LongType

# COMMAND ----------

storage_account = "cmsdevadls"

bronze_medicare_path  = f"abfss://bronze@{storage_account}.dfs.core.windows.net/medicare"
bronze_medicaid_path  = f"abfss://bronze@{storage_account}.dfs.core.windows.net/medicaid_fraud"
silver_medicare_path  = f"abfss://silver@{storage_account}.dfs.core.windows.net/medicare"
silver_medicaid_path  = f"abfss://silver@{storage_account}.dfs.core.windows.net/medicaid"
silver_overlap_path   = f"abfss://silver@{storage_account}.dfs.core.windows.net/provider_overlap"

# COMMAND ----------
# DBTITLE 1,Medicare — clean and cast

medicare_raw = spark.read.format("delta").load(bronze_medicare_path)

medicare = (
    medicare_raw
    .select(
        F.col("Rndrng_NPI").alias("provider_npi"),
        F.col("Rndrng_Prvdr_Last_Org_Name").alias("provider_last_org_name"),
        F.col("Rndrng_Prvdr_First_Name").alias("provider_first_name"),
        F.col("Rndrng_Prvdr_Crdntls").alias("provider_credentials"),
        F.col("Rndrng_Prvdr_Type").alias("provider_specialty"),
        F.col("Rndrng_Prvdr_State_Abrvtn").alias("provider_state"),
        F.col("Rndrng_Prvdr_City").alias("provider_city"),
        F.col("Rndrng_Prvdr_Zip5").alias("provider_zip"),
        F.col("Rndrng_Prvdr_Ent_Cd").alias("provider_entity_type"),
        F.col("Rndrng_Prvdr_Mdcr_Prtcptg_Ind").alias("medicare_participating"),
        F.col("HCPCS_Cd").alias("hcpcs_code"),
        F.col("HCPCS_Desc").alias("hcpcs_description"),
        F.col("HCPCS_Drug_Ind").alias("hcpcs_drug_indicator"),
        F.col("Place_Of_Srvc").alias("place_of_service"),
        F.col("Tot_Benes").cast(IntegerType()).alias("total_beneficiaries"),
        F.col("Tot_Srvcs").cast(DoubleType()).alias("total_services"),
        F.col("Tot_Bene_Day_Srvcs").cast(IntegerType()).alias("total_beneficiary_day_services"),
        F.col("Avg_Sbmtd_Chrg").cast(DoubleType()).alias("avg_submitted_charge"),
        F.col("Avg_Mdcr_Alowd_Amt").cast(DoubleType()).alias("avg_medicare_allowed_amt"),
        F.col("Avg_Mdcr_Pymt_Amt").cast(DoubleType()).alias("avg_medicare_payment_amt"),
        F.col("Avg_Mdcr_Stdzd_Amt").cast(DoubleType()).alias("avg_medicare_standardized_amt"),
        F.col("_ingested_at"),
        F.col("_source"),
    )
    .filter(F.col("provider_npi").isNotNull())
    .filter(F.col("hcpcs_code").isNotNull())
    .withColumn("_transformed_at", F.current_timestamp())
    .withColumn("snapshot_year", F.lit(2022))
)

(
    medicare.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(silver_medicare_path)
)

spark.sql(f"OPTIMIZE delta.`{silver_medicare_path}`")
print(f"Medicare silver: {medicare.count():,} rows")

# COMMAND ----------
# DBTITLE 1,Medicaid — clean, filter to 2022, cast

medicaid_raw = spark.read.format("delta").load(bronze_medicaid_path)

# Aggregate to billing provider grain (provider_npi + hcpcs_code + claim_month)
# Raw data includes separate rows per servicing provider — sum across them
medicaid = (
    medicaid_raw
    .filter(F.col("CLAIM_FROM_MONTH").between("2022-01", "2022-12"))
    .filter(F.col("BILLING_PROVIDER_NPI_NUM").isNotNull())
    .filter(F.col("HCPCS_CODE").isNotNull())
    .groupBy(
        F.col("BILLING_PROVIDER_NPI_NUM").alias("provider_npi"),
        F.col("HCPCS_CODE").alias("hcpcs_code"),
        F.col("CLAIM_FROM_MONTH").alias("claim_month"),
    )
    .agg(
        F.sum("TOTAL_PATIENTS").cast(LongType()).alias("total_patients"),
        F.sum("TOTAL_CLAIM_LINES").cast(LongType()).alias("total_claim_lines"),
        F.sum("TOTAL_PAID").cast(DoubleType()).alias("total_paid"),
    )
    .withColumn("claim_year", F.substring(F.col("claim_month"), 1, 4).cast(IntegerType()))
    .withColumn("_transformed_at", F.current_timestamp())
)

(
    medicaid.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(silver_medicaid_path)
)

spark.sql(f"OPTIMIZE delta.`{silver_medicaid_path}`")
print(f"Medicaid silver (2022): {medicaid.count():,} rows")

# COMMAND ----------
# DBTITLE 1,Provider overlap — join Medicare and Medicaid on NPI + HCPCS

# Aggregate Medicaid to provider+HCPCS level (sum across months)
medicaid_agg = (
    medicaid
    .groupBy("provider_npi", "hcpcs_code")
    .agg(
        F.sum("total_patients").alias("medicaid_total_patients"),
        F.sum("total_claim_lines").alias("medicaid_total_claim_lines"),
        F.sum("total_paid").alias("medicaid_total_paid"),
        F.countDistinct("claim_month").alias("medicaid_months_billed"),
    )
)

# Aggregate Medicare to provider+HCPCS level
# Exclude place_of_service, city, zip — a provider may bill same HCPCS from multiple
# locations, which would break the 1:1 grain with the Medicaid aggregation
medicare_agg = (
    medicare
    .groupBy("provider_npi", "hcpcs_code", "hcpcs_description")
    .agg(
        F.first("provider_specialty").alias("provider_specialty"),
        F.first("provider_state").alias("provider_state"),
        F.sum("total_beneficiaries").alias("medicare_total_beneficiaries"),
        F.sum("total_services").alias("medicare_total_services"),
        F.avg("avg_submitted_charge").alias("medicare_avg_submitted_charge"),
        F.avg("avg_medicare_payment_amt").alias("medicare_avg_payment_amt"),
    )
)

overlap = (
    medicare_agg
    .join(medicaid_agg, on=["provider_npi", "hcpcs_code"], how="inner")
    .withColumn("_transformed_at", F.current_timestamp())
    .withColumn("snapshot_year", F.lit(2022))
)

(
    overlap.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(silver_overlap_path)
)

spark.sql(f"OPTIMIZE delta.`{silver_overlap_path}`")
print(f"Provider overlap silver: {overlap.count():,} rows")

# COMMAND ----------

# Verify all three silver tables
for name, path in [("medicare", silver_medicare_path), ("medicaid", silver_medicaid_path), ("overlap", silver_overlap_path)]:
    count = spark.read.format("delta").load(path).count()
    print(f"silver/{name}: {count:,} rows")
