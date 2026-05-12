# Databricks notebook source
# Bronze Layer: Medicaid Provider Spending by HCPCS (Fraud/Waste Dataset)
# Source: https://opendata.hhs.gov/datasets/medicaid-provider-spending/
# Released: 2026-02-14 — provider-level spending data for unusual billing pattern detection

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# Unity Catalog external location handles ADLS auth via managed identity
storage_account = "cmsdevadls"
bronze_path = f"abfss://bronze@{storage_account}.dfs.core.windows.net/medicaid_fraud"
staging_path = f"abfss://bronze@{storage_account}.dfs.core.windows.net/staging/medicaid_provider_spending.parquet"

# COMMAND ----------

# Copy parquet file from public URL directly to ADLS staging area
SOURCE_URL = (
    "https://stopendataprod.blob.core.windows.net/datasets/"
    "medicaid-provider-spending/2026-02-09/dataset/medicaid-provider-spending.parquet"
)

print("Copying Medicaid dataset to ADLS staging...")
dbutils.fs.cp(SOURCE_URL, staging_path)
print("Copy complete.")

# COMMAND ----------

df = spark.read.parquet(staging_path)

print("Schema:")
df.printSchema()
print(f"Row count: {df.count():,}")

# COMMAND ----------

(
    df
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_source", F.lit("hhs_medicaid_provider_spending_2026_02_09"))
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(bronze_path)
)

print(f"Written to bronze Delta table at {bronze_path}")

# COMMAND ----------

# Clean up staging file
dbutils.fs.rm(staging_path)

# Compact any small files produced during write
spark.sql(f"OPTIMIZE delta.`{bronze_path}`")
print("OPTIMIZE complete.")

# COMMAND ----------

# Verify
display(spark.read.format("delta").load(bronze_path).limit(5))
