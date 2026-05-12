# Databricks notebook source
# Bronze Layer: Medicare Provider Utilization and Payment Data
# Source: https://data.cms.gov/provider-summary-by-type-of-service/medicare-physician-other-practitioners/medicare-physician-other-practitioners-by-provider-and-service

# COMMAND ----------

import requests
from pyspark.sql import functions as F

# COMMAND ----------

# Unity Catalog external location handles ADLS auth via managed identity
storage_account = "cmsdevadls"
bronze_path = f"abfss://bronze@{storage_account}.dfs.core.windows.net/medicare"

# COMMAND ----------

# CMS open data API — Medicare Physician & Other Practitioners by Provider and Service
CMS_API_URL = (
    "https://data.cms.gov/data-api/v1/dataset/"
    "92396110-2aed-4d63-a6a2-5d6207d46a29/data"
)

BATCH_SIZE = 5000
offset = 0
total_written = 0
first_batch = True

print("Downloading Medicare provider data from CMS API (streaming batches to Delta)...")

while True:
    response = requests.get(
        CMS_API_URL,
        params={"size": BATCH_SIZE, "offset": offset},
        timeout=60
    )
    response.raise_for_status()
    batch = response.json()

    if not batch:
        break

    df = (
        spark.createDataFrame(batch)
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source", F.lit("cms_medicare_provider_service_2022"))
    )

    write_mode = "overwrite" if first_batch else "append"
    overwrite_schema = "true" if first_batch else "false"

    (
        df.write
        .format("delta")
        .mode(write_mode)
        .option("overwriteSchema", overwrite_schema)
        .save(bronze_path)
    )

    total_written += len(batch)
    offset += BATCH_SIZE
    first_batch = False
    print(f"  Written {total_written:,} rows so far...")

    if len(batch) < BATCH_SIZE:
        break

print(f"Ingestion complete. Total rows written: {total_written:,}")

# COMMAND ----------

# Compact small files created by batch writes
spark.sql(f"OPTIMIZE delta.`{bronze_path}`")
print("OPTIMIZE complete.")

# COMMAND ----------

# Verify
display(spark.read.format("delta").load(bronze_path).limit(5))
