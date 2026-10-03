# Databricks notebook source
# COMMAND ----------
# bronze_vacant_storefronts_commercial.py
#
# Ingests NYC Storefronts Reported Vacant or Not (Commercial Vacancy Registry).
# Dataset ID: 92uh-c6xg
# Primary Key: bbl, survey_year
# Provides annual storefront occupancy status, leased vs. vacant indicators,
# and square footage for ground-floor commercial units across NYC.
# COMMAND ----------
import sys
import os
import uuid

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit

notebook_path = os.getcwd()
if notebook_path not in sys.path:
    sys.path.append(notebook_path)

from src.utils.config import (
    BRONZE_DB,
    DATASETS,
    DOMAIN_NYC_OPEN_DATA,
    NYC_OPEN_DATA_APP_TOKEN,
)
from src.utils.socrata_client import SocrataClient, flatten_records

spark = SparkSession.builder.appName("Bronze_Vacant_Storefronts_Commercial").getOrCreate()
spark.sql(f"CREATE DATABASE IF NOT EXISTS {BRONZE_DB}")

# COMMAND ----------
dataset_meta = DATASETS["vacant_storefronts_commercial"]
TARGET_TABLE = dataset_meta["bronze_table"]
DATASET_ID   = dataset_meta["dataset_id"]

client = SocrataClient(
    domain=DOMAIN_NYC_OPEN_DATA,
    app_token=NYC_OPEN_DATA_APP_TOKEN,
    timeout=90,
)

batch_id       = str(uuid.uuid4())
source_uri     = f"socrata://{DOMAIN_NYC_OPEN_DATA}/{DATASET_ID}"
total_ingested = 0

# COMMAND ----------
for page in client.fetch_all(
    dataset_id=DATASET_ID,
    page_size=25000,
):
    if not page:
        continue
    cleaned = flatten_records(page)
    df_chunk = spark.createDataFrame(cleaned)
    df_chunk = (
        df_chunk
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_batch_id",    lit(batch_id))
        .withColumn("_source",      lit(source_uri))
    )
    (
        df_chunk.write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable(TARGET_TABLE)
    )
    total_ingested += len(page)
    print(f"Ingested {len(page):,} records (cumulative: {total_ingested:,})")

print(f"Done. {total_ingested:,} commercial storefront records → {TARGET_TABLE}")

# COMMAND ----------
if spark.catalog.tableExists(TARGET_TABLE):
    spark.sql(f"SELECT count(*) as total_records FROM {TARGET_TABLE}").show()
    spark.sql(f"SELECT * FROM {TARGET_TABLE} LIMIT 5").show(truncate=False)
