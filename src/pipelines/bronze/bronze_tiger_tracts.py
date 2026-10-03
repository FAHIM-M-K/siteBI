# Databricks notebook source
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
from src.utils.socrata_client import SocrataClient

spark = SparkSession.builder.appName("Bronze_TIGER_Tracts").getOrCreate()
spark.sql(f"CREATE DATABASE IF NOT EXISTS {BRONZE_DB}")

# COMMAND ----------
dataset_meta = DATASETS["census_tiger_tracts"]
TARGET_TABLE = dataset_meta["bronze_table"]
# NYC 2020 Census Tracts dataset on NYC Open Data
DATASET_ID = "63ge-mke6"

client = SocrataClient(
    domain=DOMAIN_NYC_OPEN_DATA,
    app_token=NYC_OPEN_DATA_APP_TOKEN,
    timeout=90,
)

batch_id = str(uuid.uuid4())
source_uri = f"socrata://{DOMAIN_NYC_OPEN_DATA}/{DATASET_ID}"
raw_records = []

for page in client.fetch_all(
    dataset_id=DATASET_ID,
    page_size=10000,
    max_records=10000,
):
    for r in page:
        row = {}
        for k, v in r.items():
            row[k] = str(v) if isinstance(v, (dict, list)) else v
        raw_records.append(row)

if raw_records:
    df_raw = spark.createDataFrame(raw_records)
    df_bronze = (
        df_raw
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_batch_id", lit(batch_id))
        .withColumn("_source", lit(source_uri))
    )
    (
        df_bronze.write
        .format("delta")
        .mode("overwrite")
        .option("mergeSchema", "true")
        .saveAsTable(TARGET_TABLE)
    )

# COMMAND ----------
if spark.catalog.tableExists(TARGET_TABLE):
    spark.sql(f"SELECT count(*) as total_tracts FROM {TARGET_TABLE}").show()
    spark.sql(f"SELECT * FROM {TARGET_TABLE} LIMIT 5").show()
