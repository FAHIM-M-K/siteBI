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
from src.utils.socrata_client import SocrataClient, flatten_records

spark = SparkSession.builder.appName("Bronze_Restaurant_Inspections").getOrCreate()
spark.sql(f"CREATE DATABASE IF NOT EXISTS {BRONZE_DB}")

# COMMAND ----------
dataset_meta = DATASETS["restaurant_inspections"]
TARGET_TABLE = dataset_meta["bronze_table"]
DATASET_ID = dataset_meta["dataset_id"]
WATERMARK_COL = dataset_meta["watermark_col"]
DEFAULT_WATERMARK = dataset_meta["default_watermark"]

client = SocrataClient(
    domain=DOMAIN_NYC_OPEN_DATA,
    app_token=NYC_OPEN_DATA_APP_TOKEN,
    timeout=90,
)

where_filter = f"{WATERMARK_COL} >= '{DEFAULT_WATERMARK}'"
batch_id = str(uuid.uuid4())
source_uri = f"socrata://{DOMAIN_NYC_OPEN_DATA}/{DATASET_ID}"
total_ingested = 0

for page in client.fetch_all(
    dataset_id=DATASET_ID,
    page_size=25000,
    max_records=150000,
    where=where_filter,
    order=f"{WATERMARK_COL} ASC",
):
    if not page:
        continue
    cleaned = flatten_records(page)
    df_chunk = spark.createDataFrame(cleaned)
    df_chunk = (
        df_chunk
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_batch_id", lit(batch_id))
        .withColumn("_source", lit(source_uri))
    )
    (
        df_chunk.write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable(TARGET_TABLE)
    )
    total_ingested += len(page)

# COMMAND ----------
if spark.catalog.tableExists(TARGET_TABLE):
    spark.sql(f"SELECT count(*) as total_inspections FROM {TARGET_TABLE}").show()
    spark.sql(f"""
        SELECT cuisine_description, count(*) as count
        FROM {TARGET_TABLE}
        WHERE _batch_id = '{batch_id}'
        GROUP BY 1
        ORDER BY 2 DESC
        LIMIT 10
    """).show(truncate=False)
