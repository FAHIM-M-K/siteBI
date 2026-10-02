# Databricks notebook source
# COMMAND ----------
# MAGIC %pip install requests

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
    SILVER_DB,
    DATASETS,
    DOMAIN_NYC_OPEN_DATA,
    NYC_OPEN_DATA_APP_TOKEN,
)
from src.utils.socrata_client import SocrataClient

spark = SparkSession.builder.appName("Bronze_DCA_Licenses").getOrCreate()
spark.sql(f"CREATE DATABASE IF NOT EXISTS {BRONZE_DB}")

# COMMAND ----------
dataset_meta = DATASETS["dca_licenses"]
TARGET_TABLE = dataset_meta["bronze_table"]
SILVER_TABLE = dataset_meta["silver_table"]
DATASET_ID = dataset_meta["dataset_id"]
WATERMARK_COL = dataset_meta["watermark_col"]
DEFAULT_WATERMARK = dataset_meta["default_watermark"]

def get_latest_watermark(spark_session: SparkSession, silver_tbl: str, col_name: str, fallback: str) -> str:
    try:
        if spark_session.catalog.tableExists(silver_tbl):
            res = spark_session.sql(f"SELECT MAX({col_name}) AS max_val FROM {silver_tbl}").collect()
            if res and res[0]["max_val"]:
                return str(res[0]["max_val"])
    except Exception:
        pass
    return fallback

watermark_value = get_latest_watermark(spark, SILVER_TABLE, WATERMARK_COL, DEFAULT_WATERMARK)

# COMMAND ----------
client = SocrataClient(
    domain=DOMAIN_NYC_OPEN_DATA,
    app_token=NYC_OPEN_DATA_APP_TOKEN,
    timeout=90,
)

where_filter = f"{WATERMARK_COL} >= '{watermark_value}'"
order_clause = f"{WATERMARK_COL} ASC"

raw_records = []
for page in client.fetch_all(
    dataset_id=DATASET_ID,
    page_size=25000,
    max_records=100000,
    where=where_filter,
    order=order_clause,
):
    raw_records.extend(page)

# COMMAND ----------
batch_id = str(uuid.uuid4())
source_uri = f"socrata://{DOMAIN_NYC_OPEN_DATA}/{DATASET_ID}"

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
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable(TARGET_TABLE)
    )

# COMMAND ----------
if spark.catalog.tableExists(TARGET_TABLE):
    spark.sql(f"SELECT count(*) as total_raw_count FROM {TARGET_TABLE}").show()
    spark.sql(f"""
        SELECT _batch_id, date_trunc('second', _ingested_at) as ingested_time, count(*) as count 
        FROM {TARGET_TABLE} 
        GROUP BY _batch_id, date_trunc('second', _ingested_at)
        ORDER BY ingested_time DESC 
        LIMIT 5
    """).show(truncate=False)
