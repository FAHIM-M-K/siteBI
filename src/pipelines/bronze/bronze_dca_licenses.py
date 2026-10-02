# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # Bronze Pipeline: NYC DCA/DCWP Issued Licenses
# MAGIC
# MAGIC **Purpose:** Ingest raw business licenses from NYC Open Data (Socrata API `w7w3-xahh`) into the raw landing layer.
# MAGIC
# MAGIC **Pipeline Rules:**
# MAGIC - **Write Mode:** Append-only into `bronze.dca_licenses_raw`
# MAGIC - **Audit Columns:** `_ingested_at`, `_batch_id`, `_source`
# MAGIC - **Watermark Strategy:** Pulls records where `license_creation_date >= watermark`
# MAGIC - **Schema Policy:** `mergeSchema = true` (tolerate source schema drift)

# COMMAND ----------
# MAGIC %pip install requests

# COMMAND ----------
import sys
import os
import uuid

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit

# Add repo root to sys.path if running in Databricks Repos
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

# Initialize Spark Session
spark = SparkSession.builder.appName("Bronze_DCA_Licenses").getOrCreate()

# Ensure Bronze Metastore database exists
spark.sql(f"CREATE DATABASE IF NOT EXISTS {BRONZE_DB}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Watermark Detection

# COMMAND ----------
dataset_meta = DATASETS["dca_licenses"]
TARGET_TABLE = dataset_meta["bronze_table"]
SILVER_TABLE = dataset_meta["silver_table"]
DATASET_ID = dataset_meta["dataset_id"]
WATERMARK_COL = dataset_meta["watermark_col"]
DEFAULT_WATERMARK = dataset_meta["default_watermark"]


def get_latest_watermark(spark_session: SparkSession, silver_tbl: str, col_name: str, fallback: str) -> str:
    """
    Retrieves the maximum watermark date from Silver table.
    Defaults to fallback date if Silver table does not exist or has no records.
    """
    try:
        if spark_session.catalog.tableExists(silver_tbl):
            res = spark_session.sql(f"SELECT MAX({col_name}) AS max_val FROM {silver_tbl}").collect()
            if res and res[0]["max_val"]:
                return str(res[0]["max_val"])
    except Exception as e:
        print(f"[*] Note: Table {silver_tbl} not yet initialized or empty ({e}). Using default watermark.")
    return fallback


watermark_value = get_latest_watermark(spark, SILVER_TABLE, WATERMARK_COL, DEFAULT_WATERMARK)
print(f"[*] Watermark for DCA Licenses: {WATERMARK_COL} >= '{watermark_value}'")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Ingest Raw Data from NYC Open Data (Socrata API)

# COMMAND ----------
client = SocrataClient(
    domain=DOMAIN_NYC_OPEN_DATA,
    app_token=NYC_OPEN_DATA_APP_TOKEN,
    timeout=90,
)

where_filter = f"{WATERMARK_COL} >= '{watermark_value}'"
order_clause = f"{WATERMARK_COL} ASC"

print(f"[*] Extracting records from {DATASET_ID} with filter: {where_filter}")

# Fetch records using generator
raw_records = []
total_records = 0
BATCH_RECORD_LIMIT = 100000  # Safe batch limit for community edition memory

for page in client.fetch_all(
    dataset_id=DATASET_ID,
    page_size=25000,
    max_records=BATCH_RECORD_LIMIT,
    where=where_filter,
    order=order_clause,
):
    raw_records.extend(page)
    total_records += len(page)
    print(f"[*] Accumulated {total_records} records so far...")

print(f"[✓] Completed extraction. Total raw records fetched: {len(raw_records)}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Append to Delta Bronze Table with Audit Metadata

# COMMAND ----------
batch_id = str(uuid.uuid4())
source_uri = f"socrata://{DOMAIN_NYC_OPEN_DATA}/{DATASET_ID}"

if raw_records:
    # Convert raw python dictionaries to Spark DataFrame
    df_raw = spark.createDataFrame(raw_records)
    
    # Add Bronze audit metadata columns
    df_bronze = (
        df_raw
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_batch_id", lit(batch_id))
        .withColumn("_source", lit(source_uri))
    )
    
    # Append-only write to Delta Lake
    (
        df_bronze.write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable(TARGET_TABLE)
    )
    
    print(f"[✓] Successfully appended {len(raw_records)} records to {TARGET_TABLE} (Batch ID: {batch_id})")
else:
    print(f"[!] No new records found matching watermark filter. Bronze table remains unchanged.")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4. Ingestion Summary Check

# COMMAND ----------
if spark.catalog.tableExists(TARGET_TABLE):
    count_df = spark.sql(f"SELECT count(*) as total_raw_count FROM {TARGET_TABLE}")
    count_df.show()
    
    batches_df = spark.sql(f"""
        SELECT _batch_id, date_trunc('second', _ingested_at) as ingested_time, count(*) as count 
        FROM {TARGET_TABLE} 
        GROUP BY _batch_id, date_trunc('second', _ingested_at)
        ORDER BY ingested_time DESC 
        LIMIT 5
    """)
    batches_df.show(truncate=False)
