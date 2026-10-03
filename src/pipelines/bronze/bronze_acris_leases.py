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

spark = SparkSession.builder.appName("Bronze_ACRIS_Leases").getOrCreate()
spark.sql(f"CREATE DATABASE IF NOT EXISTS {BRONZE_DB}")

# COMMAND ----------
dataset_meta = DATASETS["acris_leases"]
TARGET_TABLE = dataset_meta["bronze_table"]
MASTER_ID    = dataset_meta["master_dataset_id"]
DOC_TYPES    = dataset_meta["doc_type_filter"]
DEFAULT_WM   = dataset_meta["default_watermark"]

client = SocrataClient(
    domain=DOMAIN_NYC_OPEN_DATA,
    app_token=NYC_OPEN_DATA_APP_TOKEN,
    timeout=90,
)

watermark = DEFAULT_WM
if spark.catalog.tableExists(TARGET_TABLE):
    try:
        max_ts = (
            spark.sql(f"SELECT MAX(recorded_datetime) AS max_dt FROM {TARGET_TABLE}")
            .collect()[0]["max_dt"]
        )
        if max_ts:
            watermark = max_ts
    except Exception:
        pass

doc_types_str = ", ".join(f"'{dt}'" for dt in DOC_TYPES)
master_where  = f"doc_type in ({doc_types_str}) AND recorded_datetime >= '{watermark}'"

batch_id       = str(uuid.uuid4())
source_uri     = f"socrata://{DOMAIN_NYC_OPEN_DATA}/{MASTER_ID}"
total_ingested = 0

# COMMAND ----------
for page in client.fetch_all(
    dataset_id=MASTER_ID,
    page_size=50000,
    where=master_where,
    order="recorded_datetime ASC",
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
    print(f"Ingested {len(page):,} lease records (cumulative: {total_ingested:,})")

print(f"Done. {total_ingested:,} ACRIS lease records -> {TARGET_TABLE}")

# COMMAND ----------
if spark.catalog.tableExists(TARGET_TABLE):
    spark.sql(f"SELECT count(*) as total_records FROM {TARGET_TABLE}").show()
    spark.sql(f"SELECT document_id, doc_type, recorded_datetime, doc_amount, recorded_borough FROM {TARGET_TABLE} LIMIT 5").show(truncate=False)
