# Databricks notebook source
# COMMAND ----------
import sys
import os
import uuid

import pandas as pd
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

spark = SparkSession.builder.appName("Bronze_DOF_Valuations").getOrCreate()
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")
spark.sql(f"CREATE DATABASE IF NOT EXISTS {BRONZE_DB}")

# COMMAND ----------
dataset_meta = DATASETS["dof_valuations"]
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

SELECT_FIELDS = "bble,boro,block,lot,owner,bldgcl,taxclass,stories,fullval,avland,avtot,exland,extot,staddr,year,valtype"
# Filter to the latest assessment roll (1.1M records total across all NYC tax lots)
# Without this filter, the dataset attempts to pull 10+ years of historical rolls (9.85M records)
LATEST_ROLL_YEAR = "2018/19"

# COMMAND ----------
for page in client.fetch_all(
    dataset_id=DATASET_ID,
    page_size=50000,
    select=SELECT_FIELDS,
    where=f"year = '{LATEST_ROLL_YEAR}'",
):
    if not page:
        continue
    cleaned = flatten_records(page)
    pdf = pd.DataFrame(cleaned).astype(str)
    df_chunk = spark.createDataFrame(pdf)
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

print(f"Done. {total_ingested:,} valuation records -> {TARGET_TABLE}")

# COMMAND ----------
if spark.catalog.tableExists(TARGET_TABLE):
    spark.sql(f"SELECT count(*) as total_records FROM {TARGET_TABLE}").show()
    spark.sql(f"SELECT bble, boro, block, lot, fullval, avtot, year FROM {TARGET_TABLE} LIMIT 5").show(truncate=False)
