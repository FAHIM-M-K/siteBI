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

spark = SparkSession.builder.appName("Bronze_PLUTO").getOrCreate()
spark.sql(f"CREATE DATABASE IF NOT EXISTS {BRONZE_DB}")

# COMMAND ----------
dataset_meta = DATASETS["pluto"]
TARGET_TABLE = dataset_meta["bronze_table"]
DATASET_ID = dataset_meta["dataset_id"]

client = SocrataClient(
    domain=DOMAIN_NYC_OPEN_DATA,
    app_token=NYC_OPEN_DATA_APP_TOKEN,
    timeout=120,
)

SELECT_FIELDS = (
    "bbl,borough,block,lot,cd,zonedist1,zonedist2,overlay1,overlay2,spdist1,"
    "landuse,bldgclass,numfloors,yearbuilt,lotarea,bldgarea,comarea,resarea,"
    "officearea,retailarea,garagearea,strgearea,factryarea,otherarea,"
    "latitude,longitude"
)

raw_records = []
for page in client.fetch_all(
    dataset_id=DATASET_ID,
    page_size=25000,
    max_records=100000,
    select=SELECT_FIELDS,
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
    spark.sql(f"SELECT count(*) as total_tax_lots FROM {TARGET_TABLE}").show()
    spark.sql(f"""
        SELECT zonedist1, count(*) as count 
        FROM {TARGET_TABLE} 
        WHERE _batch_id = '{batch_id}' 
        GROUP BY zonedist1 
        ORDER BY count DESC 
        LIMIT 10
    """).show()
