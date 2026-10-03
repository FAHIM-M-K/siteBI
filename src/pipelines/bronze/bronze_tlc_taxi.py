# Databricks notebook source
# COMMAND ----------
import sys
import os
import uuid
import urllib.request
import pandas as pd

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit

notebook_path = os.getcwd()
if notebook_path not in sys.path:
    sys.path.append(notebook_path)

from src.utils.config import BRONZE_DB, DATASETS

spark = SparkSession.builder.appName("Bronze_TLC_Taxi").getOrCreate()
spark.sql(f"CREATE DATABASE IF NOT EXISTS {BRONZE_DB}")

# COMMAND ----------
dataset_meta = DATASETS["tlc_trips"]
TARGET_TABLE = dataset_meta["bronze_table"]

# 1. Ingest TLC Taxi Zone Lookup (Base spatial mapping) via Pandas (bypasses DBFS restrictions)
zone_url = "https://d37ci6vzurychx.cloudfront.net/misc/taxi+_zone_lookup.csv"
pdf_zones = pd.read_csv(zone_url)
df_zones = spark.createDataFrame(pdf_zones)
(
    df_zones.write
    .format("delta")
    .mode("overwrite")
    .option("mergeSchema", "true")
    .saveAsTable(f"{BRONZE_DB}.tlc_zones_raw")
)

# 2. Ingest Sample Taxi Trips (Yellow cab 2024 sample)
trip_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet"
local_trip_file = "/tmp/yellow_tripdata_sample.parquet"

urllib.request.urlretrieve(trip_url, local_trip_file)

# Read locally via pandas (no DBFS requirement)
pdf_trips = pd.read_parquet(local_trip_file).head(100000)

# Convert timestamp columns to ISO strings to ensure seamless PySpark schema ingestion
for col in pdf_trips.columns:
    if "date" in col.lower() or "time" in col.lower():
        pdf_trips[col] = pdf_trips[col].astype(str)

df_trips = spark.createDataFrame(pdf_trips)

batch_id = str(uuid.uuid4())
df_bronze = (
    df_trips
    .withColumn("_ingested_at", current_timestamp())
    .withColumn("_batch_id", lit(batch_id))
    .withColumn("_source", lit(trip_url))
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
    spark.sql(f"SELECT count(*) as total_sampled_trips FROM {TARGET_TABLE}").show()
    spark.sql(f"SELECT count(*) as total_zones FROM {BRONZE_DB}.tlc_zones_raw").show()
