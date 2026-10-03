# Databricks notebook source
# COMMAND ----------
# MAGIC %pip install pyarrow

# COMMAND ----------
import sys
import os
import uuid
import urllib.request

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

# Ingest TLC Taxi Zone Lookup (Base spatial mapping)
zone_url = "https://d37ci6vzurychx.cloudfront.net/misc/taxi+_zone_lookup.csv"
local_zone_file = "/tmp/taxi_zone_lookup.csv"
urllib.request.urlretrieve(zone_url, local_zone_file)

df_zones = spark.read.csv(local_zone_file, header=True, inferSchema=True)
df_zones.write.format("delta").mode("overwrite").saveAsTable(f"{BRONZE_DB}.tlc_zones_raw")

# Sample monthly taxi trips (Yellow cab sample)
trip_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet"
local_trip_file = "/tmp/yellow_tripdata_sample.parquet"

# Download parquet and read with Spark limit
urllib.request.urlretrieve(trip_url, local_trip_file)
df_trips = spark.read.parquet(local_trip_file).limit(100000)

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
