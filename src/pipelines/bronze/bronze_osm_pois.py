# Databricks notebook source
# COMMAND ----------
import sys
import os
import uuid
import requests

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit

notebook_path = os.getcwd()
if notebook_path not in sys.path:
    sys.path.append(notebook_path)

from src.utils.config import (
    BRONZE_DB,
    DATASETS,
    NYC_LAT_MIN,
    NYC_LAT_MAX,
    NYC_LON_MIN,
    NYC_LON_MAX,
)

spark = SparkSession.builder.appName("Bronze_OSM_POIs").getOrCreate()
spark.sql(f"CREATE DATABASE IF NOT EXISTS {BRONZE_DB}")

# COMMAND ----------
dataset_meta = DATASETS["osm_pois"]
TARGET_TABLE = dataset_meta["bronze_table"]

overpass_url = "https://overpass-api.de/api/interpreter"
query = f"""
[out:json][timeout:90];
(
  node["amenity"]({NYC_LAT_MIN},{NYC_LON_MIN},{NYC_LAT_MAX},{NYC_LON_MAX});
  node["shop"]({NYC_LAT_MIN},{NYC_LON_MIN},{NYC_LAT_MAX},{NYC_LON_MAX});
);
out body 25000;
"""

batch_id = str(uuid.uuid4())
source_uri = "https://overpass-api.de/api/interpreter"

resp = requests.post(overpass_url, data={"data": query}, timeout=120)
if resp.status_code == 200:
    elements = resp.json().get("elements", [])
    records = []
    for el in elements:
        tags = el.get("tags", {})
        records.append({
            "osm_id": str(el.get("id")),
            "lat": el.get("lat"),
            "lon": el.get("lon"),
            "name": tags.get("name"),
            "amenity": tags.get("amenity"),
            "shop": tags.get("shop"),
            "cuisine": tags.get("cuisine"),
            "brand": tags.get("brand"),
        })
    
    if records:
        df_raw = spark.createDataFrame(records)
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
    spark.sql(f"SELECT count(*) as total_osm_pois FROM {TARGET_TABLE}").show()
    spark.sql(f"""
        SELECT COALESCE(amenity, shop) as category, count(*) as count
        FROM {TARGET_TABLE}
        WHERE _batch_id = '{batch_id}'
        GROUP BY 1
        ORDER BY 2 DESC
        LIMIT 10
    """).show(truncate=False)
