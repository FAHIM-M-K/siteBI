# Databricks notebook source
# COMMAND ----------
# bronze_tlc_taxi.py
#
# Strategy: Download monthly Yellow Cab Parquet from TLC (2019–present), aggregate to
# taxi-zone level immediately, and persist ONLY the zone aggregates to Bronze.
# Raw trip rows are NEVER written to Delta — keeps storage CE-friendly (<500 MB total).
# Zone aggregates are joined to PLUTO/H3 in the Silver layer via taxi zone polygons.
# COMMAND ----------
import sys
import os
import uuid
import urllib.request
import pandas as pd
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit

notebook_path = os.getcwd()
if notebook_path not in sys.path:
    sys.path.append(notebook_path)

from src.utils.config import BRONZE_DB, DATASETS

spark = SparkSession.builder.appName("Bronze_TLC_ZoneAggregates").getOrCreate()
spark.sql(f"CREATE DATABASE IF NOT EXISTS {BRONZE_DB}")

# COMMAND ----------
dataset_meta = DATASETS["tlc_trips"]
TARGET_TABLE  = dataset_meta["bronze_table"]          # bronze.tlc_zone_agg_raw
ZONES_TABLE   = f"{BRONZE_DB}.tlc_zones_raw"
BASE_URL      = dataset_meta["base_url"]
ZONE_URL      = dataset_meta["zone_lookup_url"]
YEARS         = dataset_meta["years"]                 # 2019–2025

# COMMAND ----------
# Step 1 — Ingest taxi zone lookup (once; small static file)
pdf_zones = pd.read_csv(ZONE_URL)
(
    spark.createDataFrame(pdf_zones)
    .write.format("delta")
    .mode("overwrite")
    .option("mergeSchema", "true")
    .saveAsTable(ZONES_TABLE)
)
print(f"Zone lookup loaded: {len(pdf_zones)} zones → {ZONES_TABLE}")

# COMMAND ----------
# Step 2 — Iterate over every year/month, download monthly Parquet,
#           aggregate to zone level IN MEMORY, append only aggregates to Bronze.
batch_id = str(uuid.uuid4())
months_processed = 0
current_year  = datetime.now().year
current_month = datetime.now().month

for year in YEARS:
    for month in range(1, 13):
        # Skip future months
        if year == current_year and month >= current_month:
            break

        url = BASE_URL.format(year=year, month=month)
        local_path = f"/tmp/tlc_{year}_{month:02d}.parquet"

        try:
            urllib.request.urlretrieve(url, local_path)
        except Exception as e:
            print(f"  SKIP {year}-{month:02d}: {e}")
            continue

        # Load only the columns we need — minimise memory pressure on CE
        cols_needed = [
            "tpep_pickup_datetime", "PULocationID", "DOLocationID",
            "passenger_count", "trip_distance", "fare_amount",
        ]
        pdf = pd.read_parquet(local_path, columns=cols_needed)

        # Aggregate to zone level (drop-off zone × hour-of-day × day-of-week)
        pdf["pickup_hour"]    = pd.to_datetime(pdf["tpep_pickup_datetime"]).dt.hour
        pdf["pickup_dow"]     = pd.to_datetime(pdf["tpep_pickup_datetime"]).dt.dayofweek
        pdf["year"]           = year
        pdf["month"]          = month

        agg = (
            pdf.groupby(["DOLocationID", "pickup_hour", "pickup_dow", "year", "month"])
            .agg(
                trip_count      = ("fare_amount", "count"),
                avg_fare        = ("fare_amount", "mean"),
                avg_distance_mi = ("trip_distance", "mean"),
            )
            .reset_index()
            .rename(columns={"DOLocationID": "dolocationid"})
        )

        df_agg = spark.createDataFrame(agg)
        df_agg = (
            df_agg
            .withColumn("_ingested_at", current_timestamp())
            .withColumn("_batch_id",    lit(batch_id))
            .withColumn("_source",      lit(url))
        )

        (
            df_agg.write
            .format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .saveAsTable(TARGET_TABLE)
        )

        # Clean up temp file to free DBFS space
        os.remove(local_path)
        months_processed += 1
        print(f"  ✓ {year}-{month:02d}: {len(agg):,} zone-hour rows appended")

print(f"\nDone. {months_processed} months processed → {TARGET_TABLE}")

# COMMAND ----------
if spark.catalog.tableExists(TARGET_TABLE):
    spark.sql(f"SELECT count(*) as total_zone_agg_rows FROM {TARGET_TABLE}").show()
    spark.sql(f"""
        SELECT year, month, count(*) as rows
        FROM {TARGET_TABLE}
        GROUP BY year, month
        ORDER BY year, month
        LIMIT 20
    """).show()
