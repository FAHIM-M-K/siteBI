# Databricks notebook source
# COMMAND ----------
# bronze_inside_airbnb.py
#
# Ingests Inside Airbnb NYC listing snapshots (gzipped CSV).
# Download from: http://data.insideairbnb.com/united-states/ny/new-york-city/
# Upload to DBFS at: /FileStore/sitebi/airbnb/
#
# Each snapshot file is named with its date, e.g.: listings_2024-09-04.csv.gz
# A snapshot_date column is derived from the filename to track each quarterly snapshot.
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

from src.utils.config import BRONZE_DB, DATASETS

spark = SparkSession.builder.appName("Bronze_InsideAirbnb").getOrCreate()
spark.sql(f"CREATE DATABASE IF NOT EXISTS {BRONZE_DB}")

# COMMAND ----------
dataset_meta  = DATASETS["inside_airbnb"]
TARGET_TABLE  = dataset_meta["bronze_table"]
DBFS_PATH     = dataset_meta["dbfs_input_path"]

# Only ingest these columns — full listing CSV has 70+ columns we don't need
COLS_TO_KEEP = [
    "id", "name", "host_id", "neighbourhood_cleansed", "neighbourhood_group_cleansed",
    "latitude", "longitude", "room_type", "accommodates", "bedrooms", "beds",
    "price", "minimum_nights", "availability_365", "number_of_reviews",
    "reviews_per_month", "calculated_host_listings_count", "license",
]

# COMMAND ----------
batch_id        = str(uuid.uuid4())
files_processed = 0

try:
    snapshot_files = [
        f.path for f in dbutils.fs.ls(DBFS_PATH)    # noqa: F821
        if f.path.endswith(".csv.gz") or f.path.endswith(".csv")
    ]
except Exception:
    local_dir = DBFS_PATH.replace("/dbfs", "").replace("dbfs:", "")
    snapshot_files = [
        os.path.join(local_dir, f)
        for f in os.listdir(local_dir)
        if f.endswith(".csv.gz") or f.endswith(".csv")
    ]

if not snapshot_files:
    raise FileNotFoundError(
        f"No Airbnb CSV files found at {DBFS_PATH}. "
        "Download from http://data.insideairbnb.com/united-states/ny/new-york-city/"
    )

for path in snapshot_files:
    # Derive snapshot date from filename (e.g. listings_2024-09-04.csv.gz → 2024-09-04)
    basename = os.path.basename(path)
    # Extract YYYY-MM-DD pattern from filename
    import re
    date_match = re.search(r"(\d{4}-\d{2}-\d{2})", basename)
    snapshot_date = date_match.group(1) if date_match else "unknown"

    local_path = path.replace("dbfs:", "/dbfs")
    compression = "gzip" if local_path.endswith(".gz") else None

    pdf = pd.read_csv(
        local_path,
        compression=compression,
        usecols=lambda c: c in COLS_TO_KEEP,
        low_memory=False,
    )
    pdf["snapshot_date"] = snapshot_date

    # Strip $ from price column
    if "price" in pdf.columns:
        pdf["price"] = (
            pdf["price"].astype(str)
            .str.replace(r"[\$,]", "", regex=True)
            .pipe(pd.to_numeric, errors="coerce")
        )

    df = spark.createDataFrame(pdf)
    df = (
        df
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_batch_id",    lit(batch_id))
        .withColumn("_source",      lit(path))
    )

    (
        df.write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable(TARGET_TABLE)
    )
    files_processed += 1
    print(f"  ✓ {basename} (snapshot={snapshot_date}): {len(pdf):,} listings")

print(f"\nDone. {files_processed} snapshots → {TARGET_TABLE}")

# COMMAND ----------
if spark.catalog.tableExists(TARGET_TABLE):
    spark.sql(f"SELECT count(*) as total_records FROM {TARGET_TABLE}").show()
    spark.sql(f"""
        SELECT snapshot_date, count(*) as listings, round(avg(price),2) as avg_nightly_price
        FROM {TARGET_TABLE}
        GROUP BY snapshot_date ORDER BY snapshot_date
    """).show()
