# Databricks notebook source
# COMMAND ----------
import sys
import os
import re
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
dataset_meta = DATASETS["inside_airbnb"]
TARGET_TABLE = dataset_meta["bronze_table"]
DBFS_PATH    = dataset_meta["dbfs_input_path"]

COLS_TO_KEEP = [
    "id", "name", "host_id", "neighbourhood_cleansed", "neighbourhood_group_cleansed",
    "latitude", "longitude", "room_type", "accommodates", "bedrooms", "beds",
    "price", "minimum_nights", "availability_365", "number_of_reviews",
    "reviews_per_month", "calculated_host_listings_count", "license",
]

# Ensure staging directory exists
try:
    dbutils.fs.mkdirs(DBFS_PATH)  # noqa: F821
except Exception:
    local_dir = "/dbfs" + DBFS_PATH if os.path.exists("/dbfs") else DBFS_PATH
    os.makedirs(local_dir, exist_ok=True)

# List CSV / gzip files
snapshot_files = []
try:
    snapshot_files = [
        f.path for f in dbutils.fs.ls(DBFS_PATH)  # noqa: F821
        if f.path.endswith(".csv.gz") or f.path.endswith(".csv")
    ]
except Exception:
    local_dir = "/dbfs" + DBFS_PATH if os.path.exists("/dbfs") else DBFS_PATH
    if os.path.exists(local_dir):
        snapshot_files = [
            os.path.join(local_dir, f)
            for f in os.listdir(local_dir)
            if f.endswith(".csv.gz") or f.endswith(".csv")
        ]

if not snapshot_files:
    print(f"No Airbnb files found in {DBFS_PATH}. Upload listings CSV/GZ to this folder to run.")
    try:
        dbutils.notebook.exit(f"Skipped: No files in {DBFS_PATH}")  # noqa: F821
    except Exception:
        sys.exit(0)

# COMMAND ----------
batch_id        = str(uuid.uuid4())
files_processed = 0

for path in snapshot_files:
    basename = os.path.basename(path)
    date_match = re.search(r"(\d{4}-\d{2}-\d{2})", basename)
    snapshot_date = date_match.group(1) if date_match else "unknown"

    local_path = path.replace("dbfs:", "/dbfs")
    compression = "gzip" if local_path.endswith(".gz") else None

    pdf = pd.read_csv(local_path, compression=compression, low_memory=False)
    available_cols = [c for c in COLS_TO_KEEP if c in pdf.columns]
    pdf = pdf[available_cols].copy()

    if "price" in pdf.columns:
        pdf["price"] = (
            pdf["price"]
            .astype(str)
            .str.replace("$", "", regex=False)
            .str.replace(",", "", regex=False)
            .str.strip()
        )
        pdf["price"] = pd.to_numeric(pdf["price"], errors="coerce")

    pdf["snapshot_date"] = snapshot_date

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
    print(f"Processed {basename} ({snapshot_date}): {len(pdf):,} rows")

print(f"Done. {files_processed} files -> {TARGET_TABLE}")

# COMMAND ----------
if spark.catalog.tableExists(TARGET_TABLE):
    spark.sql(f"SELECT count(*) as total_records FROM {TARGET_TABLE}").show()
    spark.sql(f"SELECT * FROM {TARGET_TABLE} LIMIT 5").show(truncate=False)
