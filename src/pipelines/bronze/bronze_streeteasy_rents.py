# Databricks notebook source
# COMMAND ----------
# bronze_streeteasy_rents.py
#
# Ingests StreetEasy Median Asking Rent CSVs (residential).
# StreetEasy has no public API — CSVs must be manually downloaded from:
#   https://streeteasy.com/blog/data-dashboard/
# and uploaded to DBFS at: /FileStore/sitebi/streeteasy/
#
# Each CSV file has the structure: one row per neighborhood per month.
# Files are typically named by bedroom type, e.g.:
#   - medianAskingRent_All.csv
#   - medianAskingRent_Studio.csv
#   - medianAskingRent_OneBd.csv
#   - medianAskingRent_TwoBd.csv
#   - medianAskingRent_ThreePlusBd.csv
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

spark = SparkSession.builder.appName("Bronze_StreetEasy_Rents").getOrCreate()
spark.sql(f"CREATE DATABASE IF NOT EXISTS {BRONZE_DB}")

# COMMAND ----------
dataset_meta  = DATASETS["streeteasy_rents"]
TARGET_TABLE  = dataset_meta["bronze_table"]
DBFS_PATH     = dataset_meta["dbfs_input_path"]

# Bedroom size label inferred from filename
BEDROOM_MAP = {
    "all":         "all",
    "studio":      "studio",
    "onebd":       "1br",
    "twobd":       "2br",
    "threeplusbd": "3br+",
}

# COMMAND ----------
batch_id        = str(uuid.uuid4())
files_processed = 0

# List all CSVs in the DBFS staging folder
try:
    csv_files = [
        f.path for f in dbutils.fs.ls(DBFS_PATH)    # noqa: F821  (dbutils injected by Databricks)
        if f.path.endswith(".csv")
    ]
except Exception:
    # Fallback for local testing outside Databricks
    local_dir = DBFS_PATH.replace("/dbfs", "").replace("dbfs:", "")
    csv_files = [
        os.path.join(local_dir, f)
        for f in os.listdir(local_dir) if f.endswith(".csv")
    ]

if not csv_files:
    raise FileNotFoundError(
        f"No CSVs found at {DBFS_PATH}. "
        "Download from https://streeteasy.com/blog/data-dashboard/ and upload to DBFS."
    )

for path in csv_files:
    filename = os.path.basename(path).lower().replace("medianaskingrents_", "").replace(".csv", "")
    bedroom_size = BEDROOM_MAP.get(filename, filename)

    local_path = path.replace("dbfs:", "/dbfs")
    pdf = pd.read_csv(local_path)

    # StreetEasy CSVs: first column = area_name, remaining = month columns (YYYY-MM format)
    # Melt from wide → long format
    id_col = pdf.columns[0]   # usually "areaName" or "Area"
    pdf = pdf.rename(columns={id_col: "area_name"})
    pdf_long = pdf.melt(id_vars=["area_name"], var_name="period_start", value_name="median_asking_rent")
    pdf_long["bedroom_size"] = bedroom_size
    pdf_long = pdf_long.dropna(subset=["median_asking_rent"])

    df = spark.createDataFrame(pdf_long)
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
    print(f"  ✓ {os.path.basename(path)} ({bedroom_size}): {len(pdf_long):,} rows")

print(f"\nDone. {files_processed} files → {TARGET_TABLE}")

# COMMAND ----------
if spark.catalog.tableExists(TARGET_TABLE):
    spark.sql(f"SELECT count(*) as total_records FROM {TARGET_TABLE}").show()
    spark.sql(f"""
        SELECT bedroom_size, count(*) as rows, min(period_start) as earliest, max(period_start) as latest
        FROM {TARGET_TABLE}
        GROUP BY bedroom_size ORDER BY bedroom_size
    """).show()
