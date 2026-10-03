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

from src.utils.config import BRONZE_DB, DATASETS

spark = SparkSession.builder.appName("Bronze_StreetEasy_Rents").getOrCreate()
spark.sql(f"CREATE DATABASE IF NOT EXISTS {BRONZE_DB}")

# COMMAND ----------
dataset_meta = DATASETS["streeteasy_rents"]
TARGET_TABLE = dataset_meta["bronze_table"]
DBFS_PATH    = dataset_meta["dbfs_input_path"]

BEDROOM_MAP = {
    "all":         "all",
    "studio":      "studio",
    "onebd":       "1br",
    "twobd":       "2br",
    "threeplusbd": "3br+",
}

# Ensure staging directory exists
try:
    dbutils.fs.mkdirs(DBFS_PATH)  # noqa: F821
except Exception:
    pass

# List CSV files
csv_files = []
try:
    csv_files = [
        f.path for f in dbutils.fs.ls(DBFS_PATH)  # noqa: F821
        if f.path.endswith(".csv")
    ]
except Exception:
    csv_files = []

if not csv_files:
    print(f"No CSVs found in {DBFS_PATH}. Upload StreetEasy CSV files to this folder to run.")
    try:
        dbutils.notebook.exit(f"Skipped: No files in {DBFS_PATH}")  # noqa: F821
    except Exception:
        sys.exit(0)

# COMMAND ----------
batch_id        = str(uuid.uuid4())
files_processed = 0

for path in csv_files:
    filename = os.path.basename(path).lower().replace("medianaskingrents_", "").replace(".csv", "")
    bedroom_size = BEDROOM_MAP.get(filename, filename)

    local_path = path.replace("dbfs:", "/dbfs")
    pdf = pd.read_csv(local_path)

    id_col = pdf.columns[0]
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
    print(f"Processed {os.path.basename(path)} ({bedroom_size}): {len(pdf_long):,} rows")

print(f"Done. {files_processed} files -> {TARGET_TABLE}")

# COMMAND ----------
if spark.catalog.tableExists(TARGET_TABLE):
    spark.sql(f"SELECT count(*) as total_records FROM {TARGET_TABLE}").show()
    spark.sql(f"SELECT * FROM {TARGET_TABLE} LIMIT 5").show(truncate=False)
