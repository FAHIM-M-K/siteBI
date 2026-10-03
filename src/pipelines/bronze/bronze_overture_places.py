# Databricks notebook source
# COMMAND ----------
# MAGIC %pip install -q duckdb

# COMMAND ----------
import sys
import os
import uuid
import duckdb

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

spark = SparkSession.builder.appName("Bronze_Overture_Places").getOrCreate()
spark.sql(f"CREATE DATABASE IF NOT EXISTS {BRONZE_DB}")

# COMMAND ----------
dataset_meta = DATASETS["overture_places"]
TARGET_TABLE = dataset_meta["bronze_table"]

# Query Overture Maps S3 Parquet directly via DuckDB with bounding box
con = duckdb.connect()
con.execute("INSTALL spatial; LOAD spatial;")
con.execute("INSTALL httpfs; LOAD httpfs;")

query = f"""
SELECT 
    id,
    names.primary as name,
    categories.primary as category,
    confidence,
    ST_Y(geometry) as latitude,
    ST_X(geometry) as longitude
FROM read_parquet('s3://overturemaps-us-west-2/release/2024-02-15-alpha.0/theme=places/type=place/*', filename=true, hive_partitioning=1)
WHERE bbox.xmin >= {NYC_LON_MIN} 
  AND bbox.xmax <= {NYC_LON_MAX} 
  AND bbox.ymin >= {NYC_LAT_MIN} 
  AND bbox.ymax <= {NYC_LAT_MAX}
LIMIT 50000
"""

try:
    df_places_pd = con.execute(query).df()
    if not df_places_pd.empty:
        batch_id = str(uuid.uuid4())
        source_uri = "s3://overturemaps-us-west-2"

        df_raw = spark.createDataFrame(df_places_pd)
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
except Exception as e:
    print(f"Overture Maps S3 read notice: {e}")

# COMMAND ----------
if spark.catalog.tableExists(TARGET_TABLE):
    spark.sql(f"SELECT count(*) as total_overture_places FROM {TARGET_TABLE}").show()
    spark.sql(f"SELECT category, count(*) as count FROM {TARGET_TABLE} GROUP BY 1 ORDER BY 2 DESC LIMIT 10").show()
