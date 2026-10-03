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

from src.utils.config import BRONZE_DB, DATASETS

spark = SparkSession.builder.appName("Bronze_RentGuidelines").getOrCreate()
spark.sql(f"CREATE DATABASE IF NOT EXISTS {BRONZE_DB}")

# COMMAND ----------
dataset_meta = DATASETS["rent_guidelines"]
TARGET_TABLE = dataset_meta["bronze_table"]

RGB_ORDERS_DATA = [
    {"order_number": 45, "order_year": 2013, "effective_start": "2013-10-01", "effective_end": "2014-09-30", "lease_type": "1_year", "increase_pct": 4.00},
    {"order_number": 45, "order_year": 2013, "effective_start": "2013-10-01", "effective_end": "2014-09-30", "lease_type": "2_year", "increase_pct": 7.75},
    {"order_number": 46, "order_year": 2014, "effective_start": "2014-10-01", "effective_end": "2015-09-30", "lease_type": "1_year", "increase_pct": 1.00},
    {"order_number": 46, "order_year": 2014, "effective_start": "2014-10-01", "effective_end": "2015-09-30", "lease_type": "2_year", "increase_pct": 2.75},
    {"order_number": 47, "order_year": 2015, "effective_start": "2015-10-01", "effective_end": "2016-09-30", "lease_type": "1_year", "increase_pct": 0.00},
    {"order_number": 47, "order_year": 2015, "effective_start": "2015-10-01", "effective_end": "2016-09-30", "lease_type": "2_year", "increase_pct": 2.00},
    {"order_number": 48, "order_year": 2016, "effective_start": "2016-10-01", "effective_end": "2017-09-30", "lease_type": "1_year", "increase_pct": 0.00},
    {"order_number": 48, "order_year": 2016, "effective_start": "2016-10-01", "effective_end": "2017-09-30", "lease_type": "2_year", "increase_pct": 2.00},
    {"order_number": 49, "order_year": 2017, "effective_start": "2017-10-01", "effective_end": "2018-09-30", "lease_type": "1_year", "increase_pct": 1.25},
    {"order_number": 49, "order_year": 2017, "effective_start": "2017-10-01", "effective_end": "2018-09-30", "lease_type": "2_year", "increase_pct": 2.00},
    {"order_number": 50, "order_year": 2018, "effective_start": "2018-10-01", "effective_end": "2019-09-30", "lease_type": "1_year", "increase_pct": 1.50},
    {"order_number": 50, "order_year": 2018, "effective_start": "2018-10-01", "effective_end": "2019-09-30", "lease_type": "2_year", "increase_pct": 2.50},
    {"order_number": 51, "order_year": 2019, "effective_start": "2019-10-01", "effective_end": "2020-09-30", "lease_type": "1_year", "increase_pct": 1.50},
    {"order_number": 51, "order_year": 2019, "effective_start": "2019-10-01", "effective_end": "2020-09-30", "lease_type": "2_year", "increase_pct": 2.50},
    {"order_number": 52, "order_year": 2020, "effective_start": "2020-10-01", "effective_end": "2021-09-30", "lease_type": "1_year", "increase_pct": 0.00},
    {"order_number": 52, "order_year": 2020, "effective_start": "2020-10-01", "effective_end": "2021-09-30", "lease_type": "2_year", "increase_pct": 1.00},
    {"order_number": 53, "order_year": 2021, "effective_start": "2021-10-01", "effective_end": "2022-09-30", "lease_type": "1_year", "increase_pct": 1.50},
    {"order_number": 53, "order_year": 2021, "effective_start": "2021-10-01", "effective_end": "2022-09-30", "lease_type": "2_year", "increase_pct": 2.50},
    {"order_number": 54, "order_year": 2022, "effective_start": "2022-10-01", "effective_end": "2023-09-30", "lease_type": "1_year", "increase_pct": 3.25},
    {"order_number": 54, "order_year": 2022, "effective_start": "2022-10-01", "effective_end": "2023-09-30", "lease_type": "2_year", "increase_pct": 5.00},
    {"order_number": 55, "order_year": 2023, "effective_start": "2023-10-01", "effective_end": "2024-09-30", "lease_type": "1_year", "increase_pct": 3.00},
    {"order_number": 55, "order_year": 2023, "effective_start": "2023-10-01", "effective_end": "2024-09-30", "lease_type": "2_year", "increase_pct": 2.75},
    {"order_number": 56, "order_year": 2024, "effective_start": "2024-10-01", "effective_end": "2025-09-30", "lease_type": "1_year", "increase_pct": 2.75},
    {"order_number": 56, "order_year": 2024, "effective_start": "2024-10-01", "effective_end": "2025-09-30", "lease_type": "2_year", "increase_pct": 5.25},
]

batch_id   = str(uuid.uuid4())
source_uri = "https://rentguidelinesboard.cityofnewyork.us/orders/"

# COMMAND ----------
df = spark.createDataFrame(RGB_ORDERS_DATA)
df = (
    df
    .withColumn("_ingested_at", current_timestamp())
    .withColumn("_batch_id",    lit(batch_id))
    .withColumn("_source",      lit(source_uri))
)

(
    df.write
    .format("delta")
    .mode("overwrite")
    .option("mergeSchema", "true")
    .saveAsTable(TARGET_TABLE)
)

print(f"Done. {len(RGB_ORDERS_DATA)} RGB order rate records -> {TARGET_TABLE}")

# COMMAND ----------
if spark.catalog.tableExists(TARGET_TABLE):
    spark.sql(f"SELECT * FROM {TARGET_TABLE} ORDER BY order_year DESC, lease_type").show(truncate=False)
