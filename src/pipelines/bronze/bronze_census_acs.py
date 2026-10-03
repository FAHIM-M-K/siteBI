# Databricks notebook source
# COMMAND ----------
# MAGIC %pip install requests

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
    CENSUS_API_KEY,
)

spark = SparkSession.builder.appName("Bronze_Census_ACS").getOrCreate()
spark.sql(f"CREATE DATABASE IF NOT EXISTS {BRONZE_DB}")

# COMMAND ----------
dataset_meta = DATASETS["census_acs"]
TARGET_TABLE = dataset_meta["bronze_table"]

# NYC FIPS: State 36 (NY), Counties: 005 (Bronx), 047 (Brooklyn), 061 (Manhattan), 081 (Queens), 085 (Staten Island)
NYC_COUNTIES = ["005", "047", "061", "081", "085"]
VARIABLES = ["NAME", "B01003_001E", "B19013_001E", "B01002_001E", "B25001_001E", "B25077_001E"]
var_str = ",".join(VARIABLES)

all_records = []
batch_id = str(uuid.uuid4())
source_uri = "https://api.census.gov/data/2022/acs/acs5"

for county in NYC_COUNTIES:
    url = f"{source_uri}?get={var_str}&for=tract:*&in=state:36&in=county:{county}"
    if CENSUS_API_KEY:
        url += f"&key={CENSUS_API_KEY}"
    
    resp = requests.get(url, timeout=60)
    if resp.status_code == 200:
        data = resp.json()
        if len(data) > 1:
            headers = [h.lower() for h in data[0]]
            for row in data[1:]:
                record = dict(zip(headers, row))
                all_records.append(record)

if all_records:
    df_raw = spark.createDataFrame(all_records)
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
    spark.sql(f"SELECT count(*) as total_census_tracts FROM {TARGET_TABLE}").show()
    spark.sql(f"SELECT * FROM {TARGET_TABLE} LIMIT 5").show()
