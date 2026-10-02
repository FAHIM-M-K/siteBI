# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # Production Pipeline: NYC DCA Business Licenses (Bronze -> Silver)
# MAGIC
# MAGIC ### Architecture & Design Patterns:
# MAGIC 1. **Watermark / Incremental Fetch:** Queries Socrata API for records modified since the latest watermark.
# MAGIC 2. **Bronze Landing:** Raw JSON/dictionary format saved with ingestion audit metadata (`_ingested_at`, `_batch_id`, `_source`).
# MAGIC 3. **Silver Enrichment:** 
# MAGIC    - Coordinate validation (bounding box for NYC).
# MAGIC    - Uber H3 Spatial Index generation (Resolution 8 & 9).
# MAGIC    - Industry category normalization.
# MAGIC 4. **Idempotency:** Delta Lake `MERGE INTO` (Upsert) on `license_nbr`.
# MAGIC 5. **Physical Layout:** Delta table `Z-ORDER BY (h3_res8, industry)` for sub-second spatial and category filters.

# COMMAND ----------
# MAGIC %pip install h3 sodapy

# COMMAND ----------
import os
import uuid
from datetime import datetime, timezone
import h3

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, current_timestamp, lit, udf, when, trim, upper, to_date
)
from pyspark.sql.types import (
    StringType, DoubleType, StructType, StructField, IntegerType
)
from delta.tables import DeltaTable

# Initialize Spark
spark = SparkSession.builder.appName("NYC_Licenses_Bronze_Silver").getOrCreate()

# Ensure target databases exist
spark.sql("CREATE DATABASE IF NOT EXISTS bronze")
spark.sql("CREATE DATABASE IF NOT EXISTS silver")
spark.sql("CREATE DATABASE IF NOT EXISTS gold")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Ingestion Configuration & Watermark Detection

# COMMAND ----------
DATASET_ID = "w7w3-xahh"  # NYC DCWP Issued Licenses
APP_TOKEN = os.getenv("NYC_OPEN_DATA_APP_TOKEN", None)  # Optional: increases API rate limits

def get_latest_silver_watermark():
    """
    Finds the latest license_creation_date or update timestamp in silver.dca_licenses.
    If the table doesn't exist yet, returns a default historical start date.
    """
    try:
        if spark.catalog.tableExists("silver.dca_licenses"):
            max_date = spark.sql("SELECT MAX(license_creation_date) AS max_date FROM silver.dca_licenses").collect()[0]["max_date"]
            if max_date:
                return str(max_date)
    except Exception as e:
        print(f"Notice: Initial run or table not found ({e}). Defaulting to full load.")
    return "2015-01-01T00:00:00.000"

watermark = get_latest_silver_watermark()
print(f"[*] Pipeline Watermark: Pulling records modified on or after {watermark}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Bronze Layer: Ingest Raw Data (Socrata API -> Delta Bronze)

# COMMAND ----------
from sodapy import Socrata

def extract_bronze_licenses(watermark_date, batch_limit=50000):
    """
    Extracts raw licenses from NYC Open Data via Socrata client.
    Uses $where clause for incremental pulls.
    """
    client = Socrata("data.cityofnewyork.us", APP_TOKEN, timeout=60)
    
    # Filter incrementally using Socrata SoQL
    where_clause = f"license_creation_date >= '{watermark_date}'"
    print(f"[*] Querying Socrata with: {where_clause}")
    
    results = client.get(
        DATASET_ID,
        where=where_clause,
        limit=batch_limit,
        order="license_creation_date ASC"
    )
    
    if not results:
        print("[!] No new records found since watermark.")
        return None
        
    print(f"[✓] Retrieved {len(results)} raw records.")
    return results

raw_data = extract_bronze_licenses(watermark)

# COMMAND ----------
batch_id = str(uuid.uuid4())
ingestion_time = datetime.now(timezone.utc)

if raw_data:
    # Convert raw python dict list to Spark DataFrame
    df_raw = spark.createDataFrame(raw_data)
    
    # Append Bronze audit metadata
    df_bronze = (
        df_raw
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_batch_id", lit(batch_id))
        .withColumn("_source", lit(f"socrata://data.cityofnewyork.us/{DATASET_ID}"))
    )
    
    # Save append-only to Bronze Delta Table
    (
        df_bronze.write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable("bronze.dca_licenses_raw")
    )
    print(f"[✓] Successfully appended {df_bronze.count()} records to bronze.dca_licenses_raw (Batch: {batch_id})")
else:
    df_bronze = None

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Silver Layer: Clean, Standardize, and Generate H3 Spatial Indices

# COMMAND ----------
# Register User-Defined Functions (UDF) for H3 Hexagonal Binning
@udf(returnType=StringType())
def lat_lng_to_h3_res8(lat, lng):
    """Generates H3 Index at Resolution 8 (~460m edge length - neighborhood block level)"""
    try:
        if lat is not None and lng is not None:
            return h3.geo_to_h3(float(lat), float(lng), resolution=8)
    except:
        return None
    return None

@udf(returnType=StringType())
def lat_lng_to_h3_res9(lat, lng):
    """Generates H3 Index at Resolution 9 (~174m edge length - street corner / immediate trade area)"""
    try:
        if lat is not None and lng is not None:
            return h3.geo_to_h3(float(lat), float(lng), resolution=9)
    except:
        return None
    return None

# COMMAND ----------
if df_bronze:
    # 1. Cast and parse coordinates
    df_clean = (
        df_bronze
        .filter(col("_batch_id") == batch_id)
        .withColumn("latitude_val", col("latitude").cast(DoubleType()))
        .withColumn("longitude_val", col("longitude").cast(DoubleType()))
    )
    
    # 2. Filter invalid NYC coordinates (NYC Bounding Box: Lat 40.48 - 40.95, Lon -74.30 - -73.65)
    df_valid_geo = df_clean.filter(
        (col("latitude_val").between(40.48, 40.95)) &
        (col("longitude_val").between(-74.30, -73.65))
    )
    
    # 3. Attach spatial indexes and normalize schema
    df_silver = (
        df_valid_geo
        .withColumn("license_nbr", trim(col("license_nbr")))
        .withColumn("business_name", upper(trim(col("business_name"))))
        .withColumn("business_name_2", upper(trim(col("business_name_2"))))
        .withColumn("industry", upper(trim(col("industry"))))
        .withColumn("license_status", upper(trim(col("license_status"))))
        .withColumn("license_creation_date", to_date(col("license_creation_date")))
        .withColumn("license_exp_date", to_date(col("license_exp_date")))
        .withColumn("zip_code", trim(col("address_zip")))
        .withColumn("borough", upper(trim(col("address_borough"))))
        .withColumn("h3_res8", lat_lng_to_h3_res8(col("latitude_val"), col("longitude_val")))
        .withColumn("h3_res9", lat_lng_to_h3_res9(col("latitude_val"), col("longitude_val")))
        .withColumn("_updated_at", current_timestamp())
        .select(
            "license_nbr",
            "business_name",
            "business_name_2",
            "industry",
            "license_status",
            "license_creation_date",
            "license_exp_date",
            "address_building",
            "address_street_name",
            "zip_code",
            "borough",
            col("latitude_val").alias("latitude"),
            col("longitude_val").alias("longitude"),
            "h3_res8",
            "h3_res9",
            "_updated_at"
        )
        # Deduplicate within batch on primary key
        .dropDuplicates(["license_nbr"])
    )

    print(f"[✓] Cleaned Silver records prepared: {df_silver.count()}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4. Delta Lake Upsert (`MERGE INTO`) & Optimization

# COMMAND ----------
if df_bronze and df_silver:
    TARGET_TABLE = "silver.dca_licenses"
    
    # Check if target table exists
    if not spark.catalog.tableExists(TARGET_TABLE):
        print(f"[*] Initializing table {TARGET_TABLE}...")
        df_silver.write.format("delta").mode("overwrite").saveAsTable(TARGET_TABLE)
    else:
        print(f"[*] Executing Delta MERGE INTO on {TARGET_TABLE}...")
        target_delta = DeltaTable.forName(spark, TARGET_TABLE)
        
        # Idempotent Upsert on Primary Key (license_nbr)
        (
            target_delta.alias("target")
            .merge(
                df_silver.alias("source"),
                "target.license_nbr = source.license_nbr"
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
        
    print(f"[✓] Successfully upserted batch into {TARGET_TABLE}!")
    
    # 5. Delta Optimization (Z-Order by spatial hex and industry for fast filtering)
    print("[*] Optimizing Delta physical layout with Z-ORDER...")
    spark.sql(f"OPTIMIZE {TARGET_TABLE} ZORDER BY (h3_res8, industry)")
    print("[✓] Table optimized!")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 5. Quality Verification / Data Profiling

# COMMAND ----------
# MAGIC %sql
# MAGIC SELECT 
# MAGIC     industry, 
# MAGIC     count(*) as active_count,
# MAGIC     count(distinct h3_res8) as unique_neighborhood_hexes
# MAGIC FROM silver.dca_licenses
# MAGIC WHERE license_status = 'ACTIVE'
# MAGIC GROUP BY industry
# MAGIC ORDER BY active_count DESC
# MAGIC LIMIT 10;
