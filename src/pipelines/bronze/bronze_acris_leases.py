# Databricks notebook source
# COMMAND ----------
# bronze_acris_leases.py
#
# Ingests NYC ACRIS Commercial Lease Records by combining:
#   1. Real Property Master (bnx9-e6tj): filtered by doc_type in (LEAS, STLE, ASST)
#   2. Real Property Legals (8h5j-fqxa): joined on document_id to obtain BBL
# Tracks commercial leasing activity and contract dates across NYC parcels.
# COMMAND ----------
import sys
import os
import uuid

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit

notebook_path = os.getcwd()
if notebook_path not in sys.path:
    sys.path.append(notebook_path)

from src.utils.config import (
    BRONZE_DB,
    DATASETS,
    DOMAIN_NYC_OPEN_DATA,
    NYC_OPEN_DATA_APP_TOKEN,
)
from src.utils.socrata_client import SocrataClient, flatten_records

spark = SparkSession.builder.appName("Bronze_ACRIS_Leases").getOrCreate()
spark.sql(f"CREATE DATABASE IF NOT EXISTS {BRONZE_DB}")

# COMMAND ----------
dataset_meta     = DATASETS["acris_leases"]
TARGET_TABLE     = dataset_meta["bronze_table"]
MASTER_ID        = dataset_meta["master_dataset_id"]
LEGALS_ID        = dataset_meta["legals_dataset_id"]
DOC_TYPES        = dataset_meta["doc_type_filter"]
DEFAULT_WM       = dataset_meta["default_watermark"]

client = SocrataClient(
    domain=DOMAIN_NYC_OPEN_DATA,
    app_token=NYC_OPEN_DATA_APP_TOKEN,
    timeout=90,
)

# Determine incremental watermark
watermark = DEFAULT_WM
if spark.catalog.tableExists(TARGET_TABLE):
    try:
        max_ts = (
            spark.sql(f"SELECT MAX(recorded_datetime) AS max_dt FROM {TARGET_TABLE}")
            .collect()[0]["max_dt"]
        )
        if max_ts:
            watermark = max_ts
    except Exception:
        pass

print(f"Ingesting ACRIS leases with recorded_datetime >= {watermark}")

# Format SoQL filter
doc_types_str = ", ".join(f"'{dt}'" for dt in DOC_TYPES)
master_where  = f"doc_type in ({doc_types_str}) AND recorded_datetime >= '{watermark}'"

batch_id       = str(uuid.uuid4())
source_uri     = f"socrata://{DOMAIN_NYC_OPEN_DATA}/{MASTER_ID}+{LEGALS_ID}"
total_ingested = 0

# COMMAND ----------
def fetch_legals_for_doc_ids(client: SocrataClient, doc_ids: list, chunk_size: int = 40) -> list:
    """Fetches legal records (BBLs) for a list of document IDs in batches."""
    legals = []
    for i in range(0, len(doc_ids), chunk_size):
        chunk = doc_ids[i:i + chunk_size]
        ids_str = ", ".join(f"'{doc_id}'" for doc_id in chunk)
        where_clause = f"document_id in ({ids_str})"
        records = client.fetch_page(
            dataset_id=LEGALS_ID,
            limit=5000,
            where=where_clause,
        )
        if records:
            legals.extend(records)
    return legals

# Process master pages and join corresponding parcel legals
for master_page in client.fetch_all(dataset_id=MASTER_ID, page_size=2000, where=master_where, order="recorded_datetime ASC"):
    if not master_page:
        continue

    doc_ids = list({row["document_id"] for row in master_page if "document_id" in row})
    legals_records = fetch_legals_for_doc_ids(client, doc_ids)

    df_master = spark.createDataFrame(flatten_records(master_page))

    if legals_records:
        df_legals = spark.createDataFrame(flatten_records(legals_records))
        # Keep relevant parcel identifier columns from legals
        legal_cols = [c for c in ["document_id", "borough", "block", "lot", "property_type", "street_number", "street_name", "unit_address"] if c in df_legals.columns]
        df_legals = df_legals.select(legal_cols)
        df_batch = df_master.join(df_legals, on="document_id", how="left")
    else:
        df_batch = df_master

    df_batch = (
        df_batch
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_batch_id",    lit(batch_id))
        .withColumn("_source",      lit(source_uri))
    )

    (
        df_batch.write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable(TARGET_TABLE)
    )
    total_ingested += len(master_page)
    print(f"Ingested batch: {len(master_page)} master leases (cumulative: {total_ingested:,})")

print(f"Done. {total_ingested:,} master lease records joined and written → {TARGET_TABLE}")

# COMMAND ----------
if spark.catalog.tableExists(TARGET_TABLE):
    spark.sql(f"SELECT count(*) as total_records FROM {TARGET_TABLE}").show()
    spark.sql(f"SELECT document_id, doc_type, recorded_datetime, doc_amount, borough, block, lot FROM {TARGET_TABLE} LIMIT 5").show(truncate=False)
