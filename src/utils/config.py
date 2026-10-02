"""
SiteBI Configuration Module
Centralized settings for Medallion databases, datasets, spatial resolutions,
NYC bounding box coordinates, and environment credentials.
"""

import os
from typing import Dict, Any

# Databricks / Hive Metastore Databases
BRONZE_DB: str = "bronze"
SILVER_DB: str = "silver"
GOLD_DB: str = "gold"

# Spatial Resolutions (Uber H3)
# Res 8: ~460m edge length (neighborhood / trade area level, PK for Gold master features)
# Res 9: ~174m edge length (street block / immediate corridor precision)
H3_RES_NEIGHBORHOOD: int = 8
H3_RES_STREET: int = 9

# New York City Geographic Bounding Box
NYC_LAT_MIN: float = 40.48
NYC_LAT_MAX: float = 40.95
NYC_LON_MIN: float = -74.30
NYC_LON_MAX: float = -73.65

# API Access Tokens
NYC_OPEN_DATA_APP_TOKEN: str = os.getenv("NYC_OPEN_DATA_APP_TOKEN", "")
NY_STATE_OPEN_DATA_APP_TOKEN: str = os.getenv("NY_STATE_OPEN_DATA_APP_TOKEN", NYC_OPEN_DATA_APP_TOKEN)
CENSUS_API_KEY: str = os.getenv("CENSUS_API_KEY", "")

# Standardized Socrata Domains
DOMAIN_NYC_OPEN_DATA: str = "data.cityofnewyork.us"
DOMAIN_NY_STATE_OPEN_DATA: str = "data.ny.gov"

# Dataset Metadata Catalog
DATASETS: Dict[str, Dict[str, Any]] = {
    # 1. DCA / DCWP Issued Licenses
    "dca_licenses": {
        "domain": DOMAIN_NYC_OPEN_DATA,
        "dataset_id": "w7w3-xahh",
        "primary_key": ["license_nbr"],
        "bronze_table": f"{BRONZE_DB}.dca_licenses_raw",
        "silver_table": f"{SILVER_DB}.dca_licenses",
        "watermark_col": "license_creation_date",
        "default_watermark": "2015-01-01T00:00:00.000",
    },
    # 2. PLUTO (Primary Land Use Tax Lot Output)
    "pluto": {
        "domain": DOMAIN_NYC_OPEN_DATA,
        "dataset_id": "64uk-42ks",
        "primary_key": ["bbl"],
        "bronze_table": f"{BRONZE_DB}.pluto_raw",
        "silver_table": f"{SILVER_DB}.pluto",
        "watermark_col": None,  # Full snapshot overwrite
    },
    # 3. MTA Subway Hourly Ridership
    "mta_ridership": {
        "domain": DOMAIN_NY_STATE_OPEN_DATA,
        "dataset_id": "wujg-7c2s",
        "primary_key": ["transit_timestamp", "station_complex_id", "fare_class_category"],
        "bronze_table": f"{BRONZE_DB}.mta_ridership_raw",
        "silver_table": f"{SILVER_DB}.mta_ridership",
        "watermark_col": "transit_timestamp",
        "default_watermark": "2024-01-01T00:00:00.000",
    },
    # 4. MTA Subway Entrances and Exits
    "mta_subway_entrances": {
        "domain": DOMAIN_NY_STATE_OPEN_DATA,
        "dataset_id": "i9rv-hdr5",
        "primary_key": ["station_name", "line", "latitude", "longitude"],
        "bronze_table": f"{BRONZE_DB}.mta_subway_entrances_raw",
        "silver_table": f"{SILVER_DB}.mta_subway_entrances",
        "watermark_col": None,  # Reference snapshot
    },
    # 5. DOHMH Restaurant Inspection Results
    "restaurant_inspections": {
        "domain": DOMAIN_NYC_OPEN_DATA,
        "dataset_id": "43nn-pn8j",
        "primary_key": ["camis", "inspection_date", "violation_code"],
        "bronze_table": f"{BRONZE_DB}.restaurant_inspections_raw",
        "silver_table": f"{SILVER_DB}.restaurant_inspections",
        "watermark_col": "inspection_date",
        "default_watermark": "2020-01-01T00:00:00.000",
    },
    # 6. 311 Service Requests
    "311_requests": {
        "domain": DOMAIN_NYC_OPEN_DATA,
        "dataset_id": "erm2-nwe9",
        "primary_key": ["unique_key"],
        "bronze_table": f"{BRONZE_DB}.service_requests_311_raw",
        "silver_table": f"{SILVER_DB}.service_requests_311",
        "watermark_col": "created_date",
        "default_watermark": "2023-01-01T00:00:00.000",
    },
}

# Borough Code & Name Normalization Map
BOROUGH_CODE_MAP: Dict[str, str] = {
    "1": "MANHATTAN",
    "2": "BRONX",
    "3": "BROOKLYN",
    "4": "QUEENS",
    "5": "STATEN ISLAND",
    "MN": "MANHATTAN",
    "BX": "BRONX",
    "BK": "BROOKLYN",
    "QN": "QUEENS",
    "SI": "STATEN ISLAND",
    "MAN": "MANHATTAN",
    "BRX": "BRONX",
    "BKN": "BROOKLYN",
    "QNS": "QUEENS",
    "STN": "STATEN ISLAND",
}
