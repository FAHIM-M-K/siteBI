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
        "watermark_col": None,
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
        "dataset_id": "i9wp-a4ja",
        "primary_key": ["station_name", "line", "latitude", "longitude"],
        "bronze_table": f"{BRONZE_DB}.mta_subway_entrances_raw",
        "silver_table": f"{SILVER_DB}.mta_subway_entrances",
        "watermark_col": None,
    },
    # 5. DOHMH Restaurant Inspection Results
    "restaurant_inspections": {
        "domain": DOMAIN_NYC_OPEN_DATA,
        "dataset_id": "43nn-pn8j",
        "primary_key": ["camis", "inspection_date", "violation_code"],
        "bronze_table": f"{BRONZE_DB}.restaurant_inspections_raw",
        "silver_table": f"{SILVER_DB}.restaurant_inspections",
        "watermark_col": "inspection_date",
        "default_watermark": "2023-01-01T00:00:00.000",
    },
    # 6. 311 Service Requests
    "311_requests": {
        "domain": DOMAIN_NYC_OPEN_DATA,
        "dataset_id": "erm2-nwe9",
        "primary_key": ["unique_key"],
        "bronze_table": f"{BRONZE_DB}.service_requests_311_raw",
        "silver_table": f"{SILVER_DB}.service_requests_311",
        "watermark_col": "created_date",
        "default_watermark": "2025-01-01T00:00:00.000",
    },
    # 7. Storefronts Reported Vacant or Not
    "vacant_storefronts": {
        "domain": DOMAIN_NYC_OPEN_DATA,
        "dataset_id": "92iy-9c3n",
        "primary_key": ["borough_block_lot", "reporting_year"],
        "bronze_table": f"{BRONZE_DB}.vacant_storefronts_raw",
        "silver_table": f"{SILVER_DB}.vacant_storefronts",
        "watermark_col": None,
    },
    # 8. NYC Rolling Calendar Sales
    "rolling_sales": {
        "domain": DOMAIN_NYC_OPEN_DATA,
        "dataset_id": "w2pb-icbu",
        "primary_key": ["borough", "block", "lot", "sale_price", "sale_date"],
        "bronze_table": f"{BRONZE_DB}.rolling_sales_raw",
        "silver_table": f"{SILVER_DB}.rolling_sales",
        "watermark_col": "sale_date",
        "default_watermark": "2024-01-01T00:00:00.000",
    },
    # 9. DOB Job Application Filings
    "dob_job_filings": {
        "domain": DOMAIN_NYC_OPEN_DATA,
        "dataset_id": "ic3t-wcy2",
        "primary_key": ["job__"],
        "bronze_table": f"{BRONZE_DB}.dob_job_filings_raw",
        "silver_table": f"{SILVER_DB}.dob_job_filings",
        "watermark_col": "latest_action_date",
        "default_watermark": "2024-01-01T00:00:00.000",
    },
    # 10. NYC Wi-Fi Hotspot Locations
    "wifi_hotspots": {
        "domain": DOMAIN_NYC_OPEN_DATA,
        "dataset_id": "yjub-udmw",
        "primary_key": ["objectid"],
        "bronze_table": f"{BRONZE_DB}.wifi_hotspots_raw",
        "silver_table": f"{SILVER_DB}.wifi_hotspots",
        "watermark_col": None,
    },
    # 11. Bi-Annual Pedestrian Counts
    "pedestrian_counts": {
        "domain": DOMAIN_NYC_OPEN_DATA,
        "dataset_id": "6fi9-q3ta",
        "primary_key": ["location", "hour_beginning"],
        "bronze_table": f"{BRONZE_DB}.pedestrian_counts_raw",
        "silver_table": f"{SILVER_DB}.pedestrian_counts",
        "watermark_col": None,
    },
    # 12. Bicycle and Pedestrian Count Sensors
    "bike_ped_sensors": {
        "domain": DOMAIN_NYC_OPEN_DATA,
        "dataset_id": "ct66-47at",
        "primary_key": ["sensor_id", "timestamp"],
        "bronze_table": f"{BRONZE_DB}.bike_ped_sensors_raw",
        "silver_table": f"{SILVER_DB}.bike_ped_sensors",
        "watermark_col": "date",
        "default_watermark": "2024-01-01T00:00:00.000",
    },
    # 13. Zoning GIS Data (DCP Portal / Open Data)
    "zoning_gis": {
        "domain": DOMAIN_NYC_OPEN_DATA,
        "dataset_id": "hgx4-8ukb",
        "primary_key": ["project_id"],
        "bronze_table": f"{BRONZE_DB}.zoning_gis_raw",
        "silver_table": f"{SILVER_DB}.zoning_gis",
        "watermark_col": None,
    },
    # 14. US Census ACS 5-Year Data
    "census_acs": {
        "primary_key": ["state", "county", "tract"],
        "bronze_table": f"{BRONZE_DB}.census_acs_raw",
        "silver_table": f"{SILVER_DB}.census_acs",
        "watermark_col": None,
    },
    # 15. Census TIGER/Line Shapefiles (Tract Boundaries)
    "census_tiger_tracts": {
        "primary_key": ["geoid"],
        "bronze_table": f"{BRONZE_DB}.census_tiger_tracts_raw",
        "silver_table": f"{SILVER_DB}.census_tiger_tracts",
        "watermark_col": None,
    },
    # 16. TLC Trip Record Data (NYC TLC Taxi & Limousine Commission)
    "tlc_trips": {
        "primary_key": ["pulocationid", "dolocationid", "pickup_datetime"],
        "bronze_table": f"{BRONZE_DB}.tlc_trips_raw",
        "silver_table": f"{SILVER_DB}.tlc_trips",
        "watermark_col": None,
    },
    # 17. OpenStreetMap POIs (Overpass API)
    "osm_pois": {
        "primary_key": ["osm_id"],
        "bronze_table": f"{BRONZE_DB}.osm_pois_raw",
        "silver_table": f"{SILVER_DB}.osm_pois",
        "watermark_col": None,
    },
    # 18. Overture Maps Places
    "overture_places": {
        "primary_key": ["id"],
        "bronze_table": f"{BRONZE_DB}.overture_places_raw",
        "silver_table": f"{SILVER_DB}.overture_places",
        "watermark_col": None,
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
