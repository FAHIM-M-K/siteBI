"""
Geographic Validators & Coordinate Parsers for NYC Datasets
Validates bounding box constraints, parses coordinates, and normalizes borough designations.
"""

from typing import Any, Optional, Tuple
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, udf, when, trim, upper
from pyspark.sql.types import StringType, BooleanType

from src.utils.config import (
    NYC_LAT_MIN,
    NYC_LAT_MAX,
    NYC_LON_MIN,
    NYC_LON_MAX,
    BOROUGH_CODE_MAP,
)

VALID_BOROUGHS = {"MANHATTAN", "BRONX", "BROOKLYN", "QUEENS", "STATEN ISLAND"}


def is_valid_nyc_coordinate(lat: Optional[float], lon: Optional[float]) -> bool:
    """
    Checks if a coordinate pair falls within the defined NYC bounding box.
    """
    if lat is None or lon is None:
        return False
    try:
        lat_f = float(lat)
        lon_f = float(lon)
        return (NYC_LAT_MIN <= lat_f <= NYC_LAT_MAX) and (NYC_LON_MIN <= lon_f <= NYC_LON_MAX)
    except (ValueError, TypeError):
        return False


def parse_coordinate(coord_val: Any) -> Optional[float]:
    """
    Safely parses an input string or numeric to a float coordinate.
    """
    if coord_val is None:
        return None
    try:
        clean_str = str(coord_val).strip().replace("(", "").replace(")", "").replace(",", "")
        if not clean_str:
            return None
        val = float(clean_str)
        return val if not (val != val) else None  # Check for NaN
    except (ValueError, TypeError):
        return None


def normalize_borough(borough_raw: Optional[str]) -> Optional[str]:
    """
    Normalizes borough names and codes into standard uppercase names:
    'MANHATTAN', 'BRONX', 'BROOKLYN', 'QUEENS', 'STATEN ISLAND'.
    """
    if not borough_raw:
        return None
    cleaned = str(borough_raw).strip().upper()
    if cleaned in VALID_BOROUGHS:
        return cleaned
    return BOROUGH_CODE_MAP.get(cleaned, None)


# ---------------------------------------------------------------------------
# PySpark Utilities & UDFs
# ---------------------------------------------------------------------------

@udf(returnType=BooleanType())
def udf_is_valid_nyc_coordinate(lat, lon):
    """PySpark UDF to check if lat/lon coordinate is within NYC boundaries."""
    return is_valid_nyc_coordinate(lat, lon)


@udf(returnType=StringType())
def udf_normalize_borough(borough):
    """PySpark UDF to standardize borough codes and strings."""
    return normalize_borough(borough)


def filter_valid_nyc_coordinates(
    df: DataFrame,
    lat_col: str = "latitude",
    lon_col: str = "longitude"
) -> DataFrame:
    """
    Applies an optimized native Spark filter for valid NYC coordinates.
    Filters out NULLs, zeroes, NaNs, and coordinates outside the NYC bounding box.
    """
    return df.filter(
        col(lat_col).isNotNull() &
        col(lon_col).isNotNull() &
        col(lat_col).between(NYC_LAT_MIN, NYC_LAT_MAX) &
        col(lon_col).between(NYC_LON_MIN, NYC_LON_MAX)
    )
