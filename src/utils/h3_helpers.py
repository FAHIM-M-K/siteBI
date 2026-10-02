"""
Uber H3 Spatial Index Helpers
Provides cross-version compatibility for H3 (v3 and v4), coordinate-to-hexagon mapping,
k-ring neighborhood lookups, and reusable PySpark User-Defined Functions (UDFs).
"""

from typing import List, Optional, Tuple, Set
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, ArrayType

try:
    import h3
except ImportError:
    h3 = None

# Determine H3 API version (v3 vs v4)
_IS_H3_V4 = hasattr(h3, "latlng_to_cell") if h3 else False


def latlng_to_h3(lat: Optional[float], lng: Optional[float], resolution: int = 8) -> Optional[str]:
    """
    Converts latitude and longitude to an H3 index string at the given resolution.
    Returns None if inputs are invalid or out of bounds.
    """
    if h3 is None or lat is None or lng is None:
        return None
    try:
        lat_f = float(lat)
        lng_f = float(lng)
        if not (-90.0 <= lat_f <= 90.0 and -180.0 <= lng_f <= 180.0):
            return None
        if _IS_H3_V4:
            return h3.latlng_to_cell(lat_f, lng_f, resolution)
        return h3.geo_to_h3(lat_f, lng_f, resolution)
    except Exception:
        return None


def h3_to_latlng(h3_index: Optional[str]) -> Optional[Tuple[float, float]]:
    """
    Returns the centroid coordinates (latitude, longitude) for a given H3 index.
    """
    if h3 is None or not h3_index:
        return None
    try:
        if _IS_H3_V4:
            return h3.cell_to_latlng(h3_index)
        return h3.h3_to_geo(h3_index)
    except Exception:
        return None


def get_k_ring(h3_index: Optional[str], k: int = 1) -> List[str]:
    """
    Returns all H3 indices within distance k of the origin hexagon (inclusive of origin).
    """
    if h3 is None or not h3_index or k < 0:
        return []
    try:
        if _IS_H3_V4:
            cells = h3.grid_disk(h3_index, k)
            return list(cells)
        return list(h3.k_ring(h3_index, k))
    except Exception:
        return []


def h3_to_parent(h3_index: Optional[str], parent_res: int = 8) -> Optional[str]:
    """
    Converts a fine-resolution H3 index (e.g., res 9) to its enclosing parent index (e.g., res 8).
    """
    if h3 is None or not h3_index:
        return None
    try:
        if _IS_H3_V4:
            return h3.cell_to_parent(h3_index, parent_res)
        return h3.h3_to_parent(h3_index, parent_res)
    except Exception:
        return None


# ---------------------------------------------------------------------------
# PySpark User-Defined Functions (UDFs)
# ---------------------------------------------------------------------------

@udf(returnType=StringType())
def udf_latlng_to_h3_res8(lat, lng):
    """PySpark UDF: Converts lat, lng -> H3 Resolution 8 index (~460m)."""
    return latlng_to_h3(lat, lng, resolution=8)


@udf(returnType=StringType())
def udf_latlng_to_h3_res9(lat, lng):
    """PySpark UDF: Converts lat, lng -> H3 Resolution 9 index (~174m)."""
    return latlng_to_h3(lat, lng, resolution=9)


@udf(returnType=StringType())
def udf_h3_res9_to_res8_parent(h3_res9):
    """PySpark UDF: Converts Res 9 index to its Res 8 parent index."""
    return h3_to_parent(h3_res9, parent_res=8)


@udf(returnType=ArrayType(StringType()))
def udf_h3_k_ring(h3_index, k):
    """PySpark UDF: Computes k-ring neighbors array for an H3 index."""
    return get_k_ring(h3_index, int(k) if k is not None else 1)
