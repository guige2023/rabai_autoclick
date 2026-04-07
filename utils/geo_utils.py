"""
Geographic utilities for coordinate calculations and spatial operations.

Provides:
- Distance calculations (Haversine, Vincenty)
- Bearing/azimuth calculations
- Coordinate transformations
- Bounding box operations
- Geohash encoding/decoding
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Iterator


@dataclass(frozen=True, slots=True)
class Coordinate:
    """Represents a geographic coordinate."""

    latitude: float
    longitude: float

    def __post_init__(self) -> None:
        if not -90 <= self.latitude <= 90:
            raise ValueError(f"Latitude must be in [-90, 90], got {self.latitude}")
        if not -180 <= self.longitude <= 180:
            raise ValueError(f"Longitude must be in [-180, 180], got {self.longitude}")


EARTH_RADIUS_KM = 6371.0
EARTH_RADIUS_MILES = 3958.8
EARTH_RADIUS_METERS = 6371000.0


def haversine_distance(
    lat1: float,
    lon1: float,
    lat2: float,
    lon2: float,
    unit: str = "km",
) -> float:
    """
    Calculate great-circle distance between two points using Haversine formula.

    Args:
        lat1: Latitude of point 1 (degrees)
        lon1: Longitude of point 1 (degrees)
        lat2: Latitude of point 2 (degrees)
        lon2: Longitude of point 2 (degrees)
        unit: Output unit - 'km', 'miles', or 'meters'

    Returns:
        Distance in the specified unit

    Example:
        >>> haversine_distance(40.7128, -74.0060, 34.0522, -118.2437, "km")
        3935.75
    """
    radii = {"km": EARTH_RADIUS_KM, "miles": EARTH_RADIUS_MILES, "meters": EARTH_RADIUS_METERS}
    if unit not in radii:
        raise ValueError(f"Unit must be one of {list(radii.keys())}, got {unit}")

    lat1_rad = math.radians(lat1)
    lat2_rad = math.radians(lat2)
    delta_lat = math.radians(lat2 - lat1)
    delta_lon = math.radians(lon2 - lon1)

    a = math.sin(delta_lat / 2) ** 2 + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(delta_lon / 2) ** 2
    c = 2 * math.asin(math.sqrt(a))

    return radii[unit] * c


def vincenty_distance(lat1: float, lon1: float, lat2: float, lon2: float, unit: str = "km") -> float:
    """
    Calculate distance using Vincenty formula (more accurate than Haversine for long distances).

    Args:
        lat1: Latitude of point 1 (degrees)
        lon1: Longitude of point 1 (degrees)
        lat2: Latitude of point 2 (degrees)
        lon2: Longitude of point 2 (degrees)
        unit: Output unit - 'km', 'miles', or 'meters'

    Returns:
        Distance in the specified unit
    """
    radii = {"km": EARTH_RADIUS_KM, "miles": EARTH_RADIUS_MILES, "meters": EARTH_RADIUS_METERS}
    if unit not in radii:
        raise ValueError(f"Unit must be one of {list(radii.keys())}, got {unit}")

    lat1_rad = math.radians(lat1)
    lat2_rad = math.radians(lat2)
    lon1_rad = math.radians(lon1)
    lon2_rad = math.radians(lon2)

    a = 6378137.0
    f = 1 / 298.257223563
    b = a * (1 - f)

    p1_lat = lat1_rad
    p1_lon = lon1_rad
    p2_lat = lat2_rad
    p2_lon = lon2_rad

    L = p2_lon - p1_lon
    U1 = math.atan((1 - f) * math.tan(p1_lat))
    U2 = math.atan((1 - f) * math.tan(p2_lat))
    sin_U1 = math.sin(U1)
    cos_U1 = math.cos(U1)
    sin_U2 = math.sin(U2)
    cos_U2 = math.cos(U2)

    lam = L
    for _ in range(1000):
        sin_lam = math.sin(lam)
        cos_lam = math.cos(lam)
        sin_sigma = math.sqrt((cos_U2 * sin_lam) ** 2 + (cos_U1 * sin_U2 - sin_U1 * cos_U2 * cos_lam) ** 2)
        if abs(sin_sigma) < 1e-12:
            return 0.0
        cos_sigma = sin_U1 * sin_U2 + cos_U1 * cos_U2 * cos_lam
        sigma = math.atan2(sin_sigma, cos_sigma)
        sin_alpha = cos_U1 * cos_U2 * sin_lam / sin_sigma
        cos_sq_alpha = 1 - sin_alpha**2
        if abs(cos_sq_alpha) > 1e-12:
            cos_2sigma_m = cos_sigma - 2 * sin_U1 * sin_U2 / cos_sq_alpha
        else:
            cos_2sigma_m = 0.0
        C = f / 16 * cos_sq_alpha * (4 + f * (4 - 3 * cos_sq_alpha))
        lam_next = L + (1 - C) * f * sin_alpha * (sigma + C * sin_sigma * (cos_2sigma_m + C * cos_sigma * (-1 + 2 * cos_2sigma_m**2)))
        if abs(lam_next - lam) < 1e-12:
            break
        lam = lam_next

    u_sq = cos_sq_alpha * (a**2 - b**2) / b**2
    A = 1 + u_sq / 16384 * (4096 + u_sq * (-768 + u_sq * (320 - 175 * u_sq)))
    B = u_sq / 1024 * (256 + u_sq * (-128 + u_sq * (74 - 47 * u_sq)))
    delta_sigma = B * sin_sigma * (cos_2sigma_m + B / 4 * (cos_sigma * (-1 + 2 * cos_2sigma_m**2) - B / 6 * cos_2sigma_m * (-3 + 4 * sin_sigma**2) * (-3 + 4 * cos_2sigma_m**2)))

    return b * A * (sigma - delta_sigma)


def calculate_bearing(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """
    Calculate initial bearing (azimuth) from point 1 to point 2.

    Args:
        lat1: Latitude of point 1 (degrees)
        lon1: Longitude of point 1 (degrees)
        lat2: Latitude of point 2 (degrees)
        lon2: Longitude of point 2 (degrees)

    Returns:
        Bearing in degrees [0, 360)

    Example:
        >>> calculate_bearing(0, 0, 45, 90)
        54.13
    """
    lat1_rad = math.radians(lat1)
    lat2_rad = math.radians(lat2)
    delta_lon = math.radians(lon2 - lon1)

    x = math.sin(delta_lon) * math.cos(lat2_rad)
    y = math.cos(lat1_rad) * math.sin(lat2_rad) - math.sin(lat1_rad) * math.cos(lat2_rad) * math.cos(delta_lon)

    bearing = math.atan2(x, y)
    bearing_deg = math.degrees(bearing)

    return (bearing_deg + 360) % 360


def destination_point(lat: float, lon: float, bearing: float, distance: float, unit: str = "km") -> tuple[float, float]:
    """
    Calculate destination point given start point, bearing and distance.

    Args:
        lat: Starting latitude (degrees)
        lon: Starting longitude (degrees)
        bearing: Bearing in degrees
        distance: Distance to travel
        unit: Distance unit - 'km', 'miles', or 'meters'

    Returns:
        Tuple of (latitude, longitude) of destination

    Example:
        >>> destination_point(0, 0, 45, 100, "km")
        (0.899, 0.899)
    """
    radii = {"km": EARTH_RADIUS_KM, "miles": EARTH_RADIUS_MILES, "meters": EARTH_RADIUS_METERS}
    if unit not in radii:
        raise ValueError(f"Unit must be one of {list(radii.keys())}, got {unit}")

    R = radii[unit]
    bearing_rad = math.radians(bearing)
    lat_rad = math.radians(lat)
    lon_rad = math.radians(lon)

    lat2 = math.asin(math.sin(lat_rad) * math.cos(distance / R) + math.cos(lat_rad) * math.sin(distance / R) * math.cos(bearing_rad))
    lon2 = lon_rad + math.atan2(math.sin(bearing_rad) * math.sin(distance / R) * math.cos(lat_rad), math.cos(distance / R) - math.sin(lat_rad) * math.sin(lat2))

    return (math.degrees(lat2), math.degrees(lon2))


def midpoint(lat1: float, lon1: float, lat2: float, lon2: float) -> tuple[float, float]:
    """
    Calculate the midpoint between two geographic points.

    Args:
        lat1, lon1: First point coordinates (degrees)
        lat2, lon2: Second point coordinates (degrees)

    Returns:
        Tuple of (latitude, longitude) of midpoint
    """
    lat1_rad = math.radians(lat1)
    lat2_rad = math.radians(lat2)
    lon1_rad = math.radians(lon1)
    delta_lon = math.radians(lon2 - lon1)

    bx = math.cos(lat2_rad) * math.cos(delta_lon)
    by = math.cos(lat2_rad) * math.sin(delta_lon)

    lat_mid = math.atan2(math.sin(lat1_rad) + math.sin(lat2_rad), math.sqrt((math.cos(lat1_rad) + bx) ** 2 + by**2))
    lon_mid = lon1_rad + math.atan2(by, math.cos(lat1_rad) + bx)

    return (math.degrees(lat_mid), math.degrees(lon_mid))


def bounding_box(lat: float, lon: float, radius_km: float) -> tuple[float, float, float, float]:
    """
    Calculate bounding box around a point.

    Args:
        lat: Center latitude (degrees)
        lon: Center longitude (degrees)
        radius_km: Radius in kilometers

    Returns:
        (min_lat, min_lon, max_lat, max_lon)
    """
    lat_rad = math.radians(lat)
    lon_rad = math.radians(lon)
    delta_lat = math.degrees(radius_km / EARTH_RADIUS_KM)
    delta_lon = math.degrees(radius_km / (EARTH_RADIUS_KM * math.cos(lat_rad)))

    return (lat - delta_lat, lon - delta_lon, lat + delta_lat, lon + delta_lon)


def is_within_radius(
    center_lat: float,
    center_lon: float,
    point_lat: float,
    point_lon: float,
    radius_km: float,
) -> bool:
    """Check if a point is within a given radius of a center point."""
    return haversine_distance(center_lat, center_lon, point_lat, point_lon, "km") <= radius_km


def geohash_encode(lat: float, lon: float, precision: int = 9) -> str:
    """
    Encode a coordinate to a geohash string.

    Args:
        lat: Latitude (-90 to 90)
        lon: Longitude (-180 to 180)
        precision: Number of characters in hash (1-12)

    Returns:
        Geohash string

    Example:
        >>> geohash_encode(40.7128, -74.0060, 8)
        'dr5regw3'
    """
    if not 1 <= precision <= 12:
        raise ValueError("Precision must be between 1 and 12")

    lat_range, lon_range = (-90.0, 90.0), (-180.0, 180.0)
    geohash_base32 = "0123456789bcdefghjkmnpqrstuvwxyz"
    result = []

    bits = 0
    bit_count = 0
    is_lon = True
    combined = 0

    while len(result) < precision:
        if is_lon:
            mid = (lon_range[0] + lon_range[1]) / 2
            if lon >= mid:
                combined = combined * 2 + 1
                lon_range = (mid, lon_range[1])
            else:
                combined = combined * 2
                lon_range = (lon_range[0], mid)
        else:
            mid = (lat_range[0] + lat_range[1]) / 2
            if lat >= mid:
                combined = combined * 2 + 1
                lat_range = (mid, lat_range[1])
            else:
                combined = combined * 2
                lat_range = (lat_range[0], mid)

        bits += 1
        is_lon = not is_lon

        if bits == 5:
            result.append(geohash_base32[combined])
            bits = 0
            combined = 0

    return "".join(result)


def geohash_decode(geohash: str) -> tuple[float, float]:
    """
    Decode a geohash string to a coordinate (center of bounding box).

    Args:
        geohash: Geohash string

    Returns:
        Tuple of (latitude, longitude)

    Example:
        >>> geohash_decode("dr5regw3")
        (40.7127..., -74.006...)
    """
    geohash_base32 = "0123456789bcdefghjkmnpqrstuvwxyz"
    lat_range, lon_range = (-90.0, 90.0), (-180.0, 180.0)
    is_lon = False

    for char in geohash.lower():
        idx = geohash_base32.index(char)
        for i in range(4, -1, -1):
            bit = (idx >> i) & 1
            if is_lon:
                mid = (lon_range[0] + lon_range[1]) / 2
                lon_range = (mid if bit else lon_range[0], lon_range[1] if bit else mid)
            else:
                mid = (lat_range[0] + lat_range[1]) / 2
                lat_range = (mid if bit else lat_range[0], lat_range[1] if bit else mid)
            is_lon = not is_lon

    return ((lat_range[0] + lat_range[1]) / 2, (lon_range[0] + lon_range[1]) / 2)


def coordinates_from_box(min_lat: float, min_lon: float, max_lat: float, max_lon: float, step: float) -> Iterator[tuple[float, float]]:
    """
    Generate coordinates within a bounding box at regular intervals.

    Args:
        min_lat, min_lon: Southwest corner
        max_lat, max_lon: Northeast corner
        step: Step size in degrees

    Yields:
        Tuples of (latitude, longitude)
    """
    lat = min_lat
    while lat <= max_lat:
        lon = min_lon
        while lon <= max_lon:
            yield (lat, lon)
            lon += step
        lat += step
