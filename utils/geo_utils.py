"""
Geographic utilities for coordinate calculations, distance, and geocoding.
"""

import math
from typing import Tuple, Optional, List
from dataclasses import dataclass


@dataclass(frozen=True)
class Coordinate:
    """Immutable geographic coordinate with validation."""
    latitude: float
    longitude: float

    def __post_init__(self) -> None:
        if not -90 <= self.latitude <= 90:
            raise ValueError(f"Latitude must be in [-90, 90], got {self.latitude}")
        if not -180 <= self.longitude <= 180:
            raise ValueError(f"Longitude must be in [-180, 180], got {self.longitude}")

    def to_tuple(self) -> Tuple[float, float]:
        return (self.latitude, self.longitude)

    def distance_to(self, other: "Coordinate", unit: str = "km") -> float:
        """Calculate distance to another coordinate."""
        return haversine_distance(self, other, unit)


def haversine_distance(
    coord1: Coordinate,
    coord2: Coordinate,
    unit: str = "km"
) -> float:
    """
    Calculate the great-circle distance between two points using Haversine formula.

    Args:
        coord1: First coordinate
        coord2: Second coordinate
        unit: Unit of measurement - "km", "m", "mi", "ft", "nm" (nautical miles)

    Returns:
        Distance in the specified unit
    """
    R = {
        "km": 6371.0,
        "m": 6371000.0,
        "mi": 3958.8,
        "ft": 3958.8 * 5280,
        "nm": 3440.0,
    }.get(unit, 6371.0)

    lat1, lon1 = math.radians(coord1.latitude), math.radians(coord1.longitude)
    lat2, lon2 = math.radians(coord2.latitude), math.radians(coord2.longitude)

    dlat = lat2 - lat1
    dlon = lon2 - lon1

    a = math.sin(dlat / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

    return R * c


def calculate_bearing(
    coord1: Coordinate,
    coord2: Coordinate
) -> float:
    """
    Calculate the initial bearing (forward azimuth) from coord1 to coord2.

    Args:
        coord1: Starting coordinate
        coord2: Ending coordinate

    Returns:
        Bearing in degrees [0, 360)
    """
    lat1 = math.radians(coord1.latitude)
    lat2 = math.radians(coord2.latitude)
    dlon = math.radians(coord2.longitude - coord1.longitude)

    x = math.sin(dlon) * math.cos(lat2)
    y = math.cos(lat1) * math.sin(lat2) - math.sin(lat1) * math.cos(lat2) * math.cos(dlon)

    bearing = math.atan2(x, y)
    return (math.degrees(bearing) + 360) % 360


def destination_point(
    coord: Coordinate,
    bearing: float,
    distance: float,
    unit: str = "km"
) -> Coordinate:
    """
    Calculate destination point given start, bearing, and distance.

    Args:
        coord: Starting coordinate
        bearing: Bearing in degrees
        distance: Distance to travel
        unit: Unit of distance - "km", "m", "mi"
    """
    R = {"km": 6371.0, "m": 6371000.0, "mi": 3958.8}.get(unit, 6371.0)

    lat1 = math.radians(coord.latitude)
    lon1 = math.radians(coord.longitude)
    bearing_rad = math.radians(bearing)
    angular_dist = distance / R

    lat2 = math.asin(
        math.sin(lat1) * math.cos(angular_dist) +
        math.cos(lat1) * math.sin(angular_dist) * math.cos(bearing_rad)
    )

    lon2 = lon1 + math.atan2(
        math.sin(bearing_rad) * math.sin(angular_dist) * math.cos(lat1),
        math.cos(angular_dist) - math.sin(lat1) * math.sin(lat2)
    )

    return Coordinate(
        latitude=math.degrees(lat2),
        longitude=math.degrees(lon2) % 360
    )


def is_within_radius(
    center: Coordinate,
    point: Coordinate,
    radius_km: float
) -> bool:
    """Check if a point is within a radius of a center point."""
    return center.distance_to(point, "km") <= radius_km


def midpoint(
    coord1: Coordinate,
    coord2: Coordinate
) -> Coordinate:
    """Calculate the midpoint between two coordinates."""
    lat1 = math.radians(coord1.latitude)
    lat2 = math.radians(coord2.latitude)
    lon1 = math.radians(coord1.longitude)
    dlon = math.radians(coord2.longitude - coord1.longitude)

    bx = math.cos(lat2) * math.cos(dlon)
    by = math.cos(lat2) * math.sin(dlon)

    lat3 = math.atan2(
        math.sin(lat1) + math.sin(lat2),
        math.sqrt((math.cos(lat1) + bx) ** 2 + by ** 2)
    )
    lon3 = lon1 + math.atan2(by, math.cos(lat1) + bx)

    return Coordinate(
        latitude=math.degrees(lat3),
        longitude=math.degrees(lon3)
    )


def bounding_box(
    coord: Coordinate,
    radius_km: float,
    points: int = 8
) -> List[Coordinate]:
    """
    Generate bounding box coordinates around a center point.

    Args:
        coord: Center coordinate
        radius_km: Radius in kilometers
        points: Number of points to generate
    """
    bearings = [360 / points * i for i in range(points)]
    return [destination_point(coord, b, radius_km, "km") for b in bearings]
