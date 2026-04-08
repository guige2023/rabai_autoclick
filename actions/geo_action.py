"""
Geolocation and geographic utilities - distance calculation, coordinate parsing, geohashing.
"""
from typing import Any, Dict, List, Optional, Tuple
import math
import logging

logger = logging.getLogger(__name__)


class BaseAction:
    """Base class for all actions."""

    def execute(self, context: Dict[str, Any], params: Dict[str, Any]) -> Dict[str, Any]:
        raise NotImplementedError


def _haversine(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Calculate distance in km between two points using Haversine formula."""
    R = 6371.0
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return R * c


def _parse_coord(coord_str: str) -> Optional[Tuple[float, float]]:
    import re
    patterns = [
        r"(-?\d+\.?\d*)\s*,\s*(-?\d+\.?\d*)",
        r"(-?\d+\.?\d*)\s+(-?\d+\.?\d*)",
        r"\((-?\d+\.?\d*),\s*(-?\d+\.?\d*)\)",
    ]
    for pattern in patterns:
        match = re.match(pattern, coord_str.strip())
        if match:
            return float(match.group(1)), float(match.group(2))
    return None


def _bbox(points: List[Tuple[float, float]]) -> Dict[str, float]:
    if not points:
        return {}
    lats = [p[0] for p in points]
    lons = [p[1] for p in points]
    return {
        "min_lat": min(lats), "max_lat": max(lats),
        "min_lon": min(lons), "max_lon": max(lons),
        "center_lat": sum(lats) / len(lats),
        "center_lon": sum(lons) / len(lons),
    }


def _geohash_encode(lat: float, lon: float, precision: int = 9) -> str:
    """Simple geohash encoding."""
    base32 = "0123456789bcdefghjkmnpqrstuvwxyz"
    lat_range, lon_range = (-90.0, 90.0), (-180.0, 180.0)
    hash_str = []
    bit = 0
    ch = 0
    is_lon = True
    while len(hash_str) < precision:
        if is_lon:
            mid = (lon_range[0] + lon_range[1]) / 2
            if lon >= mid:
                ch |= 1 << (4 - bit)
                lon_range = (mid, lon_range[1])
            else:
                lon_range = (lon_range[0], mid)
        else:
            mid = (lat_range[0] + lat_range[1]) / 2
            if lat >= mid:
                ch |= 1 << (4 - bit)
                lat_range = (mid, lat_range[1])
            else:
                lat_range = (lat_range[0], mid)
        is_lon = not is_lon
        bit += 1
        if bit == 5:
            hash_str.append(base32[ch])
            bit = 0
            ch = 0
    return "".join(hash_str)


def _bearing(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dlambda = math.radians(lon2 - lon1)
    x = math.sin(dlambda) * math.cos(phi2)
    y = math.cos(phi1) * math.sin(phi2) - math.sin(phi1) * math.cos(phi2) * math.cos(dlambda)
    theta = math.atan2(x, y)
    return (math.degrees(theta) + 360) % 360


def _destination_point(lat: float, lon: float, bearing: float, distance_km: float) -> Tuple[float, float]:
    R = 6371.0
    phi1 = math.radians(lat)
    theta = math.radians(bearing)
    d = distance_km / R
    phi2 = math.asin(math.sin(phi1) * math.cos(d) + math.cos(phi1) * math.sin(d) * math.cos(theta))
    lambda2 = math.radians(lon) + math.atan2(math.sin(theta) * math.sin(d) * math.cos(phi1),
                                              math.cos(d) - math.sin(phi1) * math.sin(phi2))
    return math.degrees(phi2), (math.degrees(lambda2) + 540) % 360 - 180


class GeoAction(BaseAction):
    """Geolocation and geographic operations.

    Provides distance calculation, coordinate parsing, bounding box, geohashing, bearing.
    """

    def execute(self, context: Dict[str, Any], params: Dict[str, Any]) -> Dict[str, Any]:
        operation = params.get("operation", "distance")
        lat1 = float(params.get("lat1", 0))
        lon1 = float(params.get("lon1", 0))
        lat2 = float(params.get("lat2", 0))
        lon2 = float(params.get("lon2", 0))

        try:
            if operation == "distance":
                unit = params.get("unit", "km")
                km = _haversine(lat1, lon1, lat2, lon2)
                conversions = {"km": 1.0, "m": 1000.0, "mi": 0.621371, "ft": 3280.84, "nm": 0.539957}
                distance = km * conversions.get(unit, 1.0)
                return {"success": True, "distance": round(distance, 4), "unit": unit, "km": round(km, 4)}

            elif operation == "parse_coord":
                coord_str = params.get("coord", "")
                parsed = _parse_coord(coord_str)
                if parsed:
                    return {"success": True, "lat": parsed[0], "lon": parsed[1]}
                return {"success": False, "error": f"Could not parse: {coord_str}"}

            elif operation == "bbox":
                points_str = params.get("points", [])
                points = []
                for p in points_str:
                    if isinstance(p, (list, tuple)):
                        points.append((float(p[0]), float(p[1])))
                    elif isinstance(p, str):
                        parsed = _parse_coord(p)
                        if parsed:
                            points.append(parsed)
                if not points:
                    return {"success": False, "error": "Valid points required"}
                result = _bbox(points)
                return {"success": True, **result}

            elif operation == "geohash_encode":
                lat = float(params.get("lat", 0))
                lon = float(params.get("lon", 0))
                precision = int(params.get("precision", 9))
                h = _geohash_encode(lat, lon, precision)
                return {"success": True, "geohash": h, "lat": lat, "lon": lon}

            elif operation == "bearing":
                bearing_deg = _bearing(lat1, lon1, lat2, lon2)
                directions = ["N", "NNE", "NE", "ENE", "E", "ESE", "SE", "SSE",
                             "S", "SSW", "SW", "WSW", "W", "WNW", "NW", "NNW"]
                idx = int((bearing_deg + 11.25) / 22.5) % 16
                return {"success": True, "bearing_degrees": round(bearing_deg, 2), "direction": directions[idx]}

            elif operation == "destination":
                bearing = float(params.get("bearing", 0))
                distance_km = float(params.get("distance_km", 1))
                dest_lat, dest_lon = _destination_point(lat1, lon1, bearing, distance_km)
                return {"success": True, "lat": round(dest_lat, 6), "lon": round(dest_lon, 6)}

            elif operation == "midpoint":
                lat1_r, lon1_r = math.radians(lat1), math.radians(lon1)
                lat2_r, lon2_r = math.radians(lat2), math.radians(lon2)
                bx = math.cos(lat2_r) * math.cos(lon2_r - lon1_r)
                by = math.cos(lat2_r) * math.sin(lon2_r - lon1_r)
                lat_m = math.atan2(math.sin(lat1_r) + math.sin(lat2_r),
                                    math.sqrt((math.cos(lat1_r) + bx) ** 2 + by ** 2))
                lon_m = lon1_r + math.atan2(by, math.cos(lat1_r) + bx)
                return {"success": True, "lat": round(math.degrees(lat_m), 6), "lon": round(math.degrees(lon_m), 6)}

            elif operation == "is_within_radius":
                center_lat = float(params.get("center_lat", lat1))
                center_lon = float(params.get("center_lon", lon1))
                radius_km = float(params.get("radius_km", 1))
                distance_km = _haversine(center_lat, center_lon, lat2, lon2)
                return {"success": True, "within": distance_km <= radius_km, "distance_km": round(distance_km, 4)}

            elif operation == "area":
                points_str = params.get("points", [])
                points = []
                for p in points_str:
                    if isinstance(p, (list, tuple)):
                        points.append((float(p[0]), float(p[1])))
                    elif isinstance(p, str):
                        parsed = _parse_coord(p)
                        if parsed:
                            points.append(parsed)
                if len(points) < 3:
                    return {"success": False, "error": "At least 3 points required"}
                area = 0.0
                n = len(points)
                for i in range(n):
                    j = (i + 1) % n
                    area += math.radians(points[j][1] - points[i][1]) * (2 + math.sin(math.radians(points[i][0])) + math.sin(math.radians(points[j][0])))
                area = abs(area * 6371.0 ** 2 / 2.0)
                return {"success": True, "area_km2": round(area, 4)}

            elif operation == "format_coord":
                lat = lat1 if lat1 else float(params.get("lat", 0))
                lon = lon1 if lon1 else float(params.get("lon", 0))
                fmt = params.get("format", "dms")
                lat_dir = "N" if lat >= 0 else "S"
                lon_dir = "E" if lon >= 0 else "W"
                if fmt == "dms":
                    lat_dms = f"{abs(int(lat))}°{int((abs(lat) % 1) * 60)}'{((abs(lat) % 1) * 60 % 1) * 60:.2f}\""
                    lon_dms = f"{abs(int(lon))}°{int((abs(lon) % 1) * 60)}'{((abs(lon) % 1) * 60 % 1) * 60:.2f}\""
                    return {"success": True, "formatted": f"{lat_dms}{lat_dir}, {lon_dms}{lon_dir}"}
                elif fmt == "dm":
                    lat_dm = f"{abs(int(lat))}°{abs(lat) % 1 * 60:.4f}'"
                    lon_dm = f"{abs(int(lon))}°{abs(lon) % 1 * 60:.4f}'"
                    return {"success": True, "formatted": f"{lat_dm}{lat_dir}, {lon_dm}{lon_dir}"}
                else:
                    return {"success": True, "formatted": f"{lat},{lon}"}

            else:
                return {"success": False, "error": f"Unknown operation: {operation}"}

        except Exception as e:
            logger.error(f"GeoAction error: {e}")
            return {"success": False, "error": str(e)}


def execute(context: Dict[str, Any], params: Dict[str, Any]) -> Dict[str, Any]:
    """Entry point for geo operations."""
    return GeoAction().execute(context, params)
