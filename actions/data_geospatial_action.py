"""
Data Geospatial Action Module.

Processes geospatial data including coordinates, distances,
geohashing, and spatial operations.

Author: RabAi Team
"""

from __future__ import annotations

import math
import sys
import os
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class GeoFormat(Enum):
    """Geospatial coordinate formats."""
    DECIMAL_DEGREES = "decimal_degrees"
    DEGREES_MINUTES_SECONDS = "dms"
    DEGREES_DECIMAL_MINUTES = "ddm"
    GEOHASH = "geohash"
    GEOJSON = "geojson"
    WELL_KNOWN_TEXT = "wkt"


@dataclass
class Coordinate:
    """A geographic coordinate."""
    latitude: float
    longitude: float
    altitude: Optional[float] = None
    precision: int = 6


@dataclass
class BoundingBox:
    """A bounding box region."""
    min_lat: float
    min_lon: float
    max_lat: float
    max_lon: float


class Geohash:
    """Geohash encoding/decoding."""
    BASE32 = "0123456789bcdefghjkmnpqrstuvwxyz"
    
    @classmethod
    def encode(cls, lat: float, lon: float, precision: int = 9) -> str:
        """Encode lat/lon to geohash."""
        lat_range, lon_range = (-90.0, 90.0), (-180.0, 180.0)
        hash_chars = []
        bits = 0
        bit_count = 0
        is_lon = True
        
        while len(hash_chars) < precision:
            if is_lon:
                mid = (lon_range[0] + lon_range[1]) / 2
                if lon >= mid:
                    bits = (bits << 1) | 1
                    lon_range = (mid, lon_range[1])
                else:
                    bits = bits << 1
                    lon_range = (lon_range[0], mid)
            else:
                mid = (lat_range[0] + lat_range[1]) / 2
                if lat >= mid:
                    bits = (bits << 1) | 1
                    lat_range = (mid, lat_range[1])
                else:
                    bits = bits << 1
                    lat_range = (lat_range[0], mid)
            
            is_lon = not is_lon
            bit_count += 1
            
            if bit_count == 5:
                hash_chars.append(cls.BASE32[bits])
                bits = 0
                bit_count = 0
        
        return "".join(hash_chars)
    
    @classmethod
    def decode(cls, hash_str: str) -> Tuple[float, float]:
        """Decode geohash to lat/lon (center point)."""
        lat_range, lon_range = (-90.0, 90.0), (-180.0, 180.0)
        is_lon = True
        
        for char in hash_str.lower():
            idx = cls.BASE32.index(char)
            for i in range(4, -1, -1):
                bit = (idx >> i) & 1
                if is_lon:
                    mid = (lon_range[0] + lon_range[1]) / 2
                    if bit:
                        lon_range = (mid, lon_range[1])
                    else:
                        lon_range = (lon_range[0], mid)
                else:
                    mid = (lat_range[0] + lat_range[1]) / 2
                    if bit:
                        lat_range = (mid, lat_range[1])
                    else:
                        lat_range = (lat_range[0], mid)
                is_lon = not is_lon
        
        lat = (lat_range[0] + lat_range[1]) / 2
        lon = (lon_range[0] + lon_range[1]) / 2
        return lat, lon
    
    @classmethod
    def neighbors(cls, hash_str: str) -> List[str]:
        """Get neighboring geohashes."""
        lat, lon = cls.decode(hash_str)
        precision = len(hash_str)
        
        lat_delta = 180.0 / (2 ** (precision * 5 // 2))
        lon_delta = 360.0 / (2 ** (precision * 5 // 2 + 1))
        
        neighbors = []
        for dlat in [-lat_delta, 0, lat_delta]:
            for dlon in [-lon_delta, 0, lon_delta]:
                if dlat == 0 and dlon == 0:
                    continue
                neighbors.append(cls.encode(lat + dlat, lon + dlon, precision))
        
        return neighbors


class DataGeospatialAction(BaseAction):
    """Data geospatial action.
    
    Processes geospatial data with coordinate conversion,
    distance calculation, geohashing, and spatial operations.
    """
    action_type = "data_geospatial"
    display_name = "地理空间"
    description = "地理空间数据处理"
    
    EARTH_RADIUS_KM = 6371.0
    EARTH_RADIUS_MI = 3959.0
    
    def __init__(self):
        super().__init__()
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Process geospatial data.
        
        Args:
            context: The execution context.
            params: Dictionary containing:
                - operation: calculate_distance/within_bbox/encode_geohash/
                           decode_geohash/convert_format/centroid/area
                - coordinates: List of [lat, lon] pairs
                - lat1, lon1: First coordinate for distance calc
                - lat2, lon2: Second coordinate for distance calc
                - geohash: Geohash string
                - precision: Geohash precision
                - format: Output format
                
        Returns:
            ActionResult with processed data.
        """
        start_time = time.time()
        
        operation = params.get("operation", "calculate_distance")
        
        try:
            if operation == "calculate_distance":
                result = self._calculate_distance(params, start_time)
            elif operation == "within_bbox":
                result = self._within_bbox(params, start_time)
            elif operation == "encode_geohash":
                result = self._encode_geohash(params, start_time)
            elif operation == "decode_geohash":
                result = self._decode_geohash(params, start_time)
            elif operation == "convert_format":
                result = self._convert_format(params, start_time)
            elif operation == "centroid":
                result = self._centroid(params, start_time)
            elif operation == "area":
                result = self._area(params, start_time)
            elif operation == "bearing":
                result = self._bearing(params, start_time)
            elif operation == "destination":
                result = self._destination(params, start_time)
            elif operation == "bbox":
                result = self._bbox(params, start_time)
            else:
                return ActionResult(
                    success=False,
                    message=f"Unknown operation: {operation}",
                    duration=time.time() - start_time
                )
            
            return result
            
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Geospatial operation failed: {str(e)}",
                duration=time.time() - start_time
            )
    
    def _haversine_distance(
        self, lat1: float, lon1: float, lat2: float, lon2: float, unit: str = "km"
    ) -> float:
        """Calculate great circle distance using Haversine formula."""
        R = self.EARTH_RADIUS_KM if unit in ("km", "kilometers") else self.EARTH_RADIUS_MI
        
        lat1_rad = math.radians(lat1)
        lat2_rad = math.radians(lat2)
        dlat = math.radians(lat2 - lat1)
        dlon = math.radians(lon2 - lon1)
        
        a = math.sin(dlat / 2) ** 2 + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(dlon / 2) ** 2
        c = 2 * math.asin(math.sqrt(a))
        
        return R * c
    
    def _vincenty_distance(
        self, lat1: float, lon1: float, lat2: float, lon2: float, unit: str = "km"
    ) -> float:
        """Calculate distance using Vincenty formula (more accurate for short distances)."""
        R = self.EARTH_RADIUS_KM if unit in ("km", "kilometers") else self.EARTH_RADIUS_MI
        
        lat1_rad = math.radians(lat1)
        lat2_rad = math.radians(lat2)
        dlon_rad = math.radians(lon2 - lon1)
        
        y = math.sin(dlon_rad) * math.cos(lat2_rad)
        x = math.cos(lat1_rad) * math.sin(lat2_rad) - math.sin(lat1_rad) * math.cos(lat2_rad) * math.cos(dlon_rad)
        
        d = math.atan2(math.sqrt(y ** 2 + x ** 2), x)
        
        return R * d
    
    def _calculate_distance(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Calculate distance between two coordinates."""
        lat1 = params.get("lat1", 0)
        lon1 = params.get("lon1", 0)
        lat2 = params.get("lat2", 0)
        lon2 = params.get("lon2", 0)
        method = params.get("method", "haversine")
        unit = params.get("unit", "km")
        
        if method == "vincenty":
            distance = self._vincenty_distance(lat1, lon1, lat2, lon2, unit)
        else:
            distance = self._haversine_distance(lat1, lon1, lat2, lon2, unit)
        
        return ActionResult(
            success=True,
            message=f"Distance calculated: {distance:.4f} {unit}",
            data={
                "distance": distance,
                "unit": unit,
                "from": {"lat": lat1, "lon": lon1},
                "to": {"lat": lat2, "lon": lon2},
                "method": method
            },
            duration=time.time() - start_time
        )
    
    def _within_bbox(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Check if coordinates are within bounding box."""
        lat = params.get("lat", 0)
        lon = params.get("lon", 0)
        min_lat = params.get("min_lat")
        min_lon = params.get("min_lon")
        max_lat = params.get("max_lat")
        max_lon = params.get("max_lon")
        
        if None in (min_lat, min_lon, max_lat, max_lon):
            return ActionResult(
                success=False,
                message="Missing bounding box parameters",
                duration=time.time() - start_time
            )
        
        within = min_lat <= lat <= max_lat and min_lon <= lon <= max_lon
        
        return ActionResult(
            success=True,
            message=f"{'Within' if within else 'Outside'} bounding box",
            data={
                "within": within,
                "coordinate": {"lat": lat, "lon": lon},
                "bbox": {"min_lat": min_lat, "min_lon": min_lon, "max_lat": max_lat, "max_lon": max_lon}
            },
            duration=time.time() - start_time
        )
    
    def _encode_geohash(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Encode coordinates to geohash."""
        lat = params.get("lat", 0)
        lon = params.get("lon", 0)
        precision = params.get("precision", 9)
        
        hash_str = Geohash.encode(lat, lon, precision)
        neighbors = Geohash.neighbors(hash_str)
        
        return ActionResult(
            success=True,
            message=f"Encoded to geohash: {hash_str}",
            data={
                "geohash": hash_str,
                "coordinates": {"lat": lat, "lon": lon},
                "precision": precision,
                "neighbors": neighbors
            },
            duration=time.time() - start_time
        )
    
    def _decode_geohash(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Decode geohash to coordinates."""
        geohash = params.get("geohash", "")
        
        if not geohash:
            return ActionResult(
                success=False,
                message="Missing geohash parameter",
                duration=time.time() - start_time
            )
        
        lat, lon = Geohash.decode(geohash)
        precision = len(geohash)
        
        lat_delta = 180.0 / (2 ** (precision * 5 // 2))
        lon_delta = 360.0 / (2 ** (precision * 5 // 2 + 1))
        
        return ActionResult(
            success=True,
            message=f"Decoded geohash to: ({lat:.6f}, {lon:.6f})",
            data={
                "geohash": geohash,
                "coordinates": {"lat": lat, "lon": lon},
                "precision": precision,
                "bbox": {
                    "min_lat": lat - lat_delta / 2,
                    "max_lat": lat + lat_delta / 2,
                    "min_lon": lon - lon_delta / 2,
                    "max_lon": lon + lon_delta / 2
                }
            },
            duration=time.time() - start_time
        )
    
    def _convert_format(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Convert coordinate format."""
        lat = params.get("lat", 0)
        lon = params.get("lon", 0)
        input_format = params.get("input_format", "decimal_degrees")
        output_format = params.get("output_format", "geohash")
        
        if output_format == "geohash":
            precision = params.get("precision", 9)
            result = Geohash.encode(lat, lon, precision)
        elif output_format == "dms":
            lat_dir = "N" if lat >= 0 else "S"
            lon_dir = "E" if lon >= 0 else "W"
            result = {
                "latitude": f"{abs(lat):.0f}°{abs((lat % 1) * 60):.0f}'{abs((lat % 1) % 60 * 60):.2f}\" {lat_dir}",
                "longitude": f"{abs(lon):.0f}°{abs((lon % 1) * 60):.0f}'{abs((lon % 1) % 60 * 60):.2f}\" {lon_dir}"
            }
        elif output_format == "ddm":
            lat_dir = "N" if lat >= 0 else "S"
            lon_dir = "E" if lon >= 0 else "W"
            result = {
                "latitude": f"{abs(lat):.0f}°{abs((lat % 1) * 60):.4f}' {lat_dir}",
                "longitude": f"{abs(lon):.0f}°{abs((lon % 1) * 60):.4f}' {lon_dir}"
            }
        elif output_format == "geojson":
            result = {
                "type": "Point",
                "coordinates": [lon, lat]
            }
        elif output_format == "wkt":
            result = f"POINT ({lon} {lat})"
        else:
            result = {"latitude": lat, "longitude": lon}
        
        return ActionResult(
            success=True,
            message=f"Converted to {output_format}",
            data={
                "input": {"lat": lat, "lon": lon, "format": input_format},
                "output": result,
                "output_format": output_format
            },
            duration=time.time() - start_time
        )
    
    def _centroid(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Calculate centroid of coordinates."""
        coords = params.get("coordinates", [])
        
        if not coords:
            return ActionResult(
                success=False,
                message="No coordinates provided",
                duration=time.time() - start_time
            )
        
        total_lat = sum(c[0] for c in coords)
        total_lon = sum(c[1] for c in coords)
        n = len(coords)
        
        return ActionResult(
            success=True,
            message=f"Centroid: ({total_lat/n:.6f}, {total_lon/n:.6f})",
            data={
                "centroid": {"lat": total_lat / n, "lon": total_lon / n},
                "count": n
            },
            duration=time.time() - start_time
        )
    
    def _area(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Calculate area of polygon using Shoelace formula (approximate)."""
        coords = params.get("coordinates", [])
        
        if len(coords) < 3:
            return ActionResult(
                success=False,
                message="Need at least 3 coordinates for area",
                duration=time.time() - start_time
            )
        
        n = len(coords)
        area = 0.0
        
        for i in range(n):
            j = (i + 1) % n
            area += coords[i][1] * coords[j][0]
            area -= coords[j][1] * coords[i][0]
        
        area = abs(area) / 2.0
        
        return ActionResult(
            success=True,
            message=f"Area: {area:.4f} square degrees",
            data={
                "area": area,
                "unit": "square_degrees",
                "vertices": n
            },
            duration=time.time() - start_time
        )
    
    def _bearing(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Calculate bearing between two points."""
        lat1 = math.radians(params.get("lat1", 0))
        lon1 = math.radians(params.get("lon1", 0))
        lat2 = math.radians(params.get("lat2", 0))
        lon2 = math.radians(params.get("lon2", 0))
        
        dlon = lon2 - lon1
        
        x = math.sin(dlon) * math.cos(lat2)
        y = math.cos(lat1) * math.sin(lat2) - math.sin(lat1) * math.cos(lat2) * math.cos(dlon)
        
        bearing = math.atan2(x, y)
        bearing = math.degrees(bearing)
        bearing = (bearing + 360) % 360
        
        return ActionResult(
            success=True,
            message=f"Bearing: {bearing:.2f}°",
            data={
                "bearing_degrees": bearing,
                "bearing_radians": bearing * math.pi / 180,
                "direction": self._bearing_to_direction(bearing)
            },
            duration=time.time() - start_time
        )
    
    def _bearing_to_direction(self, bearing: float) -> str:
        """Convert bearing to cardinal direction."""
        directions = ["N", "NE", "E", "SE", "S", "SW", "W", "NW"]
        idx = round(bearing / 45) % 8
        return directions[idx]
    
    def _destination(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Calculate destination point from bearing and distance."""
        lat1 = math.radians(params.get("lat", 0))
        lon1 = math.radians(params.get("lon", 0))
        bearing_deg = params.get("bearing", 0)
        distance = params.get("distance", 0)
        unit = params.get("unit", "km")
        
        R = self.EARTH_RADIUS_KM if unit in ("km", "kilometers") else self.EARTH_RADIUS_MI
        bearing = math.radians(bearing_deg)
        
        lat2 = math.asin(
            math.sin(lat1) * math.cos(distance / R) +
            math.cos(lat1) * math.sin(distance / R) * math.cos(bearing)
        )
        
        lon2 = lon1 + math.atan2(
            math.sin(bearing) * math.sin(distance / R) * math.cos(lat1),
            math.cos(distance / R) - math.sin(lat1) * math.sin(lat2)
        )
        
        return ActionResult(
            success=True,
            message=f"Destination: ({math.degrees(lat2):.6f}, {math.degrees(lon2):.6f})",
            data={
                "destination": {"lat": math.degrees(lat2), "lon": math.degrees(lon2)},
                "bearing": bearing_deg,
                "distance": distance,
                "unit": unit
            },
            duration=time.time() - start_time
        )
    
    def _bbox(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Calculate bounding box of coordinates."""
        coords = params.get("coordinates", [])
        
        if not coords:
            return ActionResult(
                success=False,
                message="No coordinates provided",
                duration=time.time() - start_time
            )
        
        lats = [c[0] for c in coords]
        lons = [c[1] for c in coords]
        
        return ActionResult(
            success=True,
            message="Bounding box calculated",
            data={
                "bbox": {
                    "min_lat": min(lats),
                    "max_lat": max(lats),
                    "min_lon": min(lons),
                    "max_lon": max(lons)
                },
                "count": len(coords)
            },
            duration=time.time() - start_time
        )
    
    def validate_params(self, params: Dict[str, Any]) -> Tuple[bool, str]:
        """Validate geospatial parameters."""
        return True, ""
    
    def get_required_params(self) -> List[str]:
        """Return required parameters."""
        return []
