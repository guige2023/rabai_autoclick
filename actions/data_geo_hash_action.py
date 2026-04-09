"""
Data Geohash Action Module

Provides geohash encoding/decoding for geographic coordinate compression.
"""

from typing import Any, Dict, List, Optional, Tuple
import math


# Geohash alphabet (base32)
BASE32 = "0123456789bcdefghjkmnpqrstuvwxyz"

# Latitude and longitude bounds
LAT_MIN, LAT_MAX = -90.0, 90.0
LON_MIN, LON_MAX = -180.0, 180.0


class Geohash:
    """
    Geohash encoding and decoding implementation.

    Geohash is a hierarchical spatial data structure which subdivides
    space into buckets of grid shape.
    """

    BITS = [16, 8, 4, 2, 1]
    BASE32_CODES = {c: i for i, c in enumerate(BASE32)}

    @classmethod
    def encode(cls, latitude: float, longitude: float, precision: int = 9) -> str:
        """
        Encode latitude/longitude to geohash string.

        Args:
            latitude: Latitude in degrees (-90 to 90)
            longitude: Longitude in degrees (-180 to 180)
            precision: Number of characters in geohash (default 9)

        Returns:
            Geohash string

        Example:
            >>> Geohash.encode(37.7749, -122.4194, 9)
            '9q8yyk2k0'
        """
        lat_range = (LAT_MIN, LAT_MAX)
        lon_range = (LON_MIN, LON_MAX)
        hash_string = []
        bits = 0
        bit_count = 0
        is_lon = True  # Start with longitude

        while len(hash_string) < precision:
            if is_lon:
                mid = (lon_range[0] + lon_range[1]) / 2
                if longitude >= mid:
                    bits |= cls.BITS[bit_count]
                    lon_range = (mid, lon_range[1])
                else:
                    lon_range = (lon_range[0], mid)
            else:
                mid = (lat_range[0] + lat_range[1]) / 2
                if latitude >= mid:
                    bits |= cls.BITS[bit_count]
                    lat_range = (mid, lat_range[1])
                else:
                    lat_range = (lat_range[0], mid)

            is_lon = not is_lon
            bit_count += 1

            if bit_count == 5:
                hash_string.append(BASE32[bits])
                bits = 0
                bit_count = 0

        return "".join(hash_string)

    @classmethod
    def decode(cls, geohash: str) -> Tuple[float, float, float, float]:
        """
        Decode geohash to bounding box.

        Args:
            geohash: Geohash string

        Returns:
            Tuple of (center_lat, center_lon, lat_err, lon_err)
            lat_err and lon_err are half the width/height of the box

        Example:
            >>> Geohash.decode("9q8yyk2k0")
            (37.7749..., -122.4194..., 0.00068..., 0.00034...)
        """
        lat_range = (LAT_MIN, LAT_MAX)
        lon_range = (LON_MIN, LON_MAX)
        is_lon = True

        for char in geohash.lower():
            idx = cls.BASE32_CODES.get(char)
            if idx is None:
                continue

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

        center_lat = (lat_range[0] + lat_range[1]) / 2
        center_lon = (lon_range[0] + lon_range[1]) / 2
        lat_err = (lat_range[1] - lat_range[0]) / 2
        lon_err = (lon_range[1] - lon_range[0]) / 2

        return center_lat, center_lon, lat_err, lon_err

    @classmethod
    def neighbors(cls, geohash: str) -> List[str]:
        """
        Get all 8 neighboring geohashes.

        Args:
            geohash: Center geohash

        Returns:
            List of 8 neighboring geohash strings
        """
        lat, lon, lat_err, lon_err = cls.decode(geohash)
        precision = len(geohash)

        neighbors = []
        for d_lat in [-1, 0, 1]:
            for d_lon in [-1, 0, 1]:
                if d_lat == 0 and d_lon == 0:
                    continue
                neighbor_lat = lat + d_lat * 2 * lat_err
                neighbor_lon = lon + d_lon * 2 * lon_err

                # Clamp to valid range
                neighbor_lat = max(LAT_MIN, min(LAT_MAX, neighbor_lat))
                neighbor_lon = max(LON_MIN, min(LON_MAX, neighbor_lon))

                neighbors.append(cls.encode(neighbor_lat, neighbor_lon, precision))

        return neighbors

    @classmethod
    def bbox(cls, geohash: str) -> Dict[str, float]:
        """
        Get bounding box of geohash.

        Returns:
            Dict with min_lat, max_lat, min_lon, max_lon
        """
        lat, lon, lat_err, lon_err = cls.decode(geohash)
        return {
            "min_lat": lat - lat_err,
            "max_lat": lat + lat_err,
            "min_lon": lon - lon_err,
            "max_lon": lon + lon_err
        }

    @classmethod
    def contains(cls, parent: str, child: str) -> bool:
        """Check if parent geohash contains child."""
        return child.startswith(parent)

    @classmethod
    def intersection(cls, hash1: str, hash2: str) -> Optional[str]:
        """Get common prefix of two geohashes."""
        common = []
        for c1, c2 in zip(hash1, hash2):
            if c1 == c2:
                common.append(c1)
            else:
                break
        return "".join(common) if common else None


class GeoHashIndex:
    """
    Spatial index using geohash for efficient nearby queries.
    """

    def __init__(self, precision: int = 6):
        """
        Initialize geohash index.

        Args:
            precision: Geohash precision (1-12)
        """
        self.precision = precision
        self.buckets: Dict[str, List[Any]] = {}

    def insert(self, latitude: float, longitude: float, data: Any) -> str:
        """
        Insert data point.

        Returns:
            Geohash string
        """
        h = Geohash.encode(latitude, longitude, self.precision)
        if h not in self.buckets:
            self.buckets[h] = []
        self.buckets[h].append((latitude, longitude, data))
        return h

    def query_nearby(
        self,
        latitude: float,
        longitude: float,
        radius_km: float = 1.0,
        max_results: int = 100
    ) -> List[Tuple[Any, float]]:
        """
        Query nearby points.

        Args:
            latitude: Center latitude
            longitude: Center longitude
            radius_km: Search radius in kilometers
            max_results: Maximum results to return

        Returns:
            List of (data, distance_km) tuples
        """
        center_hash = Geohash.encode(latitude, longitude, self.precision)
        center_lat, center_lon, _, _ = Geohash.decode(center_hash)

        # Start with center bucket and expand
        candidates = set([center_hash])
        results = []

        # Get all neighbors recursively
        to_expand = [center_hash]
        checked = set()

        while to_expand:
            current = to_expand.pop()
            if current in checked:
                continue
            checked.add(current)

            # Check precision - expand neighbors if needed
            if len(current) < self.precision:
                for n in Geohash.neighbors(current):
                    if n not in checked:
                        to_expand.append(n)

            if current in self.buckets:
                for lat, lon, data in self.buckets[current]:
                    dist = self._haversine(latitude, longitude, lat, lon)
                    if dist <= radius_km:
                        results.append((data, dist))

                    if len(results) >= max_results:
                        break

            if len(results) >= max_results:
                break

        # Sort by distance
        results.sort(key=lambda x: x[1])
        return results[:max_results]

    def _haversine(
        self,
        lat1: float,
        lon1: float,
        lat2: float,
        lon2: float
    ) -> float:
        """Calculate distance between two points in km."""
        R = 6371  # Earth radius in km

        lat1_rad = math.radians(lat1)
        lat2_rad = math.radians(lat2)
        dlat = math.radians(lat2 - lat1)
        dlon = math.radians(lon2 - lon1)

        a = (math.sin(dlat / 2) ** 2 +
             math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(dlon / 2) ** 2)
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

        return R * c

    def query_box(
        self,
        min_lat: float,
        min_lon: float,
        max_lat: float,
        max_lon: float
    ) -> List[Any]:
        """
        Query points within bounding box.

        Args:
            min_lat, min_lon, max_lat, max_lon: Bounding box corners

        Returns:
            List of data points within the box
        """
        results = []
        center_lat = (min_lat + max_lat) / 2
        center_lon = (min_lon + max_lon) / 2
        precision = max(1, min(12, self.precision))

        # Get geohash for center
        center_hash = Geohash.encode(center_lat, center_lon, precision)
        bbox = Geohash.bbox(center_hash)

        # Get all potential hashes
        hashes_to_check = [center_hash]
        checked = set()

        while hashes_to_check:
            h = hashes_to_check.pop()
            if h in checked:
                continue
            checked.add(h)

            bbox_h = Geohash.bbox(h)

            # If this bucket is fully inside query box
            if (bbox_h["min_lat"] >= min_lat and bbox_h["max_lat"] <= max_lat and
                bbox_h["min_lon"] >= min_lon and bbox_h["max_lon"] <= max_lon):
                if h in self.buckets:
                    results.extend([d for _, _, d in self.buckets[h]])
            # If this bucket intersects query box
            elif (bbox_h["max_lat"] >= min_lat and bbox_h["min_lat"] <= max_lat and
                  bbox_h["max_lon"] >= min_lon and bbox_h["min_lon"] <= max_lon):
                if h in self.buckets:
                    for lat, lon, data in self.buckets[h]:
                        if (min_lat <= lat <= max_lat and min_lon <= lon <= max_lon):
                            results.append(data)
                # Expand for finer precision
                if len(h) < self.precision:
                    for n in Geohash.neighbors(h):
                        if n not in checked:
                            hashes_to_check.append(n)

        return results


class DataGeoHashAction:
    """
    Geohash encoding/decoding and spatial indexing for geographic data.

    Provides efficient encoding of lat/lon coordinates and nearby queries.

    Example:
        gh = DataGeoHashAction()
        h = gh.encode(37.7749, -122.4194)  # "9q8yyk2k0"
        lat, lon, lat_err, lon_err = gh.decode(h)

        # Using index for nearby queries
        index = gh.create_index(precision=6)
        index.insert(37.7749, -122.4194, {"name": "SF"})
        results = index.query_nearby(37.78, -122.42, radius_km=5)
    """

    def __init__(self, precision: int = 9):
        """
        Initialize geohash action.

        Args:
            precision: Default geohash precision (1-12)
        """
        self.precision = precision

    def encode(self, latitude: float, longitude: float, precision: Optional[int] = None) -> str:
        """Encode coordinates to geohash."""
        p = precision or self.precision
        return Geohash.encode(latitude, longitude, p)

    def decode(self, geohash: str) -> Tuple[float, float, float, float]:
        """Decode geohash to coordinates and error bounds."""
        return Geohash.decode(geohash)

    def neighbors(self, geohash: str) -> List[str]:
        """Get neighboring geohashes."""
        return Geohash.neighbors(geohash)

    def bbox(self, geohash: str) -> Dict[str, float]:
        """Get bounding box of geohash."""
        return Geohash.bbox(geohash)

    def create_index(self, precision: Optional[int] = None) -> GeoHashIndex:
        """Create a new geohash-based spatial index."""
        p = precision or self.precision
        return GeoHashIndex(p)

    def get_stats(self) -> Dict[str, Any]:
        """Get geohash statistics."""
        return {
            "precision": self.precision,
            "lat_range": (LAT_MIN, LAT_MAX),
            "lon_range": (LON_MIN, LON_MAX)
        }
