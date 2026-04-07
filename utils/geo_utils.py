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
