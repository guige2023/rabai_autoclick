"""
Element bounds utilities for computing element bounding boxes.

Provides utilities for computing union bounds, intersection bounds,
and bounds operations for UI element analysis.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Optional


@dataclass
class Bounds:
    """Bounding box with helper methods."""
    x: float
    y: float
    width: float
    height: float

    @property
    def x2(self) -> float:
        return self.x + self.width

    @property
    def y2(self) -> float:
        return self.y + self.height

    @property
    def center_x(self) -> float:
        return self.x + self.width / 2

    @property
    def center_y(self) -> float:
        return self.y + self.height / 2

    @property
    def area(self) -> float:
        return self.width * self.height

    @property
    def is_valid(self) -> bool:
        return self.width > 0 and self.height > 0

    def contains_point(self, px: float, py: float) -> bool:
        return self.x <= px <= self.x2 and self.y <= py <= self.y2

    def intersects(self, other: Bounds) -> bool:
        return not (
            self.x2 < other.x or other.x2 < self.x or
            self.y2 < other.y or other.y2 < self.y
        )

    def union(self, other: Bounds) -> Bounds:
        """Compute union of two bounds."""
        x = min(self.x, other.x)
        y = min(self.y, other.y)
        x2 = max(self.x2, other.x2)
        y2 = max(self.y2, other.y2)
        return Bounds(x=x, y=y, width=x2 - x, height=y2 - y)

    def intersection(self, other: Bounds) -> Optional[Bounds]:
        """Compute intersection of two bounds."""
        x = max(self.x, other.x)
        y = max(self.y, other.y)
        x2 = min(self.x2, other.x2)
        y2 = min(self.y2, other.y2)

        if x >= x2 or y >= y2:
            return None

        return Bounds(x=x, y=y, width=x2 - x, height=y2 - y)

    def distance_to(self, other: Bounds) -> float:
        """Compute minimum distance between two bounds."""
        dx = max(0, max(other.x - self.x2, self.x - other.x2))
        dy = max(0, max(other.y - self.y2, self.y - other.y2))
        return math.hypot(dx, dy)

    def expand(self, margin: float) -> Bounds:
        """Expand bounds by a margin."""
        return Bounds(
            x=self.x - margin,
            y=self.y - margin,
            width=self.width + margin * 2,
            height=self.height + margin * 2,
        )

    def shrink(self, margin: float) -> Bounds:
        """Shrink bounds by a margin."""
        return Bounds(
            x=self.x + margin,
            y=self.y + margin,
            width=max(0, self.width - margin * 2),
            height=max(0, self.height - margin * 2),
        )

    def aspect_ratio(self) -> float:
        """Compute aspect ratio (width / height)."""
        if self.height == 0:
            return 0.0
        return self.width / self.height

    def to_tuple(self) -> tuple[float, float, float, float]:
        return (self.x, self.y, self.width, self.height)

    @staticmethod
    def from_tuple(t: tuple[float, float, float, float]) -> Bounds:
        return Bounds(x=t[0], y=t[1], width=t[2], height=t[3])


class BoundsCalculator:
    """Calculates bounds for collections of elements."""

    @staticmethod
    def union_all(bounds_list: list[Bounds]) -> Optional[Bounds]:
        """Compute union of multiple bounds."""
        if not bounds_list:
            return None
        result = bounds_list[0]
        for b in bounds_list[1:]:
            result = result.union(b)
        return result

    @staticmethod
    def intersection_all(bounds_list: list[Bounds]) -> Optional[Bounds]:
        """Compute intersection of multiple bounds."""
        if not bounds_list:
            return None
        result = bounds_list[0]
        for b in bounds_list[1:]:
            result = result.intersection(b)
            if result is None:
                return None
        return result

    @staticmethod
    def center_of_mass(bounds_list: list[Bounds]) -> tuple[float, float]:
        """Compute center of mass of multiple bounds."""
        if not bounds_list:
            return (0.0, 0.0)
        total_x = sum(b.center_x for b in bounds_list)
        total_y = sum(b.center_y for b in bounds_list)
        count = len(bounds_list)
        return (total_x / count, total_y / count)

    @staticmethod
    def sort_by_area(bounds_list: list[Bounds], descending: bool = True) -> list[Bounds]:
        """Sort bounds by area."""
        return sorted(bounds_list, key=lambda b: b.area, reverse=descending)

    @staticmethod
    def sort_by_position(bounds_list: list[Bounds], axis: str = "y") -> list[Bounds]:
        """Sort bounds by position on an axis."""
        key = "center_x" if axis == "x" else "center_y"
        return sorted(bounds_list, key=lambda b: getattr(b, key))


__all__ = ["Bounds", "BoundsCalculator"]
