"""
Coordinate Utilities

Provides utilities for coordinate transformations
and spatial operations in UI automation.

Author: Agent3
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any
import math


@dataclass
class Point:
    """Represents a 2D point."""
    x: float
    y: float

    def distance_to(self, other: Point) -> float:
        """Calculate distance to another point."""
        dx = self.x - other.x
        dy = self.y - other.y
        return math.sqrt(dx * dx + dy * dy)

    def angle_to(self, other: Point) -> float:
        """Calculate angle to another point in radians."""
        return math.atan2(other.y - self.y, other.x - self.x)

    def translate(self, dx: float, dy: float) -> Point:
        """Translate point by offset."""
        return Point(self.x + dx, self.y + dy)

    def scale(self, factor: float) -> Point:
        """Scale point from origin."""
        return Point(self.x * factor, self.y * factor)

    def rotate(self, angle: float, center: Point | None = None) -> Point:
        """Rotate point around a center."""
        if center is None:
            center = Point(0, 0)
        cos_a = math.cos(angle)
        sin_a = math.sin(angle)
        dx = self.x - center.x
        dy = self.y - center.y
        return Point(
            center.x + dx * cos_a - dy * sin_a,
            center.y + dx * sin_a + dy * cos_a,
        )


@dataclass
class Rect:
    """Represents a rectangle."""
    x: float
    y: float
    width: float
    height: float

    @property
    def center(self) -> Point:
        """Get center point."""
        return Point(self.x + self.width / 2, self.y + self.height / 2)

    @property
    def top_left(self) -> Point:
        """Get top-left corner."""
        return Point(self.x, self.y)

    @property
    def top_right(self) -> Point:
        """Get top-right corner."""
        return Point(self.x + self.width, self.y)

    @property
    def bottom_left(self) -> Point:
        """Get bottom-left corner."""
        return Point(self.x, self.y + self.height)

    @property
    def bottom_right(self) -> Point:
        """Get bottom-right corner."""
        return Point(self.x + self.width, self.y + self.height)

    def contains_point(self, point: Point) -> bool:
        """Check if point is inside rectangle."""
        return (self.x <= point.x <= self.x + self.width and
                self.y <= point.y <= self.y + self.height)

    def intersects(self, other: Rect) -> bool:
        """Check if rectangles intersect."""
        return not (self.x + self.width < other.x or
                    other.x + other.width < self.x or
                    self.y + self.height < other.y or
                    other.y + other.height < self.y)


def normalize_coordinates(
    x: int,
    y: int,
    source_size: tuple[int, int],
    target_size: tuple[int, int],
) -> tuple[int, int]:
    """
    Normalize coordinates from one coordinate system to another.
    
    Args:
        x, y: Source coordinates.
        source_size: (width, height) of source.
        target_size: (width, height) of target.
        
    Returns:
        Normalized (x, y) for target.
    """
    scale_x = target_size[0] / source_size[0]
    scale_y = target_size[1] / source_size[1]
    return (int(x * scale_x), int(y * scale_y))


def clamp(value: float, min_val: float, max_val: float) -> float:
    """Clamp a value between min and max."""
    return max(min_val, min(value, max_val))
