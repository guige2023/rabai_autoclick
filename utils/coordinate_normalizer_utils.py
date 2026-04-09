"""
Coordinate normalization and conversion utilities for UI automation.

This module provides utilities for normalizing coordinates across different
coordinate systems, including screen, window, and element-relative coordinates.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Tuple, Optional, Union


@dataclass(frozen=True)
class Point:
    """Immutable 2D point representation."""
    x: float
    y: float

    def __add__(self, other: Point) -> Point:
        return Point(self.x + other.x, self.y + other.y)

    def __sub__(self, other: Point) -> Point:
        return Point(self.x - other.x, self.y - other.y)

    def __mul__(self, scalar: float) -> Point:
        return Point(self.x * scalar, self.y * scalar)

    def distance_to(self, other: Point) -> float:
        """Calculate Euclidean distance to another point."""
        dx = self.x - other.x
        dy = self.y - other.y
        return math.sqrt(dx * dx + dy * dy)

    def angle_to(self, other: Point) -> float:
        """Calculate angle in radians to another point."""
        return math.atan2(other.y - self.y, other.x - self.x)

    def rotate(self, center: Point, angle: float) -> Point:
        """Rotate point around center by angle in radians."""
        cos_a = math.cos(angle)
        sin_a = math.sin(angle)
        dx = self.x - center.x
        dy = self.y - center.y
        return Point(
            center.x + dx * cos_a - dy * sin_a,
            center.y + dx * sin_a + dy * cos_a,
        )

    def scale(self, center: Point, factor: float) -> Point:
        """Scale point relative to center by factor."""
        return center + (self - center) * factor


@dataclass(frozen=True)
class Rect:
    """Immutable rectangle representation."""
    x: float
    y: float
    width: float
    height: float

    @property
    def left(self) -> float:
        return self.x

    @property
    def right(self) -> float:
        return self.x + self.width

    @property
    def top(self) -> float:
        return self.y

    @property
    def bottom(self) -> float:
        return self.y + self.height

    @property
    def center(self) -> Point:
        return Point(self.x + self.width / 2, self.y + self.height / 2)

    @property
    def area(self) -> float:
        return self.width * self.height

    def contains_point(self, point: Point) -> bool:
        """Check if point is inside rectangle."""
        return (
            self.left <= point.x < self.right
            and self.top <= point.y < self.bottom
        )

    def intersects(self, other: Rect) -> bool:
        """Check if rectangle intersects with another."""
        return (
            self.left < other.right
            and self.right > other.left
            and self.top < other.bottom
            and self.bottom > other.top
        )

    def intersection(self, other: Rect) -> Optional[Rect]:
        """Get intersection rectangle with another."""
        if not self.intersects(other):
            return None
        x = max(self.left, other.left)
        y = max(self.top, other.top)
        w = min(self.right, other.right) - x
        h = min(self.bottom, other.bottom) - y
        return Rect(x, y, w, h)

    def union(self, other: Rect) -> Rect:
        """Get minimal rectangle containing both."""
        x = min(self.left, other.left)
        y = min(self.top, other.top)
        w = max(self.right, other.right) - x
        h = max(self.bottom, other.bottom) - y
        return Rect(x, y, w, h)

    def inset(self, dx: float, dy: float) -> Rect:
        """Inset rectangle by delta values."""
        return Rect(
            self.x + dx,
            self.y + dy,
            max(0, self.width - 2 * dx),
            max(0, self.height - 2 * dy),
        )

    def expand(self, dx: float, dy: float) -> Rect:
        """Expand rectangle by delta values."""
        return self.inset(-dx, -dy)


class CoordinateNormalizer:
    """
    Normalizes coordinates between different reference frames.

    Supports conversion between:
    - Screen coordinates (absolute)
    - Window coordinates (relative to window origin)
    - Element coordinates (relative to element bounds)
    - Normalized coordinates (0.0 to 1.0 within bounds)
    """

    def __init__(
        self,
        screen_bounds: Rect,
        window_bounds: Optional[Rect] = None,
        dpi_scale: float = 1.0,
    ) -> None:
        self._screen_bounds = screen_bounds
        self._window_bounds = window_bounds
        self._dpi_scale = dpi_scale

    def screen_to_window(self, point: Point) -> Point:
        """Convert screen coordinates to window coordinates."""
        if self._window_bounds is None:
            raise ValueError("Window bounds not set")
        return Point(
            (point.x - self._window_bounds.x) / self._dpi_scale,
            (point.y - self._window_bounds.y) / self._dpi_scale,
        )

    def window_to_screen(self, point: Point) -> Point:
        """Convert window coordinates to screen coordinates."""
        if self._window_bounds is None:
            raise ValueError("Window bounds not set")
        return Point(
            point.x * self._dpi_scale + self._window_bounds.x,
            point.y * self._dpi_scale + self._window_bounds.y,
        )

    def normalize(self, point: Point, bounds: Rect) -> Point:
        """Normalize point to 0.0-1.0 range within bounds."""
        return Point(
            (point.x - bounds.x) / bounds.width,
            (point.y - bounds.y) / bounds.height,
        )

    def denormalize(self, point: Point, bounds: Rect) -> Point:
        """Convert normalized point back to absolute coordinates."""
        return Point(
            point.x * bounds.width + bounds.x,
            point.y * bounds.height + bounds.y,
        )

    def snap_to_pixel(self, point: Point) -> Point:
        """Snap coordinates to nearest pixel boundary."""
        return Point(round(point.x), round(point.y))

    def snap_to_grid(self, point: Point, grid_size: int) -> Point:
        """Snap coordinates to nearest grid point."""
        return Point(
            round(point.x / grid_size) * grid_size,
            round(point.y / grid_size) * grid_size,
        )

    def lerp(self, p1: Point, p2: Point, t: float) -> Point:
        """Linear interpolation between two points."""
        return Point(
            p1.x + (p2.x - p1.x) * t,
            p1.y + (p2.y - p1.y) * t,
        )

    def midpoint(self, p1: Point, p2: Point) -> Point:
        """Get midpoint between two points."""
        return self.lerp(p1, p2, 0.5)


def normalize_coordinates(
    value: Union[int, float, Tuple[int, int], Point],
) -> Point:
    """Convert various coordinate representations to Point."""
    if isinstance(value, (int, float)):
        raise TypeError("Single number requires both x and y")
    if isinstance(value, tuple):
        return Point(float(value[0]), float(value[1]))
    if isinstance(value, Point):
        return value
    raise TypeError(f"Cannot normalize {type(value)}")
