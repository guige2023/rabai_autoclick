"""Point-in-shape detection utilities for UI automation.

Provides geometric point-in-shape tests for various shapes
used in UI element detection and hit testing.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from enum import Enum, auto
from typing import Optional, Sequence


class ShapeType(Enum):
    """Types of geometric shapes."""
    POINT = auto()
    RECTANGLE = auto()
    CIRCLE = auto()
    ELLIPSE = auto()
    POLYGON = auto()
    LINE = auto()
    SEGMENT = auto()


@dataclass
class Point:
    """A 2D point."""
    x: float
    y: float

    def distance_to(self, other: Point) -> float:
        """Euclidean distance to another point."""
        return math.sqrt((self.x - other.x) ** 2 + (self.y - other.y) ** 2)

    def angle_to(self, other: Point) -> float:
        """Angle in radians from this point to another."""
        return math.atan2(other.y - self.y, other.x - self.x)

    def midpoint_to(self, other: Point) -> Point:
        """Midpoint between this point and another."""
        return Point((self.x + other.x) / 2, (self.y + other.y) / 2)


@dataclass
class Rectangle:
    """A rectangle defined by position and size."""
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
    def center(self) -> Point:
        return Point(self.x + self.width / 2, self.y + self.height / 2)

    @property
    def top_left(self) -> Point:
        return Point(self.x, self.y)

    @property
    def top_right(self) -> Point:
        return Point(self.x2, self.y)

    @property
    def bottom_left(self) -> Point:
        return Point(self.x, self.y2)

    @property
    def bottom_right(self) -> Point:
        return Point(self.x2, self.y2)

    def contains_point(self, px: float, py: float) -> bool:
        """Check if point is inside rectangle."""
        return self.x <= px < self.x2 and self.y <= py < self.y2

    def contains_point_strict(self, px: float, py: float) -> bool:
        """Check if point is strictly inside (not on boundary)."""
        return self.x < px < self.x2 and self.y < py < self.y2


@dataclass
class Circle:
    """A circle defined by center and radius."""
    cx: float
    cy: float
    radius: float

    @property
    def center(self) -> Point:
        return Point(self.cx, self.cy)

    def contains_point(self, px: float, py: float) -> bool:
        """Check if point is inside or on the circle."""
        dx = px - self.cx
        dy = py - self.cy
        return dx * dx + dy * dy <= self.radius * self.radius


@dataclass
class Ellipse:
    """An ellipse defined by center and radii."""
    cx: float
    cy: float
    rx: float
    ry: float

    def contains_point(self, px: float, py: float) -> bool:
        """Check if point is inside or on the ellipse."""
        dx = px - self.cx
        dy = py - self.cy
        return (dx * dx) / (self.rx * self.rx) + (dy * dy) / (self.ry * self.ry) <= 1.0


@dataclass
class Polygon:
    """A polygon defined by a sequence of vertices."""
    vertices: list[Point]

    def contains_point(self, px: float, py: float) -> bool:
        """Check if point is inside polygon using ray casting."""
        if len(self.vertices) < 3:
            return False

        inside = False
        j = len(self.vertices) - 1
        for i in range(len(self.vertices)):
            xi, yi = self.vertices[i].x, self.vertices[i].y
            xj, yj = self.vertices[j].x, self.vertices[j].y

            if ((yi > py) != (yj > py)) and (
                px < (xj - xi) * (py - yi) / (yj - yi) + xi
            ):
                inside = not inside
            j = i

        return inside


def point_in_rectangle(
    px: float, py: float,
    rx: float, ry: float,
    rw: float, rh: float,
) -> bool:
    """Quick point-in-rectangle test."""
    return rx <= px < rx + rw and ry <= py < ry + rh


def point_in_circle(
    px: float, py: float,
    cx: float, cy: float,
    radius: float,
) -> bool:
    """Quick point-in-circle test."""
    dx = px - cx
    dy = py - cy
    return dx * dx + dy * dy <= radius * radius


def point_in_ellipse(
    px: float, py: float,
    cx: float, cy: float,
    rx: float, ry: float,
) -> bool:
    """Point-in-ellipse test."""
    dx = px - cx
    dy = py - cy
    return (dx * dx) / (rx * rx) + (dy * dy) / (ry * ry) <= 1.0


def point_in_polygon(
    px: float, py: float,
    vertices: Sequence[tuple[float, float]],
) -> bool:
    """Point-in-polygon test using ray casting algorithm."""
    n = len(vertices)
    if n < 3:
        return False

    inside = False
    j = n - 1
    for i in range(n):
        xi, yi = vertices[i]
        xj, yj = vertices[j]
        if ((yi > py) != (yj > py)) and (
            px < (xj - xi) * (py - yi) / (yj - yi) + xi
        ):
            inside = not inside
        j = i

    return inside


def distance_point_to_rectangle(
    px: float, py: float,
    rx: float, ry: float,
    rw: float, rh: float,
) -> float:
    """Minimum distance from a point to a rectangle."""
    dx = max(rx - px, 0, px - (rx + rw))
    dy = max(ry - py, 0, py - (ry + rh))
    return math.sqrt(dx * dx + dy * dy)
