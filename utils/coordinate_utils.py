"""Coordinate and geometry utilities for screen-positioned automation.

This module provides helpers for working with screen coordinates,
including coordinate transformation between displays, DPI scaling,
rectangle intersection/containment checks, and point-in-region
detection for multi-monitor setups.

Example:
    >>> from utils.coordinate_utils import screen_to_local, rect_contains_point
    >>> rect = (0, 0, 1920, 1080)
    >>> contains = rect_contains_point(rect, 100, 100)
"""

from __future__ import annotations

import math
from typing import NamedTuple

__all__ = [
    "Point",
    "Rect",
    "Size",
    "rect_contains_point",
    "rect_intersects",
    "rect_intersection",
    "rect_union",
    "screen_to_local",
    "local_to_screen",
    "distance",
    "manhattan_distance",
    "clamp_coordinates",
    "normalize_rect",
    "expand_rect",
    "center_of_rect",
    "point_in_polygon",
    "bounding_box_of_points",
]


class Point(NamedTuple):
    """A 2D point."""

    x: float
    y: float


class Size(NamedTuple):
    """A 2D size."""

    width: float
    height: float


class Rect(NamedTuple):
    """A rectangle defined by origin (x, y) and size (width, height)."""

    x: float
    y: float
    width: float
    height: float

    @property
    def left(self) -> float:
        return self.x

    @property
    def top(self) -> float:
        return self.y

    @property
    def right(self) -> float:
        return self.x + self.width

    @property
    def bottom(self) -> float:
        return self.y + self.height

    @property
    def center(self) -> Point:
        return Point(self.x + self.width / 2, self.y + self.height / 2)

    @property
    def area(self) -> float:
        return self.width * self.height

    def contains(self, x: float, y: float) -> bool:
        return self.left <= x < self.right and self.top <= y < self.bottom

    def intersects(self, other: Rect) -> bool:
        return (
            self.left < other.right
            and self.right > other.left
            and self.top < other.bottom
            and self.bottom > other.top
        )


def rect_contains_point(rect: tuple[float, float, float, float], x: float, y: float) -> bool:
    """Check if a point is inside a rectangle.

    Args:
        rect: Tuple of (x, y, width, height).
        x: Point X coordinate.
        y: Point Y coordinate.

    Returns:
        True if the point is strictly inside the rectangle.
    """
    rx, ry, rw, rh = rect
    return rx <= x < rx + rw and ry <= y < ry + rh


def rect_intersects(
    a: tuple[float, float, float, float],
    b: tuple[float, float, float, float],
) -> bool:
    """Check whether two rectangles intersect.

    Args:
        a: First rectangle as (x, y, width, height).
        b: Second rectangle as (x, y, width, height).

    Returns:
        True if the rectangles share any overlapping area.
    """
    ax, ay, aw, ah = a
    bx, by, bw, bh = b
    return ax < bx + bw and ax + aw > bx and ay < by + bh and ay + ah > by


def rect_intersection(
    a: tuple[float, float, float, float],
    b: tuple[float, float, float, float],
) -> tuple[float, float, float, float] | None:
    """Compute the intersection of two rectangles.

    Args:
        a: First rectangle as (x, y, width, height).
        b: Second rectangle as (x, y, width, height).

    Returns:
        Intersection rectangle, or None if they do not overlap.
    """
    ax, ay, aw, ah = a
    bx, by, bw, bh = b

    x1 = max(ax, bx)
    y1 = max(ay, by)
    x2 = min(ax + aw, bx + bw)
    y2 = min(ay + ah, by + bh)

    if x1 >= x2 or y1 >= y2:
        return None

    return (x1, y1, x2 - x1, y2 - y1)


def rect_union(
    a: tuple[float, float, float, float],
    b: tuple[float, float, float, float],
) -> tuple[float, float, float, float]:
    """Compute the bounding box that contains both rectangles.

    Args:
        a: First rectangle as (x, y, width, height).
        b: Second rectangle as (x, y, width, height).

    Returns:
        The minimal bounding rectangle covering both inputs.
    """
    ax, ay, aw, ah = a
    bx, by, bw, bh = b

    x1 = min(ax, bx)
    y1 = min(ay, by)
    x2 = max(ax + aw, bx + bw)
    y2 = max(ay + ah, by + bh)

    return (x1, y1, x2 - x1, y2 - y1)


def screen_to_local(
    x: float,
    y: float,
    display_bounds: tuple[float, float, float, float],
) -> tuple[float, float]:
    """Convert screen-absolute coordinates to display-local coordinates.

    Args:
        x: Screen X coordinate.
        y: Screen Y coordinate.
        display_bounds: Display rect as (x, y, width, height).

    Returns:
        Tuple of (local_x, local_y) relative to the display origin.
    """
    dx, dy, dw, dh = display_bounds
    return (x - dx, y - dy)


def local_to_screen(
    x: float,
    y: float,
    display_bounds: tuple[float, float, float, float],
) -> tuple[float, float]:
    """Convert display-local coordinates to screen-absolute coordinates.

    Args:
        x: Local X coordinate.
        y: Local Y coordinate.
        display_bounds: Display rect as (x, y, width, height).

    Returns:
        Tuple of (screen_x, screen_y).
    """
    dx, dy, _, _ = display_bounds
    return (x + dx, y + dy)


def distance(p1: Point, p2: Point) -> float:
    """Euclidean distance between two points.

    Args:
        p1: First point (x, y).
        p2: Second point (x, y).

    Returns:
        Distance in the same units as the coordinates.
    """
    return math.sqrt((p1.x - p2.x) ** 2 + (p1.y - p2.y) ** 2)


def manhattan_distance(p1: Point, p2: Point) -> float:
    """Manhattan (taxicab) distance between two points.

    Args:
        p1: First point (x, y).
        p2: Second point (x, y).

    Returns:
        Sum of absolute coordinate differences.
    """
    return abs(p1.x - p2.x) + abs(p1.y - p2.y)


def clamp_coordinates(
    x: float,
    y: float,
    bounds: tuple[float, float, float, float],
) -> tuple[float, float]:
    """Clamp coordinates to stay within a bounding rectangle.

    Args:
        x: X coordinate to clamp.
        y: Y coordinate to clamp.
        bounds: Boundary rectangle (x, y, width, height).

    Returns:
        Tuple of (clamped_x, clamped_y).
    """
    bx, by, bw, bh = bounds
    cx = max(bx, min(x, bx + bw))
    cy = max(by, min(y, by + bh))
    return (cx, cy)


def normalize_rect(rect: tuple[float, float, float, float]) -> tuple[float, float, float, float]:
    """Ensure a rect has non-negative width and height.

    Handles cases where width/height may be negative due to
    coordinate ordering (e.g., bottom-left to top-right).
    """
    x, y, w, h = rect
    nx = x if w >= 0 else x + w
    ny = y if h >= 0 else y + h
    nw = abs(w)
    nh = abs(h)
    return (nx, ny, nw, nh)


def expand_rect(
    rect: tuple[float, float, float, float],
    dx: float,
    dy: float,
) -> tuple[float, float, float, float]:
    """Expand a rectangle by a fixed amount in both directions.

    Args:
        rect: Original rectangle (x, y, width, height).
        dx: Horizontal expansion (positive = outward).
        dy: Vertical expansion (positive = outward).

    Returns:
        Expanded rectangle.
    """
    x, y, w, h = rect
    return (x - dx, y - dy, w + 2 * dx, h + 2 * dy)


def center_of_rect(rect: tuple[float, float, float, float]) -> tuple[float, float]:
    """Get the center point of a rectangle.

    Args:
        rect: Rectangle as (x, y, width, height).

    Returns:
        Tuple of (center_x, center_y).
    """
    x, y, w, h = rect
    return (x + w / 2, y + h / 2)


def point_in_polygon(x: float, y: float, polygon: list[tuple[float, float]]) -> bool:
    """Ray-casting point-in-polygon test.

    Args:
        x: Point X coordinate.
        y: Point Y coordinate.
        polygon: List of (x, y) vertices in order.

    Returns:
        True if the point is inside the polygon.
    """
    n = len(polygon)
    inside = False
    j = n - 1
    for i in range(n):
        xi, yi = polygon[i]
        xj, yj = polygon[j]
        if (yi > y) != (yj > y) and x < (xj - xi) * (y - yi) / (yj - yi) + xi:
            inside = not inside
        j = i
    return inside


def bounding_box_of_points(points: list[tuple[float, float]]) -> tuple[float, float, float, float] | None:
    """Compute the axis-aligned bounding box of a set of points.

    Args:
        points: List of (x, y) coordinate tuples.

    Returns:
        Rect (x, y, width, height), or None if the list is empty.
    """
    if not points:
        return None
    xs = [p[0] for p in points]
    ys = [p[1] for p in points]
    min_x, max_x = min(xs), max(xs)
    min_y, max_y = min(ys), max(ys)
    return (min_x, min_y, max_x - min_x, max_y - min_y)
