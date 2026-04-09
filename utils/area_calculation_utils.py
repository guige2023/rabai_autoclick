"""Utilities for area and region calculations in UI automation.

This module provides geometric utilities for computing areas, perimeters,
and intersections of UI regions for automation purposes.
"""

from __future__ import annotations

import math
from typing import Sequence, NamedTuple


class Point(NamedTuple):
    """2D point representation."""
    x: float
    y: float


class Rect(NamedTuple):
    """Axis-aligned rectangle representation.

    Attributes:
        x: Left edge X coordinate.
        y: Top edge Y coordinate.
        width: Width of rectangle (positive value).
        height: Height of rectangle (positive value).
    """
    x: float
    y: float
    width: float
    height: float

    @property
    def left(self) -> float:
        """Left edge X coordinate."""
        return self.x

    @property
    def top(self) -> float:
        """Top edge Y coordinate."""
        return self.y

    @property
    def right(self) -> float:
        """Right edge X coordinate."""
        return self.x + self.width

    @property
    def bottom(self) -> float:
        """Bottom edge Y coordinate."""
        return self.y + self.height

    @property
    def center_x(self) -> float:
        """Center X coordinate."""
        return self.x + self.width / 2

    @property
    def center_y(self) -> float:
        """Center Y coordinate."""
        return self.y + self.height / 2

    @property
    def center(self) -> Point:
        """Center point of rectangle."""
        return Point(self.center_x, self.center_y)

    @property
    def area(self) -> float:
        """Geometric area of rectangle."""
        return self.width * self.height

    @property
    def perimeter(self) -> float:
        """Perimeter of rectangle."""
        return 2 * (self.width + self.height)

    @property
    def aspect_ratio(self) -> float:
        """Width to height ratio."""
        if self.height == 0:
            return float('inf')
        return self.width / self.height

    def contains_point(self, px: float, py: float) -> bool:
        """Check if a point is inside this rectangle.

        Args:
            px: X coordinate of point.
            py: Y coordinate of point.

        Returns:
            True if point is inside or on boundary, False otherwise.
        """
        return self.left <= px <= self.right and self.top <= py <= self.bottom

    def contains(self, other: Rect) -> bool:
        """Check if another rectangle is fully contained within this one.

        Args:
            other: Rectangle to test.

        Returns:
            True if other is fully inside this rectangle.
        """
        return (self.left <= other.left and self.right >= other.right and
                self.top <= other.top and self.bottom >= other.bottom)

    def intersects(self, other: Rect) -> bool:
        """Check if this rectangle intersects another rectangle.

        Args:
            other: Rectangle to test.

        Returns:
            True if rectangles overlap at any point.
        """
        return (self.left < other.right and self.right > other.left and
                self.top < other.bottom and self.bottom > other.top)

    def intersection(self, other: Rect) -> Rect | None:
        """Compute the intersection of two rectangles.

        Args:
            other: Rectangle to intersect with.

        Returns:
            The intersection rectangle, or None if no overlap.
        """
        if not self.intersects(other):
            return None

        inter_left = max(self.left, other.left)
        inter_top = max(self.top, other.top)
        inter_right = min(self.right, other.right)
        inter_bottom = min(self.bottom, other.bottom)

        return Rect(
            x=inter_left,
            y=inter_top,
            width=inter_right - inter_left,
            height=inter_bottom - inter_top
        )

    def union(self, other: Rect) -> Rect:
        """Compute the minimal bounding rectangle containing both rectangles.

        Args:
            other: Rectangle to union with.

        Returns:
            The minimal bounding rectangle.
        """
        union_left = min(self.left, other.left)
        union_top = min(self.top, other.top)
        union_right = max(self.right, other.right)
        union_bottom = max(self.bottom, other.bottom)

        return Rect(
            x=union_left,
            y=union_top,
            width=union_right - union_left,
            height=union_bottom - union_top
        )

    def distance_to_point(self, px: float, py: float) -> float:
        """Compute Euclidean distance from rectangle center to a point.

        Args:
            px: X coordinate of point.
            py: Y coordinate of point.

        Returns:
            Euclidean distance to center.
        """
        cx, cy = self.center
        return math.sqrt((cx - px) ** 2 + (cy - py) ** 2)

    def distance_to_rect(self, other: Rect) -> float:
        """Compute minimum Euclidean distance between two rectangles.

        Args:
            other: Other rectangle.

        Returns:
            Minimum distance between any point on each rectangle.
        """
        dx = max(self.left - other.right, other.left - self.right, 0)
        dy = max(self.top - other.bottom, other.top - self.bottom, 0)
        return math.sqrt(dx * dx + dy * dy)

    def expand(self, dx: float, dy: float) -> Rect:
        """Expand rectangle by given amounts in each direction.

        Args:
            dx: Amount to expand left and right.
            dy: Amount to expand top and bottom.

        Returns:
            Expanded rectangle.
        """
        return Rect(
            x=self.x - dx,
            y=self.y - dy,
            width=self.width + 2 * dx,
            height=self.height + 2 * dy
        )

    def shrink(self, dx: float, dy: float) -> Rect:
        """Shrink rectangle by given amounts in each direction.

        Args:
            dx: Amount to shrink left and right.
            dy: Amount to shrink top and bottom.

        Returns:
            Shrunk rectangle.
        """
        return self.expand(-dx, -dy)

    def scale(self, factor: float) -> Rect:
        """Scale rectangle uniformly from its center.

        Args:
            factor: Scaling factor (>0).

        Returns:
            Scaled rectangle.
        """
        cx, cy = self.center
        new_width = self.width * factor
        new_height = self.height * factor
        return Rect(
            x=cx - new_width / 2,
            y=cy - new_height / 2,
            width=new_width,
            height=new_height
        )


class Polygon:
    """Polygon defined by a sequence of points.

    Attributes:
        vertices: List of vertices in order (clockwise or counter-clockwise).
    """

    def __init__(self, vertices: Sequence[Point | tuple[float, float]]) -> None:
        """Initialize polygon with given vertices.

        Args:
            vertices: Sequence of points defining polygon boundary.
        """
        self.vertices = [
            Point(p[0], p[1]) if isinstance(p, tuple) else p
            for p in vertices
        ]

    @property
    def area(self) -> float:
        """Compute polygon area using shoelace formula.

        Returns:
            Signed area (positive for CCW, negative for CW).
        """
        n = len(self.vertices)
        if n < 3:
            return 0.0

        total: float = 0
        for i in range(n):
            j = (i + 1) % n
            total += self.vertices[i].x * self.vertices[j].y
            total -= self.vertices[j].x * self.vertices[i].y

        return abs(total) / 2

    @property
    def perimeter(self) -> float:
        """Compute polygon perimeter.

        Returns:
            Sum of edge lengths.
        """
        n = len(self.vertices)
        if n < 2:
            return 0.0

        total: float = 0
        for i in range(n):
            j = (i + 1) % n
            p1 = self.vertices[i]
            p2 = self.vertices[j]
            total += math.sqrt((p2.x - p1.x) ** 2 + (p2.y - p1.y) ** 2)

        return total

    def contains_point(self, px: float, py: float) -> bool:
        """Check if a point is inside the polygon using ray casting.

        Args:
            px: X coordinate of point.
            py: Y coordinate of point.

        Returns:
            True if point is inside polygon.
        """
        n = len(self.vertices)
        inside = False

        j = n - 1
        for i in range(n):
            xi, yi = self.vertices[i].x, self.vertices[i].y
            xj, yj = self.vertices[j].x, self.vertices[j].y

            if ((yi > py) != (yj > py) and
                    px < (xj - xi) * (py - yi) / (yj - yi) + xi):
                inside = not inside

            j = i

        return inside

    def bounding_rect(self) -> Rect:
        """Compute minimal axis-aligned bounding rectangle.

        Returns:
            Bounding rectangle.
        """
        if not self.vertices:
            return Rect(0, 0, 0, 0)

        xs = [v.x for v in self.vertices]
        ys = [v.y for v in self.vertices]

        min_x, max_x = min(xs), max(xs)
        min_y, max_y = min(ys), max(ys)

        return Rect(
            x=min_x,
            y=min_y,
            width=max_x - min_x,
            height=max_y - min_y
        )


def iou(rect1: Rect, rect2: Rect) -> float:
    """Compute Intersection over Union (IoU) of two rectangles.

    IoU measures overlap between two bounding boxes, commonly used in
    object detection and UI element localization.

    Args:
        rect1: First rectangle.
        rect2: Second rectangle.

    Returns:
        IoU score between 0.0 (no overlap) and 1.0 (identical boxes).
    """
    intersection = rect1.intersection(rect2)
    if intersection is None:
        return 0.0

    intersection_area = intersection.area
    union_area = rect1.area + rect2.area - intersection_area

    if union_area == 0:
        return 0.0

    return intersection_area / union_area


def coverage_ratio(inner: Rect, outer: Rect) -> float:
    """Compute how much of inner rectangle is covered by outer rectangle.

    Args:
        inner: Inner rectangle to measure coverage of.
        outer: Outer rectangle to measure coverage within.

    Returns:
        Ratio between 0.0 (no coverage) and 1.0 (fully covered).
    """
    if not outer.intersects(inner):
        return 0.0

    intersection = outer.intersection(inner)
    if intersection is None:
        return 0.0

    return intersection.area / inner.area


def spatial_hash(rect: Rect, cell_size: float = 50.0) -> tuple[int, int]:
    """Compute spatial hash grid cell for a rectangle.

    Args:
        rect: Rectangle to hash.
        cell_size: Size of grid cell in pixels.

    Returns:
        Tuple of (grid_x, grid_y) cell coordinates.
    """
    return (int(rect.center_x / cell_size), int(rect.center_y / cell_size))
