"""
Screen region and area utilities for UI automation.

This module provides utilities for defining, manipulating, and
querying screen regions and areas.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import List, Tuple, Optional, Callable, Union


@dataclass
class Point:
    """2D point."""
    x: float
    y: float

    def distance_to(self, other: Point) -> float:
        """Calculate distance to another point."""
        dx = self.x - other.x
        dy = self.y - other.y
        return math.sqrt(dx*dx + dy*dy)

    def __add__(self, other: Point) -> Point:
        return Point(self.x + other.x, self.y + other.y)

    def __sub__(self, other: Point) -> Point:
        return Point(self.x - other.x, self.y - other.y)

    def __mul__(self, scalar: float) -> Point:
        return Point(self.x * scalar, self.y * scalar)


@dataclass
class Size:
    """2D size."""
    width: float
    height: float

    @property
    def area(self) -> float:
        return self.width * self.height


@dataclass
class Rect:
    """
    Rectangle defined by position and size.

    Attributes:
        x: Left edge X coordinate.
        y: Top edge Y coordinate.
        width: Rectangle width.
        height: Rectangle height.
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
    def right(self) -> float:
        """Right edge X coordinate."""
        return self.x + self.width

    @property
    def top(self) -> float:
        """Top edge Y coordinate."""
        return self.y

    @property
    def bottom(self) -> float:
        """Bottom edge Y coordinate."""
        return self.y + self.height

    @property
    def center(self) -> Point:
        """Center point of rectangle."""
        return Point(self.x + self.width / 2, self.y + self.height / 2)

    @property
    def center_x(self) -> float:
        """Center X coordinate."""
        return self.x + self.width / 2

    @property
    def center_y(self) -> float:
        """Center Y coordinate."""
        return self.y + self.height / 2

    @property
    def area(self) -> float:
        """Area of rectangle."""
        return self.width * self.height

    @property
    def perimeter(self) -> float:
        """Perimeter of rectangle."""
        return 2 * (self.width + self.height)

    def contains_point(self, point: Point) -> bool:
        """Check if point is inside rectangle."""
        return (self.left <= point.x < self.right and
                self.top <= point.y < self.bottom)

    def contains_point_xy(self, x: float, y: float) -> bool:
        """Check if (x, y) coordinates are inside rectangle."""
        return self.left <= x < self.right and self.top <= y < self.bottom

    def intersects(self, other: Rect) -> bool:
        """Check if rectangle intersects with another."""
        return not (self.right <= other.left or
                    self.left >= other.right or
                    self.bottom <= other.top or
                    self.top >= other.bottom)

    def intersection(self, other: Rect) -> Optional[Rect]:
        """Get intersection rectangle with another."""
        if not self.intersects(other):
            return None

        left = max(self.left, other.left)
        top = max(self.top, other.top)
        right = min(self.right, other.right)
        bottom = min(self.bottom, other.bottom)

        return Rect(left, top, right - left, bottom - top)

    def union(self, other: Rect) -> Rect:
        """Get minimal rectangle containing both."""
        left = min(self.left, other.left)
        top = min(self.top, other.top)
        right = max(self.right, other.right)
        bottom = max(self.bottom, other.bottom)

        return Rect(left, top, right - left, bottom - top)

    def inset(self, dx: float, dy: float) -> Rect:
        """Inset rectangle by dx/dy on each side."""
        return Rect(
            self.x + dx,
            self.y + dy,
            max(0, self.width - 2 * dx),
            max(0, self.height - 2 * dy),
        )

    def expand(self, dx: float, dy: float) -> Rect:
        """Expand rectangle by dx/dy on each side."""
        return self.inset(-dx, -dy)

    def translate(self, dx: float, dy: float) -> Rect:
        """Move rectangle by offset."""
        return Rect(self.x + dx, self.y + dy, self.width, self.height)

    def scale(self, factor_x: float, factor_y: Optional[float] = None) -> Rect:
        """Scale rectangle from center."""
        if factor_y is None:
            factor_y = factor_x

        cx, cy = self.center_x, self.center_y
        new_width = self.width * factor_x
        new_height = self.height * factor_y

        return Rect(
            cx - new_width / 2,
            cy - new_height / 2,
            new_width,
            new_height,
        )

    def to_tuple(self) -> Tuple[float, float, float, float]:
        """Convert to (x, y, width, height) tuple."""
        return (self.x, self.y, self.width, self.height)

    @classmethod
    def from_points(cls, p1: Point, p2: Point) -> Rect:
        """Create rectangle from two opposite corners."""
        left = min(p1.x, p2.x)
        top = min(p1.y, p2.y)
        right = max(p1.x, p2.x)
        bottom = max(p1.y, p2.y)
        return cls(left, top, right - left, bottom - top)

    @classmethod
    def from_center(cls, center: Point, half_width: float, half_height: float) -> Rect:
        """Create rectangle from center and half-extents."""
        return cls(
            center.x - half_width,
            center.y - half_height,
            half_width * 2,
            half_height * 2,
        )


class ScreenRegion:
    """
    Represents a region of the screen for targeted automation.

    Provides methods for querying and manipulating regions.
    """

    def __init__(self, rect: Rect, name: str = "") -> None:
        self._rect = rect
        self._name = name

    @property
    def rect(self) -> Rect:
        """Get the region rectangle."""
        return self._rect

    @property
    def name(self) -> str:
        """Get region name."""
        return self._name

    def contains(self, x: float, y: float) -> bool:
        """Check if coordinates are within region."""
        return self._rect.contains_point_xy(x, y)

    def snap_to_element(self, element_bounds: Rect) -> Rect:
        """
        Snap region to element bounds.

        Returns a rect that contains the element with padding.
        """
        return self._rect.union(element_bounds)

    def divided_horizontal(self, count: int) -> List[Rect]:
        """Divide region into horizontal slices."""
        slice_height = self._rect.height / count
        return [
            Rect(self._rect.x, self._rect.y + i * slice_height,
                 self._rect.width, slice_height)
            for i in range(count)
        ]

    def divided_vertical(self, count: int) -> List[Rect]:
        """Divide region into vertical slices."""
        slice_width = self._rect.width / count
        return [
            Rect(self._rect.x + i * slice_width, self._rect.y,
                 slice_width, self._rect.height)
            for i in range(count)
        ]

    def divided_grid(self, rows: int, cols: int) -> List[Rect]:
        """Divide region into a grid of smaller regions."""
        cell_width = self._rect.width / cols
        cell_height = self._rect.height / rows

        regions: List[Rect] = []
        for row in range(rows):
            for col in range(cols):
                regions.append(Rect(
                    self._rect.x + col * cell_width,
                    self._rect.y + row * cell_height,
                    cell_width,
                    cell_height,
                ))

        return regions

    def center_quadrants(self) -> List[Tuple[str, Rect]]:
        """Divide region into four quadrants centered on middle."""
        cx = self._rect.center_x
        cy = self._rect.center_y
        half_w = self._rect.width / 2
        half_h = self._rect.height / 2

        return [
            ("top_left", Rect(self._rect.x, self._rect.y, half_w, half_h)),
            ("top_right", Rect(cx, self._rect.y, half_w, half_h)),
            ("bottom_left", Rect(self._rect.x, cy, half_w, half_h)),
            ("bottom_right", Rect(cx, cy, half_w, half_h)),
        ]


class RegionMatcher:
    """
    Matches points or elements against screen regions.

    Useful for determining which region an element belongs to.
    """

    def __init__(self, regions: List[ScreenRegion]) -> None:
        self._regions = regions

    def find_region_for_point(self, x: float, y: float) -> Optional[ScreenRegion]:
        """Find the region containing the point."""
        for region in self._regions:
            if region.contains(x, y):
                return region
        return None

    def find_region_for_rect(self, rect: Rect) -> Optional[ScreenRegion]:
        """Find the smallest region that contains the rect."""
        best_match: Optional[ScreenRegion] = None
        best_area = float('inf')

        for region in self._regions:
            if region.rect.contains_point(Point(rect.x, rect.y)):
                area = region.rect.area
                if area < best_area:
                    best_area = area
                    best_match = region

        return best_match

    def get_regions_intersecting(self, rect: Rect) -> List[ScreenRegion]:
        """Get all regions that intersect with rect."""
        return [
            region for region in self._regions
            if region.rect.intersects(rect)
        ]


def create_grid_regions(
    x: float,
    y: float,
    width: float,
    height: float,
    rows: int,
    cols: int,
) -> List[ScreenRegion]:
    """Create a grid of screen regions."""
    cell_width = width / cols
    cell_height = height / rows

    regions: List[ScreenRegion] = []
    for row in range(rows):
        for col in range(cols):
            rect = Rect(
                x + col * cell_width,
                y + row * cell_height,
                cell_width,
                cell_height,
            )
            regions.append(ScreenRegion(rect, f"r{row}_c{col}"))

    return regions
