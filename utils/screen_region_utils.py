"""Screen region detection and manipulation utilities.

Provides utilities for defining, comparing, and manipulating screen regions
(rectangles). Useful for targeting clicks to specific UI areas, storing
region presets, and performing region-based operations like capture or
color sampling.

Example:
    >>> from utils.screen_region_utils import Region, regions_from_grid
    >>> region = Region(100, 100, 300, 200)
    >>> print(region.center)
    (200, 150)
    >>> region.contains_point(200, 150)
    True
    >>> regions = regions_from_grid(4, 3)
    >>> for r in regions:
    ...     print(r)
"""
from __future__ import annotations

import math
from typing import Iterator

__all__ = [
    "Region",
    "regions_from_grid",
    "region_overlaps",
    "region_union",
    "region_intersection",
    "normalize_region",
]


class Region:
    """A rectangular screen region defined by top-left and bottom-right corners.

    Attributes:
        x: Left edge X coordinate.
        y: Top edge Y coordinate.
        width: Width of the region.
        height: Height of the region.

    Example:
        >>> r = Region(10, 20, 100, 200)
        >>> r.width
        90
        >>> r.height
        180
        >>> r.center
        (55, 110)
        >>> r.contains_point(50, 100)
        True
    """

    def __init__(
        self,
        x: int | float,
        y: int | float,
        x2: int | float,
        y2: int | float,
    ) -> None:
        self.x = min(x, x2)
        self.y = min(y, y2)
        self.x2 = max(x, x2)
        self.y2 = max(y, y2)

    @property
    def width(self) -> int | float:
        """Return the width of the region."""
        return self.x2 - self.x

    @property
    def height(self) -> int | float:
        """Return the height of the region."""
        return self.y2 - self.y

    @property
    def center(self) -> tuple[int | float, int | float]:
        """Return the center point as (x, y)."""
        return ((self.x + self.x2) / 2, (self.y + self.y2) / 2)

    @property
    def area(self) -> int | float:
        """Return the area of the region."""
        return self.width * self.height

    @property
    def top_left(self) -> tuple[int | float, int | float]:
        """Return the top-left corner as (x, y)."""
        return (self.x, self.y)

    @property
    def top_right(self) -> tuple[int | float, int | float]:
        """Return the top-right corner as (x, y)."""
        return (self.x2, self.y)

    @property
    def bottom_left(self) -> tuple[int | float, int | float]:
        """Return the bottom-left corner as (x, y)."""
        return (self.x, self.y2)

    @property
    def bottom_right(self) -> tuple[int | float, int | float]:
        """Return the bottom-right corner as (x, y)."""
        return (self.x2, self.y2)

    @property
    def corners(self) -> list[tuple[int | float, int | float]]:
        """Return all four corners as (x, y) tuples."""
        return [
            self.top_left,
            self.top_right,
            self.bottom_right,
            self.bottom_left,
        ]

    def contains_point(self, px: int | float, py: int | float) -> bool:
        """Check if a point is inside the region.

        Args:
            px: X coordinate of the point.
            py: Y coordinate of the point.

        Returns:
            True if the point is inside or on the boundary.
        """
        return self.x <= px <= self.x2 and self.y <= py <= self.y2

    def contains_region(self, other: Region) -> bool:
        """Check if another region is fully contained within this region.

        Args:
            other: The region to check.

        Returns:
            True if other is fully inside self.
        """
        return (
            self.x <= other.x
            and self.y <= other.y
            and self.x2 >= other.x2
            and self.y2 >= other.y2
        )

    def overlaps(self, other: Region) -> bool:
        """Check if this region overlaps with another region.

        Args:
            other: The region to check.

        Returns:
            True if the regions share any area.
        """
        return (
            self.x < other.x2
            and self.x2 > other.x
            and self.y < other.y2
            and self.y2 > other.y
        )

    def distance_to(self, px: int | float, py: int | float) -> float:
        """Compute Euclidean distance from a point to the nearest edge.

        Args:
            px: X coordinate of the point.
            py: Y coordinate of the point.

        Returns:
            Distance in pixels. 0 if the point is inside the region.
        """
        import math

        if self.contains_point(px, py):
            return 0.0
        dx = max(self.x - px, 0, px - self.x2)
        dy = max(self.y - py, 0, py - self.y2)
        return math.sqrt(dx * dx + dy * dy)

    def random_point(self) -> tuple[int | float, int | float]:
        """Return a random point within the region.

        Returns:
            A random (x, y) coordinate inside the region.
        """
        import random

        return (
            random.uniform(self.x, self.x2),
            random.uniform(self.y, self.y2),
        )

    def expand(self, dx: int | float, dy: int | float) -> Region:
        """Return a new region expanded by dx/dy in each direction.

        Args:
            dx: Pixels to expand left and right.
            dy: Pixels to expand top and bottom.

        Returns:
            A new expanded Region.
        """
        return Region(self.x - dx, self.y - dy, self.x2 + dx, self.y2 + dy)

    def shrink(self, dx: int | float, dy: int | float) -> Region:
        """Return a new region shrunk by dx/dy in each direction.

        Args:
            dx: Pixels to shrink left and right.
            dy: Pixels to shrink top and bottom.

        Returns:
            A new shrunk Region.
        """
        return self.expand(-dx, -dy)

    def subdivide(
        self, cols: int, rows: int
    ) -> list[Region]:
        """Subdivide the region into a grid of smaller regions.

        Args:
            cols: Number of columns.
            rows: Number of rows.

        Returns:
            A flat list of Region objects covering the grid.
        """
        col_w = self.width / cols
        row_h = self.height / rows
        regions: list[Region] = []
        for row in range(rows):
            for col in range(cols):
                x = self.x + col * col_w
                y = self.y + row * row_h
                regions.append(
                    Region(x, y, x + col_w, y + row_h)
                )
        return regions

    def to_dict(self) -> dict[str, int | float]:
        """Return a dictionary representation."""
        return {
            "x": self.x,
            "y": self.y,
            "x2": self.x2,
            "y2": self.y2,
            "width": self.width,
            "height": self.height,
        }

    @classmethod
    def from_dict(cls, d: dict[str, int | float]) -> Region:
        """Create a Region from a dictionary."""
        return cls(d["x"], d["y"], d["x2"], d["y2"])

    def __repr__(self) -> str:
        return f"Region({self.x}, {self.y}, {self.x2}, {self.y2})"

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Region):
            return NotImplemented
        return (
            math.isclose(self.x, other.x)
            and math.isclose(self.y, other.y)
            and math.isclose(self.x2, other.x2)
            and math.isclose(self.y2, other.y2)
        )


def normalize_region(
    x: int | float,
    y: int | float,
    x2: int | float,
    y2: int | float,
) -> tuple[int | float, int | float, int | float, int | float]:
    """Normalize a region so that x < x2 and y < y2.

    Args:
        x: First X coordinate.
        y: First Y coordinate.
        x2: Second X coordinate.
        y2: Second Y coordinate.

    Returns:
        Normalized (x, y, x2, y2) tuple.
    """
    return (min(x, x2), min(y, y2), max(x, x2), max(y, y2))


def region_overlaps(a: Region, b: Region) -> bool:
    """Return True if two regions overlap."""
    return a.overlaps(b)


def region_union(a: Region, b: Region) -> Region:
    """Return the minimal region containing both regions.

    Args:
        a: First region.
        b: Second region.

    Returns:
        A Region that encompasses both regions.
    """
    return Region(
        min(a.x, b.x),
        min(a.y, b.y),
        max(a.x2, b.x2),
        max(a.y2, b.y2),
    )


def region_intersection(a: Region, b: Region) -> Region | None:
    """Return the overlapping region of two regions, or None if no overlap.

    Args:
        a: First region.
        b: Second region.

    Returns:
        The overlapping Region, or None if no overlap.
    """
    x = max(a.x, b.x)
    y = max(a.y, b.y)
    x2 = min(a.x2, b.x2)
    y2 = min(a.y2, b.y2)
    if x < x2 and y < y2:
        return Region(x, y, x2, y2)
    return None


def regions_from_grid(
    cols: int,
    rows: int,
    screen_width: int = 1920,
    screen_height: int = 1080,
) -> list[Region]:
    """Create a list of regions covering the screen in a grid.

    Args:
        cols: Number of columns.
        rows: Number of rows.
        screen_width: Total screen width.
        screen_height: Total screen height.

    Returns:
        List of Region objects.
    """
    cell_w = screen_width / cols
    cell_h = screen_height / rows
    regions: list[Region] = []
    for row in range(rows):
        for col in range(cols):
            x = col * cell_w
            y = row * cell_h
            regions.append(Region(x, y, x + cell_w, y + cell_h))
    return regions
