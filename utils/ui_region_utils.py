"""UI Region utilities for rectangular region operations.

This module provides utilities for working with UI regions (rectangles),
including intersection, containment, merging, and splitting operations.
"""

from typing import List, Tuple, Optional, Union
from dataclasses import dataclass


@dataclass
class Region:
    """Represents a rectangular region with x, y, width, height."""
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
    def center(self) -> Tuple[float, float]:
        return (self.x + self.width / 2, self.y + self.height / 2)

    @property
    def area(self) -> float:
        return self.width * self.height

    def contains_point(self, px: float, py: float) -> bool:
        """Check if a point is within this region."""
        return self.left <= px < self.right and self.top <= py < self.bottom

    def contains_region(self, other: 'Region') -> bool:
        """Check if another region is fully contained within this one."""
        return (self.contains_point(other.left, other.top) and
                self.contains_point(other.right - 1, other.bottom - 1))

    def intersects(self, other: 'Region') -> bool:
        """Check if this region intersects with another region."""
        return not (self.right <= other.left or
                    self.left >= other.right or
                    self.bottom <= other.top or
                    self.top >= other.bottom)

    def intersection(self, other: 'Region') -> Optional['Region']:
        """Get the intersection region with another region."""
        if not self.intersects(other):
            return None

        left = max(self.left, other.left)
        top = max(self.top, other.top)
        right = min(self.right, other.right)
        bottom = min(self.bottom, other.bottom)

        return Region(left, top, right - left, bottom - top)

    def union(self, other: 'Region') -> 'Region':
        """Get the minimal bounding region containing both regions."""
        left = min(self.left, other.left)
        top = min(self.top, other.top)
        right = max(self.right, other.right)
        bottom = max(self.bottom, other.bottom)

        return Region(left, top, right - left, bottom - top)

    def merge_if_adjacent(self, other: 'Region', tolerance: float = 1.0) -> Optional['Region']:
        """Merge with another region if they are adjacent or overlapping."""
        if self.intersects(other) or self._is_adjacent_to(other, tolerance):
            return self.union(other)
        return None

    def _is_adjacent_to(self, other: 'Region', tolerance: float) -> bool:
        """Check if regions are adjacent within tolerance."""
        # Horizontal adjacency
        if (abs(self.right - other.left) <= tolerance or
            abs(self.left - other.right) <= tolerance):
            vertical_overlap = (self.top < other.bottom and
                              self.bottom > other.top)
            if vertical_overlap:
                return True

        # Vertical adjacency
        if (abs(self.bottom - other.top) <= tolerance or
            abs(self.top - other.bottom) <= tolerance):
            horizontal_overlap = (self.left < other.right and
                                self.right > other.left)
            if horizontal_overlap:
                return True

        return False

    def split_horizontal(self, ratio: float) -> Tuple['Region', 'Region']:
        """Split region horizontally at a ratio point."""
        split_x = self.x + self.width * ratio
        left_region = Region(self.x, self.y, split_x - self.x, self.height)
        right_region = Region(split_x, self.y, self.right - split_x, self.height)
        return left_region, right_region

    def split_vertical(self, ratio: float) -> Tuple['Region', 'Region']:
        """Split region vertically at a ratio point."""
        split_y = self.y + self.height * ratio
        top_region = Region(self.x, self.y, self.width, split_y - self.y)
        bottom_region = Region(self.x, split_y, self.width, self.bottom - split_y)
        return top_region, bottom_region

    def expand(self, dx: float, dy: float) -> 'Region':
        """Expand region by dx and dy on all sides."""
        return Region(
            self.x - dx,
            self.y - dy,
            self.width + 2 * dx,
            self.height + 2 * dy
        )

    def shrink(self, dx: float, dy: float) -> 'Region':
        """Shrink region by dx and dy on all sides."""
        return self.expand(-dx, -dy)

    def grid(self, rows: int, cols: int) -> List['Region']:
        """Divide region into a grid of smaller regions."""
        cell_width = self.width / cols
        cell_height = self.height / rows

        regions = []
        for row in range(rows):
            for col in range(cols):
                regions.append(Region(
                    self.x + col * cell_width,
                    self.y + row * cell_height,
                    cell_width,
                    cell_height
                ))
        return regions

    def distance_to_point(self, px: float, py: float) -> float:
        """Calculate minimum distance from a point to this region."""
        dx = max(self.left - px, 0, px - self.right)
        dy = max(self.top - py, 0, py - self.bottom)
        return (dx ** 2 + dy ** 2) ** 0.5

    def normalize(self) -> 'Region':
        """Ensure width and height are positive."""
        x = self.x if self.width >= 0 else self.x + self.width
        y = self.y if self.height >= 0 else self.y + self.height
        width = abs(self.width)
        height = abs(self.height)
        return Region(x, y, width, height)

    def aspect_ratio(self) -> float:
        """Return width / height ratio."""
        if self.height == 0:
            return float('inf')
        return self.width / self.height

    def is_square(self, tolerance: float = 0.01) -> bool:
        """Check if region is approximately square."""
        return abs(self.aspect_ratio() - 1.0) < tolerance

    def to_tuple(self) -> Tuple[float, float, float, float]:
        """Convert to tuple format (x, y, width, height)."""
        return (self.x, self.y, self.width, self.height)

    def to_bbox(self) -> Tuple[float, float, float, float]:
        """Convert to bounding box format (left, top, right, bottom)."""
        return (self.left, self.top, self.right, self.bottom)

    @classmethod
    def from_bbox(cls, left: float, top: float, right: float, bottom: float) -> 'Region':
        """Create region from bounding box coordinates."""
        return cls(left, top, right - left, bottom - top)

    @classmethod
    def from_points(cls, p1: Tuple[float, float], p2: Tuple[float, float]) -> 'Region':
        """Create region from two corner points."""
        left = min(p1[0], p2[0])
        top = min(p1[1], p2[1])
        right = max(p1[0], p2[0])
        bottom = max(p1[1], p2[1])
        return cls.from_bbox(left, top, right, bottom)


def merge_regions(regions: List[Region], tolerance: float = 1.0) -> List[Region]:
    """Merge overlapping or adjacent regions into minimal bounding regions."""
    if not regions:
        return []

    result = list(regions)
    merged = True

    while merged:
        merged = False
        new_result = []

        while result:
            current = result.pop(0)
            found_merge = False

            for i, other in enumerate(result):
                merged_region = current.merge_if_adjacent(other, tolerance)
                if merged_region:
                    result[i] = merged_region
                    found_merge = True
                    merged = True
                    break

            if not found_merge:
                new_result.append(current)

            result = result[1:] + [current] if found_merge else new_result

    return result


def filter_regions_by_area(regions: List[Region],
                            min_area: Optional[float] = None,
                            max_area: Optional[float] = None) -> List[Region]:
    """Filter regions by area constraints."""
    filtered = []
    for region in regions:
        if min_area is not None and region.area < min_area:
            continue
        if max_area is not None and region.area > max_area:
            continue
        filtered.append(region)
    return filtered


def sort_regions_by_position(regions: List[Region],
                             order: str = 'left_to_right') -> List[Region]:
    """Sort regions by position in specified order."""
    if order == 'left_to_right':
        return sorted(regions, key=lambda r: (r.y, r.x))
    elif order == 'top_to_bottom':
        return sorted(regions, key=lambda r: (r.x, r.y))
    elif order == 'area_descending':
        return sorted(regions, key=lambda r: -r.area)
    elif order == 'area_ascending':
        return sorted(regions, key=lambda r: r.area)
    else:
        raise ValueError(f"Unknown sort order: {order}")


def remove_contained_regions(regions: List[Region]) -> List[Region]:
    """Remove regions that are fully contained within other regions."""
    result = []
    for i, region in enumerate(regions):
        is_contained = False
        for j, other in enumerate(regions):
            if i != j and other.contains_region(region):
                is_contained = True
                break
        if not is_contained:
            result.append(region)
    return result
