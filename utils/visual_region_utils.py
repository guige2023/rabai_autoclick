"""Visual Region and ROI (Region of Interest) Utilities.

Defines, manages, and operates on visual regions within screenshots.
Supports region operations, masking, and multi-region workflows.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Optional


class RegionRelation(Enum):
    """Spatial relationships between regions."""

    CONTAINS = auto()
    CONTAINED_BY = auto()
    OVERLAPS = auto()
    DISJOINT = auto()
    ADJACENT = auto()


@dataclass
class VisualRegion:
    """Represents a rectangular region in visual space.

    Attributes:
        x: Left edge X coordinate.
        y: Top edge Y coordinate.
        width: Region width in pixels.
        height: Region height in pixels.
        label: Optional label for the region.
    """

    x: float
    y: float
    width: float
    height: float
    label: str = ""

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
    def center_x(self) -> float:
        """Center X coordinate."""
        return self.x + self.width / 2

    @property
    def center_y(self) -> float:
        """Center Y coordinate."""
        return self.y + self.height / 2

    @property
    def center(self) -> tuple[float, float]:
        """Center coordinates."""
        return (self.center_x, self.center_y)

    @property
    def area(self) -> float:
        """Region area in pixels."""
        return self.width * self.height

    @property
    def bounds(self) -> tuple[float, float, float, float]:
        """Get as (x, y, width, height) tuple."""
        return (self.x, self.y, self.width, self.height)

    @property
    def corners(self) -> tuple[tuple[float, float], tuple[float, float], tuple[float, float], tuple[float, float]]:
        """Get corner coordinates (top-left, top-right, bottom-right, bottom-left)."""
        return (
            (self.x, self.y),
            (self.right, self.y),
            (self.right, self.bottom),
            (self.x, self.bottom),
        )

    def contains_point(self, px: float, py: float) -> bool:
        """Check if a point is inside this region.

        Args:
            px: Point X coordinate.
            py: Point Y coordinate.

        Returns:
            True if point is inside the region.
        """
        return self.left <= px <= self.right and self.top <= py <= self.bottom

    def contains_region(self, other: "VisualRegion") -> bool:
        """Check if this region fully contains another.

        Args:
            other: Region to check.

        Returns:
            True if this region contains the other.
        """
        return (
            self.left <= other.left
            and self.right >= other.right
            and self.top <= other.top
            and self.bottom >= other.bottom
        )

    def intersects(self, other: "VisualRegion") -> bool:
        """Check if this region intersects another.

        Args:
            other: Region to check.

        Returns:
            True if regions overlap.
        """
        return not (
            self.right < other.left
            or self.left > other.right
            or self.bottom < other.top
            or self.top > other.bottom
        )

    def intersection(self, other: "VisualRegion") -> Optional["VisualRegion"]:
        """Get the intersection of two regions.

        Args:
            other: Region to intersect with.

        Returns:
            Intersection region or None if no overlap.
        """
        if not self.intersects(other):
            return None

        left = max(self.left, other.left)
        top = max(self.top, other.top)
        right = min(self.right, other.right)
        bottom = min(self.bottom, other.bottom)

        return VisualRegion(
            x=left,
            y=top,
            width=right - left,
            height=bottom - top,
        )

    def union(self, other: "VisualRegion") -> "VisualRegion":
        """Get the bounding box that contains both regions.

        Args:
            other: Region to unite with.

        Returns:
            Minimal region containing both.
        """
        left = min(self.left, other.left)
        top = min(self.top, other.top)
        right = max(self.right, other.right)
        bottom = max(self.bottom, other.bottom)

        return VisualRegion(
            x=left,
            y=top,
            width=right - left,
            height=bottom - top,
        )

    def distance_to(self, other: "VisualRegion") -> float:
        """Calculate minimum distance to another region.

        Args:
            other: Target region.

        Returns:
            Minimum distance between regions.
        """
        if self.intersects(other):
            return 0.0

        dx = max(0, max(other.left - self.right, self.left - other.right))
        dy = max(0, max(other.top - self.bottom, self.top - other.bottom))
        return (dx * dx + dy * dy) ** 0.5

    def expand(self, padding: float) -> "VisualRegion":
        """Expand region by padding amount.

        Args:
            padding: Pixels to add on each side.

        Returns:
            Expanded region.
        """
        return VisualRegion(
            x=self.x - padding,
            y=self.y - padding,
            width=self.width + 2 * padding,
            height=self.height + 2 * padding,
            label=self.label,
        )

    def shrink(self, padding: float) -> "VisualRegion":
        """Shrink region by padding amount.

        Args:
            padding: Pixels to remove from each side.

        Returns:
            Shrunk region.
        """
        return self.expand(-padding)

    def subdivide(
        self,
        rows: int,
        cols: int,
    ) -> list["VisualRegion"]:
        """Subdivide region into grid of smaller regions.

        Args:
            rows: Number of rows.
            cols: Number of columns.

        Returns:
            List of sub-regions.
        """
        cell_width = self.width / cols
        cell_height = self.height / rows
        regions = []

        for row in range(rows):
            for col in range(cols):
                regions.append(VisualRegion(
                    x=self.x + col * cell_width,
                    y=self.y + row * cell_height,
                    width=cell_width,
                    height=cell_height,
                    label=f"{self.label}_r{row}_c{col}" if self.label else "",
                ))

        return regions

    def normalize(self, image_width: float, image_height: float) -> "VisualRegion":
        """Normalize region coordinates to 0-1 range.

        Args:
            image_width: Image width for normalization.
            image_height: Image height for normalization.

        Returns:
            Normalized region.
        """
        return VisualRegion(
            x=self.x / image_width,
            y=self.y / image_height,
            width=self.width / image_width,
            height=self.height / image_height,
            label=self.label,
        )

    def denormalize(self, image_width: float, image_height: float) -> "VisualRegion":
        """Convert normalized coordinates back to pixel coordinates.

        Args:
            image_width: Image width for denormalization.
            image_height: Image height for denormalization.

        Returns:
            Pixel-space region.
        """
        return VisualRegion(
            x=self.x * image_width,
            y=self.y * image_height,
            width=self.width * image_width,
            height=self.height * image_height,
            label=self.label,
        )


class RegionManager:
    """Manages collections of visual regions.

    Example:
        manager = RegionManager()
        manager.add_region(VisualRegion(100, 100, 200, 200, "header"))
        manager.add_region(VisualRegion(100, 300, 200, 200, "content"))
        overlaps = manager.find_overlaps()
    """

    def __init__(self):
        """Initialize the region manager."""
        self._regions: list[VisualRegion] = []

    def add_region(self, region: VisualRegion) -> None:
        """Add a region to the manager.

        Args:
            region: VisualRegion to add.
        """
        self._regions.append(region)

    def remove_region(self, label: str) -> bool:
        """Remove a region by label.

        Args:
            label: Label of region to remove.

        Returns:
            True if region was found and removed.
        """
        for i, region in enumerate(self._regions):
            if region.label == label:
                self._regions.pop(i)
                return True
        return False

    def get_region(self, label: str) -> Optional[VisualRegion]:
        """Get a region by label.

        Args:
            label: Region label.

        Returns:
            Region or None if not found.
        """
        for region in self._regions:
            if region.label == label:
                return region
        return None

    def find_overlaps(self) -> list[tuple[VisualRegion, VisualRegion]]:
        """Find all overlapping region pairs.

        Returns:
            List of (region1, region2) tuples that overlap.
        """
        overlaps = []
        for i, r1 in enumerate(self._regions):
            for r2 in self._regions[i + 1:]:
                if r1.intersects(r2):
                    overlaps.append((r1, r2))
        return overlaps

    def find_containing(self, region: VisualRegion) -> list[VisualRegion]:
        """Find regions that contain the given region.

        Args:
            region: Region to check.

        Returns:
            List of containing regions.
        """
        return [r for r in self._regions if r.contains_region(region)]

    def find_within_distance(
        self,
        region: VisualRegion,
        max_distance: float,
    ) -> list[tuple[VisualRegion, float]]:
        """Find regions within a certain distance.

        Args:
            region: Reference region.
            max_distance: Maximum distance.

        Returns:
            List of (region, distance) tuples.
        """
        results = []
        for r in self._regions:
            if r != region:
                dist = r.distance_to(region)
                if dist <= max_distance:
                    results.append((r, dist))
        return sorted(results, key=lambda x: x[1])

    def get_roi(self) -> Optional[VisualRegion]:
        """Get the region of interest that contains all regions.

        Returns:
            Minimal region containing all managed regions.
        """
        if not self._regions:
            return None
        result = self._regions[0]
        for region in self._regions[1:]:
            result = result.union(region)
        return result


class RegionMask:
    """Creates masks for visual regions.

    Example:
        mask = RegionMask(image_width=1920, image_height=1080)
        mask.add_excluded(region)
        binary = mask.get_binary_mask()
    """

    def __init__(self, image_width: int, image_height: int):
        """Initialize the mask.

        Args:
            image_width: Image width in pixels.
            image_height: Image height in pixels.
        """
        self.image_width = image_width
        self.image_height = image_height
        self._included: list[VisualRegion] = []
        self._excluded: list[VisualRegion] = []

    def add_included(self, region: VisualRegion) -> None:
        """Add an included region.

        Args:
            region: Region to include.
        """
        self._included.append(region)

    def add_excluded(self, region: VisualRegion) -> None:
        """Add an excluded region.

        Args:
            region: Region to exclude.
        """
        self._excluded.append(region)

    def is_point_included(self, px: float, py: float) -> bool:
        """Check if a point is included in the mask.

        Args:
            px: Point X coordinate.
            py: Point Y coordinate.

        Returns:
            True if point is included.
        """
        # Check exclusions first
        for excluded in self._excluded:
            if excluded.contains_point(px, py):
                return False

        # If no inclusions defined, point is included
        if not self._included:
            return True

        # Check inclusions
        for included in self._included:
            if included.contains_point(px, py):
                return True

        return False

    def get_binary_mask(self) -> list[list[int]]:
        """Get binary mask as 2D array.

        Returns:
            2D list where 1 = included, 0 = excluded.
        """
        mask = []
        for y in range(self.image_height):
            row = []
            for x in range(self.image_width):
                row.append(1 if self.is_point_included(x, y) else 0)
            mask.append(row)
        return mask

    def get_included_area(self) -> float:
        """Calculate total included pixel area.

        Returns:
            Number of included pixels.
        """
        if not self._included:
            total = self.image_width * self.image_height
            for excluded in self._excluded:
                total -= excluded.area
            return max(0, total)

        total = 0.0
        for region in self._included:
            total += region.area
            for excluded in self._excluded:
                intersection = region.intersection(excluded)
                if intersection:
                    total -= intersection.area
        return max(0, total)
