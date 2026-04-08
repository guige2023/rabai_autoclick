"""
Capture Region Utilities

Provides utilities for defining and manipulating
capture regions for screenshots and element capture.

Author: Agent3
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass
class CaptureRegion:
    """Represents a rectangular capture region."""
    x: int
    y: int
    width: int
    height: int
    label: str = ""

    def contains_point(self, px: int, py: int) -> bool:
        """Check if a point is within the region."""
        return (self.x <= px <= self.x + self.width and
                self.y <= py <= self.y + self.height)

    def contains_region(self, other: CaptureRegion) -> bool:
        """Check if another region is fully contained."""
        return (self.x <= other.x and
                self.y <= other.y and
                self.x + self.width >= other.x + other.width and
                self.y + self.height >= other.y + other.height)

    def intersects(self, other: CaptureRegion) -> bool:
        """Check if two regions intersect."""
        return not (self.x + self.width < other.x or
                    other.x + other.width < self.x or
                    self.y + self.height < other.y or
                    other.y + other.height < self.y)

    def expand(self, dx: int, dy: int) -> CaptureRegion:
        """Expand the region by dx/dy on each side."""
        return CaptureRegion(
            x=self.x - dx,
            y=self.y - dy,
            width=self.width + 2 * dx,
            height=self.height + 2 * dy,
            label=self.label,
        )

    def shrink(self, dx: int, dy: int) -> CaptureRegion:
        """Shrink the region by dx/dy on each side."""
        return self.expand(-dx, -dy)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "x": self.x,
            "y": self.y,
            "width": self.width,
            "height": self.height,
            "label": self.label,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> CaptureRegion:
        """Create from dictionary."""
        return cls(
            x=data["x"],
            y=data["y"],
            width=data["width"],
            height=data["height"],
            label=data.get("label", ""),
        )

    @classmethod
    def from_points(
        cls,
        x1: int,
        y1: int,
        x2: int,
        y2: int,
    ) -> CaptureRegion:
        """Create region from two corner points."""
        return cls(
            x=min(x1, x2),
            y=min(y1, y2),
            width=abs(x2 - x1),
            height=abs(y2 - y1),
        )

    @classmethod
    def from_center(
        cls,
        cx: int,
        cy: int,
        width: int,
        height: int,
    ) -> CaptureRegion:
        """Create region from center point."""
        return cls(
            x=cx - width // 2,
            y=cy - height // 2,
            width=width,
            height=height,
        )


def merge_regions(regions: list[CaptureRegion]) -> CaptureRegion | None:
    """Merge multiple regions into a single bounding region."""
    if not regions:
        return None
    min_x = min(r.x for r in regions)
    min_y = min(r.y for r in regions)
    max_x = max(r.x + r.width for r in regions)
    max_y = max(r.y + r.height for r in regions)
    return CaptureRegion(
        x=min_x,
        y=min_y,
        width=max_x - min_x,
        height=max_y - min_y,
    )
