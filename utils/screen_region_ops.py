"""Screen region operations for UI automation.

Provides geometric operations on screen regions including
union, intersection, and difference calculations.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Optional


@dataclass
class Rect:
    """A rectangle defined by origin point and size."""
    x: float
    y: float
    width: float
    height: float

    @classmethod
    def from_points(cls, x1: float, y1: float, x2: float, y2: float) -> Rect:
        """Create from two opposite corners."""
        return cls(min(x1, x2), min(y1, y2), abs(x2 - x1), abs(y2 - y1))

    @property
    def x2(self) -> float:
        """Right edge."""
        return self.x + self.width

    @property
    def y2(self) -> float:
        """Bottom edge."""
        return self.y + self.height

    @property
    def center(self) -> tuple[float, float]:
        """Center point."""
        return (self.x + self.width / 2, self.y + self.height / 2)

    @property
    def area(self) -> float:
        """Area of the rectangle."""
        return self.width * self.height

    @property
    def is_empty(self) -> bool:
        """Return True if rectangle has no area."""
        return self.width <= 0 or self.height <= 0

    def contains(self, x: float, y: float) -> bool:
        """Check if point is inside rectangle."""
        return self.x <= x < self.x2 and self.y <= y < self.y2

    def contains_rect(self, other: Rect) -> bool:
        """Check if another rect is fully inside this one."""
        return (
            self.x <= other.x
            and self.y <= other.y
            and self.x2 >= other.x2
            and self.y2 >= other.y2
        )

    def intersects(self, other: Rect) -> bool:
        """Check if this rect intersects another."""
        return not (
            self.x2 <= other.x
            or other.x2 <= self.x
            or self.y2 <= other.y
            or other.y2 <= self.y
        )

    def intersection(self, other: Rect) -> Optional[Rect]:
        """Return intersection rectangle, or None if no overlap."""
        if not self.intersects(other):
            return None
        x1 = max(self.x, other.x)
        y1 = max(self.y, other.y)
        x2 = min(self.x2, other.x2)
        y2 = min(self.y2, other.y2)
        return Rect.from_points(x1, y1, x2, y2)

    def union(self, other: Rect) -> Rect:
        """Return bounding box containing both rectangles."""
        x1 = min(self.x, other.x)
        y1 = min(self.y, other.y)
        x2 = max(self.x2, other.x2)
        y2 = max(self.y2, other.y2)
        return Rect.from_points(x1, y1, x2, y2)

    def expand(self, margin: float) -> Rect:
        """Expand rectangle by margin in all directions."""
        return Rect(
            self.x - margin,
            self.y - margin,
            self.width + 2 * margin,
            self.height + 2 * margin,
        )

    def shrink(self, margin: float) -> Rect:
        """Shrink rectangle by margin in all directions."""
        return self.expand(-margin)

    def aspect_ratio(self) -> float:
        """Return width/height aspect ratio."""
        return self.width / self.height if self.height > 0 else 0.0

    def distance_to(self, x: float, y: float) -> float:
        """Return minimum distance from point to rectangle."""
        dx = max(self.x - x, 0, x - self.x2)
        dy = max(self.y - y, 0, y - self.y2)
        return math.sqrt(dx * dx + dy * dy)

    def closest_point(self, x: float, y: float) -> tuple[float, float]:
        """Return closest point on rectangle to given point."""
        return (
            max(self.x, min(x, self.x2)),
            max(self.y, min(y, self.y2)),
        )

    def grid_cells(self, cell_width: float, cell_height: float) -> list[Rect]:
        """Divide rectangle into a grid of cells."""
        cells = []
        cols = max(1, int(math.ceil(self.width / cell_width)))
        rows = max(1, int(math.ceil(self.height / cell_height)))
        for row in range(rows):
            for col in range(cols):
                cx = self.x + col * cell_width
                cy = self.y + row * cell_height
                cw = min(cell_width, self.x2 - cx)
                ch = min(cell_height, self.y2 - cy)
                cells.append(Rect(cx, cy, cw, ch))
        return cells


def clip_to_screen(rect: Rect, screen_width: float, screen_height: float) -> Rect:
    """Clip a rectangle to screen bounds."""
    return Rect(
        max(0, rect.x),
        max(0, rect.y),
        min(rect.width, screen_width - rect.x),
        min(rect.height, screen_height - rect.y),
    )
