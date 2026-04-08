"""
Region Caliper Utilities

Provides utilities for measuring and analyzing
screen regions in automation workflows.

Author: Agent3
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass
class RegionMetrics:
    """Metrics for a screen region."""
    x: int
    y: int
    width: int
    height: int
    area: int
    aspect_ratio: float


class RegionCaliper:
    """
    Measures and analyzes screen regions.
    
    Provides metrics like area, aspect ratio,
    and position relative to other regions.
    """

    def __init__(self) -> None:
        self._regions: dict[str, tuple[int, int, int, int]] = {}

    def add_region(
        self,
        name: str,
        x: int,
        y: int,
        width: int,
        height: int,
    ) -> None:
        """Register a named region."""
        self._regions[name] = (x, y, width, height)

    def get_metrics(self, name: str) -> RegionMetrics | None:
        """Get metrics for a named region."""
        if name not in self._regions:
            return None
        x, y, w, h = self._regions[name]
        return RegionMetrics(
            x=x,
            y=y,
            width=w,
            height=h,
            area=w * h,
            aspect_ratio=w / h if h > 0 else 0.0,
        )

    def distance_between(
        self,
        name1: str,
        name2: str,
    ) -> float | None:
        """Calculate distance between region centers."""
        m1 = self.get_metrics(name1)
        m2 = self.get_metrics(name2)
        if not m1 or not m2:
            return None
        cx1, cy1 = m1.x + m1.width / 2, m1.y + m1.height / 2
        cx2, cy2 = m2.x + m2.width / 2, m2.y + m2.height / 2
        return ((cx2 - cx1) ** 2 + (cy2 - cy1) ** 2) ** 0.5

    def overlaps(self, name1: str, name2: str) -> bool:
        """Check if two regions overlap."""
        if name1 not in self._regions or name2 not in self._regions:
            return False
        x1, y1, w1, h1 = self._regions[name1]
        x2, y2, w2, h2 = self._regions[name2]
        return not (x1 + w1 < x2 or x2 + w2 < x1 or y1 + h1 < y2 or y2 + h2 < y1)
