"""
Screen Region Action Module

Defines and manages screen regions for targeted automation,
including capture areas, click zones, and monitoring regions.

MIT License - Copyright (c) 2025 RabAi Research
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


class RegionType(Enum):
    """Region types."""

    CAPTURE = "capture"
    CLICK_ZONE = "click_zone"
    MONITOR = "monitor"
    EXCLUDE = "exclude"


@dataclass
class ScreenRegion:
    """Defines a screen region."""

    id: str
    name: str
    x: int
    y: int
    width: int
    height: int
    region_type: RegionType = RegionType.CAPTURE
    monitor_index: int = 0
    enabled: bool = True
    metadata: Dict[str, Any] = field(default_factory=dict)

    @property
    def bounds(self) -> Tuple[int, int, int, int]:
        """Get bounds tuple."""
        return (self.x, self.y, self.width, self.height)

    @property
    def center(self) -> Tuple[int, int]:
        """Get center point."""
        return (self.x + self.width // 2, self.y + self.height // 2)

    @property
    def area(self) -> int:
        """Get area size."""
        return self.width * self.height

    def contains_point(self, x: int, y: int) -> bool:
        """Check if point is within region."""
        return self.x <= x < self.x + self.width and self.y <= y < self.y + self.height

    def intersects(self, other: "ScreenRegion") -> bool:
        """Check if region intersects another."""
        return not (
            self.x >= other.x + other.width
            or self.x + self.width <= other.x
            or self.y >= other.y + other.height
            or self.y + self.height <= other.y
        )


class ScreenRegionManager:
    """
    Manages screen regions for automation.

    Supports region creation, editing, grouping,
    and spatial queries.
    """

    def __init__(self):
        self._regions: Dict[str, ScreenRegion] = {}
        self._groups: Dict[str, List[str]] = {}

    def add_region(
        self,
        region_id: str,
        name: str,
        x: int,
        y: int,
        width: int,
        height: int,
        region_type: RegionType = RegionType.CAPTURE,
        monitor_index: int = 0,
    ) -> ScreenRegion:
        """Add a screen region."""
        region = ScreenRegion(
            id=region_id,
            name=name,
            x=x,
            y=y,
            width=width,
            height=height,
            region_type=region_type,
            monitor_index=monitor_index,
        )
        self._regions[region_id] = region
        return region

    def remove_region(self, region_id: str) -> bool:
        """Remove a region."""
        if region_id in self._regions:
            del self._regions[region_id]
            for group in self._groups.values():
                if region_id in group:
                    group.remove(region_id)
            return True
        return False

    def get_region(self, region_id: str) -> Optional[ScreenRegion]:
        """Get region by ID."""
        return self._regions.get(region_id)

    def get_all_regions(self) -> List[ScreenRegion]:
        """Get all regions."""
        return list(self._regions.values())

    def get_regions_by_type(self, region_type: RegionType) -> List[ScreenRegion]:
        """Get regions of specific type."""
        return [r for r in self._regions.values() if r.region_type == region_type]

    def get_region_at(self, x: int, y: int) -> Optional[ScreenRegion]:
        """Get region containing point."""
        for region in self._regions.values():
            if region.enabled and region.contains_point(x, y):
                return region
        return None

    def find_intersecting(
        self,
        x: int,
        y: int,
        width: int,
        height: int,
    ) -> List[ScreenRegion]:
        """Find all regions intersecting with rectangle."""
        test_region = ScreenRegion(
            id="__test__",
            name="__test__",
            x=x,
            y=y,
            width=width,
            height=height,
        )
        return [r for r in self._regions.values() if r.enabled and r.intersects(test_region)]

    def create_group(self, group_id: str, region_ids: List[str]) -> bool:
        """Create a region group."""
        if all(rid in self._regions for rid in region_ids):
            self._groups[group_id] = list(region_ids)
            return True
        return False

    def get_group(self, group_id: str) -> List[ScreenRegion]:
        """Get all regions in a group."""
        region_ids = self._groups.get(group_id, [])
        return [self._regions[rid] for rid in region_ids if rid in self._regions]

    def enable_region(self, region_id: str, enabled: bool = True) -> bool:
        """Enable or disable a region."""
        if region_id in self._regions:
            self._regions[region_id].enabled = enabled
            return True
        return False


def create_screen_region_manager() -> ScreenRegionManager:
    """Factory function."""
    return ScreenRegionManager()
