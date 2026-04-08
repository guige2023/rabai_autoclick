"""
Zone Manager Utilities

Provides utilities for managing screen zones
in UI automation workflows.

Author: Agent3
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass
class Zone:
    """A rectangular zone on screen."""
    name: str
    x: int
    y: int
    width: int
    height: int


class ZoneManager:
    """
    Manages screen zones for element categorization.
    
    Divides screen into named zones and provides
    zone lookup by position.
    """

    def __init__(self) -> None:
        self._zones: dict[str, Zone] = {}

    def add_zone(
        self,
        name: str,
        x: int,
        y: int,
        width: int,
        height: int,
    ) -> None:
        """Add a named zone."""
        self._zones[name] = Zone(name=name, x=x, y=y, width=width, height=height)

    def remove_zone(self, name: str) -> bool:
        """Remove a zone."""
        return self._zones.pop(name, None) is not None

    def get_zone_at_point(self, x: int, y: int) -> Zone | None:
        """Find zone containing a point."""
        for zone in self._zones.values():
            if (zone.x <= x <= zone.x + zone.width and
                    zone.y <= y <= zone.y + zone.height):
                return zone
        return None

    def get_zone(self, name: str) -> Zone | None:
        """Get zone by name."""
        return self._zones.get(name)

    def list_zones(self) -> list[str]:
        """List all zone names."""
        return list(self._zones.keys())
