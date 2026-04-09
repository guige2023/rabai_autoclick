"""
UI Zone Utilities - Zone-based UI region management and detection.

This module provides utilities for defining, managing, and detecting
UI zones (regions) for automation workflows. Zones can represent areas
of the screen with specific behaviors or purposes.

Author: rabai_autoclick team
License: MIT
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from typing import Callable, Iterator, Optional, Sequence


@dataclass
class Zone:
    """Represents a rectangular zone/region on screen.
    
    Attributes:
        id: Unique identifier for the zone.
        name: Human-readable name for the zone.
        x: X coordinate of top-left corner.
        y: Y coordinate of top-left corner.
        width: Width of the zone in pixels.
        height: Height of the zone in pixels.
        metadata: Optional dictionary of additional zone data.
        tags: Set of tags associated with the zone.
    """
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    name: str = ""
    x: int = 0
    y: int = 0
    width: int = 0
    height: int = 0
    metadata: dict = field(default_factory=dict)
    tags: set[str] = field(default_factory=set)
    
    @property
    def bounds(self) -> tuple[int, int, int, int]:
        """Get zone bounds as (x, y, width, height)."""
        return (self.x, self.y, self.width, self.height)
    
    @property
    def center(self) -> tuple[int, int]:
        """Get zone center point as (x, y)."""
        return (self.x + self.width // 2, self.y + self.height // 2)
    
    @property
    def left(self) -> int:
        """Get left edge X coordinate."""
        return self.x
    
    @property
    def right(self) -> int:
        """Get right edge X coordinate."""
        return self.x + self.width
    
    @property
    def top(self) -> int:
        """Get top edge Y coordinate."""
        return self.y
    
    @property
    def bottom(self) -> int:
        """Get bottom edge Y coordinate."""
        return self.y + self.height
    
    def contains_point(self, px: int, py: int) -> bool:
        """Check if a point is inside the zone.
        
        Args:
            px: X coordinate of point.
            py: Y coordinate of point.
            
        Returns:
            True if point is within zone bounds.
        """
        return self.x <= px < self.right and self.y <= py < self.bottom
    
    def contains_bounds(
        self,
        bx: int,
        by: int,
        bw: int,
        bh: int
    ) -> bool:
        """Check if bounds are fully contained within the zone.
        
        Args:
            bx: X coordinate of bounds.
            by: Y coordinate of bounds.
            bw: Width of bounds.
            bh: Height of bounds.
            
        Returns:
            True if bounds are fully inside the zone.
        """
        return (
            self.x <= bx
            and self.y <= by
            and self.right >= bx + bw
            and self.bottom >= by + bh
        )
    
    def intersects(self, other: Zone) -> bool:
        """Check if this zone intersects with another.
        
        Args:
            other: Another zone to check.
            
        Returns:
            True if zones overlap in any way.
        """
        return not (
            self.right <= other.x
            or other.right <= self.x
            or self.bottom <= other.y
            or other.bottom <= self.y
        )
    
    def overlap_area(self, other: Zone) -> int:
        """Calculate the overlapping area with another zone.
        
        Args:
            other: Another zone to check.
            
        Returns:
            Area of overlap in square pixels.
        """
        if not self.intersects(other):
            return 0
        
        overlap_left = max(self.x, other.x)
        overlap_top = max(self.y, other.y)
        overlap_right = min(self.right, other.right)
        overlap_bottom = min(self.bottom, other.bottom)
        
        return (overlap_right - overlap_left) * (overlap_bottom - overlap_top)
    
    def distance_to(self, other: Zone) -> float:
        """Calculate minimum distance to another zone.
        
        Args:
            other: Another zone.
            
        Returns:
            Minimum distance between zones in pixels.
        """
        dx = max(0, max(self.x - other.right, other.x - self.right))
        dy = max(0, max(self.y - other.bottom, other.y - self.bottom))
        return (dx ** 2 + dy ** 2) ** 0.5
    
    def add_tag(self, tag: str) -> None:
        """Add a tag to the zone.
        
        Args:
            tag: Tag to add.
        """
        self.tags.add(tag)
    
    def remove_tag(self, tag: str) -> None:
        """Remove a tag from the zone.
        
        Args:
            tag: Tag to remove.
        """
        self.tags.discard(tag)
    
    def has_tag(self, tag: str) -> bool:
        """Check if zone has a specific tag.
        
        Args:
            tag: Tag to check.
            
        Returns:
            True if zone has the tag.
        """
        return tag in self.tags
    
    def expand(self, dx: int, dy: int) -> Zone:
        """Create a new zone expanded by given amounts.
        
        Args:
            dx: Amount to expand horizontally (total).
            dy: Amount to expand vertically (total).
            
        Returns:
            New expanded Zone.
        """
        return Zone(
            id=self.id,
            name=self.name,
            x=self.x - dx // 2,
            y=self.y - dy // 2,
            width=self.width + dx,
            height=self.height + dy,
            metadata=self.metadata.copy(),
            tags=self.tags.copy()
        )


class ZoneManager:
    """Manages a collection of zones with detection and lookup.
    
    Provides methods for adding, removing, finding zones by
    various criteria, and processing point/region queries.
    
    Example:
        >>> manager = ZoneManager()
        >>> manager.add_zone(Zone(name="header", x=0, y=0, width=1920, height=100))
        >>> zone = manager.find_zone_at(100, 50)
        >>> print(zone.name if zone else "Not found")
    """
    
    def __init__(self) -> None:
        """Initialize the zone manager."""
        self._zones: dict[str, Zone] = {}
        self._name_index: dict[str, Zone] = {}
    
    def add_zone(self, zone: Zone) -> str:
        """Add a zone to the manager.
        
        Args:
            zone: Zone to add.
            
        Returns:
            The zone's unique ID.
        """
        self._zones[zone.id] = zone
        if zone.name:
            self._name_index[zone.name] = zone
        return zone.id
    
    def remove_zone(self, zone_id: str) -> bool:
        """Remove a zone by ID.
        
        Args:
            zone_id: ID of zone to remove.
            
        Returns:
            True if zone was removed, False if not found.
        """
        zone = self._zones.pop(zone_id, None)
        if zone and zone.name:
            self._name_index.pop(zone.name, None)
        return zone is not None
    
    def get_zone(self, zone_id: str) -> Optional[Zone]:
        """Get a zone by ID.
        
        Args:
            zone_id: Zone ID.
            
        Returns:
            Zone if found, None otherwise.
        """
        return self._zones.get(zone_id)
    
    def get_zone_by_name(self, name: str) -> Optional[Zone]:
        """Get a zone by name.
        
        Args:
            name: Zone name.
            
        Returns:
            Zone if found, None otherwise.
        """
        return self._name_index.get(name)
    
    def find_zone_at(self, x: int, y: int) -> Optional[Zone]:
        """Find zone containing a point.
        
        Args:
            x: X coordinate.
            y: Y coordinate.
            
        Returns:
            First zone containing the point, or None.
        """
        for zone in self._zones.values():
            if zone.contains_point(x, y):
                return zone
        return None
    
    def find_zones_at(self, x: int, y: int) -> list[Zone]:
        """Find all zones containing a point.
        
        Args:
            x: X coordinate.
            y: Y coordinate.
            
        Returns:
            List of all zones containing the point.
        """
        return [z for z in self._zones.values() if z.contains_point(x, y)]
    
    def find_zones_in_bounds(
        self,
        x: int,
        y: int,
        width: int,
        height: int
    ) -> list[Zone]:
        """Find zones that intersect with given bounds.
        
        Args:
            x: X coordinate of bounds.
            y: Y coordinate of bounds.
            width: Width of bounds.
            height: Height of bounds.
            
        Returns:
            List of intersecting zones.
        """
        query_zone = Zone(x=x, y=y, width=width, height=height)
        return [z for z in self._zones.values() if z.intersects(query_zone)]
    
    def find_zones_by_tag(self, tag: str) -> list[Zone]:
        """Find all zones with a specific tag.
        
        Args:
            tag: Tag to search for.
            
        Returns:
            List of zones with the tag.
        """
        return [z for z in self._zones.values() if z.has_tag(tag)]
    
    def find_zones_by_tags(self, tags: set[str]) -> list[Zone]:
        """Find zones containing all specified tags.
        
        Args:
            tags: Set of tags (AND logic).
            
        Returns:
            List of zones containing all tags.
        """
        return [
            z for z in self._zones.values()
            if tags.issubset(z.tags)
        ]
    
    def filter_zones(
        self,
        predicate: Callable[[Zone], bool]
    ) -> list[Zone]:
        """Filter zones by a custom predicate.
        
        Args:
            predicate: Function returning True for zones to keep.
            
        Returns:
            List of matching zones.
        """
        return [z for z in self._zones.values() if predicate(z)]
    
    def iterate_zones(self) -> Iterator[Zone]:
        """Iterate over all zones.
        
        Yields:
            Each Zone in the manager.
        """
        yield from self._zones.values()
    
    def get_zones_sorted_by_area(self) -> list[Zone]:
        """Get zones sorted by area (smallest first).
        
        Returns:
            List of zones sorted by area.
        """
        return sorted(
            self._zones.values(),
            key=lambda z: z.width * z.height
        )
    
    def get_zones_sorted_by_distance(
        self,
        x: int,
        y: int
    ) -> list[Zone]:
        """Get zones sorted by distance from a point.
        
        Args:
            x: Reference X coordinate.
            y: Reference Y coordinate.
            
        Returns:
            List of zones sorted by distance.
        """
        def center_distance(z: Zone) -> float:
            cx, cy = z.center
            return ((x - cx) ** 2 + (y - cy) ** 2) ** 0.5
        
        return sorted(self._zones.values(), key=center_distance)
    
    def clear(self) -> None:
        """Remove all zones from the manager."""
        self._zones.clear()
        self._name_index.clear()
    
    def __len__(self) -> int:
        """Get number of zones."""
        return len(self._zones)


@dataclass
class ZoneLayout:
    """Represents a layout of zones within a screen region.
    
    Attributes:
        screen_width: Total screen width.
        screen_height: Total screen height.
        zones: List of zones in the layout.
        margin: Margin between zones.
        padding: Padding inside zones.
    """
    screen_width: int
    screen_height: int
    zones: list[Zone] = field(default_factory=list)
    margin: int = 0
    padding: int = 0
    
    def divide_horizontal(
        self,
        name_prefix: str = "zone",
        count: int = 2
    ) -> list[Zone]:
        """Divide screen horizontally into equal zones.
        
        Args:
            name_prefix: Prefix for zone names.
            count: Number of zones to create.
            
        Returns:
            List of created zones.
        """
        zone_height = self.screen_height // count
        self.zones.clear()
        
        for i in range(count):
            zone = Zone(
                name=f"{name_prefix}_{i}",
                x=self.margin,
                y=self.margin + i * zone_height,
                width=self.screen_width - 2 * self.margin,
                height=zone_height - self.margin
            )
            self.zones.append(zone)
        
        return self.zones
    
    def divide_vertical(
        self,
        name_prefix: str = "zone",
        count: int = 2
    ) -> list[Zone]:
        """Divide screen vertically into equal zones.
        
        Args:
            name_prefix: Prefix for zone names.
            count: Number of zones to create.
            
        Returns:
            List of created zones.
        """
        zone_width = self.screen_width // count
        self.zones.clear()
        
        for i in range(count):
            zone = Zone(
                name=f"{name_prefix}_{i}",
                x=self.margin + i * zone_width,
                y=self.margin,
                width=zone_width - self.margin,
                height=self.screen_height - 2 * self.margin
            )
            self.zones.append(zone)
        
        return self.zones
    
    def divide_grid(
        self,
        rows: int,
        columns: int,
        name_prefix: str = "zone"
    ) -> list[Zone]:
        """Divide screen into a grid of zones.
        
        Args:
            rows: Number of rows.
            columns: Number of columns.
            name_prefix: Prefix for zone names.
            
        Returns:
            List of created zones in row-major order.
        """
        cell_width = self.screen_width // columns
        cell_height = self.screen_height // rows
        self.zones.clear()
        
        for row in range(rows):
            for col in range(columns):
                zone = Zone(
                    name=f"{name_prefix}_{row}_{col}",
                    x=self.margin + col * cell_width,
                    y=self.margin + row * cell_height,
                    width=cell_width - self.margin,
                    height=cell_height - self.margin
                )
                self.zones.append(zone)
        
        return self.zones
