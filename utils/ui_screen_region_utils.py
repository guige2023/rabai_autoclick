"""
UI Screen Region Utilities - Screen region detection and monitoring.

This module provides utilities for defining, detecting, and monitoring
screen regions. Regions can be used to limit element searches, detect
screen changes, and manage multi-monitor setups.

Author: rabai_autoclick team
License: MIT
"""

from __future__ import annotations

import uuid
import time
from dataclasses import dataclass, field
from typing import Callable, Iterator, Optional, Sequence


@dataclass
class ScreenRegion:
    """Represents a rectangular screen region.
    
    Attributes:
        id: Unique identifier for this region.
        name: Human-readable name.
        x: Left edge X coordinate.
        y: Top edge Y coordinate.
        width: Region width in pixels.
        height: Region height in pixels.
        monitor: Monitor index (0 for primary).
        priority: Region priority for overlapping regions.
        tags: Set of tags for categorization.
    """
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    name: Optional[str] = None
    x: int = 0
    y: int = 0
    width: int = 0
    height: int = 0
    monitor: int = 0
    priority: int = 0
    tags: set[str] = field(default_factory=set)
    
    @property
    def bounds(self) -> tuple[int, int, int, int]:
        """Get bounds as (x, y, width, height)."""
        return (self.x, self.y, self.width, self.height)
    
    @property
    def right(self) -> int:
        """Get right edge X coordinate."""
        return self.x + self.width
    
    @property
    def bottom(self) -> int:
        """Get bottom edge Y coordinate."""
        return self.y + self.height
    
    @property
    def center_x(self) -> int:
        """Get center X coordinate."""
        return self.x + self.width // 2
    
    @property
    def center_y(self) -> int:
        """Get center Y coordinate."""
        return self.y + self.height // 2
    
    @property
    def center(self) -> tuple[int, int]:
        """Get center coordinates."""
        return (self.center_x, self.center_y)
    
    @property
    def area(self) -> int:
        """Get region area in pixels."""
        return self.width * self.height
    
    def contains_point(self, px: int, py: int) -> bool:
        """Check if point is inside region.
        
        Args:
            px: X coordinate.
            py: Y coordinate.
            
        Returns:
            True if point is within bounds.
        """
        return self.x <= px < self.right and self.y <= py < self.bottom
    
    def contains_bounds(
        self,
        bx: int,
        by: int,
        bw: int,
        bh: int
    ) -> bool:
        """Check if bounds are fully contained.
        
        Args:
            bx: X coordinate.
            by: Y coordinate.
            bw: Width.
            bh: Height.
            
        Returns:
            True if bounds are fully inside region.
        """
        return (
            self.x <= bx
            and self.y <= by
            and self.right >= bx + bw
            and self.bottom >= by + bh
        )
    
    def intersects(self, other: ScreenRegion) -> bool:
        """Check if regions intersect.
        
        Args:
            other: Other region to check.
            
        Returns:
            True if regions overlap.
        """
        return not (
            self.right <= other.x
            or other.right <= self.x
            or self.bottom <= other.y
            or other.bottom <= self.y
        )
    
    def intersection(
        self,
        other: ScreenRegion
    ) -> Optional[ScreenRegion]:
        """Get intersection with another region.
        
        Args:
            other: Other region.
            
        Returns:
            Intersection region, or None if no overlap.
        """
        left = max(self.x, other.x)
        top = max(self.y, other.y)
        right = min(self.right, other.right)
        bottom = min(self.bottom, other.bottom)
        
        if left >= right or top >= bottom:
            return None
        
        return ScreenRegion(
            x=left,
            y=top,
            width=right - left,
            height=bottom - top,
            monitor=self.monitor
        )
    
    def add_tag(self, tag: str) -> None:
        """Add a tag to the region."""
        self.tags.add(tag)
    
    def remove_tag(self, tag: str) -> None:
        """Remove a tag from the region."""
        self.tags.discard(tag)
    
    def has_tag(self, tag: str) -> bool:
        """Check if region has a tag."""
        return tag in self.tags


class ScreenRegionManager:
    """Manages a collection of screen regions.
    
    Provides methods for adding, removing, finding, and
    monitoring screen regions.
    
    Example:
        >>> manager = ScreenRegionManager()
        >>> manager.add_region(ScreenRegion(name="app", x=0, y=0, width=800, height=600))
        >>> regions = manager.find_regions_at(100, 100)
    """
    
    def __init__(self) -> None:
        """Initialize an empty region manager."""
        self._regions: dict[str, ScreenRegion] = {}
    
    def add_region(self, region: ScreenRegion) -> str:
        """Add a region to the manager.
        
        Args:
            region: Region to add.
            
        Returns:
            Region ID.
        """
        self._regions[region.id] = region
        return region.id
    
    def remove_region(self, region_id: str) -> bool:
        """Remove a region by ID.
        
        Args:
            region_id: ID of region to remove.
            
        Returns:
            True if region was removed.
        """
        return region_id in self._regions and not not self._regions.pop(region_id, None)
    
    def get_region(self, region_id: str) -> Optional[ScreenRegion]:
        """Get a region by ID.
        
        Args:
            region_id: Region ID.
            
        Returns:
            ScreenRegion if found.
        """
        return self._regions.get(region_id)
    
    def update_region(
        self,
        region_id: str,
        **kwargs
    ) -> Optional[ScreenRegion]:
        """Update a region's properties.
        
        Args:
            region_id: Region to update.
            **kwargs: Properties to update.
            
        Returns:
            Updated region, or None if not found.
        """
        region = self._regions.get(region_id)
        if not region:
            return None
        
        for key, value in kwargs.items():
            if hasattr(region, key):
                setattr(region, key, value)
        
        return region
    
    def find_regions_at(self, x: int, y: int) -> list[ScreenRegion]:
        """Find all regions containing a point.
        
        Args:
            x: X coordinate.
            y: Y coordinate.
            
        Returns:
            List of regions containing the point.
        """
        return [
            r for r in self._regions.values()
            if r.contains_point(x, y)
        ]
    
    def find_regions_for_bounds(
        self,
        x: int,
        y: int,
        width: int,
        height: int
    ) -> list[ScreenRegion]:
        """Find regions that contain or intersect bounds.
        
        Args:
            x: X coordinate.
            y: Y coordinate.
            width: Width.
            height: Height.
            
        Returns:
            List of intersecting regions.
        """
        query = ScreenRegion(x=x, y=y, width=width, height=height)
        return [
            r for r in self._regions.values()
            if r.intersects(query)
        ]
    
    def find_regions_by_tag(self, tag: str) -> list[ScreenRegion]:
        """Find regions with a specific tag.
        
        Args:
            tag: Tag to search for.
            
        Returns:
            List of matching regions.
        """
        return [r for r in self._regions.values() if r.has_tag(tag)]
    
    def find_regions_by_monitor(self, monitor: int) -> list[ScreenRegion]:
        """Find regions on a specific monitor.
        
        Args:
            monitor: Monitor index.
            
        Returns:
            List of regions on the monitor.
        """
        return [r for r in self._regions.values() if r.monitor == monitor]
    
    def get_highest_priority_at(
        self,
        x: int,
        y: int
    ) -> Optional[ScreenRegion]:
        """Get highest priority region at a point.
        
        Args:
            x: X coordinate.
            y: Y coordinate.
            
        Returns:
            Highest priority region at point, or None.
        """
        regions = self.find_regions_at(x, y)
        if not regions:
            return None
        return max(regions, key=lambda r: r.priority)
    
    def iterate_regions(self) -> Iterator[ScreenRegion]:
        """Iterate over all regions.
        
        Yields:
            Each ScreenRegion.
        """
        yield from self._regions.values()
    
    def clear(self) -> None:
        """Remove all regions."""
        self._regions.clear()
    
    def __len__(self) -> int:
        """Get number of regions."""
        return len(self._regions)


@dataclass
class RegionChange:
    """Represents a detected change in a screen region.
    
    Attributes:
        id: Unique identifier.
        region_id: ID of the changed region.
        change_type: Type of change (appeared, disappeared, moved, changed).
        timestamp: Time of change detection.
        previous_bounds: Previous bounds if moved.
        current_bounds: Current bounds if moved.
        element_id: Associated element ID if applicable.
    """
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    region_id: Optional[str] = None
    change_type: str = "changed"
    timestamp: float = field(default_factory=time.time)
    previous_bounds: Optional[tuple[int, int, int, int]] = None
    current_bounds: Optional[tuple[int, int, int, int]] = None
    element_id: Optional[str] = None


class RegionMonitor:
    """Monitors screen regions for changes.
    
    Provides callback-based monitoring of region changes
    for automation workflows.
    
    Example:
        >>> monitor = RegionMonitor()
        >>> monitor.watch(region_id, callback)
        >>> monitor.start()
    """
    
    def __init__(self) -> None:
        """Initialize the region monitor."""
        self._watchers: dict[str, list[Callable[[RegionChange], None]]] = {}
        self._history: list[RegionChange] = []
        self._running: bool = False
    
    def watch(
        self,
        region_id: str,
        callback: Callable[[RegionChange], None]
    ) -> None:
        """Register a callback for region changes.
        
        Args:
            region_id: Region to watch.
            callback: Function to call on changes.
        """
        if region_id not in self._watchers:
            self._watchers[region_id] = []
        self._watchers[region_id].append(callback)
    
    def unwatch(
        self,
        region_id: str,
        callback: Callable[[RegionChange], None]
    ) -> bool:
        """Unregister a callback.
        
        Args:
            region_id: Region ID.
            callback: Callback to remove.
            
        Returns:
            True if callback was removed.
        """
        if region_id in self._watchers:
            try:
                self._watchers[region_id].remove(callback)
                return True
            except ValueError:
                pass
        return False
    
    def notify_change(self, change: RegionChange) -> None:
        """Notify watchers of a change.
        
        Args:
            change: The change to report.
        """
        self._history.append(change)
        
        if change.region_id and change.region_id in self._watchers:
            for callback in self._watchers[change.region_id]:
                callback(change)
    
    def get_history(
        self,
        region_id: Optional[str] = None,
        since: Optional[float] = None
    ) -> list[RegionChange]:
        """Get change history.
        
        Args:
            region_id: Optional region filter.
            since: Optional timestamp filter.
            
        Returns:
            List of changes.
        """
        changes = self._history
        
        if region_id:
            changes = [c for c in changes if c.region_id == region_id]
        if since:
            changes = [c for c in changes if c.timestamp >= since]
        
        return changes
    
    def clear_history(self) -> None:
        """Clear change history."""
        self._history.clear()


def create_region_from_element(
    element: dict,
    padding: int = 0,
    name: Optional[str] = None
) -> ScreenRegion:
    """Create a screen region from an element.
    
    Args:
        element: Element dictionary with x, y, width, height.
        padding: Padding to add around element.
        name: Optional region name.
        
    Returns:
        ScreenRegion for the element.
    """
    x = element.get("x", 0)
    y = element.get("y", 0)
    width = element.get("width", 0)
    height = element.get("height", 0)
    
    return ScreenRegion(
        name=name or element.get("name"),
        x=x - padding,
        y=y - padding,
        width=width + 2 * padding,
        height=height + 2 * padding
    )


def divide_screen_regions(
    monitor: int = 0,
    rows: int = 1,
    columns: int = 1,
    margin: int = 0
) -> list[ScreenRegion]:
    """Create regions that divide a screen into a grid.
    
    Args:
        monitor: Monitor index.
        rows: Number of rows.
        columns: Number of columns.
        margin: Margin between regions.
        
    Returns:
        List of ScreenRegions.
    """
    screen_width = 1920
    screen_height = 1080
    
    cell_width = screen_width // columns
    cell_height = screen_height // rows
    
    regions = []
    for row in range(rows):
        for col in range(columns):
            region = ScreenRegion(
                name=f"region_{row}_{col}",
                x=margin + col * cell_width,
                y=margin + row * cell_height,
                width=cell_width - 2 * margin,
                height=cell_height - 2 * margin,
                monitor=monitor
            )
            regions.append(region)
    
    return regions
