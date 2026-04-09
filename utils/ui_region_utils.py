"""UI region utilities for UI automation.

Provides utilities for managing UI regions, grid layouts,
hotspot detection, and region-based operations.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional, Set, Tuple


@dataclass
class Region:
    """Represents a 2D region/rectangle."""
    x: float
    y: float
    width: float
    height: float
    
    @property
    def left(self) -> float:
        return self.x
    
    @property
    def top(self) -> float:
        return self.y
    
    @property
    def right(self) -> float:
        return self.x + self.width
    
    @property
    def bottom(self) -> float:
        return self.y + self.height
    
    @property
    def center(self) -> Tuple[float, float]:
        return (self.x + self.width / 2, self.y + self.height / 2)
    
    @property
    def center_x(self) -> float:
        return self.x + self.width / 2
    
    @property
    def center_y(self) -> float:
        return self.y + self.height / 2
    
    @property
    def area(self) -> float:
        return self.width * self.height
    
    @property
    def is_empty(self) -> bool:
        return self.width <= 0 or self.height <= 0
    
    def contains_point(self, x: float, y: float) -> bool:
        """Check if point is inside region."""
        return self.left <= x <= self.right and self.top <= y <= self.bottom
    
    def contains_region(self, other: "Region") -> bool:
        """Check if this region fully contains another."""
        return (
            self.left <= other.left and
            self.top <= other.top and
            self.right >= other.right and
            self.bottom >= other.bottom
        )
    
    def intersects(self, other: "Region") -> bool:
        """Check if regions intersect."""
        return not (
            self.right < other.left or
            self.left > other.right or
            self.bottom < other.top or
            self.top > other.bottom
        )
    
    def intersection(self, other: "Region") -> Optional["Region"]:
        """Get intersection of two regions."""
        if not self.intersects(other):
            return None
        
        left = max(self.left, other.left)
        top = max(self.top, other.top)
        right = min(self.right, other.right)
        bottom = min(self.bottom, other.bottom)
        
        return Region(left, top, right - left, bottom - top)
    
    def union(self, other: "Region") -> "Region":
        """Get union of two regions."""
        left = min(self.left, other.left)
        top = min(self.top, other.top)
        right = max(self.right, other.right)
        bottom = max(self.bottom, other.bottom)
        
        return Region(left, top, right - left, bottom - top)
    
    def distance_to_point(self, x: float, y: float) -> float:
        """Get distance from point to region."""
        if self.contains_point(x, y):
            return 0.0
        
        dx = max(self.left - x, 0, x - self.right)
        dy = max(self.top - y, 0, y - self.bottom)
        
        return math.sqrt(dx * dx + dy * dy)
    
    def clip(self, bounds: "Region") -> "Region":
        """Clip this region to bounds."""
        result = self.intersection(bounds)
        return result if result else Region(0, 0, 0, 0)
    
    def expand(self, dx: float, dy: float) -> "Region":
        """Expand region by dx/dy on each side."""
        return Region(
            self.left - dx,
            self.top - dy,
            self.width + 2 * dx,
            self.height + 2 * dy
        )
    
    def shrink(self, dx: float, dy: float) -> "Region":
        """Shrink region by dx/dy on each side."""
        return self.expand(-dx, -dy)
    
    def subdivide(
        self,
        rows: int,
        cols: int
    ) -> List["Region"]:
        """Subdivide region into grid.
        
        Args:
            rows: Number of rows.
            cols: Number of columns.
            
        Returns:
            List of sub-regions.
        """
        if rows <= 0 or cols <= 0:
            return []
        
        cell_width = self.width / cols
        cell_height = self.height / rows
        
        regions = []
        for row in range(rows):
            for col in range(cols):
                x = self.left + col * cell_width
                y = self.top + row * cell_height
                regions.append(Region(x, y, cell_width, cell_height))
        
        return regions


@dataclass
class Hotspot:
    """A clickable region with metadata."""
    region: Region
    name: str
    action: Optional[Callable[[], None]] = None
    tags: Set[str] = field(default_factory=set)
    priority: int = 0
    enabled: bool = True


class RegionManager:
    """Manages UI regions and hotspots.
    
    Provides utilities for creating, organizing, and
    querying regions and hotspots.
    """
    
    def __init__(self) -> None:
        """Initialize the region manager."""
        self._regions: Dict[str, Region] = {}
        self._hotspots: Dict[str, Hotspot] = {}
        self._region_groups: Dict[str, Set[str]] = {}
    
    def add_region(
        self,
        region_id: str,
        x: float,
        y: float,
        width: float,
        height: float
    ) -> Region:
        """Add a region.
        
        Args:
            region_id: Unique identifier.
            x: Left coordinate.
            y: Top coordinate.
            width: Region width.
            height: Region height.
            
        Returns:
            Created region.
        """
        region = Region(x, y, width, height)
        self._regions[region_id] = region
        return region
    
    def add_region_from_region(
        self,
        region_id: str,
        region: Region
    ) -> Region:
        """Add a region from another region object.
        
        Args:
            region_id: Unique identifier.
            region: Region to add.
            
        Returns:
            Added region.
        """
        self._regions[region_id] = region
        return region
    
    def get_region(self, region_id: str) -> Optional[Region]:
        """Get a region by ID.
        
        Args:
            region_id: Region identifier.
            
        Returns:
            Region or None.
        """
        return self._regions.get(region_id)
    
    def remove_region(self, region_id: str) -> bool:
        """Remove a region.
        
        Args:
            region_id: Region to remove.
            
        Returns:
            True if removed.
        """
        if region_id in self._regions:
            del self._regions[region_id]
            return True
        return False
    
    def get_regions_in_point(
        self,
        x: float,
        y: float
    ) -> List[Tuple[str, Region]]:
        """Get all regions containing a point.
        
        Args:
            x: X coordinate.
            y: Y coordinate.
            
        Returns:
            List of (region_id, region) tuples.
        """
        return [
            (rid, r) for rid, r in self._regions.items()
            if r.contains_point(x, y)
        ]
    
    def get_region_at_point(
        self,
        x: float,
        y: float
    ) -> Optional[Tuple[str, Region]]:
        """Get the topmost region at a point.
        
        Args:
            x: X coordinate.
            y: Y coordinate.
            
        Returns:
            (region_id, region) tuple or None.
        """
        regions = self.get_regions_in_point(x, y)
        if regions:
            return regions[-1]
        return None
    
    def add_hotspot(
        self,
        hotspot_id: str,
        region: Region,
        name: str,
        action: Optional[Callable[[], None]] = None,
        tags: Optional[Set[str]] = None
    ) -> Hotspot:
        """Add a hotspot.
        
        Args:
            hotspot_id: Unique identifier.
            region: Hotspot region.
            name: Display name.
            action: Action to perform on activation.
            tags: Tags for categorization.
            
        Returns:
            Created hotspot.
        """
        hotspot = Hotspot(
            region=region,
            name=name,
            action=action,
            tags=tags or set()
        )
        self._hotspots[hotspot_id] = hotspot
        return hotspot
    
    def get_hotspot(self, hotspot_id: str) -> Optional[Hotspot]:
        """Get a hotspot by ID.
        
        Args:
            hotspot_id: Hotspot identifier.
            
        Returns:
            Hotspot or None.
        """
        return self._hotspots.get(hotspot_id)
    
    def get_hotspots_in_point(
        self,
        x: float,
        y: float
    ) -> List[Hotspot]:
        """Get all hotspots containing a point.
        
        Args:
            x: X coordinate.
            y: Y coordinate.
            
        Returns:
            List of hotspots.
        """
        return [
            h for h in self._hotspots.values()
            if h.enabled and h.region.contains_point(x, y)
        ]
    
    def get_hotspots_by_tag(self, tag: str) -> List[Hotspot]:
        """Get hotspots with a specific tag.
        
        Args:
            tag: Tag to search for.
            
        Returns:
            List of matching hotspots.
        """
        return [
            h for h in self._hotspots.values()
            if tag in h.tags
        ]
    
    def activate_hotspot(self, hotspot_id: str) -> bool:
        """Activate a hotspot's action.
        
        Args:
            hotspot_id: Hotspot to activate.
            
        Returns:
            True if activated.
        """
        hotspot = self._hotspots.get(hotspot_id)
        if not hotspot or not hotspot.action:
            return False
        
        hotspot.action()
        return True
    
    def group_regions(
        self,
        group_id: str,
        region_ids: List[str]
    ) -> None:
        """Group multiple regions.
        
        Args:
            group_id: Group identifier.
            region_ids: Region IDs in the group.
        """
        self._region_groups[group_id] = set(region_ids)
    
    def get_group_regions(self, group_id: str) -> List[Region]:
        """Get all regions in a group.
        
        Args:
            group_id: Group identifier.
            
        Returns:
            List of regions in the group.
        """
        region_ids = self._region_groups.get(group_id, set())
        return [
            self._regions[rid]
            for rid in region_ids
            if rid in self._regions
        ]


class GridLayout:
    """Creates and manages grid-based layouts."""
    
    def __init__(
        self,
        x: float,
        y: float,
        width: float,
        height: float,
        rows: int,
        cols: int
    ) -> None:
        """Initialize the grid layout.
        
        Args:
            x: Left coordinate.
            y: Top coordinate.
            width: Total width.
            height: Total height.
            rows: Number of rows.
            cols: Number of columns.
        """
        self.bounds = Region(x, y, width, height)
        self.rows = rows
        self.cols = cols
        self._cells: Optional[List[Region]] = None
    
    @property
    def cells(self) -> List[Region]:
        """Get all cells in the grid.
        
        Returns:
            List of cell regions.
        """
        if self._cells is None:
            self._cells = self.bounds.subdivide(self.rows, self.cols)
        return self._cells
    
    def get_cell(self, row: int, col: int) -> Optional[Region]:
        """Get a specific cell.
        
        Args:
            row: Row index.
            col: Column index.
            
        Returns:
            Cell region or None.
        """
        if row < 0 or row >= self.rows or col < 0 or col >= self.cols:
            return None
        
        idx = row * self.cols + col
        return self.cells[idx] if idx < len(self.cells) else None
    
    def get_cell_at_point(
        self,
        x: float,
        y: float
    ) -> Optional[Tuple[int, int, Region]]:
        """Get cell at a point.
        
        Args:
            x: X coordinate.
            y: Y coordinate.
            
        Returns:
            (row, col, cell) tuple or None.
        """
        if not self.bounds.contains_point(x, y):
            return None
        
        rel_x = x - self.bounds.left
        rel_y = y - self.bounds.top
        
        cell_width = self.bounds.width / self.cols
        cell_height = self.bounds.height / self.rows
        
        col = int(rel_x / cell_width)
        row = int(rel_y / cell_height)
        
        cell = self.get_cell(row, col)
        if cell:
            return (row, col, cell)
        
        return None
    
    def get_row(self, row: int) -> List[Region]:
        """Get all cells in a row.
        
        Args:
            row: Row index.
            
        Returns:
            List of cells in the row.
        """
        if row < 0 or row >= self.rows:
            return []
        
        start = row * self.cols
        end = start + self.cols
        return self.cells[start:end]
    
    def get_col(self, col: int) -> List[Region]:
        """Get all cells in a column.
        
        Args:
            col: Column index.
            
        Returns:
            List of cells in the column.
        """
        if col < 0 or col >= self.cols:
            return []
        
        return [self.cells[i] for i in range(col, len(self.cells), self.cols)]
