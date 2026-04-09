"""
Window geometry utilities.

Calculate and manipulate window geometries.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Optional, Tuple


@dataclass
class Rect:
    """Rectangle geometry."""
    x: int
    y: int
    width: int
    height: int
    
    def contains(self, px: int, py: int) -> bool:
        """Check if point is inside rect."""
        return self.x <= px <= self.x + self.width and self.y <= py <= self.y + self.height
    
    def intersects(self, other: "Rect") -> bool:
        """Check if this rect intersects another."""
        return not (
            self.x + self.width < other.x or
            other.x + other.width < self.x or
            self.y + self.height < other.y or
            other.y + other.height < self.y
        )
    
    def union(self, other: "Rect") -> "Rect":
        """Get bounding rect of union."""
        x = min(self.x, other.x)
        y = min(self.y, other.y)
        right = max(self.x + self.width, other.x + other.width)
        bottom = max(self.y + self.height, other.y + other.height)
        return Rect(x, y, right - x, bottom - y)
    
    def intersection(self, other: "Rect") -> Optional["Rect"]:
        """Get intersection of two rects."""
        if not self.intersects(other):
            return None
        
        x = max(self.x, other.x)
        y = max(self.y, other.y)
        right = min(self.x + self.width, other.x + other.width)
        bottom = min(self.y + self.height, other.y + other.height)
        return Rect(x, y, right - x, bottom - y)
    
    def center(self) -> Tuple[int, int]:
        """Get center point."""
        return (self.x + self.width // 2, self.y + self.height // 2)
    
    def distance_to(self, other: "Rect") -> float:
        """Get minimum distance to another rect."""
        cx1, cy1 = self.center()
        cx2, cy2 = other.center()
        return math.sqrt((cx2 - cx1) ** 2 + (cy2 - cy1) ** 2)


class WindowGeometryCalculator:
    """Calculate window geometries."""
    
    @staticmethod
    def calculate_centered_geometry(
        parent: Rect,
        child_width: int,
        child_height: int
    ) -> Rect:
        """Calculate centered geometry within parent."""
        x = parent.x + (parent.width - child_width) // 2
        y = parent.y + (parent.height - child_height) // 2
        return Rect(x, y, child_width, child_height)
    
    @staticmethod
    def calculate_tile_horizontal(
        windows: list[Rect],
        parent: Rect
    ) -> list[Rect]:
        """Tile windows horizontally."""
        if not windows:
            return []
        
        tile_width = parent.width // len(windows)
        result = []
        
        for i, window in enumerate(windows):
            x = parent.x + i * tile_width
            result.append(Rect(x, parent.y, tile_width, parent.height))
        
        return result
    
    @staticmethod
    def calculate_tile_vertical(
        windows: list[Rect],
        parent: Rect
    ) -> list[Rect]:
        """Tile windows vertically."""
        if not windows:
            return []
        
        tile_height = parent.height // len(windows)
        result = []
        
        for i, window in enumerate(windows):
            y = parent.y + i * tile_height
            result.append(Rect(parent.x, y, parent.width, tile_height))
        
        return result
    
    @staticmethod
    def calculate_grid(
        windows: list[Rect],
        parent: Rect,
        cols: int = 2
    ) -> list[Rect]:
        """Calculate grid layout."""
        if not windows:
            return []
        
        rows = (len(windows) + cols - 1) // cols
        cell_width = parent.width // cols
        cell_height = parent.height // rows
        result = []
        
        for i, window in enumerate(windows):
            col = i % cols
            row = i // cols
            x = parent.x + col * cell_width
            y = parent.y + row * cell_height
            result.append(Rect(x, y, cell_width, cell_height))
        
        return result
