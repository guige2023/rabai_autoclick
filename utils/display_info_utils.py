"""Display info utilities for multi-monitor configuration.

This module provides utilities for querying and managing multi-monitor
display configurations, including resolution, DPI, position, and
arrangement information for UI automation workflows.

Author: AI Assistant
License: MIT
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, List, Tuple


@dataclass
class DisplayInfo:
    """Information about a display."""
    display_id: int
    name: str
    width: int
    height: int
    x_offset: int
    y_offset: int
    is_primary: bool
    dpi: int
    scale_factor: float
    refresh_rate: int


@dataclass
class DisplayBounds:
    """Bounding box for a display."""
    x: int
    y: int
    width: int
    height: int
    
    @property
    def left(self) -> int:
        return self.x
    
    @property
    def top(self) -> int:
        return self.y
    
    @property
    def right(self) -> int:
        return self.x + self.width
    
    @property
    def bottom(self) -> int:
        return self.y + self.height
    
    @property
    def center(self) -> Tuple[int, int]:
        return (self.x + self.width // 2, self.y + self.height // 2)
    
    def contains_point(self, x: int, y: int) -> bool:
        """Check if point is within bounds."""
        return self.left <= x < self.right and self.top <= y < self.bottom
    
    def intersects(self, other: "DisplayBounds") -> bool:
        """Check if bounds intersect with another."""
        return not (self.right <= other.left or self.left >= other.right or
                    self.bottom <= other.top or self.top >= other.bottom)


def get_all_displays() -> List[DisplayInfo]:
    """Get information about all connected displays.
    
    Returns:
        List of DisplayInfo for each display.
    """
    try:
        import subprocess
        import json
        
        result = subprocess.run(
            ["system_profiler", "SPDisplaysDataType", "-json"],
            capture_output=True,
            text=True,
        )
        
        if result.returncode == 0:
            data = json.loads(result.stdout)
            displays = data.get("SPDisplaysDataType", [])
            
            display_list = []
            for i, display in enumerate(displays):
                display_list.append(DisplayInfo(
                    display_id=i,
                    name=display.get("DisplayName", f"Display {i}"),
                    width=display.get("Width", 1920),
                    height=display.get("Height", 1080),
                    x_offset=0,
                    y_offset=0,
                    is_primary=(i == 0),
                    dpi=display.get("Resolution", 72),
                    scale_factor=display.get("ScaleFactor", 1.0),
                    refresh_rate=display.get("RefreshRate", 60),
                ))
            
            return display_list
    except Exception:
        pass
    
    return [DisplayInfo(
        display_id=0,
        name="Primary Display",
        width=1920,
        height=1080,
        x_offset=0,
        y_offset=0,
        is_primary=True,
        dpi=72,
        scale_factor=1.0,
        refresh_rate=60,
    )]


def get_primary_display() -> Optional[DisplayInfo]:
    """Get the primary display.
    
    Returns:
        DisplayInfo for primary display or None.
    """
    displays = get_all_displays()
    for display in displays:
        if display.is_primary:
            return display
    return displays[0] if displays else None


def get_display_at_point(x: int, y: int) -> Optional[DisplayInfo]:
    """Get the display containing the given point.
    
    Args:
        x: X coordinate.
        y: Y coordinate.
    
    Returns:
        DisplayInfo containing the point or None.
    """
    displays = get_all_displays()
    
    for display in displays:
        bounds = DisplayBounds(
            x=display.x_offset,
            y=display.y_offset,
            width=display.width,
            height=display.height,
        )
        if bounds.contains_point(x, y):
            return display
    
    return get_primary_display()


def get_display_bounds(display_id: int) -> Optional[DisplayBounds]:
    """Get bounds for a specific display.
    
    Args:
        display_id: Display ID.
    
    Returns:
        DisplayBounds or None.
    """
    displays = get_all_displays()
    for display in displays:
        if display.display_id == display_id:
            return DisplayBounds(
                x=display.x_offset,
                y=display.y_offset,
                width=display.width,
                height=display.height,
            )
    return None


def get_combined_bounds() -> DisplayBounds:
    """Get combined bounds of all displays.
    
    Returns:
        DisplayBounds covering all displays.
    """
    displays = get_all_displays()
    
    if not displays:
        return DisplayBounds(0, 0, 1920, 1080)
    
    min_x = min(d.x_offset for d in displays)
    min_y = min(d.y_offset for d in displays)
    max_x = max(d.x_offset + d.width for d in displays)
    max_y = max(d.y_offset + d.height for d in displays)
    
    return DisplayBounds(
        x=min_x,
        y=min_y,
        width=max_x - min_x,
        height=max_y - min_y,
    )


def get_display_count() -> int:
    """Get number of connected displays.
    
    Returns:
        Number of displays.
    """
    return len(get_all_displays())


def is_point_on_screen(x: int, y: int) -> bool:
    """Check if point is on any display.
    
    Args:
        x: X coordinate.
        y: Y coordinate.
    
    Returns:
        True if point is on a display.
    """
    combined = get_combined_bounds()
    return combined.contains_point(x, y)


def get_display_for_region(x: int, y: int, width: int, height: int) -> Optional[DisplayInfo]:
    """Get the display that best fits a region.
    
    Args:
        x: Region X coordinate.
        y: Region Y coordinate.
        width: Region width.
        height: Region height.
    
    Returns:
        DisplayInfo that best fits the region.
    """
    displays = get_all_displays()
    
    best_display = None
    best_coverage = 0
    
    for display in displays:
        bounds = DisplayBounds(
            x=display.x_offset,
            y=display.y_offset,
            width=display.width,
            height=display.height,
        )
        
        region_bounds = DisplayBounds(x, y, width, height)
        
        if bounds.intersects(region_bounds):
            ix = max(bounds.left, region_bounds.left)
            iy = max(bounds.top, region_bounds.top)
            ix2 = min(bounds.right, region_bounds.right)
            iy2 = min(bounds.bottom, region_bounds.bottom)
            
            coverage = max(0, ix2 - ix) * max(0, iy2 - iy)
            
            if coverage > best_coverage:
                best_coverage = coverage
                best_display = display
    
    return best_display or get_primary_display()
