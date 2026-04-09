"""Multi-Monitor Display Utilities.

This module provides utilities for managing multiple displays, including
monitor enumeration, display configuration, resolution management, and
cross-monitor coordinate transformations.

Example:
    >>> from multi_monitor_utils import DisplayManager, DisplayInfo
    >>> manager = DisplayManager()
    >>> displays = manager.get_all_displays()
    >>> print(f"Found {len(displays)} displays")
"""

from __future__ import annotations

import subprocess
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Dict, List, Optional, Tuple


class DisplayArrangement(Enum):
    """Display arrangement modes."""
    HORIZONTAL = auto()
    VERTICAL = auto()
    CUSTOM = auto()


class DisplayRotation(Enum):
    """Display rotation angles."""
    NORMAL = 0
    ROTATED_90 = 90
    ROTATED_180 = 180
    ROTATED_270 = 270


@dataclass
class DisplayMode:
    """Represents a display resolution mode."""
    width: int
    height: int
    refresh_rate: float
    bit_depth: int = 32
    is_native: bool = False
    
    @property
    def resolution(self) -> Tuple[int, int]:
        return (self.width, self.height)
    
    @property
    def aspect_ratio(self) -> float:
        return self.width / self.height if self.height > 0 else 0.0
    
    def __str__(self) -> str:
        return f"{self.width}x{self.height}@{self.refresh_rate:.0f}Hz"


@dataclass
class DisplayGeometry:
    """Geometry information for a display."""
    x: int
    y: int
    width: int
    height: int
    bounds: Tuple[int, int, int, int] = field(init=False)
    
    def __post_init__(self):
        self.bounds = (self.x, self.y, self.width, self.height)
    
    @property
    def frame(self) -> Tuple[int, int, int, int]:
        """Get frame as (x, y, width, height)."""
        return self.bounds
    
    @property
    def center(self) -> Tuple[int, int]:
        """Get center point."""
        return (self.x + self.width // 2, self.y + self.height // 2)
    
    def contains_point(self, x: int, y: int) -> bool:
        """Check if point is within display bounds."""
        return self.x <= x < self.x + self.width and self.y <= y < self.y + self.height
    
    def intersection(self, other: DisplayGeometry) -> Optional[DisplayGeometry]:
        """Get intersection with another display geometry."""
        x1 = max(self.x, other.x)
        y1 = max(self.y, other.y)
        x2 = min(self.x + self.width, other.x + other.width)
        y2 = min(self.y + self.height, other.y + other.height)
        
        if x1 < x2 and y1 < y2:
            return DisplayGeometry(x1, y1, x2 - x1, y2 - y1)
        return None


@dataclass
class DisplayInfo:
    """Detailed information about a display."""
    display_id: int
    name: str
    is_main: bool
    is_built_in: bool
    geometry: DisplayGeometry
    modes: List[DisplayMode] = field(default_factory=list)
    current_mode: Optional[DisplayMode] = None
    origin: Tuple[int, int] = field(default_factory=tuple)
    rotation: DisplayRotation = DisplayRotation.NORMAL
    scale_factor: float = 1.0
    menu_bar_visible: bool = True
    
    @property
    def frame(self) -> Tuple[int, int, int, int]:
        """Get display frame."""
        return (
            self.origin[0],
            self.origin[1],
            self.geometry.width,
            self.geometry.height,
        )
    
    def point_in_display(self, x: int, y: int) -> bool:
        """Check if a point is within this display."""
        return self.geometry.contains_point(x, y)
    
    def transform_point(self, x: int, y: int) -> Tuple[int, int]:
        """Transform point to this display's coordinate system."""
        return (x - self.origin[0], y - self.origin[1])


class DisplayManager:
    """Manages multiple displays and their configuration.
    
    Provides access to display information, mode switching, and
    coordinate transformation across multiple monitors.
    
    Attributes:
        displays: Dictionary of display_id to DisplayInfo
    """
    
    def __init__(self):
        self._displays: Dict[int, DisplayInfo] = {}
        self._main_display_id: Optional[int] = None
        self._refresh_display_cache()
    
    def _refresh_display_cache(self) -> None:
        """Refresh internal display cache."""
        self._displays.clear()
        
        try:
            result = subprocess.run(
                ['system_profiler', 'SPDisplaysDataType', '-json'],
                capture_output=True,
                text=True,
                timeout=5,
            )
            
            if result.returncode == 0:
                import json
                data = json.loads(result.stdout)
                
                displays = data.get('SPDisplaysDataType', [])
                if isinstance(displays, list):
                    for i, display in enumerate(displays):
                        info = self._parse_display_info(display, i)
                        self._displays[info.display_id] = info
                elif isinstance(displays, dict):
                    for display_id, display in displays.items():
                        info = self._parse_display_info(display, int(display_id))
                        self._displays[info.display_id] = info
                        
        except Exception:
            self._displays[0] = DisplayInfo(
                display_id=0,
                name="Main Display",
                is_main=True,
                is_built_in=True,
                geometry=DisplayGeometry(0, 0, 1920, 1080),
            )
    
    def _parse_display_info(self, data: Dict, display_id: int) -> DisplayInfo:
        """Parse display info from system_profiler data."""
        name = data.get('_name', f'Display {display_id}')
        is_main = display_id == 0
        is_built_in = data.get('spdisplays_builtin', False)
        
        current_res = data.get('spdisplays_main', {})
        width = current_res.get('spdisplays_resolution', '1920x1080').split('x')
        w, h = int(width[0]) if len(width) > 0 else 1920, int(width[1]) if len(width) > 1 else 1080
        
        geometry = DisplayGeometry(0, 0, w, h)
        
        return DisplayInfo(
            display_id=display_id,
            name=name,
            is_main=is_main,
            is_built_in=is_built_in,
            geometry=geometry,
        )
    
    def get_all_displays(self) -> List[DisplayInfo]:
        """Get information for all connected displays.
        
        Returns:
            List of DisplayInfo objects
        """
        return list(self._displays.values())
    
    def get_display(self, display_id: int) -> Optional[DisplayInfo]:
        """Get information for a specific display.
        
        Args:
            display_id: Display identifier
            
        Returns:
            DisplayInfo or None if not found
        """
        return self._displays.get(display_id)
    
    def get_main_display(self) -> Optional[DisplayInfo]:
        """Get the main display.
        
        Returns:
            Main DisplayInfo or None
        """
        for display in self._displays.values():
            if display.is_main:
                return display
        return None if not self._displays else list(self._displays.values())[0]
    
    def get_display_at_point(self, x: int, y: int) -> Optional[DisplayInfo]:
        """Get the display containing a given point.
        
        Args:
            x: X coordinate
            y: Y coordinate
            
        Returns:
            DisplayInfo or None
        """
        for display in self._displays.values():
            if display.point_in_display(x, y):
                return display
        return None
    
    def transform_point_between_displays(
        self,
        x: int,
        y: int,
        from_display: int,
        to_display: int,
    ) -> Tuple[int, int]:
        """Transform coordinates from one display to another.
        
        Args:
            x: Source X coordinate
            y: Source Y coordinate
            from_display: Source display ID
            to_display: Target display ID
            
        Returns:
            Transformed (x, y) coordinates
        """
        from_info = self._displays.get(from_display)
        to_info = self._displays.get(to_display)
        
        if not from_info or not to_info:
            return (x, y)
        
        local_x, local_y = from_info.transform_point(x, y)
        return (local_x + to_info.origin[0], local_y + to_info.origin[1])
    
    def get_combined_geometry(self) -> DisplayGeometry:
        """Get the combined geometry of all displays.
        
        Returns:
            DisplayGeometry covering all displays
        """
        if not self._displays:
            return DisplayGeometry(0, 0, 1920, 1080)
        
        min_x = min(d.origin[0] for d in self._displays.values())
        min_y = min(d.origin[1] for d in self._displays.values())
        max_x = max(d.origin[0] + d.geometry.width for d in self._displays.values())
        max_y = max(d.origin[1] + d.geometry.height for d in self._displays.values())
        
        return DisplayGeometry(min_x, min_y, max_x - min_x, max_y - min_y)
    
    def set_display_mode(self, display_id: int, mode: DisplayMode) -> bool:
        """Set display mode for a specific display.
        
        Args:
            display_id: Display to configure
            mode: Target DisplayMode
            
        Returns:
            True if successful
        """
        return False
    
    def get_displays_arrangement(self) -> DisplayArrangement:
        """Determine display arrangement mode.
        
        Returns:
            DisplayArrangement enum value
        """
        if len(self._displays) < 2:
            return DisplayArrangement.HORIZONTAL
        
        displays = list(self._displays.values())
        if len(displays) == 2:
            d1, d2 = displays[0], displays[1]
            
            if d1.origin[1] == d2.origin[1]:
                return DisplayArrangement.HORIZONTAL
            elif d1.origin[0] == d2.origin[0]:
                return DisplayArrangement.VERTICAL
        
        return DisplayArrangement.CUSTOM
    
    def refresh(self) -> None:
        """Refresh display information."""
        self._refresh_display_cache()


class MonitorHotplugHandler:
    """Handles display hotplug events."""
    
    def __init__(self, callback=None):
        self._callback = callback
        self._previous_displays: Dict[int, DisplayInfo] = {}
    
    def check_for_changes(self, current_displays: List[DisplayInfo]) -> Tuple[List, List]:
        """Check for display changes since last check.
        
        Returns:
            (added_displays, removed_displays)
        """
        current_ids = {d.display_id for d in current_displays}
        previous_ids = set(self._previous_displays.keys())
        
        added = [d for d in current_displays if d.display_id not in previous_ids]
        removed = [self._previous_displays[id_] for id_ in previous_ids if id_ not in current_ids]
        
        self._previous_displays = {d.display_id: d for d in current_displays}
        
        return (added, removed)
    
    def notify_callback(self, event_type: str, display: DisplayInfo) -> None:
        """Notify callback of display event."""
        if self._callback:
            try:
                self._callback(event_type, display)
            except Exception:
                pass
