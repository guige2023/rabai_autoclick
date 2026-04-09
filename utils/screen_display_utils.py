"""
Screen display utilities.

Manage and query screen displays.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


@dataclass
class DisplayInfo:
    """Information about a display."""
    display_id: int
    name: str
    x: int
    y: int
    width: int
    height: int
    scale_factor: float = 1.0
    is_main: bool = False


class DisplayManager:
    """Manage multiple displays."""
    
    def __init__(self):
        self._displays: dict[int, DisplayInfo] = {}
        self._main_display_id: Optional[int] = None
    
    def add_display(self, display: DisplayInfo) -> None:
        """Add a display."""
        self._displays[display.display_id] = display
        if display.is_main:
            self._main_display_id = display.display_id
    
    def remove_display(self, display_id: int) -> bool:
        """Remove a display."""
        if display_id in self._displays:
            del self._displays[display_id]
            return True
        return False
    
    def get_display(self, display_id: int) -> Optional[DisplayInfo]:
        """Get display by ID."""
        return self._displays.get(display_id)
    
    def get_main_display(self) -> Optional[DisplayInfo]:
        """Get main display."""
        if self._main_display_id is not None:
            return self._displays.get(self._main_display_id)
        
        for display in self._displays.values():
            if display.is_main:
                return display
        
        return None if not self._displays else next(iter(self._displays.values()))
    
    def get_display_at(self, x: int, y: int) -> Optional[DisplayInfo]:
        """Get display containing point."""
        for display in self._displays.values():
            if display.x <= x < display.x + display.width and display.y <= y < display.y + display.height:
                return display
        return None
    
    def get_all_displays(self) -> list[DisplayInfo]:
        """Get all displays."""
        return list(self._displays.values())
    
    def get_total_bounds(self) -> tuple[int, int, int, int]:
        """Get bounding rect of all displays."""
        if not self._displays:
            return 0, 0, 0, 0
        
        min_x = min(d.x for d in self._displays.values())
        min_y = min(d.y for d in self._displays.values())
        max_x = max(d.x + d.width for d in self._displays.values())
        max_y = max(d.y + d.height for d in self._displays.values())
        
        return (min_x, min_y, max_x - min_x, max_y - min_y)
