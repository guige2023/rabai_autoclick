"""
Screen geometry and display layout utilities.

Provides utilities for working with multi-display setups,
screen geometry calculations, and coordinate transformations.
"""

from __future__ import annotations

import subprocess
from typing import List, Optional, Tuple, Dict, Any
from dataclasses import dataclass
from enum import Enum


class DisplayOrientation(Enum):
    """Display orientation."""
    LANDSCAPE = "landscape"
    PORTRAIT = "portrait"
    FLIPPED_LANDSCAPE = "flipped_landscape"
    FLIPPED_PORTRAIT = "flipped_portrait"


@dataclass
class DisplayMode:
    """Display mode information."""
    width: int
    height: int
    refresh_rate: float
    origin_x: int
    origin_y: int
    scale_factor: float
    orientation: DisplayOrientation


@dataclass
class Display:
    """Represents a connected display."""
    display_id: int
    name: str
    is_main: bool
    bounds: "Rect"
    modes: List[DisplayMode]
    current_mode: Optional[DisplayMode] = None


@dataclass 
class Rect:
    """Rectangle for display bounds."""
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
    def center_x(self) -> int:
        return self.x + self.width // 2
    
    @property
    def center_y(self) -> int:
        return self.y + self.height // 2
    
    @property
    def center(self) -> Tuple[int, int]:
        return (self.center_x, self.center_y)


class DisplayManager:
    """Manages displays and screen geometry."""
    
    def __init__(self):
        """Initialize display manager."""
        self._displays: Optional[List[Display]] = None
    
    def get_displays(self, force_refresh: bool = False) -> List[Display]:
        """Get all connected displays.
        
        Args:
            force_refresh: Force cache refresh
            
        Returns:
            List of Display objects
        """
        if not force_refresh and self._displays is not None:
            return self._displays
        
        displays = []
        
        try:
            import Quartz
            
            for i, screen in enumerate(Quartz.CGGetActiveDisplayList(10, None, None)[1]):
                info = Quartz.CGDisplayCopyDisplayMode(screen)
                bounds = Quartz.CGRectMakeZero()
                Quartz.CGDisplayBounds(screen, bounds)
                
                is_main = screen == Quartz.CGMainDisplayID()
                
                # Get scale factor
                screen_size = Quartz.CGDisplayScreenSize(screen)
                scale = bounds.size.width / screen_size.width if screen_size.width > 0 else 1.0
                
                # Get display modes
                modes = []
                mode_list = Quartz.CGDisplayCopyAllDisplayModes(screen, None)
                if mode_list:
                    for mode in mode_list:
                        mode_info = DisplayMode(
                            width=Quartz.CGDisplayModeGetWidth(mode),
                            height=Quartz.CGDisplayModeGetHeight(mode),
                            refresh_rate=Quartz.CGDisplayModeGetRefreshRate(mode),
                            origin_x=0,
                            origin_y=0,
                            scale_factor=1.0,
                            orientation=DisplayOrientation.LANDSCAPE
                        )
                        modes.append(mode_info)
                
                # Current mode
                current = None
                if info:
                    current = DisplayMode(
                        width=Quartz.CGDisplayModeGetWidth(info),
                        height=Quartz.CGDisplayModeGetHeight(info),
                        refresh_rate=Quartz.CGDisplayModeGetRefreshRate(info),
                        origin_x=0,
                        origin_y=0,
                        scale_factor=scale,
                        orientation=DisplayOrientation.LANDSCAPE
                    )
                
                display = Display(
                    display_id=int(screen),
                    name=self._get_display_name(screen),
                    is_main=is_main,
                    bounds=Rect(
                        x=int(bounds.origin.x),
                        y=int(bounds.origin.y),
                        width=int(bounds.size.width),
                        height=int(bounds.size.height)
                    ),
                    modes=modes,
                    current_mode=current
                )
                
                displays.append(display)
                
        except Exception:
            # Fallback to main display
            displays.append(Display(
                display_id=0,
                name="Main Display",
                is_main=True,
                bounds=Rect(0, 0, 1920, 1080),
                modes=[],
                current_mode=DisplayMode(1920, 1080, 60, 0, 0, 1.0, DisplayOrientation.LANDSCAPE)
            ))
        
        self._displays = displays
        return displays
    
    def _get_display_name(self, display_id: int) -> str:
        """Get display name.
        
        Args:
            display_id: Display ID
            
        Returns:
            Display name
        """
        try:
            info = Quartz.CGDisplayCopyDisplayMode(display_id)
            if info:
                return Quartz.CGDisplayModeGetName(info)
        except Exception:
            pass
        
        return f"Display {display_id}"
    
    def get_main_display(self) -> Optional[Display]:
        """Get the main display.
        
        Returns:
            Main Display or None
        """
        displays = self.get_displays()
        for display in displays:
            if display.is_main:
                return display
        return displays[0] if displays else None
    
    def get_display_at_point(self, x: int, y: int) -> Optional[Display]:
        """Get display containing a point.
        
        Args:
            x: X coordinate
            y: Y coordinate
            
        Returns:
            Display at point or None
        """
        displays = self.get_displays()
        for display in displays:
            if (display.bounds.x <= x < display.bounds.right and
                display.bounds.y <= y < display.bounds.bottom):
                return display
        return None
    
    def get_display_at_index(self, index: int) -> Optional[Display]:
        """Get display by index.
        
        Args:
            index: Display index
            
        Returns:
            Display or None
        """
        displays = self.get_displays()
        if 0 <= index < len(displays):
            return displays[index]
        return None
    
    def get_workspace_bounds(self) -> Rect:
        """Get bounds of all displays combined.
        
        Returns:
            Combined bounding Rect
        """
        displays = self.get_displays()
        
        if not displays:
            return Rect(0, 0, 1920, 1080)
        
        min_x = min(d.bounds.x for d in displays)
        min_y = min(d.bounds.y for d in displays)
        max_x = max(d.bounds.right for d in displays)
        max_y = max(d.bounds.bottom for d in displays)
        
        return Rect(min_x, min_y, max_x - min_x, max_y - min_y)
    
    def get_menu_bar_height(self) -> int:
        """Get height of menu bar on main display.
        
        Returns:
            Menu bar height in pixels
        """
        try:
            result = subprocess.run(
                ["defaults", "read", "com.apple.springing", "menubar"],
                capture_output=True,
                text=True,
                timeout=2
            )
            
            if result.returncode == 0:
                value = result.stdout.strip()
                if value == "true":
                    return 22
        except Exception:
            pass
        
        return 24  # Default menu bar height
    
    def get_dock_size(self) -> Tuple[int, int]:
        """Get Dock size.
        
        Returns:
            Tuple of (width, height)
        """
        try:
            result = subprocess.run(
                ["defaults", "read", "com.apple.dock", "tilesize"],
                capture_output=True,
                text=True,
                timeout=2
            )
            
            if result.returncode == 0:
                size = int(result.stdout.strip())
                magnification = subprocess.run(
                    ["defaults", "read", "com.apple.dock", "magnification"],
                    capture_output=True,
                    text=True,
                    timeout=2
                )
                
                if magnification.returncode == 0 and magnification.stdout.strip() == "1":
                    max_size = subprocess.run(
                        ["defaults", "read", "com.apple.dock", "maxsize"],
                        capture_output=True,
                        text=True,
                        timeout=2
                    )
                    if max_size.returncode == 0:
                        size = int(max_size.stdout.strip())
                
                return (size, size)
        except Exception:
            pass
        
        return (48, 48)  # Default Dock size
    
    def get_dock_position(self) -> str:
        """Get Dock position.
        
        Returns:
            Position string ('bottom', 'left', 'right')
        """
        try:
            result = subprocess.run(
                ["defaults", "read", "com.apple.dock", "orientation"],
                capture_output=True,
                text=True,
                timeout=2
            )
            
            if result.returncode == 0:
                return result.stdout.strip()
        except Exception:
            pass
        
        return "bottom"


def get_display_count() -> int:
    """Get number of connected displays.
    
    Returns:
        Display count
    """
    manager = DisplayManager()
    return len(manager.get_displays())


def get_primary_display_resolution() -> Tuple[int, int]:
    """Get primary display resolution.
    
    Returns:
        Tuple of (width, height)
    """
    manager = DisplayManager()
    main = manager.get_main_display()
    if main and main.current_mode:
        return (main.current_mode.width, main.current_mode.height)
    return (1920, 1080)


def point_in_display(point: Tuple[int, int]) -> bool:
    """Check if point is in any display bounds.
    
    Args:
        point: (x, y) tuple
        
    Returns:
        True if point is in a display
    """
    manager = DisplayManager()
    return manager.get_display_at_point(point[0], point[1]) is not None


def point_to_display_coordinates(
    x: int, 
    y: int,
    source_display_index: int = 0,
    target_display_index: int = 0
) -> Tuple[int, int]:
    """Convert coordinates between displays.
    
    Args:
        x: Source X coordinate
        y: Source Y coordinate
        source_display_index: Source display index
        target_display_index: Target display index
        
    Returns:
        Converted (x, y) coordinates
    """
    manager = DisplayManager()
    
    source = manager.get_display_at_index(source_display_index)
    target = manager.get_display_at_index(target_display_index)
    
    if not source or not target:
        return (x, y)
    
    # Offset between displays
    offset_x = target.bounds.x - source.bounds.x
    offset_y = target.bounds.y - source.bounds.y
    
    return (x + offset_x, y + offset_y)


def normalize_to_display(
    x: int,
    y: int,
    display_index: int = 0
) -> Tuple[int, int]:
    """Normalize coordinates to be relative to display origin.
    
    Args:
        x: X coordinate
        y: Y coordinate
        display_index: Display index
        
    Returns:
        Normalized (x, y) coordinates
    """
    manager = DisplayManager()
    display = manager.get_display_at_index(display_index)
    
    if not display:
        return (x, y)
    
    return (x - display.bounds.x, y - display.bounds.y)


def get_visible_workspace_bounds() -> Dict[str, int]:
    """Get workspace bounds excluding menu bar and Dock.
    
    Returns:
        Dictionary with 'x', 'y', 'width', 'height'
    """
    manager = DisplayManager()
    main = manager.get_main_display()
    
    if not main:
        return {'x': 0, 'y': 0, 'width': 1920, 'height': 1080}
    
    menu_bar_height = manager.get_menu_bar_height()
    dock_size, dock_position = manager.get_dock_size()
    
    x = main.bounds.x
    y = main.bounds.y + menu_bar_height
    
    width = main.bounds.width
    height = main.bounds.height - menu_bar_height
    
    if dock_position == "bottom":
        height -= dock_size
    elif dock_position == "left" or dock_position == "right":
        width -= dock_size
    
    return {
        'x': x,
        'y': y,
        'width': width,
        'height': height
    }
