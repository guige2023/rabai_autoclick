"""
Coordinate transformation utilities for automation.

Provides coordinate conversions between screen, display,
window, and normalized coordinate systems.
"""

from __future__ import annotations

from typing import Optional, List, Tuple, Dict, Any
from dataclasses import dataclass
from enum import Enum


class CoordinateSystem(Enum):
    """Coordinate system types."""
    SCREEN = "screen"
    DISPLAY = "display"
    WINDOW = "window"
    NORMALIZED = "normalized"
    RELATIVE = "relative"


@dataclass
class Point:
    """2D point."""
    x: float
    y: float
    
    def __add__(self, other: "Point") -> "Point":
        return Point(self.x + other.x, self.y + other.y)
    
    def __sub__(self, other: "Point") -> "Point":
        return Point(self.x - other.x, self.y - other.y)
    
    def __mul__(self, scalar: float) -> "Point":
        return Point(self.x * scalar, self.y * scalar)
    
    def to_tuple(self) -> Tuple[float, float]:
        return (self.x, self.y)


@dataclass
class Rect:
    """Rectangle bounds."""
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
    def center(self) -> Point:
        return Point(self.x + self.width / 2, self.y + self.height / 2)
    
    @property
    def center_x(self) -> float:
        return self.x + self.width / 2
    
    @property
    def center_y(self) -> float:
        return self.y + self.height / 2
    
    def contains(self, point: Point) -> bool:
        return (self.x <= point.x < self.x + self.width and
                self.y <= point.y < self.y + self.height)
    
    def to_tuple(self) -> Tuple[float, float, float, float]:
        return (self.x, self.y, self.width, self.height)


@dataclass
class DisplayInfo:
    """Display information."""
    index: int
    bounds: Rect
    is_primary: bool
    scale_factor: float = 1.0


class CoordinateTransformer:
    """Transforms coordinates between systems."""
    
    def __init__(self):
        self._displays: List[DisplayInfo] = []
        self._primary_bounds: Optional[Rect] = None
        self._refresh_displays()
    
    def _refresh_displays(self) -> None:
        """Refresh display information."""
        try:
            import Quartz
            self._displays = []
            
            for i, screen in enumerate(Quartz.NSScreen.screens()):
                frame = screen.frame()
                is_primary = (i == 0)
                
                self._displays.append(DisplayInfo(
                    index=i,
                    bounds=Rect(
                        x=frame.origin.x,
                        y=frame.origin.y,
                        width=frame.size.width,
                        height=frame.size.height
                    ),
                    is_primary=is_primary,
                    scale_factor=screen.backingScaleFactor()
                ))
            
            main = Quartz.NSScreen.main()
            if main:
                self._primary_bounds = Rect(
                    x=main.frame().origin.x,
                    y=main.frame().origin.y,
                    width=main.frame().size.width,
                    height=main.frame().size.height
                )
        except Exception:
            self._displays = [
                DisplayInfo(
                    index=0,
                    bounds=Rect(0, 0, 1920, 1080),
                    is_primary=True
                )
            ]
            self._primary_bounds = Rect(0, 0, 1920, 1080)
    
    def get_displays(self) -> List[DisplayInfo]:
        """Get all displays."""
        return self._displays.copy()
    
    def get_primary_bounds(self) -> Rect:
        """Get primary display bounds."""
        if self._primary_bounds:
            return self._primary_bounds
        return self._displays[0].bounds if self._displays else Rect(0, 0, 1920, 1080)
    
    def screen_to_normalized(self, x: float, y: float) -> Tuple[float, float]:
        """
        Convert screen coordinates to normalized [0,1].
        
        Args:
            x: Screen X.
            y: Screen Y.
            
        Returns:
            Tuple of (norm_x, norm_y).
        """
        bounds = self.get_primary_bounds()
        norm_x = (x - bounds.x) / bounds.width
        norm_y = (y - bounds.y) / bounds.height
        return (max(0.0, min(1.0, norm_x)), max(0.0, min(1.0, norm_y)))
    
    def normalized_to_screen(self, x: float, y: float) -> Tuple[int, int]:
        """
        Convert normalized [0,1] to screen coordinates.
        
        Args:
            x: Normalized X (0-1).
            y: Normalized Y (0-1).
            
        Returns:
            Tuple of (screen_x, screen_y).
        """
        bounds = self.get_primary_bounds()
        sx = int(bounds.x + x * bounds.width)
        sy = int(bounds.y + y * bounds.height)
        return (sx, sy)
    
    def screen_to_display(self, x: float, y: float, display_index: int = 0) -> Tuple[float, float]:
        """
        Convert screen coordinates to display-relative.
        
        Args:
            x: Screen X.
            y: Screen Y.
            display_index: Target display index.
            
        Returns:
            Tuple of (display_x, display_y).
        """
        if display_index < len(self._displays):
            bounds = self._displays[display_index].bounds
            return (x - bounds.x, y - bounds.y)
        return (x, y)
    
    def display_to_screen(self, x: float, y: float, display_index: int = 0) -> Tuple[int, int]:
        """
        Convert display-relative to screen coordinates.
        
        Args:
            x: Display X.
            y: Display Y.
            display_index: Source display index.
            
        Returns:
            Tuple of (screen_x, screen_y).
        """
        if display_index < len(self._displays):
            bounds = self._displays[display_index].bounds
            return (int(bounds.x + x), int(bounds.y + y))
        return (int(x), int(y))
    
    def window_to_screen(self, win_x: float, win_y: float,
                         window_bounds: Rect) -> Tuple[int, int]:
        """
        Convert window-relative to screen coordinates.
        
        Args:
            win_x: Window X.
            win_y: Window Y.
            window_bounds: Window bounds.
            
        Returns:
            Tuple of (screen_x, screen_y).
        """
        return (int(window_bounds.x + win_x), int(window_bounds.y + win_y))
    
    def screen_to_window(self, scr_x: float, scr_y: float,
                         window_bounds: Rect) -> Tuple[float, float]:
        """
        Convert screen to window-relative coordinates.
        
        Args:
            scr_x: Screen X.
            scr_y: Screen Y.
            window_bounds: Window bounds.
            
        Returns:
            Tuple of (win_x, win_y).
        """
        return (scr_x - window_bounds.x, scr_y - window_bounds.y)
    
    def transform_point(self, point: Point,
                       from_system: CoordinateSystem,
                       to_system: CoordinateSystem,
                       context: Optional[Dict[str, Any]] = None) -> Point:
        """
        Transform point between coordinate systems.
        
        Args:
            point: Source point.
            from_system: Source coordinate system.
            to_system: Target coordinate system.
            context: Optional context (window_bounds, display_index, etc.).
            
        Returns:
            Transformed Point.
        """
        x, y = point.x, point.y
        
        if from_system == to_system:
            return Point(x, y)
        
        if from_system == CoordinateSystem.NORMALIZED:
            nx, ny = x, y
            x, y = self.normalized_to_screen(nx, ny)
            from_system = CoordinateSystem.SCREEN
        
        if to_system == CoordinateSystem.NORMALIZED:
            x, y = self.screen_to_normalized(x, y)
            return Point(x, y)
        
        if to_system == CoordinateSystem.DISPLAY and context:
            display_index = context.get('display_index', 0)
            x, y = self.screen_to_display(x, y, display_index)
        
        return Point(x, y)
    
    def get_display_for_point(self, x: float, y: float) -> Optional[DisplayInfo]:
        """
        Get display containing point.
        
        Args:
            x: Screen X.
            y: Screen Y.
            
        Returns:
            DisplayInfo or None.
        """
        for display in self._displays:
            if display.bounds.contains(Point(x, y)):
                return display
        return None
    
    def get_union_bounds(self) -> Rect:
        """Get union of all display bounds."""
        if not self._displays:
            return Rect(0, 0, 1920, 1080)
        
        min_x = min(d.bounds.x for d in self._displays)
        min_y = min(d.bounds.y for d in self._displays)
        max_x = max(d.bounds.right for d in self._displays)
        max_y = max(d.bounds.bottom for d in self._displays)
        
        return Rect(min_x, min_y, max_x - min_x, max_y - min_y)
