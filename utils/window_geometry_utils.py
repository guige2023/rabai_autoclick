"""
Window geometry and bounds management utilities.

Handles window positioning, sizing, multi-monitor support,
and window arrangement operations.

Author: Auto-generated
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from enum import Enum, auto
from typing import Sequence


class AnchorPosition(Enum):
    """Anchor positions for window placement."""
    TOP_LEFT = auto()
    TOP_CENTER = auto()
    TOP_RIGHT = auto()
    CENTER_LEFT = auto()
    CENTER = auto()
    CENTER_RIGHT = auto()
    BOTTOM_LEFT = auto()
    BOTTOM_CENTER = auto()
    BOTTOM_RIGHT = auto()


@dataclass
class Geometry:
    """Window or element geometry."""
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
    def center_x(self) -> float:
        return self.x + self.width / 2
    
    @property
    def center_y(self) -> float:
        return self.y + self.height / 2
    
    @property
    def center(self) -> tuple[float, float]:
        return (self.center_x, self.center_y)
    
    @property
    def area(self) -> float:
        return self.width * self.height
    
    def contains_point(self, px: float, py: float) -> bool:
        """Check if point is within geometry."""
        return self.left <= px <= self.right and self.top <= py <= self.bottom
    
    def contains_geometry(self, other: Geometry) -> bool:
        """Check if another geometry is fully contained."""
        return (
            self.left <= other.left
            and self.right >= other.right
            and self.top <= other.top
            and self.bottom >= other.bottom
        )
    
    def intersects(self, other: Geometry) -> bool:
        """Check if geometries intersect."""
        return not (
            self.right < other.left
            or self.left > other.right
            or self.bottom < other.top
            or self.top > other.bottom
        )
    
    def distance_to(self, other: Geometry) -> float:
        """Calculate distance between geometries (center points)."""
        dx = self.center_x - other.center_x
        dy = self.center_y - other.center_y
        return math.sqrt(dx * dx + dy * dy)
    
    def union(self, other: Geometry) -> Geometry:
        """Get bounding box containing both geometries."""
        left = min(self.left, other.left)
        top = min(self.top, other.top)
        right = max(self.right, other.right)
        bottom = max(self.bottom, other.bottom)
        return Geometry(left, top, right - left, bottom - top)
    
    def intersection(self, other: Geometry) -> Geometry | None:
        """Get intersection of two geometries."""
        left = max(self.left, other.left)
        top = max(self.top, other.top)
        right = min(self.right, other.right)
        bottom = min(self.bottom, other.bottom)
        
        if left >= right or top >= bottom:
            return None
        
        return Geometry(left, top, right - left, bottom - top)
    
    def to_tuple(self) -> tuple[float, float, float, float]:
        """Convert to (x, y, width, height) tuple."""
        return (self.x, self.y, self.width, self.height)
    
    def to_bounds_tuple(self) -> tuple[float, float, float, float]:
        """Convert to (left, top, right, bottom) tuple."""
        return (self.left, self.top, self.right, self.bottom)


@dataclass
class Screen:
    """Display screen information."""
    id: str
    name: str
    geometry: Geometry
    work_area: Geometry
    scale_factor: float = 1.0
    is_primary: bool = False
    
    @property
    def bounds(self) -> Geometry:
        """Alias for geometry."""
        return self.geometry


class WindowArranger:
    """
    Arranges windows on screen.
    
    Example:
        arranger = WindowArranger()
        screens = arranger.get_screens()
        arranger.tile_windows([w1, w2, w3], screen=screens[0])
    """
    
    def __init__(self, screens: Sequence[Screen] | None = None):
        self._screens = list(screens) if screens else []
    
    def add_screen(self, screen: Screen) -> None:
        """Add a screen to the arranger."""
        self._screens.append(screen)
    
    def get_screen_at(self, x: float, y: float) -> Screen | None:
        """Get screen containing the given point."""
        for screen in self._screens:
            if screen.geometry.contains_point(x, y):
                return screen
        return None
    
    def get_primary_screen(self) -> Screen | None:
        """Get the primary screen."""
        for screen in self._screens:
            if screen.is_primary:
                return screen
        return self._screens[0] if self._screens else None
    
    def tile_windows(
        self,
        windows: Sequence[Geometry],
        screen: Screen,
        padding: float = 10,
        direction: str = "horizontal",
    ) -> list[Geometry]:
        """
        Tile windows within a screen's work area.
        
        Args:
            windows: List of window geometries
            screen: Screen to tile within
            padding: Padding between windows
            direction: 'horizontal' or 'vertical'
            
        Returns:
            List of positioned geometries
        """
        if not windows:
            return []
        
        work = screen.work_area
        n = len(windows)
        
        if direction == "horizontal":
            window_width = (work.width - padding * (n + 1)) / n
            window_height = work.height - padding * 2
            
            return [
                Geometry(
                    x=work.x + padding * (i + 1) + window_width * i,
                    y=work.y + padding,
                    width=window_width,
                    height=window_height,
                )
                for i in range(n)
            ]
        else:
            window_width = work.width - padding * 2
            window_height = (work.height - padding * (n + 1)) / n
            
            return [
                Geometry(
                    x=work.x + padding,
                    y=work.y + padding * (i + 1) + window_height * i,
                    width=window_width,
                    height=window_height,
                )
                for i in range(n)
            ]
    
    def cascade_windows(
        self,
        windows: Sequence[Geometry],
        screen: Screen,
        offset_x: float = 30,
        offset_y: float = 30,
        base_width: float | None = None,
        base_height: float | None = None,
    ) -> list[Geometry]:
        """
        Cascade windows with offset.
        
        Args:
            windows: List of window geometries
            screen: Screen to cascade within
            offset_x: Horizontal offset between windows
            offset_y: Vertical offset between windows
            base_width: Starting width (default: screen work area width)
            base_height: Starting height (default: screen work area height)
            
        Returns:
            List of positioned geometries
        """
        if not windows:
            return []
        
        work = screen.work_area
        width = base_width or work.width * 0.7
        height = base_height or work.height * 0.7
        
        return [
            Geometry(
                x=work.x + offset_x * i,
                y=work.y + offset_y * i,
                width=min(width, work.width - offset_x * i),
                height=min(height, work.height - offset_y * i),
            )
            for i in range(len(windows))
        ]
    
    def maximize_within(
        self,
        window: Geometry,
        screen: Screen,
        padding: float = 0,
    ) -> Geometry:
        """Position window to maximize within screen work area."""
        work = screen.work_area
        return Geometry(
            x=work.x + padding,
            y=work.y + padding,
            width=work.width - padding * 2,
            height=work.height - padding * 2,
        )
    
    def center_on_screen(
        self,
        window: Geometry,
        screen: Screen,
    ) -> Geometry:
        """Center window on screen."""
        return Geometry(
            x=screen.work_area.center_x - window.width / 2,
            y=screen.work_area.center_y - window.height / 2,
            width=window.width,
            height=window.height,
        )
    
    def align_to_anchor(
        self,
        window: Geometry,
        anchor: AnchorPosition,
        screen: Screen,
        margin: float = 10,
    ) -> Geometry:
        """Align window to anchor position on screen."""
        work = screen.work_area
        
        # Calculate x position
        if anchor in (
            AnchorPosition.TOP_LEFT, AnchorPosition.CENTER_LEFT,
            AnchorPosition.BOTTOM_LEFT
        ):
            x = work.x + margin
        elif anchor in (
            AnchorPosition.TOP_CENTER, AnchorPosition.CENTER,
            AnchorPosition.BOTTOM_CENTER
        ):
            x = work.center_x - window.width / 2
        else:
            x = work.right - window.width - margin
        
        # Calculate y position
        if anchor in (
            AnchorPosition.TOP_LEFT, AnchorPosition.TOP_CENTER,
            AnchorPosition.TOP_RIGHT
        ):
            y = work.y + margin
        elif anchor in (
            AnchorPosition.CENTER_LEFT, AnchorPosition.CENTER,
            AnchorPosition.CENTER_RIGHT
        ):
            y = work.center_y - window.height / 2
        else:
            y = work.bottom - window.height - margin
        
        return Geometry(x, y, window.width, window.height)


def calculate_overlap_score(
    window: Geometry,
    reference: Geometry,
    weight_x: float = 0.5,
    weight_y: float = 0.5,
) -> float:
    """
    Calculate how well a window overlaps with a reference position.
    
    Returns score from 0.0 (no overlap) to 1.0 (perfect center match).
    """
    if not window.intersects(reference):
        return 0.0
    
    # Calculate center distance
    dx = abs(window.center_x - reference.center_x) / reference.width
    dy = abs(window.center_y - reference.center_y) / reference.height
    
    # Calculate overlap area
    intersection = window.intersection(reference)
    overlap_area = intersection.area if intersection else 0
    overlap_ratio = overlap_area / max(window.area, 1)
    
    # Combined score
    center_score = 1.0 - (dx + dy) / 2
    return (center_score * weight_x + overlap_ratio * weight_y) / (weight_x + weight_y)
