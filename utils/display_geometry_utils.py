"""
Display geometry utilities for multi-monitor setups.

This module provides utilities for querying and manipulating
display geometry, including resolution, position, and arrangement.
"""

from __future__ import annotations

import platform
from typing import List, Tuple, Optional, Dict, Any
from dataclasses import dataclass, field
from enum import Enum, auto


IS_MACOS: bool = platform.system() == 'Darwin'


class DisplayOrientation(Enum):
    """Display orientation modes."""
    PORTRAIT = auto()
    LANDSCAPE = auto()
    PORTRAIT_FLIPPED = auto()
    LANDSCAPE_FLIPPED = auto()


@dataclass
class DisplayGeometry:
    """
    Complete geometry information for a display.

    Attributes:
        display_id: Unique display identifier.
        x: X position of top-left corner.
        y: Y position of top-left corner.
        width: Display width in pixels.
        height: Display height in pixels.
        work_x: X position of work area (excludes menu bar).
        work_y: Y position of work area.
        work_width: Work area width.
        work_height: Work area height.
        scale_factor: DPI scale factor.
        orientation: Display orientation.
        is_primary: Whether this is the primary display.
        is_built_in: Whether this is a built-in display.
        refresh_rate: Refresh rate in Hz.
    """
    display_id: str
    x: int
    y: int
    width: int
    height: int
    work_x: int
    work_y: int
    work_width: int
    work_height: int
    scale_factor: float = 1.0
    orientation: DisplayOrientation = DisplayOrientation.LANDSCAPE
    is_primary: bool = False
    is_built_in: bool = False
    refresh_rate: float = 60.0

    @property
    def frame(self) -> Tuple[int, int, int, int]:
        """Display frame as (x, y, width, height)."""
        return (self.x, self.y, self.width, self.height)

    @property
    def work_frame(self) -> Tuple[int, int, int, int]:
        """Work area frame as (x, y, width, height)."""
        return (self.work_x, self.work_y, self.work_width, self.work_height)

    @property
    def center(self) -> Tuple[int, int]:
        """Center point of display."""
        return (self.x + self.width // 2, self.y + self.height // 2)

    @property
    def work_center(self) -> Tuple[int, int]:
        """Center point of work area."""
        return (
            self.work_x + self.work_width // 2,
            self.work_y + self.work_height // 2
        )

    @property
    def resolution(self) -> Tuple[int, int]:
        """Resolution as (width, height)."""
        return (self.width, self.height)

    @property
    def pixel_count(self) -> int:
        """Total number of pixels."""
        return self.width * self.height

    @property
    def diagonal_inches(self) -> float:
        """Diagonal size in inches (assuming standard DPI)."""
        import math
        diag_pixels = math.sqrt(self.width ** 2 + self.height ** 2)
        # Assume 96 DPI as baseline
        return diag_pixels / 96.0

    def is_point_on_display(self, x: int, y: int) -> bool:
        """Check if a point is on this display."""
        return (
            self.x <= x < self.x + self.width
            and self.y <= y < self.y + self.height
        )

    def is_portrait(self) -> bool:
        """Check if display is in portrait orientation."""
        return self.orientation in (
            DisplayOrientation.PORTRAIT,
            DisplayOrientation.PORTRAIT_FLIPPED,
        )

    def physical_to_logical(self, x: int, y: int) -> Tuple[int, int]:
        """Convert physical pixels to logical coordinates."""
        return (int(x / self.scale_factor), int(y / self.scale_factor))

    def logical_to_physical(self, x: int, y: int) -> Tuple[int, int]:
        """Convert logical coordinates to physical pixels."""
        return (int(x * self.scale_factor), int(y * self.scale_factor))

    def to_dict(self) -> Dict[str, Any]:
        """Serialize to dictionary."""
        return {
            'display_id': self.display_id,
            'x': self.x,
            'y': self.y,
            'width': self.width,
            'height': self.height,
            'work_x': self.work_x,
            'work_y': self.work_y,
            'work_width': self.work_width,
            'work_height': self.work_height,
            'scale_factor': self.scale_factor,
            'orientation': self.orientation.name,
            'is_primary': self.is_primary,
            'is_built_in': self.is_built_in,
            'refresh_rate': self.refresh_rate,
        }


def get_display_geometries() -> List[DisplayGeometry]:
    """
    Get geometry information for all displays.

    Returns:
        List of DisplayGeometry objects.
    """
    if IS_MACOS:
        return _get_macos_displays()
    else:
        return [_get_fallback_display()]


def _get_macos_displays() -> List[DisplayGeometry]:
    """Get display geometries on macOS using Quartz."""
    from Cocoa import NSScreen
    geometries = []
    for i, screen in enumerate(NSScreen.screens()):
        f = screen.frame()
        vf = screen.visibleFrame()
        desc = screen.deviceDescription()
        is_primary = screen == NSScreen.mainScreen()

        # Determine orientation
        if screen.frame().size.width >= screen.frame().size.height:
            orientation = DisplayOrientation.LANDSCAPE
        else:
            orientation = DisplayOrientation.PORTRAIT

        geometries.append(
            DisplayGeometry(
                display_id=str(desc.get('NSScreenNumber', i)),
                x=int(f.origin.x),
                y=int(f.origin.y),
                width=int(f.size.width),
                height=int(f.size.height),
                work_x=int(vf.origin.x),
                work_y=int(vf.origin.y),
                work_width=int(vf.size.width),
                work_height=int(vf.size.height),
                scale_factor=float(screen.backingScaleFactor()),
                orientation=orientation,
                is_primary=is_primary,
                is_built_in=screen.isBuiltInDisplay(),
            )
        )
    return geometries


def _get_fallback_display() -> DisplayGeometry:
    """Get fallback display geometry."""
    import pyautogui
    w, h = pyautogui.size()
    return DisplayGeometry(
        display_id='primary',
        x=0, y=0,
        width=w, height=h,
        work_x=0, work_y=0,
        work_width=w, work_height=h,
        is_primary=True,
    )


def get_primary_display() -> Optional[DisplayGeometry]:
    """Get the primary display geometry."""
    for geo in get_display_geometries():
        if geo.is_primary:
            return geo
    return get_display_geometries()[0] if get_display_geometries() else None


def get_display_containing_point(x: int, y: int) -> Optional[DisplayGeometry]:
    """
    Find the display containing a given point.

    Args:
        x: X coordinate.
        y: Y coordinate.

    Returns:
        DisplayGeometry for the display at that point, or None.
    """
    for geo in get_display_geometries():
        if geo.is_point_on_display(x, y):
            return geo
    return None


def get_displays_by_arrangement() -> str:
    """
    Get a human-readable description of display arrangement.

    Returns:
        String describing the display arrangement.
    """
    geos = get_display_geometries()
    if len(geos) == 1:
        return "Single display"
    elif len(geos) == 2:
        g0, g1 = geos
        if g0.x == g1.x and g0.y < g1.y:
            return f"Vertical stack ({g0.height}p above {g1.height}p)"
        elif g0.y == g1.y and g0.x < g1.x:
            return f"Side-by-side ({g0.width}p left of {g1.width}p)"
        else:
            return f"Two displays at ({g0.x},{g0.y}) and ({g1.x},{g1.y})"
    else:
        return f"{len(geos)} displays"


def get_total_workspace_geometry() -> Tuple[int, int, int, int]:
    """
    Get bounding geometry covering all displays.

    Returns:
        Tuple of (min_x, min_y, max_x, max_y) covering all displays.
    """
    geos = get_display_geometries()
    if not geos:
        return (0, 0, 0, 0)
    min_x = min(g.x for g in geos)
    min_y = min(g.y for g in geos)
    max_x = max(g.x + g.width for g in geos)
    max_y = max(g.y + g.height for g in geos)
    return (min_x, min_y, max_x, max_y)


def find_gap_between_displays(
    display1: DisplayGeometry,
    display2: DisplayGeometry,
    tolerance: int = 50
) -> bool:
    """
    Check if there is a gap between two displays.

    Args:
        display1: First display.
        display2: Second display.
        tolerance: Maximum gap in pixels to consider as adjacent.

    Returns:
        True if displays are adjacent (within tolerance).
    """
    # Check horizontal adjacency
    if display1.y == display2.y:
        gap = abs((display1.x + display1.width) - display2.x)
        if gap <= tolerance or gap <= tolerance and display1.x > display2.x:
            return True
        gap = abs((display2.x + display2.width) - display1.x)
        if gap <= tolerance:
            return True

    # Check vertical adjacency
    if display1.x == display2.x:
        gap = abs((display1.y + display1.height) - display2.y)
        if gap <= tolerance:
            return True
        gap = abs((display2.y + display2.height) - display1.y)
        if gap <= tolerance:
            return True

    return False
