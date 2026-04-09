"""
Viewport management utilities for UI automation.

This module provides utilities for managing viewports, including
viewport discovery, coordinate transformation, and viewport state.
"""

from __future__ import annotations

from typing import List, Tuple, Optional, Dict, Any
from dataclasses import dataclass, field
from enum import Enum, auto
import platform


IS_MACOS: bool = platform.system() == 'Darwin'


class ViewportEdge(Enum):
    """Represents edges of a viewport."""
    TOP = auto()
    BOTTOM = auto()
    LEFT = auto()
    RIGHT = auto()
    CENTER = auto()


@dataclass
class Viewport:
    """
    Represents a rectangular viewport region.

    Attributes:
        x: X coordinate of top-left corner.
        y: Y coordinate of top-left corner.
        width: Width of the viewport.
        height: Height of the viewport.
        scale: Scale factor (DPI scaling).
        display_id: Associated display identifier.
    """
    x: int
    y: int
    width: int
    height: int
    scale: float = 1.0
    display_id: Optional[str] = None

    @property
    def x1(self) -> int:
        """Left edge x coordinate."""
        return self.x

    @property
    def y1(self) -> int:
        """Top edge y coordinate."""
        return self.y

    @property
    def x2(self) -> int:
        """Right edge x coordinate."""
        return self.x + self.width

    @property
    def y2(self) -> int:
        """Bottom edge y coordinate."""
        return self.y + self.height

    @property
    def center(self) -> Tuple[int, int]:
        """Center point as (x, y)."""
        return (self.x + self.width // 2, self.y + self.height // 2)

    @property
    def center_x(self) -> int:
        """Center x coordinate."""
        return self.x + self.width // 2

    @property
    def center_y(self) -> int:
        """Center y coordinate."""
        return self.y + self.height // 2

    @property
    def area(self) -> int:
        """Total pixel area of viewport."""
        return self.width * self.height

    @property
    def aspect_ratio(self) -> float:
        """Aspect ratio (width / height)."""
        return self.width / max(self.height, 1)

    def contains_point(self, x: int, y: int) -> bool:
        """Check if a point is within the viewport."""
        return self.x <= x < self.x + self.width and self.y <= y < self.y + self.height

    def contains_region(
        self, x: int, y: int, width: int, height: int
    ) -> bool:
        """Check if a region is fully within the viewport."""
        return (
            self.contains_point(x, y)
            and self.contains_point(x + width - 1, y + height - 1)
        )

    def overlaps_region(
        self, x: int, y: int, width: int, height: int
    ) -> bool:
        """Check if a region overlaps the viewport."""
        return not (
            x + width <= self.x
            or x >= self.x + self.width
            or y + height <= self.y
            or y >= self.y + self.height
        )

    def distance_to_edge(self, x: int, y: int) -> Dict[str, float]:
        """Calculate distance from a point to each viewport edge."""
        return {
            'top': abs(y - self.y),
            'bottom': abs(y - (self.y + self.height)),
            'left': abs(x - self.x),
            'right': abs(x - (self.x + self.width)),
        }

    def nearest_edge(self, x: int, y: int) -> Tuple[ViewportEdge, float]:
        """Find the nearest edge to a point."""
        distances = self.distance_to_edge(x, y)
        min_edge = min(distances, key=distances.get)
        edge_map = {
            'top': ViewportEdge.TOP,
            'bottom': ViewportEdge.BOTTOM,
            'left': ViewportEdge.LEFT,
            'right': ViewportEdge.RIGHT,
        }
        return edge_map[min_edge], distances[min_edge]

    def clip_to_viewport(
        self, x: int, y: int, width: int, height: int
    ) -> Tuple[int, int, int, int]:
        """Clip a region so it fits within the viewport."""
        clipped_x = max(self.x, min(x, self.x + self.width))
        clipped_y = max(self.y, min(y, self.y + self.height))
        clipped_x2 = max(self.x, min(x + width, self.x + self.width))
        clipped_y2 = max(self.y, min(y + height, self.y + self.height))
        return (
            clipped_x,
            clipped_y,
            max(0, clipped_x2 - clipped_x),
            max(0, clipped_y2 - clipped_y),
        )

    def to_screenshot_region(self) -> Dict[str, int]:
        """Convert to screenshot region dict."""
        return {
            'x': self.x,
            'y': self.y,
            'width': self.width,
            'height': self.height,
        }

    def to_dict(self) -> Dict[str, Any]:
        """Serialize to dictionary."""
        return {
            'x': self.x,
            'y': self.y,
            'width': self.width,
            'height': self.height,
            'scale': self.scale,
            'display_id': self.display_id,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> Viewport:
        """Deserialize from dictionary."""
        return cls(
            x=data['x'],
            y=data['y'],
            width=data['width'],
            height=data['height'],
            scale=data.get('scale', 1.0),
            display_id=data.get('display_id'),
        )


def get_main_viewport() -> Viewport:
    """
    Get the main (primary) viewport.

    Returns:
        Viewport representing the primary display.

    Raises:
        RuntimeError: If viewport cannot be determined.
    """
    if IS_MACOS:
        from Cocoa import NSScreen
        screen = NSScreen.mainScreen()
        frame = screen.frame()
        return Viewport(
            x=int(frame.origin.x),
            y=int(frame.origin.y),
            width=int(frame.size.width),
            height=int(frame.size.height),
            scale=float(screen.backingScaleFactor()),
            display_id=str(screen.deviceDescription()),
        )
    else:
        # Generic fallback using pyautogui
        import pyautogui
        w, h = pyautogui.size()
        return Viewport(x=0, y=0, width=w, height=h)


def get_all_viewports() -> List[Viewport]:
    """
    Get all available viewports (displays).

    Returns:
        List of Viewport objects for each display.
    """
    if IS_MACOS:
        from Cocoa import NSScreen
        viewports = []
        for i, screen in enumerate(NSScreen.screens()):
            frame = screen.frame()
            viewports.append(
                Viewport(
                    x=int(frame.origin.x),
                    y=int(frame.origin.y),
                    width=int(frame.size.width),
                    height=int(frame.size.height),
                    scale=float(screen.backingScaleFactor()),
                    display_id=f"display-{i}",
                )
            )
        return viewports
    else:
        # Fallback to single viewport
        return [get_main_viewport()]


def viewport_at_point(x: int, y: int) -> Optional[Viewport]:
    """
    Find the viewport containing the given point.

    Args:
        x: X coordinate.
        y: Y coordinate.

    Returns:
        Viewport containing the point, or None if not found.
    """
    for vp in get_all_viewports():
        if vp.contains_point(x, y):
            return vp
    return None


def point_to_viewport_coords(
    x: int, y: int, source_vp: Viewport, target_vp: Viewport
) -> Tuple[int, int]:
    """
    Transform coordinates from one viewport to another.

    Args:
        x: Source x coordinate.
        y: Source y coordinate.
        source_vp: Source viewport.
        target_vp: Target viewport.

    Returns:
        Tuple of (x, y) in target viewport coordinates.
    """
    # Translate to global coordinates
    global_x = source_vp.x + x
    global_y = source_vp.y + y
    # Translate to target viewport coordinates
    return (global_x - target_vp.x, global_y - target_vp.y)


def fit_viewport_in_viewport(
    source: Viewport, target: Viewport, padding: int = 0
) -> Tuple[int, int, int, int]:
    """
    Calculate how source viewport maps into target viewport with scaling.

    Args:
        source: Source viewport to fit.
        target: Target viewport to fit into.
        padding: Padding in pixels on each side.

    Returns:
        Tuple of (x, y, scale, fitted_width, fitted_height).
    """
    available_w = target.width - 2 * padding
    available_h = target.height - 2 * padding
    scale_w = available_w / source.width
    scale_h = available_h / source.height
    scale = min(scale_w, scale_h)
    fitted_w = int(source.width * scale)
    fitted_h = int(source.height * scale)
    fitted_x = target.x + padding + (available_w - fitted_w) // 2
    fitted_y = target.y + padding + (available_h - fitted_h) // 2
    return (fitted_x, fitted_y, scale, fitted_w, fitted_h)
