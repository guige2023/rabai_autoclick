"""Window geometry calculation utilities.

This module provides utilities for calculating window positions,
overlaps, visibility regions, and geometric relationships.
"""

from __future__ import annotations

from typing import List, Optional, Tuple
from dataclasses import dataclass


@dataclass
class Rect:
    """A rectangle defined by top-left and bottom-right corners."""
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

    @property
    def area(self) -> int:
        return self.width * self.height

    def contains_point(self, x: int, y: int) -> bool:
        """Check if a point is inside the rectangle."""
        return self.left <= x < self.right and self.top <= y < self.bottom

    def contains_rect(self, other: "Rect") -> bool:
        """Check if another rect is fully contained."""
        return (
            self.left <= other.left
            and self.right >= other.right
            and self.top <= other.top
            and self.bottom >= other.bottom
        )

    def intersects(self, other: "Rect") -> bool:
        """Check if this rect intersects another."""
        return not (
            self.right <= other.left
            or self.left >= other.right
            or self.bottom <= other.top
            or self.top >= other.bottom
        )

    def intersection(self, other: "Rect") -> Optional["Rect"]:
        """Get the intersection with another rect."""
        if not self.intersects(other):
            return None
        left = max(self.left, other.left)
        top = max(self.top, other.top)
        right = min(self.right, other.right)
        bottom = min(self.bottom, other.bottom)
        return Rect(left, top, right - left, bottom - top)

    def union(self, other: "Rect") -> "Rect":
        """Get the bounding rect that contains both."""
        left = min(self.left, other.left)
        top = min(self.top, other.top)
        right = max(self.right, other.right)
        bottom = max(self.bottom, other.bottom)
        return Rect(left, top, right - left, bottom - top)

    def distance_to(self, other: "Rect") -> float:
        """Minimum distance between this rect and another."""
        dx = max(0, max(other.left - self.right, self.left - other.right))
        dy = max(0, max(other.top - self.bottom, self.top - other.bottom))
        return (dx * dx + dy * dy) ** 0.5


@dataclass
class WindowInfo:
    """Window information with geometry and metadata."""
    rect: Rect
    title: str
    app_name: str
    is_visible: bool
    is_focused: bool
    window_id: Optional[int] = None


def arrange_grid(windows: List[Rect], columns: int) -> List[Rect]:
    """Arrange windows in a grid layout.

    Args:
        windows: List of window rects to arrange.
        columns: Number of columns in the grid.

    Returns:
        List of repositioned rects.
    """
    if not windows:
        return []
    rows = (len(windows) + columns - 1) // columns
    total_width = max(w.width for w in windows)
    total_height = max(w.height for w in windows)

    arranged = []
    for i, win in enumerate(windows):
        col = i % columns
        row = i // columns
        x = col * total_width
        y = row * total_height
        arranged.append(Rect(x, y, win.width, win.height))
    return arranged


def tile_horizontal(windows: List[Rect], screen_width: int, screen_height: int) -> List[Rect]:
    """Tile windows horizontally."""
    if not windows:
        return []
    tile_width = screen_width // len(windows)
    return [
        Rect(i * tile_width, 0, tile_width, screen_height)
        for i, _ in enumerate(windows)
    ]


def tile_vertical(windows: List[Rect], screen_width: int, screen_height: int) -> List[Rect]:
    """Tile windows vertically."""
    if not windows:
        return []
    tile_height = screen_height // len(windows)
    return [
        Rect(0, i * tile_height, screen_width, tile_height)
        for i, _ in enumerate(windows)
    ]


def cascade_windows(windows: List[Rect], offset_x: int = 30, offset_y: int = 30) -> List[Rect]:
    """Cascade windows with offset."""
    return [
        Rect(i * offset_x, i * offset_y, w.width, w.height)
        for i, w in enumerate(windows)
    ]


def fit_to_screen(rect: Rect, screen: Rect) -> Rect:
    """Fit a window rect inside a screen rect."""
    width = min(rect.width, screen.width)
    height = min(rect.height, screen.height)
    x = max(screen.left, min(rect.x, screen.right - width))
    y = max(screen.top, min(rect.y, screen.bottom - height))
    return Rect(x, y, width, height)


def visible_region(window: Rect, occluders: List[Rect]) -> Rect:
    """Calculate the visible region of a window given occluders."""
    result = window
    for occ in occluders:
        intersection = result.intersection(occ)
        if intersection:
            parts = subtract_rect(result, intersection)
            result = parts[0] if parts else Rect(0, 0, 0, 0)
    return result


def subtract_rect(rect: Rect, sub: Rect) -> List[Rect]:
    """Subtract one rect from another, returning up to 4 rects."""
    if not rect.intersects(sub):
        return [rect]

    results: List[Rect] = []
    # Top
    if sub.top > rect.top:
        results.append(Rect(rect.left, rect.top, rect.width, sub.top - rect.top))
    # Bottom
    if sub.bottom < rect.bottom:
        results.append(Rect(rect.left, sub.bottom, rect.width, rect.bottom - sub.bottom))
    # Left
    if sub.left > rect.left:
        results.append(Rect(rect.left, max(rect.top, sub.top), sub.left - rect.left, min(rect.bottom, sub.bottom) - max(rect.top, sub.top)))
    # Right
    if sub.right < rect.right:
        results.append(Rect(sub.right, max(rect.top, sub.top), rect.right - sub.right, min(rect.bottom, sub.bottom) - max(rect.top, sub.top)))
    return results


__all__ = [
    "Rect",
    "WindowInfo",
    "arrange_grid",
    "tile_horizontal",
    "tile_vertical",
    "cascade_windows",
    "fit_to_screen",
    "visible_region",
    "subtract_rect",
]
