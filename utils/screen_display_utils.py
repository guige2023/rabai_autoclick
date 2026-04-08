"""Screen display information utilities.

This module provides utilities for querying and working with
screen displays and their properties.
"""

from __future__ import annotations

from typing import List, Optional, Tuple
from dataclasses import dataclass
from enum import Enum, auto


class DisplayOrientation(Enum):
    """Display orientation."""
    PORTRAIT = auto()
    LANDSCAPE = auto()
    PORTRAIT_FLIPPED = auto()
    LANDSCAPE_FLIPPED = auto()


@dataclass
class DisplayInfo:
    """Information about a display."""
    display_id: int
    x: int
    y: int
    width: int
    height: int
    scale_factor: float = 1.0
    is_primary: bool = False
    orientation: DisplayOrientation = DisplayOrientation.LANDSCAPE
    name: str = ""

    @property
    def bounds(self) -> Tuple[int, int, int, int]:
        return (self.x, self.y, self.width, self.height)

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
    def is_portrait(self) -> bool:
        return self.orientation in (
            DisplayOrientation.PORTRAIT,
            DisplayOrientation.PORTRAIT_FLIPPED,
        )

    def contains_point(self, x: int, y: int) -> bool:
        return (
            self.x <= x < self.x + self.width
            and self.y <= y < self.y + self.height
        )


class DisplayManager:
    """Manages screen displays."""

    def __init__(self) -> None:
        self._displays: List[DisplayInfo] = []
        self._primary_index = 0

    def add_display(self, display: DisplayInfo) -> None:
        self._displays.append(display)
        if display.is_primary:
            self._primary_index = len(self._displays) - 1

    @property
    def displays(self) -> List[DisplayInfo]:
        return self._displays.copy()

    @property
    def primary(self) -> Optional[DisplayInfo]:
        if self._displays:
            return self._displays[self._primary_index]
        return None

    def get_display_at(self, x: int, y: int) -> Optional[DisplayInfo]:
        for display in self._displays:
            if display.contains_point(x, y):
                return display
        return None

    def get_nearest_display(self, x: int, y: int) -> Optional[DisplayInfo]:
        nearest = None
        min_dist = float("inf")
        for display in self._displays:
            cx, cy = display.center
            dist = ((x - cx) ** 2 + (y - cy) ** 2) ** 0.5
            if dist < min_dist:
                min_dist = dist
                nearest = display
        return nearest

    def total_width(self) -> int:
        if not self._displays:
            return 0
        min_x = min(d.x for d in self._displays)
        max_x = max(d.x + d.width for d in self._displays)
        return max_x - min_x

    def total_height(self) -> int:
        if not self._displays:
            return 0
        min_y = min(d.y for d in self._displays)
        max_y = max(d.y + d.height for d in self._displays)
        return max_y - min_y

    def unified_bounds(self) -> Tuple[int, int, int, int]:
        if not self._displays:
            return (0, 0, 0, 0)
        min_x = min(d.x for d in self._displays)
        min_y = min(d.y for d in self._displays)
        max_x = max(d.x + d.width for d in self._displays)
        max_y = max(d.y + d.height for d in self._displays)
        return (min_x, min_y, max_x - min_x, max_y - min_y)


def dpi_to_scale(dpi: int, reference_dpi: int = 72) -> float:
    """Convert DPI to scale factor.

    Args:
        dpi: Dots per inch.
        reference_dpi: Reference DPI (default 72 for macOS).

    Returns:
        Scale factor.
    """
    return dpi / reference_dpi


def scale_to_dpi(scale: float, reference_dpi: int = 72) -> int:
    """Convert scale factor to DPI.

    Args:
        scale: Scale factor.
        reference_dpi: Reference DPI.

    Returns:
        DPI value.
    """
    return int(scale * reference_dpi)


__all__ = [
    "DisplayOrientation",
    "DisplayInfo",
    "DisplayManager",
    "dpi_to_scale",
    "scale_to_dpi",
]
