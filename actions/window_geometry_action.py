"""Window geometry action for UI automation.

Handles window positioning, sizing, and arrangement:
- Window bounds and transforms
- Multi-monitor support
- Docking and tiling
- Window arrangement presets
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Callable


class WindowState(Enum):
    """Window state types."""
    NORMAL = auto()
    MINIMIZED = auto()
    MAXIMIZED = auto()
    FULLSCREEN = auto()
    HIDDEN = auto()


class ArrangementPreset(Enum):
    """Window arrangement presets."""
    LEFT_HALF = auto()
    RIGHT_HALF = auto()
    TOP_HALF = auto()
    BOTTOM_HALF = auto()
    TOP_LEFT_QUARTER = auto()
    TOP_RIGHT_QUARTER = auto()
    BOTTOM_LEFT_QUARTER = auto()
    BOTTOM_RIGHT_QUARTER = auto()
    CENTER = auto()
    FULL = auto()


@dataclass
class Rect:
    """Rectangle in screen coordinates."""
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
    def center(self) -> tuple[int, int]:
        return (self.center_x, self.center_y)

    def contains_point(self, x: int, y: int) -> bool:
        """Check if point is inside rect."""
        return self.left <= x <= self.right and self.top <= y <= self.bottom

    def intersection(self, other: Rect) -> Rect | None:
        """Get intersection with another rect."""
        x = max(self.left, other.left)
        y = max(self.top, other.top)
        right = min(self.right, other.right)
        bottom = min(self.bottom, other.bottom)
        if right <= x or bottom <= y:
            return None
        return Rect(x, y, right - x, bottom - y)

    def fits_in(self, other: Rect, margin: int = 0) -> bool:
        """Check if rect fits inside another with margin."""
        return (self.width <= other.width - 2 * margin and
                self.height <= other.height - 2 * margin and
                other.contains_point(self.left + margin, self.top + margin) and
                other.contains_point(self.right - margin, self.bottom - margin))


@dataclass
class DisplayInfo:
    """Display/monitor information."""
    id: str
    bounds: Rect
    is_main: bool = False
    scale_factor: float = 1.0
    rotation: int = 0
    dock: str | None = None  # macOS only: menubar, dock


@dataclass
class WindowInfo:
    """Window information."""
    window_id: str
    title: str
    bounds: Rect
    state: WindowState = WindowState.NORMAL
    is_focused: bool = False
    owner_name: str | None = None
    owner_pid: int | None = None
    display_id: str | None = None
    z_order: int = 0
    is_resizable: bool = True
    is_movable: bool = True


@dataclass
class ZOrderEntry:
    """Window z-order entry."""
    window_id: str
    z_order: int
    timestamp: float


class WindowGeometryService:
    """Service for window geometry operations.

    Provides:
    - Window position and size control
    - Multi-monitor awareness
    - Arrangement presets (tile, dock)
    - Z-order management
    """

    def __init__(self):
        self._windows: dict[str, WindowInfo] = {}
        self._displays: dict[str, DisplayInfo] = {}
        self._z_order: list[ZOrderEntry] = []
        self._move_func: Callable | None = None
        self._resize_func: Callable | None = None

    def set_move_func(self, func: Callable) -> None:
        """Set window move function.

        Args:
            func: Function(window_id, x, y) -> bool
        """
        self._move_func = func

    def set_resize_func(self, func: Callable) -> None:
        """Set window resize function.

        Args:
            func: Function(window_id, width, height) -> bool
        """
        self._resize_func = func

    def register_window(self, window: WindowInfo) -> None:
        """Register a window with the service."""
        self._windows[window.window_id] = window
        self._update_z_order(window.window_id)

    def register_display(self, display: DisplayInfo) -> None:
        """Register a display/monitor."""
        self._displays[display.id] = display

    def move_window(
        self,
        window_id: str,
        x: int,
        y: int,
    ) -> bool:
        """Move window to position.

        Args:
            window_id: Target window
            x: New x position
            y: New y position

        Returns:
            True if move succeeded
        """
        window = self._windows.get(window_id)
        if not window:
            return False

        if not window.is_movable:
            return False

        if self._move_func:
            success = self._move_func(window_id, x, y)
            if success:
                window.bounds.x = x
                window.bounds.y = y
                return True
            return False

        # No move function - just update local state
        window.bounds.x = x
        window.bounds.y = y
        return True

    def resize_window(
        self,
        window_id: str,
        width: int,
        height: int,
    ) -> bool:
        """Resize window.

        Args:
            window_id: Target window
            width: New width
            height: New height

        Returns:
            True if resize succeeded
        """
        window = self._windows.get(window_id)
        if not window:
            return False

        if not window.is_resizable:
            return False

        if self._resize_func:
            success = self._resize_func(window_id, width, height)
            if success:
                window.bounds.width = width
                window.bounds.height = height
                return True
            return False

        window.bounds.width = width
        window.bounds.height = height
        return True

    def set_bounds(
        self,
        window_id: str,
        bounds: Rect,
    ) -> bool:
        """Set window bounds (position and size).

        Args:
            window_id: Target window
            bounds: New bounds

        Returns:
            True if bounds set succeeded
        """
        moved = self.move_window(window_id, bounds.x, bounds.y)
        resized = self.resize_window(window_id, bounds.width, bounds.height)
        return moved and resized

    def apply_arrangement(
        self,
        window_id: str,
        preset: ArrangementPreset,
        target_display: str | None = None,
    ) -> bool:
        """Apply arrangement preset to window.

        Args:
            window_id: Target window
            preset: Arrangement preset to apply
            target_display: Display to use (uses main if None)

        Returns:
            True if arrangement applied
        """
        window = self._windows.get(window_id)
        if not window:
            return False

        # Get target display
        if target_display:
            display = self._displays.get(target_display)
        else:
            display = self._get_main_display()

        if not display:
            return False

        bounds = self._calculate_arrangement_bounds(display.bounds, preset)
        return self.set_bounds(window_id, bounds)

    def _calculate_arrangement_bounds(
        self,
        display_bounds: Rect,
        preset: ArrangementPreset,
    ) -> Rect:
        """Calculate window bounds for arrangement preset."""
        d = display_bounds
        margin = 0  # Could add spacing for dock/menubar

        arrangements = {
            ArrangementPreset.LEFT_HALF: Rect(d.x, d.y + margin, d.width // 2, d.height - margin),
            ArrangementPreset.RIGHT_HALF: Rect(d.x + d.width // 2, d.y + margin, d.width // 2, d.height - margin),
            ArrangementPreset.TOP_HALF: Rect(d.x, d.y + margin, d.width, (d.height - margin) // 2),
            ArrangementPreset.BOTTOM_HALF: Rect(d.x, d.y + margin + (d.height - margin) // 2, d.width, (d.height - margin) // 2),
            ArrangementPreset.TOP_LEFT_QUARTER: Rect(d.x, d.y + margin, d.width // 2, (d.height - margin) // 2),
            ArrangementPreset.TOP_RIGHT_QUARTER: Rect(d.x + d.width // 2, d.y + margin, d.width // 2, (d.height - margin) // 2),
            ArrangementPreset.BOTTOM_LEFT_QUARTER: Rect(d.x, d.y + margin + (d.height - margin) // 2, d.width // 2, (d.height - margin) // 2),
            ArrangementPreset.BOTTOM_RIGHT_QUARTER: Rect(d.x + d.width // 2, d.y + margin + (d.height - margin) // 2, d.width // 2, (d.height - margin) // 2),
            ArrangementPreset.CENTER: Rect(
                d.x + d.width // 4,
                d.y + d.height // 4,
                d.width // 2,
                d.height // 2,
            ),
            ArrangementPreset.FULL: Rect(d.x, d.y + margin, d.width, d.height - margin),
        }

        return arrangements.get(preset, d)

    def cascade_windows(
        self,
        window_ids: list[str],
        start_display: str | None = None,
        offset: int = 30,
    ) -> None:
        """Cascade windows with offset.

        Args:
            window_ids: List of windows to cascade
            start_display: Starting display
            offset: Pixel offset between windows
        """
        if not window_ids:
            return

        if start_display:
            display = self._displays.get(start_display)
        else:
            display = self._get_main_display()

        if not display:
            return

        bounds = Rect(
            display.bounds.x,
            display.bounds.y,
            display.bounds.width * 2 // 3,
            display.bounds.height * 2 // 3,
        )

        for i, window_id in enumerate(window_ids):
            x = bounds.x + i * offset
            y = bounds.y + i * offset
            self.set_bounds(window_id, Rect(x, y, bounds.width, bounds.height))

    def tile_windows(
        self,
        window_ids: list[str],
        direction: str = "horizontal",
        target_display: str | None = None,
    ) -> None:
        """Tile windows in grid.

        Args:
            window_ids: Windows to tile
            direction: 'horizontal', 'vertical', or 'grid'
            target_display: Target display
        """
        if not window_ids:
            return

        if target_display:
            display = self._displays.get(target_display)
        else:
            display = self._get_main_display()

        if not display:
            return

        n = len(window_ids)

        if direction == "horizontal":
            width = display.bounds.width // n
            height = display.bounds.height
            for i, window_id in enumerate(window_ids):
                bounds = Rect(
                    display.bounds.x + i * width,
                    display.bounds.y,
                    width,
                    height,
                )
                self.set_bounds(window_id, bounds)

        elif direction == "vertical":
            width = display.bounds.width
            height = display.bounds.height // n
            for i, window_id in enumerate(window_ids):
                bounds = Rect(
                    display.bounds.x,
                    display.bounds.y + i * height,
                    width,
                    height,
                )
                self.set_bounds(window_id, bounds)

        elif direction == "grid":
            cols = math.ceil(math.sqrt(n))
            rows = math.ceil(n / cols)
            width = display.bounds.width // cols
            height = display.bounds.height // rows
            for i, window_id in enumerate(window_ids):
                row = i // cols
                col = i % cols
                bounds = Rect(
                    display.bounds.x + col * width,
                    display.bounds.y + row * height,
                    width,
                    height,
                )
                self.set_bounds(window_id, bounds)

    def bring_to_front(self, window_id: str) -> bool:
        """Bring window to front (highest z-order).

        Args:
            window_id: Target window

        Returns:
            True if successful
        """
        window = self._windows.get(window_id)
        if not window:
            return False

        max_z = max((e.z_order for e in self._z_order), default=0)
        self._update_z_order(window_id, max_z + 1)
        window.z_order = max_z + 1
        return True

    def send_to_back(self, window_id: str) -> bool:
        """Send window to back (lowest z-order).

        Args:
            window_id: Target window

        Returns:
            True if successful
        """
        window = self._windows.get(window_id)
        if not window:
            return False

        min_z = min((e.z_order for e in self._z_order), default=0)
        self._update_z_order(window_id, min_z - 1)
        window.z_order = min_z - 1
        return True

    def _update_z_order(self, window_id: str, z_order: int | None = None) -> None:
        """Update window z-order."""
        if z_order is None:
            z_order = len(self._z_order)

        # Remove existing entry
        self._z_order = [e for e in self._z_order if e.window_id != window_id]

        # Add new entry
        self._z_order.append(ZOrderEntry(
            window_id=window_id,
            z_order=z_order,
            timestamp=time.time(),
        ))

    def _get_main_display(self) -> DisplayInfo | None:
        """Get main display."""
        for display in self._displays.values():
            if display.is_main:
                return display
        return next(iter(self._displays.values())) if self._displays else None

    def get_window(self, window_id: str) -> WindowInfo | None:
        """Get window info."""
        return self._windows.get(window_id)

    def list_windows(self) -> list[WindowInfo]:
        """List all registered windows."""
        return list(self._windows.values())

    def list_displays(self) -> list[DisplayInfo]:
        """List all registered displays."""
        return list(self._displays.values())


import math


def create_window_geometry_service() -> WindowGeometryService:
    """Create window geometry service."""
    return WindowGeometryService()
