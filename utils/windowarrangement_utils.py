"""
Window Arrangement Utilities.

Utilities for arranging windows in various layouts including
cascade, tile, snap, and grid arrangements across monitors.

Usage:
    from utils.windowarrangement_utils import WindowArranger

    arranger = WindowArranger()
    arranger.tile_all(windows)
    arranger.cascade(windows)
"""

from __future__ import annotations

from typing import Optional, List, Dict, Any, Tuple, TYPE_CHECKING
from dataclasses import dataclass

if TYPE_CHECKING:
    pass


@dataclass
class ArrangementPreset:
    """Predefined window arrangement preset."""
    name: str
    description: str


@dataclass
class WindowSlot:
    """A slot for a window in an arrangement."""
    x: int
    y: int
    width: int
    height: int
    display_id: Optional[int] = None


class WindowArranger:
    """
    Arrange windows in various layouts.

    Supports cascading, tiling, gridding, and snapping windows
    to screen regions or predefined layouts.

    Example:
        arranger = WindowArranger()
        arranger.tile_horizontal(windows)
    """

    def __init__(self, bridge: Optional[Any] = None) -> None:
        """
        Initialize the window arranger.

        Args:
            bridge: Optional AccessibilityBridge instance.
        """
        self._bridge = bridge

    def cascade(
        self,
        windows: List[int],
        start_x: int = 50,
        start_y: int = 50,
        offset_x: int = 30,
        offset_y: int = 30,
        width: int = 800,
        height: int = 600,
    ) -> bool:
        """
        Arrange windows in a cascade pattern.

        Args:
            windows: List of window IDs.
            start_x: Starting X position.
            start_y: Starting Y position.
            offset_x: Horizontal offset between windows.
            offset_y: Vertical offset between windows.
            width: Width of each window.
            height: Height of each window.

        Returns:
            True if all windows were arranged.
        """
        success = True

        for i, win_id in enumerate(windows):
            x = start_x + i * offset_x
            y = start_y + i * offset_y
            if not self._set_bounds(win_id, x, y, width, height):
                success = False

        return success

    def tile_horizontal(
        self,
        windows: List[int],
        screen_width: Optional[int] = None,
        screen_height: Optional[int] = None,
        gap: int = 0,
    ) -> bool:
        """
        Tile windows horizontally (side by side).

        Args:
            windows: List of window IDs.
            screen_width: Total width to tile across.
            screen_height: Height for each window.
            gap: Gap between windows in pixels.

        Returns:
            True if successful.
        """
        if not windows:
            return True

        count = len(windows)
        width = (screen_width or 1920) // count - gap
        height = screen_height or 1080

        success = True
        for i, win_id in enumerate(windows):
            x = i * (width + gap)
            y = 0
            if not self._set_bounds(win_id, x, y, width, height):
                success = False

        return success

    def tile_vertical(
        self,
        windows: List[int],
        screen_width: Optional[int] = None,
        screen_height: Optional[int] = None,
        gap: int = 0,
    ) -> bool:
        """
        Tile windows vertically (stacked).

        Args:
            windows: List of window IDs.
            screen_width: Width for each window.
            screen_height: Total height to tile across.
            gap: Gap between windows.

        Returns:
            True if successful.
        """
        if not windows:
            return True

        count = len(windows)
        width = screen_width or 1920
        height = (screen_height or 1080) // count - gap

        success = True
        for i, win_id in enumerate(windows):
            x = 0
            y = i * (height + gap)
            if not self._set_bounds(win_id, x, y, width, height):
                success = False

        return success

    def tile_grid(
        self,
        windows: List[int],
        cols: int = 2,
        screen_width: Optional[int] = None,
        screen_height: Optional[int] = None,
        gap: int = 0,
    ) -> bool:
        """
        Tile windows in a grid.

        Args:
            windows: List of window IDs.
            cols: Number of columns.
            screen_width: Total width.
            screen_height: Total height.
            gap: Gap between windows.

        Returns:
            True if successful.
        """
        if not windows:
            return True

        rows = (len(windows) + cols - 1) // cols
        width = ((screen_width or 1920) // cols) - gap
        height = ((screen_height or 1080) // rows) - gap

        success = True
        for i, win_id in enumerate(windows):
            col = i % cols
            row = i // cols
            x = col * (width + gap)
            y = row * (height + gap)
            if not self._set_bounds(win_id, x, y, width, height):
                success = False

        return success

    def snap_to_region(
        self,
        window_id: int,
        region: str = "left",
        screen_width: Optional[int] = None,
        screen_height: Optional[int] = None,
    ) -> bool:
        """
        Snap a window to a screen region (left half, right half, etc.).

        Args:
            window_id: ID of the window to snap.
            region: Region name ("left", "right", "top", "bottom", "full").
            screen_width: Screen width.
            screen_height: Screen height.

        Returns:
            True if successful.
        """
        sw = screen_width or 1920
        sh = screen_height or 1080
        gap = 0

        regions = {
            "left": (0, 0, sw // 2 - gap, sh),
            "right": (sw // 2 + gap, 0, sw // 2 - gap, sh),
            "top": (0, 0, sw, sh // 2 - gap),
            "bottom": (0, sh // 2 + gap, sw, sh // 2 - gap),
            "full": (0, 0, sw, sh),
            "tl": (0, 0, sw // 2 - gap, sh // 2 - gap),
            "tr": (sw // 2 + gap, 0, sw // 2 - gap, sh // 2 - gap),
            "bl": (0, sh // 2 + gap, sw // 2 - gap, sh // 2 - gap),
            "br": (sw // 2 + gap, sh // 2 + gap, sw // 2 - gap, sh // 2 - gap),
        }

        bounds = regions.get(region.lower())
        if bounds is None:
            return False

        x, y, w, h = bounds
        return self._set_bounds(window_id, x, y, w, h)

    def center(
        self,
        window_id: int,
        screen_width: Optional[int] = None,
        screen_height: Optional[int] = None,
    ) -> bool:
        """
        Center a window on the screen.

        Args:
            window_id: ID of the window.
            screen_width: Screen width.
            screen_height: Screen height.

        Returns:
            True if successful.
        """
        if self._bridge is None:
            return False

        try:
            bounds = self._bridge.get_window_bounds(window_id)
            if bounds is None:
                return False

            sw = screen_width or 1920
            sh = screen_height or 1080

            win_w = bounds.get("width", 800)
            win_h = bounds.get("height", 600)

            x = (sw - win_w) // 2
            y = (sh - win_h) // 2

            return self._set_bounds(window_id, x, y, win_w, win_h)
        except Exception:
            return False

    def _set_bounds(
        self,
        window_id: int,
        x: int,
        y: int,
        width: int,
        height: int,
    ) -> bool:
        """Set window bounds."""
        if self._bridge is None:
            return False

        try:
            self._bridge.set_window_bounds(window_id, x, y, width, height)
            return True
        except Exception:
            return False
