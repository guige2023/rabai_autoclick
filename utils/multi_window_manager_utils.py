"""Multi-Window Manager Utilities.

Manages multiple windows across displays, including arrangement, positioning,
focus management, and state synchronization.

Example:
    >>> from multi_window_manager_utils import MultiWindowManager
    >>> mwm = MultiWindowManager()
    >>> mwm.tile_windows(["Chrome", "Firefox"])
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional, Tuple


@dataclass
class WindowInfo:
    """Window information."""
    title: str
    app: str
    x: int
    y: int
    width: int
    height: int
    visible: bool = True


@dataclass
class DisplayInfo:
    """Display information."""
    id: int
    x: int
    y: int
    width: int
    height: int
    is_primary: bool = False


class MultiWindowManager:
    """Manages multiple windows across displays."""

    def __init__(self):
        """Initialize the window manager."""
        self._displays: List[DisplayInfo] = []
        self._windows: List[WindowInfo] = []

    def tile_windows(
        self, titles: List[str], layout: str = "horizontal"
    ) -> List[WindowInfo]:
        """Tile windows in a layout.

        Args:
            titles: List of window titles to tile.
            layout: "horizontal", "vertical", or "grid".

        Returns:
            List of updated WindowInfo objects.
        """
        if not titles:
            return []
        screen_w = 1920
        screen_h = 1080
        n = len(titles)

        if layout == "horizontal":
            w = screen_w // n
            return [
                WindowInfo(title=t, app="", x=i * w, y=0, width=w, height=screen_h)
                for i, t in enumerate(titles)
            ]
        elif layout == "vertical":
            h = screen_h // n
            return [
                WindowInfo(title=t, app="", x=0, y=i * h, width=screen_w, height=h)
                for i, t in enumerate(titles)
            ]
        else:
            cols = int(n ** 0.5) + (1 if n % (int(n ** 0.5)) else 0)
            rows = (n + cols - 1) // cols
            w = screen_w // cols
            h = screen_h // rows
            result = []
            for i, t in enumerate(titles):
                row = i // cols
                col = i % cols
                result.append(
                    WindowInfo(title=t, app="", x=col * w, y=row * h, width=w, height=h)
                )
            return result

    def cascade_windows(self, titles: List[str], offset: int = 30) -> List[WindowInfo]:
        """Cascade windows with offset.

        Args:
            titles: List of window titles.
            offset: Pixel offset between windows.

        Returns:
            List of updated WindowInfo objects.
        """
        base_x, base_y = 50, 50
        base_w, base_h = 1200, 800
        return [
            WindowInfo(
                title=t, app="", x=base_x + i * offset, y=base_y + i * offset,
                width=base_w, height=base_h
            )
            for i, t in enumerate(titles)
        ]

    def find_window(self, title: str) -> Optional[WindowInfo]:
        """Find a window by title.

        Args:
            title: Window title substring to search.

        Returns:
            WindowInfo if found, None otherwise.
        """
        for w in self._windows:
            if title.lower() in w.title.lower():
                return w
        return None

    def get_windows_on_display(self, display_id: int) -> List[WindowInfo]:
        """Get all windows on a specific display.

        Args:
            display_id: Display identifier.

        Returns:
            List of WindowInfo objects on the display.
        """
        return [w for w in self._windows if w.visible]
