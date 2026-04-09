"""
Window Manager Action Module

Provides multi-window management, window state tracking,
z-order control, and window arrangement for UI automation.

MIT License - Copyright (c) 2025 RabAi Research
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

logger = logging.getLogger(__name__)


class WindowState(Enum):
    """Window state identifiers."""

    NORMAL = "normal"
    MINIMIZED = "minimized"
    MAXIMIZED = "maximized"
    FULLSCREEN = "fullscreen"
    HIDDEN = "hidden"


@dataclass
class WindowInfo:
    """Window information structure."""

    handle: Any
    title: str
    process_name: str
    process_id: int
    bounds: Tuple[int, int, int, int]  # x, y, width, height
    state: WindowState = WindowState.NORMAL
    is_active: bool = False
    is_visible: bool = True
    monitor: Optional[str] = None


@dataclass
class WindowArrangeConfig:
    """Configuration for window arrangement."""

    padding: int = 10
    columns: int = 2
    rows: int = 2
    monitor_index: int = 0
    fill_order: str = "row"  # row, column, zigzag


@dataclass
class WindowManagerConfig:
    """Configuration for window manager."""

    default_timeout: float = 5.0
    polling_interval: float = 0.1
    track_z_order: bool = True
    cache_window_list: bool = True


class WindowManager:
    """
    Manages multiple windows for UI automation.

    Supports window enumeration, state changes, positioning,
    arrangement (tiling/cascade), and z-order control.
    """

    def __init__(
        self,
        config: Optional[WindowManagerConfig] = None,
        platform_handler: Optional[Any] = None,
    ):
        self.config = config or WindowManagerConfig()
        self.platform_handler = platform_handler
        self._window_cache: Dict[str, WindowInfo] = {}
        self._z_order: List[str] = []
        self._last_refresh: float = 0

    def refresh_window_list(self) -> List[WindowInfo]:
        """
        Refresh the list of visible windows.

        Returns:
            List of WindowInfo objects
        """
        current_time = time.time()

        if (
            self.config.cache_window_list
            and current_time - self._last_refresh < 1.0
            and self._window_cache
        ):
            return list(self._window_cache.values())

        self._window_cache.clear()
        self._z_order.clear()

        windows = self._enumerate_windows()

        for window in windows:
            self._window_cache[window.title] = window
            self._z_order.append(window.title)

        self._last_refresh = current_time
        return windows

    def _enumerate_windows(self) -> List[WindowInfo]:
        """Enumerate all visible windows using platform handler."""
        if self.platform_handler and hasattr(self.platform_handler, "enumerate_windows"):
            raw_windows = self.platform_handler.enumerate_windows()
            return [self._parse_window(w) for w in raw_windows]

        logger.debug("No platform handler, returning empty window list")
        return []

    def _parse_window(self, raw: Any) -> WindowInfo:
        """Parse raw window data into WindowInfo."""
        return WindowInfo(
            handle=raw.get("handle"),
            title=raw.get("title", ""),
            process_name=raw.get("process_name", ""),
            process_id=raw.get("pid", 0),
            bounds=raw.get("bounds", (0, 0, 800, 600)),
            state=WindowState(raw.get("state", "normal")),
            is_active=raw.get("is_active", False),
            is_visible=raw.get("is_visible", True),
            monitor=raw.get("monitor"),
        )

    def get_window(self, title: str) -> Optional[WindowInfo]:
        """
        Get window information by title.

        Args:
            title: Window title (exact or partial match)

        Returns:
            WindowInfo or None if not found
        """
        windows = self.refresh_window_list()

        for window in windows:
            if title.lower() in window.title.lower():
                return window

        return None

    def get_foreground_window(self) -> Optional[WindowInfo]:
        """Get the currently active/foreground window."""
        windows = self.refresh_window_list()
        for window in windows:
            if window.is_active:
                return window
        return None

    def set_foreground(self, title: str) -> bool:
        """
        Bring a window to the foreground.

        Args:
            title: Window title to bring forward

        Returns:
            True if successful
        """
        window = self.get_window(title)
        if not window:
            return False

        if self.platform_handler and hasattr(self.platform_handler, "set_foreground"):
            return self.platform_handler.set_foreground(window.handle)

        return False

    def set_window_state(
        self,
        title: str,
        state: WindowState,
    ) -> bool:
        """
        Set the state of a window.

        Args:
            title: Window title
            state: Target window state

        Returns:
            True if successful
        """
        window = self.get_window(title)
        if not window:
            return False

        if self.platform_handler and hasattr(self.platform_handler, "set_window_state"):
            return self.platform_handler.set_window_state(window.handle, state.value)

        return False

    def minimize(self, title: str) -> bool:
        """Minimize a window."""
        return self.set_window_state(title, WindowState.MINIMIZED)

    def maximize(self, title: str) -> bool:
        """Maximize a window."""
        return self.set_window_state(title, WindowState.MAXIMIZED)

    def restore(self, title: str) -> bool:
        """Restore a minimized/maximized window."""
        return self.set_window_state(title, WindowState.NORMAL)

    def hide(self, title: str) -> bool:
        """Hide a window."""
        return self.set_window_state(title, WindowState.HIDDEN)

    def close(self, title: str) -> bool:
        """
        Close a window.

        Args:
            title: Window title

        Returns:
            True if successful
        """
        window = self.get_window(title)
        if not window:
            return False

        if self.platform_handler and hasattr(self.platform_handler, "close_window"):
            return self.platform_handler.close_window(window.handle)

        return False

    def set_bounds(
        self,
        title: str,
        x: int,
        y: int,
        width: int,
        height: int,
    ) -> bool:
        """
        Set window position and size.

        Args:
            title: Window title
            x: New X position
            y: New Y position
            width: New width
            height: New height

        Returns:
            True if successful
        """
        window = self.get_window(title)
        if not window:
            return False

        if self.platform_handler and hasattr(self.platform_handler, "set_bounds"):
            return self.platform_handler.set_bounds(window.handle, x, y, width, height)

        return False

    def tile_windows(
        self,
        windows: Optional[List[str]] = None,
        config: Optional[WindowArrangeConfig] = None,
    ) -> bool:
        """
        Tile windows in a grid arrangement.

        Args:
            windows: List of window titles to tile (all if None)
            config: Arrangement configuration

        Returns:
            True if successful
        """
        config = config or WindowArrangeConfig()
        all_windows = self.refresh_window_list()

        if windows:
            target_windows = [w for w in all_windows if w.title in windows]
        else:
            target_windows = [w for w in all_windows if w.is_visible]

        if not target_windows:
            return False

        cols = min(config.columns, len(target_windows))
        rows = (len(target_windows) + cols - 1) // cols

        screen_width = 1920
        screen_height = 1080

        cell_width = (screen_width - config.padding * (cols + 1)) // cols
        cell_height = (screen_height - config.padding * (rows + 1)) // rows

        for i, window in enumerate(target_windows):
            if config.fill_order == "row":
                row = i // cols
                col = i % cols
            elif config.fill_order == "column":
                row = i % rows
                col = i // rows
            else:
                row = i // cols
                col = i % cols

            x = config.padding + col * (cell_width + config.padding)
            y = config.padding + row * (cell_height + config.padding)

            self.set_bounds(window.title, x, y, cell_width, cell_height)

        return True

    def cascade_windows(
        self,
        windows: Optional[List[str]] = None,
        offset_x: int = 30,
        offset_y: int = 30,
        start_x: int = 0,
        start_y: int = 0,
    ) -> bool:
        """
        Cascade windows with overlapping offset.

        Args:
            windows: List of window titles to cascade
            offset_x: Horizontal offset between windows
            offset_y: Vertical offset between windows
            start_x: Starting X position
            start_y: Starting Y position

        Returns:
            True if successful
        """
        all_windows = self.refresh_window_list()

        if windows:
            target_windows = [w for w in all_windows if w.title in windows]
        else:
            target_windows = [w for w in all_windows if w.is_visible]

        if not target_windows:
            return False

        base_width = 800
        base_height = 600

        for i, window in enumerate(target_windows):
            x = start_x + i * offset_x
            y = start_y + i * offset_y
            self.set_bounds(window.title, x, y, base_width, base_height)

        return True

    def get_z_order(self) -> List[str]:
        """Get current window z-order (front to back)."""
        if self.config.track_z_order:
            self.refresh_window_list()
            return self._z_order.copy()
        return []

    def bring_to_front(self, title: str) -> bool:
        """
        Bring a window to the front of the z-order.

        Args:
            title: Window title

        Returns:
            True if successful
        """
        if title in self._z_order:
            self._z_order.remove(title)
            self._z_order.append(title)

        return self.set_foreground(title)

    def send_to_back(self, title: str) -> bool:
        """
        Send a window to the back of the z-order.

        Args:
            title: Window title

        Returns:
            True if successful
        """
        if title in self._z_order:
            self._z_order.remove(title)
            self._z_order.insert(0, title)

        if self.platform_handler and hasattr(self.platform_handler, "send_to_back"):
            window = self.get_window(title)
            if window:
                return self.platform_handler.send_to_back(window.handle)

        return False

    def find_windows_by_process(self, process_name: str) -> List[WindowInfo]:
        """
        Find all windows belonging to a process.

        Args:
            process_name: Process executable name

        Returns:
            List of matching WindowInfo objects
        """
        windows = self.refresh_window_list()
        return [w for w in windows if process_name.lower() in w.process_name.lower()]

    def wait_for_window(
        self,
        title: str,
        timeout: Optional[float] = None,
        state: Optional[WindowState] = None,
    ) -> Optional[WindowInfo]:
        """
        Wait for a window to appear.

        Args:
            title: Window title to wait for
            timeout: Maximum wait time
            state: Optional required window state

        Returns:
            WindowInfo or None if timeout
        """
        timeout = timeout or self.config.default_timeout
        deadline = time.time() + timeout

        while time.time() < deadline:
            window = self.get_window(title)
            if window:
                if state is None or window.state == state:
                    return window
            time.sleep(self.config.polling_interval)

        return None

    def clear_cache(self) -> None:
        """Clear the window cache."""
        self._window_cache.clear()
        self._z_order.clear()
        self._last_refresh = 0


def create_window_manager(
    config: Optional[WindowManagerConfig] = None,
) -> WindowManager:
    """Factory function to create a WindowManager."""
    return WindowManager(config=config)
