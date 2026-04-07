"""Window management utilities for RabAI AutoClick.

Provides:
- Window information
- Window matching
- Window operations
"""

from dataclasses import dataclass
from typing import Callable, List, Optional


@dataclass
class WindowInfo:
    """Window information."""
    hwnd: int
    title: str
    process_name: str
    process_id: int
    rect: tuple
    visible: bool = True
    enabled: bool = True


class WindowFinder:
    """Find windows by various criteria."""

    def __init__(self) -> None:
        """Initialize window finder."""
        self._filters: List[Callable[[WindowInfo], bool]] = []

    def add_filter(self, filter_func: Callable[[WindowInfo], bool]) -> "WindowFinder":
        """Add a filter function.

        Args:
            filter_func: Function that returns True if window matches.

        Returns:
            Self for chaining.
        """
        self._filters.append(filter_func)
        return self

    def with_title(self, title: str) -> "WindowFinder":
        """Filter by title substring.

        Args:
            title: Title substring to match.

        Returns:
            Self for chaining.
        """
        self._filters.append(lambda w: title.lower() in w.title.lower())
        return self

    def with_process(self, process_name: str) -> "WindowFinder":
        """Filter by process name.

        Args:
            process_name: Process name to match.

        Returns:
            Self for chaining.
        """
        self._filters.append(
            lambda w: process_name.lower() in w.process_name.lower()
        )
        return self

    def visible_only(self) -> "WindowFinder":
        """Filter to visible windows only.

        Returns:
            Self for chaining.
        """
        self._filters.append(lambda w: w.visible)
        return self

    def enabled_only(self) -> "WindowFinder":
        """Filter to enabled windows only.

        Returns:
            Self for chaining.
        """
        self._filters.append(lambda w: w.enabled)
        return self

    def find_all(self) -> List[WindowInfo]:
        """Find all matching windows.

        Returns:
            List of matching windows.
        """
        windows = self._get_all_windows()
        for filter_func in self._filters:
            windows = [w for w in windows if filter_func(w)]
        return windows

    def find_first(self) -> Optional[WindowInfo]:
        """Find first matching window.

        Returns:
            First matching window or None.
        """
        windows = self.find_all()
        return windows[0] if windows else None

    def _get_all_windows(self) -> List[WindowInfo]:
        """Get all windows (platform-specific implementation).

        Returns:
            List of all windows.
        """
        # Default implementation returns empty list
        # Subclasses should override for actual functionality
        return []


class WindowOperations:
    """Operations on windows."""

    @staticmethod
    def bring_to_front(hwnd: int) -> bool:
        """Bring window to front.

        Args:
            hwnd: Window handle.

        Returns:
            True if successful.
        """
        try:
            import win32gui
            import win32con
            win32gui.ShowWindow(hwnd, win32con.SW_RESTORE)
            win32gui.SetForegroundWindow(hwnd)
            return True
        except Exception:
            return False

    @staticmethod
    def minimize(hwnd: int) -> bool:
        """Minimize window.

        Args:
            hwnd: Window handle.

        Returns:
            True if successful.
        """
        try:
            import win32gui
            win32gui.ShowWindow(hwnd, 6)  # SW_MINIMIZE
            return True
        except Exception:
            return False

    @staticmethod
    def maximize(hwnd: int) -> bool:
        """Maximize window.

        Args:
            hwnd: Window handle.

        Returns:
            True if successful.
        """
        try:
            import win32gui
            win32gui.ShowWindow(hwnd, 3)  # SW_MAXIMIZE
            return True
        except Exception:
            return False

    @staticmethod
    def restore(hwnd: int) -> bool:
        """Restore window.

        Args:
            hwnd: Window handle.

        Returns:
            True if successful.
        """
        try:
            import win32gui
            win32gui.ShowWindow(hwnd, 9)  # SW_RESTORE
            return True
        except Exception:
            return False

    @staticmethod
    def close(hwnd: int) -> bool:
        """Close window.

        Args:
            hwnd: Window handle.

        Returns:
            True if successful.
        """
        try:
            import win32gui
            win32gui.PostMessage(hwnd, 16, 0, 0)  # WM_CLOSE
            return True
        except Exception:
            return False

    @staticmethod
    def set_position(hwnd: int, x: int, y: int, width: int, height: int) -> bool:
        """Set window position and size.

        Args:
            hwnd: Window handle.
            x: New x position.
            y: New y position.
            width: New width.
            height: New height.

        Returns:
            True if successful.
        """
        try:
            import win32gui
            win32gui.MoveWindow(hwnd, x, y, width, height, True)
            return True
        except Exception:
            return False


class WindowMonitor:
    """Monitor window events."""

    def __init__(self) -> None:
        """Initialize window monitor."""
        self._callbacks = {}
        self._running = False

    def on_create(self, callback: Callable[[WindowInfo], None]) -> None:
        """Register callback for window created.

        Args:
            callback: Function to call.
        """
        self._callbacks["create"] = callback

    def on_destroy(self, callback: Callable[[int], None]) -> None:
        """Register callback for window destroyed.

        Args:
            callback: Function to call with hwnd.
        """
        self._callbacks["destroy"] = callback

    def on_focus(self, callback: Callable[[WindowInfo], None]) -> None:
        """Register callback for window focus changed.

        Args:
            callback: Function to call.
        """
        self._callbacks["focus"] = callback

    def start(self) -> None:
        """Start monitoring."""
        self._running = True

    def stop(self) -> None:
        """Stop monitoring."""
        self._running = False

    @property
    def is_running(self) -> bool:
        """Check if monitor is running."""
        return self._running
