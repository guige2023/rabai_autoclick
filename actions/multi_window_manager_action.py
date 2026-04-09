"""
Multi-Window Manager Action Module.

Manages multiple windows across different applications,
enabling window switching, tab management, and cross-window
element reference passing.
"""

from dataclasses import dataclass, field
from typing import Optional


@dataclass
class WindowInfo:
    """Information about a window."""
    window_id: str
    title: str
    app_name: str
    bounds: tuple[int, int, int, int]
    is_active: bool = False
    is_visible: bool = True
    tab_count: int = 1


@dataclass
class TabInfo:
    """Information about a browser tab."""
    tab_id: str
    title: str
    url: str
    is_active: bool = False
    index: int = 0


class MultiWindowManager:
    """Manages multiple windows and tabs."""

    def __init__(self):
        """Initialize multi-window manager."""
        self._windows: dict[str, WindowInfo] = {}
        self._tabs: dict[str, list[TabInfo]] = {}
        self._active_window: Optional[str] = None

    def register_window(
        self,
        window_id: str,
        title: str,
        app_name: str,
        bounds: tuple[int, int, int, int],
    ) -> None:
        """
        Register a window.

        Args:
            window_id: Unique window identifier.
            title: Window title.
            app_name: Application name.
            bounds: Window bounds (x, y, width, height).
        """
        self._windows[window_id] = WindowInfo(
            window_id=window_id,
            title=title,
            app_name=app_name,
            bounds=bounds,
            is_active=self._active_window is None,
        )
        if self._active_window is None:
            self._active_window = window_id
        if window_id not in self._tabs:
            self._tabs[window_id] = []

    def unregister_window(self, window_id: str) -> bool:
        """
        Unregister a window.

        Args:
            window_id: Window to remove.

        Returns:
            True if removed, False if not found.
        """
        if window_id in self._windows:
            del self._windows[window_id]
            self._tabs.pop(window_id, None)
            if self._active_window == window_id:
                self._active_window = next(iter(self._windows), None)
            return True
        return False

    def set_active_window(self, window_id: str) -> bool:
        """
        Set the active window.

        Args:
            window_id: Window to activate.

        Returns:
            True if successful, False if not found.
        """
        if window_id in self._windows:
            for wid, win in self._windows.items():
                win.is_active = wid == window_id
            self._active_window = window_id
            return True
        return False

    def get_active_window(self) -> Optional[WindowInfo]:
        """
        Get the currently active window.

        Returns:
            Active WindowInfo or None.
        """
        if self._active_window:
            return self._windows.get(self._active_window)
        return None

    def get_all_windows(self) -> list[WindowInfo]:
        """
        Get all registered windows.

        Returns:
            List of WindowInfo objects.
        """
        return list(self._windows.values())

    def get_windows_by_app(self, app_name: str) -> list[WindowInfo]:
        """
        Get windows belonging to an application.

        Args:
            app_name: Application name.

        Returns:
            List of matching windows.
        """
        return [w for w in self._windows.values() if w.app_name == app_name]

    def add_tab(
        self,
        window_id: str,
        tab_id: str,
        title: str,
        url: str,
    ) -> bool:
        """
        Add a tab to a window.

        Args:
            window_id: Parent window ID.
            tab_id: Tab identifier.
            title: Tab title.
            url: Tab URL.

        Returns:
            True if added, False if window not found.
        """
        if window_id not in self._windows:
            return False

        tabs = self._tabs[window_id]
        index = len(tabs)
        is_active = len(tabs) == 0

        tabs.append(TabInfo(
            tab_id=tab_id,
            title=title,
            url=url,
            is_active=is_active,
            index=index,
        ))

        if is_active:
            self._windows[window_id].tab_count = len(tabs)

        return True

    def get_tabs(self, window_id: str) -> list[TabInfo]:
        """
        Get tabs for a window.

        Args:
            window_id: Window identifier.

        Returns:
            List of tabs.
        """
        return self._tabs.get(window_id, [])

    def close_tab(self, window_id: str, tab_id: str) -> bool:
        """
        Close a tab in a window.

        Args:
            window_id: Window ID.
            tab_id: Tab to close.

        Returns:
            True if closed, False if not found.
        """
        if window_id not in self._tabs:
            return False

        tabs = self._tabs[window_id]
        for i, tab in enumerate(tabs):
            if tab.tab_id == tab_id:
                tabs.pop(i)
                self._windows[window_id].tab_count = len(tabs)
                return True
        return False
