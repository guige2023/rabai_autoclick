"""
Window State Manager Action Module.

Manages window state persistence including position, size,
maximization state, and multi-monitor handling.
"""

import time
from dataclasses import dataclass
from typing import Optional


@dataclass
class WindowState:
    """State information for a window."""
    window_id: str
    title: str
    x: int
    y: int
    width: int
    height: int
    is_maximized: bool
    is_minimized: bool
    is_fullscreen: bool
    monitor: int = 0
    saved_at: float = 0.0


class WindowStateManager:
    """Manages window state persistence."""

    def __init__(self):
        """Initialize window state manager."""
        self._saved_states: dict[str, WindowState] = {}
        self._active_windows: dict[str, WindowState] = {}

    def save_state(
        self,
        window_id: str,
        title: str,
        x: int,
        y: int,
        width: int,
        height: int,
        is_maximized: bool = False,
        is_minimized: bool = False,
        is_fullscreen: bool = False,
        monitor: int = 0,
    ) -> WindowState:
        """
        Save window state.

        Args:
            window_id: Unique window identifier.
            title: Window title.
            x: X position.
            y: Y position.
            width: Window width.
            height: Window height.
            is_maximized: Is maximized.
            is_minimized: Is minimized.
            is_fullscreen: Is fullscreen.
            monitor: Monitor index.

        Returns:
            Saved WindowState.
        """
        state = WindowState(
            window_id=window_id,
            title=title,
            x=x,
            y=y,
            width=width,
            height=height,
            is_maximized=is_maximized,
            is_minimized=is_minimized,
            is_fullscreen=is_fullscreen,
            monitor=monitor,
            saved_at=time.time(),
        )
        self._saved_states[window_id] = state
        self._active_windows[window_id] = state
        return state

    def get_saved_state(self, window_id: str) -> Optional[WindowState]:
        """Get saved state for a window."""
        return self._saved_states.get(window_id)

    def restore_state(self, window_id: str) -> Optional[WindowState]:
        """
        Get state to restore for a window.

        Returns:
            WindowState or None if not found.
        """
        return self._saved_states.get(window_id)

    def forget_state(self, window_id: str) -> bool:
        """Remove saved state for a window."""
        if window_id in self._saved_states:
            del self._saved_states[window_id]
            return True
        return False

    def get_active_windows(self) -> list[WindowState]:
        """Get all active window states."""
        return list(self._active_windows.values())

    def update_active(
        self,
        window_id: str,
        **kwargs,
    ) -> bool:
        """
        Update active window state.

        Returns:
            True if updated, False if window not tracked.
        """
        if window_id not in self._active_windows:
            return False

        state = self._active_windows[window_id]
        for key, value in kwargs.items():
            if hasattr(state, key):
                setattr(state, key, value)

        return True

    def clear_all(self) -> None:
        """Clear all saved states."""
        self._saved_states.clear()
        self._active_windows.clear()
