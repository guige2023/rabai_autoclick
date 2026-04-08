"""Window state utilities for RabAI AutoClick.

Provides:
- Window state tracking
- State serialization
- Window lifecycle management
"""

from __future__ import annotations

import time
from typing import (
    Any,
    Dict,
    List,
    NamedTuple,
    Optional,
)


class WindowState(NamedTuple):
    """State of a window."""
    window_id: str
    title: str
    x: int
    y: int
    width: int
    height: int
    is_visible: bool
    is_focused: bool
    timestamp: float


class WindowStateManager:
    """Track and manage window states."""

    def __init__(self) -> None:
        self._windows: Dict[str, WindowState] = {}

    def update(
        self,
        window_id: str,
        title: str = "",
        x: int = 0,
        y: int = 0,
        width: int = 0,
        height: int = 0,
        is_visible: bool = True,
        is_focused: bool = False,
    ) -> WindowState:
        """Update or create window state.

        Returns:
            The updated WindowState.
        """
        state = WindowState(
            window_id=window_id,
            title=title,
            x=x,
            y=y,
            width=width,
            height=height,
            is_visible=is_visible,
            is_focused=is_focused,
            timestamp=time.time(),
        )
        self._windows[window_id] = state
        return state

    def get(self, window_id: str) -> Optional[WindowState]:
        """Get window state by ID."""
        return self._windows.get(window_id)

    def get_all(self) -> List[WindowState]:
        """Get all tracked window states."""
        return list(self._windows.values())

    def get_focused(self) -> Optional[WindowState]:
        """Get the currently focused window."""
        for state in self._windows.values():
            if state.is_focused:
                return state
        return None

    def get_visible(self) -> List[WindowState]:
        """Get all visible windows."""
        return [s for s in self._windows.values() if s.is_visible]

    def remove(self, window_id: str) -> bool:
        """Remove a window from tracking."""
        if window_id in self._windows:
            del self._windows[window_id]
            return True
        return False

    def to_dict(self) -> Dict[str, Any]:
        """Serialize all states to dict."""
        return {
            wid: {
                "title": s.title,
                "x": s.x,
                "y": s.y,
                "width": s.width,
                "height": s.height,
                "is_visible": s.is_visible,
                "is_focused": s.is_focused,
                "timestamp": s.timestamp,
            }
            for wid, s in self._windows.items()
        }


__all__ = [
    "WindowState",
    "WindowStateManager",
]
