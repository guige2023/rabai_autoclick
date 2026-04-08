"""Window control utilities for UI automation.

Provides window control operations including minimize, maximize,
close, focus, and window property management.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Callable, Optional


class WindowState(Enum):
    """Window state values."""
    NORMAL = auto()
    MINIMIZED = auto()
    MAXIMIZED = auto()
    FULLSCREEN = auto()
    HIDDEN = auto()


class WindowAction(Enum):
    """Types of window control actions."""
    MINIMIZE = auto()
    MAXIMIZE = auto()
    RESTORE = auto()
    CLOSE = auto()
    FOCUS = auto()
    HIDE = auto()
    SHOW = auto()
    MOVE = auto()
    RESIZE = auto()
    SET_STATE = auto()


@dataclass
class WindowBounds:
    """Window position and size.

    Attributes:
        x: Left edge X coordinate.
        y: Top edge Y coordinate.
        width: Window width.
        height: Window height.
    """
    x: float
    y: float
    width: float
    height: float

    @property
    def x2(self) -> float:
        """Right edge X."""
        return self.x + self.width

    @property
    def y2(self) -> float:
        """Bottom edge Y."""
        return self.y + self.height

    @property
    def center(self) -> tuple[float, float]:
        """Center point."""
        return (self.x + self.width / 2, self.y + self.height / 2)

    def contains(self, x: float, y: float) -> bool:
        """Check if point is inside bounds."""
        return self.x <= x < self.x2 and self.y <= y < self.y2


@dataclass
class WindowInfo:
    """Information about a window.

    Attributes:
        window_id: Unique identifier.
        title: Window title.
        process_name: Process name.
        bounds: Current window bounds.
        state: Current window state.
        is_focused: Whether window has focus.
        is_visible: Whether window is visible.
        monitor_id: ID of the monitor containing this window.
        owner_id: ID of the owning window (for dialogs, popups).
        parent_id: ID of the parent window.
    """
    window_id: str
    title: str
    process_name: str = ""
    bounds: WindowBounds = field(
        default_factory=lambda: WindowBounds(0, 0, 800, 600)
    )
    state: WindowState = WindowState.NORMAL
    is_focused: bool = False
    is_visible: bool = True
    monitor_id: str = ""
    owner_id: str = ""
    parent_id: str = ""
    id: str = field(default_factory=lambda: str(uuid.uuid4()))


class WindowController:
    """Controls window operations.

    Provides a platform-agnostic interface for window control.
    Platform-specific implementations should override the
    action methods.
    """

    def __init__(self) -> None:
        """Initialize window controller."""
        self._windows: dict[str, WindowInfo] = {}

    def add_window(self, info: WindowInfo) -> str:
        """Register a window."""
        self._windows[info.id] = info
        return info.id

    def get_window(self, window_id: str) -> Optional[WindowInfo]:
        """Get window info."""
        return self._windows.get(window_id)

    def minimize(self, window_id: str) -> bool:
        """Minimize a window."""
        info = self._windows.get(window_id)
        if info:
            info.state = WindowState.MINIMIZED
            return True
        return False

    def maximize(self, window_id: str) -> bool:
        """Maximize a window."""
        info = self._windows.get(window_id)
        if info:
            info.state = WindowState.MAXIMIZED
            return True
        return False

    def restore(self, window_id: str) -> bool:
        """Restore a window to normal size."""
        info = self._windows.get(window_id)
        if info:
            info.state = WindowState.NORMAL
            return True
        return False

    def close(self, window_id: str) -> bool:
        """Close a window."""
        if window_id in self._windows:
            del self._windows[window_id]
            return True
        return False

    def focus(self, window_id: str) -> bool:
        """Bring window to foreground."""
        for wid, info in self._windows.items():
            info.is_focused = (wid == window_id)
        return window_id in self._windows

    def hide(self, window_id: str) -> bool:
        """Hide a window."""
        info = self._windows.get(window_id)
        if info:
            info.is_visible = False
            return True
        return False

    def show(self, window_id: str) -> bool:
        """Show a window."""
        info = self._windows.get(window_id)
        if info:
            info.is_visible = True
            return True
        return False

    def move(self, window_id: str, x: float, y: float) -> bool:
        """Move window to position."""
        info = self._windows.get(window_id)
        if info:
            info.bounds.x = x
            info.bounds.y = y
            return True
        return False

    def resize(
        self,
        window_id: str,
        width: float,
        height: float,
    ) -> bool:
        """Resize window."""
        info = self._windows.get(window_id)
        if info:
            info.bounds.width = width
            info.bounds.height = height
            return True
        return False

    def set_bounds(
        self,
        window_id: str,
        x: float,
        y: float,
        width: float,
        height: float,
    ) -> bool:
        """Set window bounds."""
        info = self._windows.get(window_id)
        if info:
            info.bounds = WindowBounds(x, y, width, height)
            return True
        return False

    def get_focused_window(self) -> Optional[WindowInfo]:
        """Get the currently focused window."""
        for info in self._windows.values():
            if info.is_focused:
                return info
        return None

    def get_visible_windows(self) -> list[WindowInfo]:
        """Get all visible windows."""
        return [w for w in self._windows.values() if w.is_visible]

    @property
    def window_count(self) -> int:
        """Return number of tracked windows."""
        return len(self._windows)
