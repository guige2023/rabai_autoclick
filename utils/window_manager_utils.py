"""
Window management utilities for UI automation.

This module provides utilities for managing windows including
positioning, sizing, focusing, and multi-window operations.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable, List, Optional, Dict, Any, Tuple
from enum import Enum, auto


class WindowState(Enum):
    """Window state values."""
    MINIMIZED = auto()
    NORMAL = auto()
    MAXIMIZED = auto()
    FULL_SCREEN = auto()
    HIDDEN = auto()


class WindowLevel(Enum):
    """Window level/z-order values."""
    DESKTOP = auto()
    NORMAL = auto()
    FLOATER = auto()
    MODAL = auto()
    POP_UP = auto()
    SCREEN_SAVER = auto()
    MAXIMUM = auto()


@dataclass
class WindowInfo:
    """
    Information about a window.

    Attributes:
        window_id: Unique window identifier.
        title: Window title.
        bounds: Window rectangle (x, y, width, height).
        state: Current window state.
        level: Window level in z-order.
        app_name: Name of owning application.
        is_active: Whether window is currently active.
        is_focused: Whether window has focus.
    """
    window_id: int
    title: str = ""
    bounds: Optional[Tuple[float, float, float, float]] = None
    state: WindowState = WindowState.NORMAL
    level: WindowLevel = WindowLevel.NORMAL
    app_name: str = ""
    is_active: bool = False
    is_focused: bool = False

    @property
    def x(self) -> float:
        """Get window X position."""
        return self.bounds[0] if self.bounds else 0

    @property
    def y(self) -> float:
        """Get window Y position."""
        return self.bounds[1] if self.bounds else 0

    @property
    def width(self) -> float:
        """Get window width."""
        return self.bounds[2] if self.bounds else 0

    @property
    def height(self) -> float:
        """Get window height."""
        return self.bounds[3] if self.bounds else 0

    @property
    def center_x(self) -> float:
        """Get center X coordinate."""
        return self.x + self.width / 2

    @property
    def center_y(self) -> float:
        """Get center Y coordinate."""
        return self.y + self.height / 2

    @property
    def frame(self) -> Tuple[float, float, float, float]:
        """Get window frame for positioning."""
        return self.bounds if self.bounds else (0, 0, 800, 600)


@dataclass
class ScreenInfo:
    """
    Information about a display/screen.

    Attributes:
        screen_id: Screen identifier.
        bounds: Screen bounds (x, y, width, height).
        is_main: Whether this is the main screen.
        scale_factor: Display scale factor.
    """
    screen_id: int
    bounds: Tuple[float, float, float, float]
    is_main: bool = False
    scale_factor: float = 1.0

    @property
    def width(self) -> float:
        """Get screen width."""
        return self.bounds[2]

    @property
    def height(self) -> float:
        """Get screen height."""
        return self.bounds[3]


class WindowManager:
    """
    Manages window operations for automation.

    Provides methods for positioning, sizing, focusing,
    and orchestrating multiple windows.
    """

    def __init__(self) -> None:
        self._windows: Dict[int, WindowInfo] = {}
        self._screens: Dict[int, ScreenInfo] = {}
        self._handler: Optional[Callable[..., None]] = None

    def set_change_handler(self, handler: Callable[..., None]) -> None:
        """Set callback for window change events."""
        self._handler = handler

    def add_window(self, window: WindowInfo) -> WindowManager:
        """Register a window with the manager."""
        self._windows[window.window_id] = window
        return self

    def remove_window(self, window_id: int) -> bool:
        """Unregister a window."""
        return self._windows.pop(window_id, None) is not None

    def get_window(self, window_id: int) -> Optional[WindowInfo]:
        """Get window by ID."""
        return self._windows.get(window_id)

    def get_all_windows(self) -> List[WindowInfo]:
        """Get all registered windows."""
        return list(self._windows.values())

    def get_windows_by_app(self, app_name: str) -> List[WindowInfo]:
        """Get all windows for an application."""
        return [w for w in self._windows.values() if w.app_name == app_name]

    def get_active_window(self) -> Optional[WindowInfo]:
        """Get the currently active window."""
        for window in self._windows.values():
            if window.is_active:
                return window
        return None

    def get_focused_window(self) -> Optional[WindowInfo]:
        """Get the window with keyboard focus."""
        for window in self._windows.values():
            if window.is_focused:
                return window
        return None

    def tile_windows(
        self,
        windows: List[WindowInfo],
        layout: str = "horizontal",
    ) -> Dict[int, Tuple[float, float, float, float]]:
        """
        Calculate tiled positions for windows.

        Returns map of window_id to new bounds.
        """
        if not windows:
            return {}

        screen = self._screens.get(0)
        if not screen:
            return {}

        screen_w = screen.width
        screen_h = screen.height

        if layout == "horizontal":
            w_width = screen_w // len(windows)
            w_height = screen_h
            positions = {
                w.window_id: (i * w_width, 0, w_width, w_height)
                for i, w in enumerate(windows)
            }
        elif layout == "vertical":
            w_width = screen_w
            w_height = screen_h // len(windows)
            positions = {
                w.window_id: (0, i * w_height, w_width, w_height)
                for i, w in enumerate(windows)
            }
        elif layout == "grid":
            cols = int(len(windows) ** 0.5) + 1
            w_width = screen_w // cols
            w_height = screen_h // ((len(windows) + cols - 1) // cols)
            positions = {}
            for i, w in enumerate(windows):
                row = i // cols
                col = i % cols
                positions[w.window_id] = (col * w_width, row * w_height, w_width, w_height)
        else:
            positions = {w.window_id: w.frame for w in windows}

        return positions

    def arrange_cascade(
        self,
        windows: List[WindowInfo],
        start_offset: int = 50,
    ) -> Dict[int, Tuple[float, float, float, float]]:
        """Calculate cascading window positions."""
        if not windows:
            return {}

        positions = {}
        for i, window in enumerate(windows):
            x = start_offset * i
            y = start_offset * i
            positions[window.window_id] = (x, y, window.width, window.height)

        return positions

    def find_window_at_point(
        self,
        x: float,
        y: float,
    ) -> Optional[WindowInfo]:
        """Find the topmost window containing a point."""
        candidates: List[Tuple[int, WindowInfo]] = []

        for window in self._windows.values():
            if window.bounds:
                wx, wy, ww, wh = window.bounds
                if wx <= x < wx + ww and wy <= y < wy + wh:
                    candidates.append((window.window_id, window))

        # Return topmost (last in list assumes z-order)
        if candidates:
            return candidates[-1][1]
        return None


class WindowTransition:
    """
    Animates window position/size transitions.

    Provides smooth animated transitions between window states.
    """

    def __init__(self, duration: float = 0.3) -> None:
        self._duration = duration
        self._easing: Callable[[float], float] = self._ease_out_quad

    def animate_position(
        self,
        window_id: int,
        from_pos: Tuple[float, float],
        to_pos: Tuple[float, float],
        progress: float,
    ) -> Tuple[float, float]:
        """Interpolate window position at given progress."""
        t = self._easing(progress)
        x = from_pos[0] + (to_pos[0] - from_pos[0]) * t
        y = from_pos[1] + (to_pos[1] - from_pos[1]) * t
        return (x, y)

    def animate_size(
        self,
        window_id: int,
        from_size: Tuple[float, float],
        to_size: Tuple[float, float],
        progress: float,
    ) -> Tuple[float, float]:
        """Interpolate window size at given progress."""
        t = self._easing(progress)
        w = from_size[0] + (to_size[0] - from_size[0]) * t
        h = from_size[1] + (to_size[1] - from_size[1]) * t
        return (w, h)

    @staticmethod
    def _ease_out_quad(t: float) -> float:
        """Quadratic ease-out."""
        return -t * (t - 2)


def get_window_title_for_app(app_bundle_id: str) -> str:
    """Get formatted window title for application."""
    app_names = {
        "com.apple.finder": "Finder",
        "com.apple.Safari": "Safari",
        "com.apple.mail": "Mail",
        "com.apple.Notes": "Notes",
        "com.apple.Terminal": "Terminal",
    }
    return app_names.get(app_bundle_id, app_bundle_id)
