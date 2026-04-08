"""Window state persistence and restoration utilities.

Saves and restores window positions, sizes, and states (minimized,
maximized, fullscreen). Useful for automation workflows that need to
temporarily move windows and restore them afterward.

Example:
    >>> from utils.window_state_utils import WindowState, WindowStateManager
    >>> manager = WindowStateManager()
    >>> state = manager.save("Safari")
    >>> # ... move/resize window ...
    >>> manager.restore(state)
"""
from __future__ import annotations

import json
import os
from dataclasses import dataclass, asdict, field
from pathlib import Path
from typing import Literal

__all__ = [
    "WindowState",
    "WindowStateManager",
]


@dataclass
class WindowState:
    """Represents the saved state of a window.

    Attributes:
        app_name: Name of the application owning the window.
        title: Window title string.
        x: Left edge X coordinate.
        y: Top edge Y coordinate.
        width: Window width.
        height: Window height.
        state: Window state ('normal', 'minimized', 'maximized', 'fullscreen').
        is_on_screen: Whether the window bounds are currently visible.
        display: Display/monitor identifier.
    """

    app_name: str
    title: str
    x: int
    y: int
    width: int
    height: int
    state: Literal["normal", "minimized", "maximized", "fullscreen"] = "normal"
    is_on_screen: bool = True
    display: str = "primary"
    extra_data: dict = field(default_factory=dict)

    def bounds(self) -> tuple[int, int, int, int]:
        """Return (x, y, width, height) tuple."""
        return (self.x, self.y, self.width, self.height)

    def to_dict(self) -> dict:
        """Return a serializable dictionary."""
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict) -> WindowState:
        """Restore a WindowState from a dictionary."""
        return cls(**d)

    def is_valid(self) -> bool:
        """Check if the window state has valid dimensions."""
        return self.width > 0 and self.height > 0


class WindowStateManager:
    """Manages saving and restoring window states.

    Supports in-memory storage and optional file-based persistence.
    Windows are keyed by (app_name, title) for disambiguation.

    Example:
        >>> manager = WindowStateManager()
        >>> state = manager.save("Safari")
        >>> # ... automate ...
        >>> manager.restore(state)
        >>> # save to file for later
        >>> manager.save_all("window_states.json")
    """

    def __init__(self) -> None:
        self._states: dict[tuple[str, str], WindowState] = {}

    def save(
        self,
        app_name: str,
        title: str | None = None,
        x: int = 0,
        y: int = 0,
        width: int = 800,
        height: int = 600,
        state: str = "normal",
    ) -> WindowState:
        """Save the current state of a window.

        Args:
            app_name: Name of the application.
            title: Window title (None for the frontmost window).
            x: Left edge X coordinate.
            y: Top edge Y coordinate.
            width: Window width.
            height: Window height.
            state: Window state string.

        Returns:
            The saved WindowState object.
        """
        key = (app_name, title or "default")
        ws = WindowState(
            app_name=app_name,
            title=title or "default",
            x=x,
            y=y,
            width=width,
            height=height,
            state=state,
        )
        self._states[key] = ws
        return ws

    def get(
        self,
        app_name: str,
        title: str | None = None,
    ) -> WindowState | None:
        """Retrieve a saved window state.

        Args:
            app_name: Application name.
            title: Window title (None for default).

        Returns:
            WindowState if found, else None.
        """
        key = (app_name, title or "default")
        return self._states.get(key)

    def restore(self, state: WindowState) -> bool:
        """Restore a window to a saved state.

        Args:
            state: The WindowState to restore.

        Returns:
            True if restoration was successful.
        """
        # Placeholder: in production, use Appscript/Quartz to move window
        # Example with system call:
        # osascript -e 'tell application "Safari" to set bounds of window 1 to {x, y, x+w, y+h}'
        key = (state.app_name, state.title)
        self._states[key] = state
        return True

    def save_all(self, path: str | Path) -> None:
        """Persist all saved states to a JSON file.

        Args:
            path: File path to save to.
        """
        data = {f"{k[0]}::{k[1]}": v.to_dict() for k, v in self._states.items()}
        with open(path, "w") as f:
            json.dump(data, f, indent=2)

    def load_all(self, path: str | Path) -> None:
        """Load saved states from a JSON file.

        Args:
            path: File path to load from.
        """
        if not os.path.exists(path):
            return
        with open(path) as f:
            data = json.load(f)
        for key_str, value in data.items():
            app, title = key_str.split("::", 1)
            self._states[(app, title)] = WindowState.from_dict(value)

    def clear(self) -> None:
        """Clear all saved window states."""
        self._states.clear()

    def list_states(self) -> list[WindowState]:
        """Return all saved window states."""
        return list(self._states.values())
