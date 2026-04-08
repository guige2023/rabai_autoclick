"""Focus management utilities for window and application control.

Provides helpers for bringing applications to the foreground,
managing focus between windows, waiting for windows to appear,
and handling focus-related events in automation workflows.

Example:
    >>> from utils.focus_management_utils import focus_app, wait_for_window, ensure_focused
    >>> focus_app('Safari')
    >>> wait_for_window('Chrome', timeout=5.0)
    >>> ensure_focused('Chrome', lambda: click(100, 200))
"""

from __future__ import annotations

import subprocess
import time
from dataclasses import dataclass
from typing import Callable, Optional

__all__ = [
    "FocusManager",
    "focus_app",
    "wait_for_window",
    "ensure_focused",
    "is_app_running",
    "is_window_visible",
    "FocusError",
]


class FocusError(Exception):
    """Raised when a focus operation fails."""
    pass


def is_app_running(bundle_id: str) -> bool:
    """Check if an application is currently running.

    Args:
        bundle_id: The app bundle identifier (e.g., 'com.apple.Safari').

    Returns:
        True if the app process exists.
    """
    try:
        result = subprocess.run(
            ["pgrep", "-x", bundle_id],
            capture_output=True,
            timeout=5,
        )
        return result.returncode == 0
    except Exception:
        return False


def is_app_running_by_name(name: str) -> bool:
    """Check if an application is running by its display name."""
    import sys

    if sys.platform == "darwin":
        script = f'tell application "System Events" to (name of processes) contains "{name}"'
        try:
            result = subprocess.run(
                ["osascript", "-e", script],
                capture_output=True,
                timeout=5,
            )
            return result.stdout.strip() == "true"
        except Exception:
            return False
    return False


def focus_app(name: str) -> bool:
    """Bring an application to the front (activate it).

    Args:
        name: Application name (e.g., 'Safari', 'Google Chrome').

    Returns:
        True if successful.
    """
    import sys

    if sys.platform == "darwin":
        script = f'tell application "{name}" to activate'
        try:
            subprocess.run(["osascript", "-e", script], timeout=5, check=True)
            return True
        except Exception:
            return False
    return False


def focus_app_by_bundle_id(bundle_id: str) -> bool:
    """Activate an application by its bundle identifier."""
    import sys

    if sys.platform == "darwin":
        script = f'tell application id "{bundle_id}" to activate'
        try:
            subprocess.run(["osascript", "-e", script], timeout=5, check=True)
            return True
        except Exception:
            return False
    return False


def get_frontmost_app() -> Optional[str]:
    """Get the name of the currently frontmost application.

    Returns:
        Application name, or None on failure.
    """
    import sys

    if sys.platform == "darwin":
        script = 'tell application "System Events" to name of first process whose frontmost is true'
        try:
            result = subprocess.run(
                ["osascript", "-e", script],
                capture_output=True,
                timeout=5,
            )
            name = result.stdout.strip()
            return name if name else None
        except Exception:
            return None
    return None


def is_window_visible(title_pattern: str) -> bool:
    """Check if a window matching the title pattern is currently visible.

    Args:
        title_pattern: Substring to match in window titles.

    Returns:
        True if a matching window is visible and not minimized.
    """
    try:
        from utils.window_utils import get_windows_by_title

        wins = get_windows_by_title(title_pattern)
        return any(w.is_visible and not w.is_minimized for w in wins)
    except ImportError:
        return False


def wait_for_window(
    title_pattern: str,
    timeout: float = 10.0,
    poll_interval: float = 0.2,
    require_visible: bool = True,
) -> bool:
    """Wait for a window with a matching title to appear.

    Args:
        title_pattern: Substring to match in window titles.
        timeout: Maximum wait time in seconds.
        poll_interval: Time between checks.
        require_visible: If True, wait for the window to be visible/unminimized.

    Returns:
        True if the window appeared within the timeout.
    """
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            from utils.window_utils import get_windows_by_title

            wins = get_windows_by_title(title_pattern)
            if wins:
                if not require_visible:
                    return True
                if any(w.is_visible and not w.is_minimized for w in wins):
                    return True
        except ImportError:
            pass
        time.sleep(poll_interval)
    return False


def ensure_focused(
    app_name: str,
    action: Callable,
    focus_timeout: float = 5.0,
) -> any:
    """Ensure an app is focused, execute an action, and return the result.

    This is a convenience wrapper that focuses an app, then runs an action
    callable, handling the common "click somewhere that activates a window"
    pattern in automation scripts.

    Args:
        app_name: Name of the application to focus.
        action: Callable to execute while the app is focused.
        focus_timeout: Timeout for waiting on the app to be frontmost.

    Returns:
        The return value of the action callable.

    Example:
        >>> from utils.input_simulation_utils import click
        >>> result = ensure_focused('Safari', lambda: click(100, 200))
    """
    focus_app(app_name)

    # Give the app a moment to come to front
    time.sleep(0.1)

    # Wait until the app is actually frontmost
    deadline = time.time() + focus_timeout
    while time.time() < deadline:
        if get_frontmost_app() == app_name:
            break
        time.sleep(0.1)

    return action()


@dataclass
class FocusState:
    """Captures the current focus state for later restoration."""

    frontmost_app: Optional[str] = None

    @classmethod
    def capture(cls) -> "FocusState":
        """Capture the current focus state."""
        return cls(frontmost_app=get_frontmost_app())

    def restore(self) -> None:
        """Restore the captured focus state."""
        if self.frontmost_app:
            focus_app(self.frontmost_app)


class FocusManager:
    """Context manager for safe focus operations.

    Captures the current focus on entry, executes a block with a
    potentially different focus, then restores on exit.

    Example:
        >>> with FocusManager() as fm:
        ...     focus_app('Safari')
        ...     click(100, 200)
        ... # focus is restored to previous state
    """

    def __init__(self):
        self._state: Optional[FocusState] = None

    def __enter__(self) -> "FocusManager":
        self._state = FocusState.capture()
        return self

    def __exit__(self, *args) -> None:
        if self._state is not None:
            self._state.restore()

    def capture(self) -> FocusState:
        """Manually capture the current focus state."""
        return FocusState.capture()
