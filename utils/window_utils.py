"""Window management utilities for querying and controlling application windows.

Provides cross-platform helpers for listing windows, activating windows
by title or process, minimizing/maximizing/closing windows, and
querying window properties such as bounds, title, and process name.

Example:
    >>> from utils.window_utils import get_windows, activate_window, set_window_bounds
    >>> wins = get_windows()
    >>> chrome = next(w for w in wins if 'Chrome' in w.title)
    >>> activate_window(chrome.id)
    >>> set_window_bounds(chrome.id, (0, 0, 800, 600))
"""

from __future__ import annotations

import subprocess
from dataclasses import dataclass
from typing import Optional

__all__ = [
    "WindowInfo",
    "get_windows",
    "get_windows_by_title",
    "get_front_window",
    "activate_window",
    "set_window_bounds",
    "minimize_window",
    "maximize_window",
    "close_window",
    "WindowError",
]


@dataclass
class WindowInfo:
    """Information about a window."""

    id: int
    title: str
    owner_name: str
    owner_pid: int
    bounds: tuple[float, float, float, float]
    is_minimized: bool
    is_visible: bool

    @property
    def x(self) -> float:
        return self.bounds[0]

    @property
    def y(self) -> float:
        return self.bounds[1]

    @property
    def width(self) -> float:
        return self.bounds[2]

    @property
    def height(self) -> float:
        return self.bounds[3]


class WindowError(Exception):
    """Raised when a window operation fails."""
    pass


def get_windows() -> list[WindowInfo]:
    """Get a list of all visible windows.

    Returns:
        List of WindowInfo objects for all windows.
    """
    import sys

    if sys.platform == "darwin":
        return _get_windows_darwin()
    elif sys.platform == "win32":
        return _get_windows_windows()
    else:
        return _get_windows_x11()


def _get_windows_darwin() -> list[WindowInfo]:
    """Query windows via osascript on macOS."""
    script = """
    tell application "System Events"
        set winList to {}
        set appList to every process whose visible is true
        repeat with appProc in appList
            try
                set winArray to windows of appProc
                repeat with win in winArray
                    set winPos to position of win
                    set winSize to size of win
                    set winTitle to name of win
                    set winMini to minimized of win
                    set winVis to visible of win
                    set appName to name of appProc
                    set appPID to unix id of appProc
                    copy {winTitle, winPos, winSize, winMini, winVis, appName, appPID} to end of winList
                end repeat
            end try
        end repeat
    end tell
    return winList
    """

    try:
        output = subprocess.check_output(
            ["osascript", "-e", script],
            timeout=10,
        )
    except Exception:
        return []

    windows = []
    for line in output.decode().strip().split("\n"):
        if not line.strip().startswith("{"):
            continue
        # Parse: {title, x, y, w, h, minimized, visible, owner, pid}
        parts = [p.strip() for p in line.strip().strip(",{}").split(",")]
        if len(parts) < 9:
            continue
        try:
            title = parts[0].strip('"')
            x = float(parts[1])
            y = float(parts[2])
            w = float(parts[3])
            h = float(parts[4])
            minimized = parts[5] == "true"
            visible = parts[6] == "true"
            owner = parts[7].strip('"')
            pid = int(parts[8])
            windows.append(
                WindowInfo(
                    id=pid * 10000 + len(windows),
                    title=title,
                    owner_name=owner,
                    owner_pid=pid,
                    bounds=(x, y, w, h),
                    is_minimized=minimized,
                    is_visible=visible,
                )
            )
        except (ValueError, IndexError):
            continue

    return windows


def _get_windows_windows() -> list[WindowInfo]:
    """Query windows via pywin32 on Windows."""
    try:
        import win32gui
        import win32con
    except ImportError:
        return []

    windows = []

    def enum_handler(hwnd, _):
        if win32gui.IsWindowVisible(hwnd):
            title = win32gui.GetWindowText(hwnd)
            rect = win32gui.GetWindowRect(hwnd)
            pid = win32gui.GetWindowThreadProcessId(hwnd)[1]
            x = rect[0]
            y = rect[1]
            w = rect[2] - rect[0]
            h = rect[3] - rect[1]
            minimized = win32gui.IsIconic(hwnd)
            windows.append(
                WindowInfo(
                    id=hwnd,
                    title=title,
                    owner_name="",
                    owner_pid=pid,
                    bounds=(x, y, w, h),
                    is_minimized=minimized,
                    is_visible=True,
                )
            )

    win32gui.EnumWindows(enum_handler, None)
    return windows


def _get_windows_x11() -> list[WindowInfo]:
    """Query windows via xdotool/xwininfo on Linux."""
    try:
        output = subprocess.check_output(
            ["bash", "-c", "xdotool search --onlyvisible . 2>/dev/null"],
            timeout=5,
        )
    except Exception:
        return []

    windows = []
    for wid in output.decode().strip().split("\n"):
        if not wid.strip():
            continue
        try:
            info = subprocess.check_output(
                ["xdotool", "getwindowname", wid.strip()],
                timeout=2,
            ).decode().strip()
            prop = subprocess.check_output(
                ["xdotool", "getwindowgeometry", "--shell", wid.strip()],
                timeout=2,
            )
            # Basic parsing - would need more robust handling
            windows.append(
                WindowInfo(
                    id=int(wid.strip()),
                    title=info,
                    owner_name="",
                    owner_pid=0,
                    bounds=(0, 0, 800, 600),
                    is_minimized=False,
                    is_visible=True,
                )
            )
        except Exception:
            continue

    return windows


def get_windows_by_title(pattern: str, case_sensitive: bool = False) -> list[WindowInfo]:
    """Find windows whose titles match a pattern.

    Args:
        pattern: Substring to search for in window titles.
        case_sensitive: Whether to match case exactly.

    Returns:
        List of matching WindowInfo objects.
    """
    wins = get_windows()
    if case_sensitive:
        return [w for w in wins if pattern in w.title]
    p = pattern.lower()
    return [w for w in wins if p in w.title.lower()]


def get_front_window() -> Optional[WindowInfo]:
    """Get the currently active (frontmost) window.

    Returns:
        WindowInfo of the front window, or None.
    """
    import sys

    if sys.platform == "darwin":
        script = """
        tell application "System Events"
            set frontApp to first application process whose frontmost is true
            set appName to name of frontApp
            set winTitle to ""
            try
                set winTitle to name of first window of frontApp
            end try
            set appPID to unix id of frontApp
        end tell
        return {appName, appPID, winTitle}
        """
        try:
            output = subprocess.check_output(["osascript", "-e", script], timeout=5)
            parts = [p.strip().strip('"') for p in output.decode().strip().split(",")]
            if len(parts) >= 3:
                owner_name, pid, title = parts[0], parts[1], parts[2]
                wins = [w for w in get_windows() if w.owner_pid == int(pid)]
                if wins:
                    return wins[0]
        except Exception:
            pass

    return None


def activate_window(window_id: int) -> bool:
    """Bring a window to the front and activate it.

    Args:
        window_id: The window ID from WindowInfo.

    Returns:
        True if successful.
    """
    import sys

    if sys.platform == "darwin":
        wins = get_windows()
        win = next((w for w in wins if w.id == window_id), None)
        if win is None:
            return False
        script = f'''
        tell application "System Events"
            set appProc to first process whose unix id is {win.owner_pid}
            set frontmost of appProc to true
        end tell
        '''
        try:
            subprocess.run(["osascript", "-e", script], timeout=5, check=True)
            return True
        except Exception:
            return False

    return False


def set_window_bounds(window_id: int, bounds: tuple[float, float, float, float]) -> bool:
    """Set the position and size of a window.

    Args:
        window_id: The window ID from WindowInfo.
        bounds: New bounds as (x, y, width, height).

    Returns:
        True if successful.
    """
    import sys

    x, y, w, h = bounds
    if sys.platform == "darwin":
        wins = get_windows()
        win = next((w_ for w_ in wins if w_.id == window_id), None)
        if win is None:
            return False
        script = f'''
        tell application "System Events"
            tell process "{win.owner_name}"
                set position of window 1 to {{{int(x)}, {int(y)}}}
                set size of window 1 to {{{int(w)}, {int(h)}}}
            end tell
        end tell
        '''
        try:
            subprocess.run(["osascript", "-e", script], timeout=5, check=True)
            return True
        except Exception:
            return False

    return False


def minimize_window(window_id: int) -> bool:
    """Minimize a window."""
    import sys

    if sys.platform == "darwin":
        wins = get_windows()
        win = next((w for w in wins if w.id == window_id), None)
        if win is None:
            return False
        script = f'''
        tell application "System Events"
            set appProc to first process whose unix id is {win.owner_pid}
            set miniaturized of (first window of appProc) to true
        end tell
        '''
        try:
            subprocess.run(["osascript", "-e", script], timeout=5, check=True)
            return True
        except Exception:
            return False

    return False


def maximize_window(window_id: int) -> bool:
    """Maximize a window (set to screen size)."""
    import sys

    if sys.platform == "darwin":
        from utils.display_utils import get_primary_display

        primary = get_primary_display()
        if primary is None:
            return False
        return set_window_bounds(window_id, primary.bounds)

    return False


def close_window(window_id: int) -> bool:
    """Close a window by sending Cmd+W (macOS) or WM_CLOSE."""
    import sys

    if sys.platform == "darwin":
        wins = get_windows()
        win = next((w for w in wins if w.id == window_id), None)
        if win is None:
            return False
        script = f'''
        tell application "System Events"
            tell process "{win.owner_name}"
                keystroke "w" using command down
            end tell
        end tell
        '''
        try:
            subprocess.run(["osascript", "-e", script], timeout=5, check=True)
            return True
        except Exception:
            return False

    return False
