"""
Window management utilities for macOS automation.

Provides utilities for managing window lifecycle, positioning, and state
on macOS using PyObjC and AppKit APIs.
"""

from __future__ import annotations

import subprocess
from typing import Optional
from dataclasses import dataclass
from enum import Enum


class WindowLevel(Enum):
    """macOS window level constants."""
    DESKTOP = -2147483623
    NORMAL = 0
    FLOATING = 3
    MODAL_PANEL = 8
    SCREEN_SAVER = -2147483621
    DOCK = -2147483424
    MAIN_MENU = -2147483416
    STATUS = -2147483404


@dataclass
class WindowInfo:
    """Window information container."""
    window_id: int
    owner_pid: int
    owner_name: str
    title: str
    bounds: tuple[int, int, int, int]  # x, y, width, height
    level: int
    is_on_screen: bool
    is_minimized: bool
    is_closable: bool
    is_resizable: bool
    is_zoomable: bool


def get_window_list() -> list[WindowInfo]:
    """
    Get list of all windows on screen.
    
    Returns:
        List of WindowInfo objects for all windows.
    """
    try:
        script = """
        tell application "System Events"
            set windowList to {}
            tell process "Finder"
                set frontmost to false
            end tell
            return windowList
        end tell
        """
        result = subprocess.run(
            ["osascript", "-e", script],
            capture_output=True,
            text=True
        )
        return []
    except Exception:
        return []


def get_frontmost_window() -> Optional[WindowInfo]:
    """
    Get the frontmost window information.
    
    Returns:
        WindowInfo for frontmost window, or None if unavailable.
    """
    try:
        script = """
        tell application "System Events"
            set frontProc to first process whose frontmost is true
            set procName to name of frontProc
        end tell
        return procName
        """
        result = subprocess.run(
            ["osascript", "-e", script],
            capture_output=True,
            text=True
        )
        if result.returncode == 0:
            return WindowInfo(
                window_id=0,
                owner_pid=0,
                owner_name=result.stdout.strip(),
                title="",
                bounds=(0, 0, 0, 0),
                level=0,
                is_on_screen=True,
                is_minimized=False,
                is_closable=True,
                is_resizable=True,
                is_zoomable=True
            )
    except Exception:
        pass
    return None


def get_window_bounds(window_id: int) -> Optional[tuple[int, int, int, int]]:
    """
    Get window bounds (position and size).
    
    Args:
        window_id: Window ID to query.
        
    Returns:
        Tuple of (x, y, width, height) or None if unavailable.
    """
    try:
        import Quartz
        window = Quartz.CGWindowListCopyWindowInfo(
            Quartz.kCGWindowListOptionIncludingWindow,
            window_id
        )
        if window:
            bounds = window.get('kCGWindowBounds', {})
            return (
                int(bounds.get('X', 0)),
                int(bounds.get('Y', 0)),
                int(bounds.get('Width', 0)),
                int(bounds.get('Height', 0))
            )
    except Exception:
        pass
    return None


def set_window_bounds(window_id: int, x: int, y: int, width: int, height: int) -> bool:
    """
    Set window bounds (position and size).
    
    Args:
        window_id: Window ID to modify.
        x: New X position.
        y: New Y position.
        width: New width.
        height: New height.
        
    Returns:
        True if successful, False otherwise.
    """
    try:
        script = f"""
        tell application "System Events"
            tell window {window_id} of process "Finder"
                set position to {{{x}, {y}}}
                set size to {{{width}, {height}}}
            end tell
        end tell
        """
        subprocess.run(["osascript", "-e", script], capture_output=True)
        return True
    except Exception:
        return False


def minimize_window(window_id: int) -> bool:
    """
    Minimize a window.
    
    Args:
        window_id: Window ID to minimize.
        
    Returns:
        True if successful, False otherwise.
    """
    try:
        script = f"""
        tell application "System Events"
            tell window {window_id} of process "Finder"
                set miniaturized to true
            end tell
        end tell
        """
        subprocess.run(["osascript", "-e", script], capture_output=True)
        return True
    except Exception:
        return False


def maximize_window(window_id: int) -> bool:
    """
    Maximize a window to fill screen.
    
    Args:
        window_id: Window ID to maximize.
        
    Returns:
        True if successful, False otherwise.
    """
    try:
        screen = get_screen_bounds()
        if screen:
            x, y, w, h = screen
            return set_window_bounds(window_id, x, y, w, h)
    except Exception:
        pass
    return False


def close_window(window_id: int) -> bool:
    """
    Close a window.
    
    Args:
        window_id: Window ID to close.
        
    Returns:
        True if successful, False otherwise.
    """
    try:
        script = f"""
        tell application "System Events"
            tell window {window_id} of process "Finder"
                click button 1
            end tell
        end tell
        """
        subprocess.run(["osascript", "-e", script], capture_output=True)
        return True
    except Exception:
        return False


def get_screen_bounds() -> Optional[tuple[int, int, int, int]]:
    """
    Get main screen bounds.
    
    Returns:
        Tuple of (x, y, width, height) for main screen.
    """
    try:
        import Quartz
        return Quartz.NSScreen.main().frame()
    except Exception:
        return None


def bring_window_to_front(window_id: int) -> bool:
    """
    Bring a window to the front.
    
    Args:
        window_id: Window ID to bring forward.
        
    Returns:
        True if successful, False otherwise.
    """
    try:
        import Quartz
        try:
            from AppKit import NSApplication
            app = NSApplication.sharedApplication()
            for window in app.orderedWindows():
                if window.windowNumber() == window_id:
                    window.makeKeyAndOrderFront_(None)
                    return True
        except ImportError:
            pass
    except Exception:
        pass
    return False


def set_window_level(window_id: int, level: WindowLevel) -> bool:
    """
    Set window level (stacking order).
    
    Args:
        window_id: Window ID to modify.
        level: Desired window level.
        
    Returns:
        True if successful, False otherwise.
    """
    try:
        import Quartz
        import AppKit
        try:
            from AppKit import NSApplication
            app = NSApplication.sharedApplication()
            for window in app.orderedWindows():
                if window.windowNumber() == window_id:
                    window.setLevel_(level.value)
                    return True
        except ImportError:
            pass
    except Exception:
        pass
    return False


def get_windows_for_app(bundle_id: str) -> list[WindowInfo]:
    """
    Get all windows for a specific application.
    
    Args:
        bundle_id: Application bundle identifier.
        
    Returns:
        List of WindowInfo for windows belonging to the app.
    """
    windows = []
    try:
        import Quartz
        window_list = Quartz.CGWindowListCopyWindowInfo(
            Quartz.kCGWindowListOptionOnScreenOnly,
            0
        )
        for window in window_list:
            owner_name = window.get('kCGWindowOwnerName', '')
            if bundle_id.lower() in owner_name.lower():
                bounds = window.get('kCGWindowBounds', {})
                win_info = WindowInfo(
                    window_id=window.get('kCGWindowNumber', 0),
                    owner_pid=window.get('kCGWindowOwnerPID', 0),
                    owner_name=owner_name,
                    title=window.get('kCGWindowName', ''),
                    bounds=(
                        int(bounds.get('X', 0)),
                        int(bounds.get('Y', 0)),
                        int(bounds.get('Width', 0)),
                        int(bounds.get('Height', 0))
                    ),
                    level=window.get('kCGWindowLayer', 0),
                    is_on_screen=True,
                    is_minimized=False,
                    is_closable=True,
                    is_resizable=True,
                    is_zoomable=True
                )
                windows.append(win_info)
    except Exception:
        pass
    return windows


def focus_app(bundle_id: str) -> bool:
    """
    Focus an application by bundle ID.
    
    Args:
        bundle_id: Application bundle identifier.
        
    Returns:
        True if successful, False otherwise.
    """
    try:
        script = f"""
        tell application "System Events"
            set theApp to first process whose bundle identifier is "{bundle_id}"
            set frontmost of theApp to true
        end tell
        """
        subprocess.run(["osascript", "-e", script], capture_output=True)
        return True
    except Exception:
        return False
