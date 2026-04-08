"""Window z-order management utilities.

This module provides utilities for managing the stacking order (z-order)
of windows on the desktop.
"""

from __future__ import annotations

import platform
import subprocess
from typing import Optional


IS_MACOS = platform.system() == "Darwin"
IS_LINUX = platform.system() == "Linux"
IS_WINDOWS = platform.system() == "Windows"


def bring_to_front(window_title: Optional[str] = None) -> bool:
    """Bring a window to the front of the z-order.
    
    Args:
        window_title: Title of the window to bring forward.
                    If None, brings the frontmost application.
    
    Returns:
        True if successful.
    """
    if IS_MACOS:
        return _bring_to_front_macos(window_title)
    elif IS_LINUX:
        return _bring_to_front_linux(window_title)
    elif IS_WINDOWS:
        return _bring_to_front_windows(window_title)
    return False


def _bring_to_front_macos(window_title: Optional[str]) -> bool:
    """Bring window to front on macOS."""
    try:
        if window_title:
            script = f'''
            tell application "System Events"
                set frontmost of (first process whose name contains "{window_title}") to true
            end tell
            '''
        else:
            script = '''
            tell application "System Events"
                tell (first process whose frontmost is true)
                    set frontmost to false
                    set frontmost to true
                end tell
            end tell
            '''
        result = subprocess.run(
            ["osascript", "-e", script],
            capture_output=True,
            timeout=5
        )
        return result.returncode == 0
    except Exception:
        return False


def _bring_to_front_linux(window_title: Optional[str]) -> bool:
    """Bring window to front on Linux using xdotool."""
    try:
        if window_title:
            # Find window ID by title
            result = subprocess.run(
                ["xdotool", "search", "--name", window_title],
                capture_output=True,
                text=True,
                timeout=5
            )
            if result.returncode == 0:
                window_id = result.stdout.strip().split("\n")[0]
                subprocess.run(
                    ["xdotool", "windowactivate", window_id],
                    capture_output=True,
                    timeout=5
                )
                return True
        return False
    except FileNotFoundError:
        return False


def _bring_to_front_windows(window_title: Optional[str]) -> bool:
    """Bring window to front on Windows."""
    try:
        import ctypes
        from ctypes import wintypes
        
        user32 = ctypes.windll.user32
        
        if window_title:
            buff = ctypes.create_unicode_buffer(256)
            user32.GetWindowTextW(user32.GetForegroundWindow(), buff, 256)
            
            # Find window by title
            enum_windows = user32.EnumWindows
            enum_windows.argtypes = []
            enum_windows.restype = wintypes.BOOL
            
            callback = ctypes.WINFUNCTYPE(
                wintypes.BOOL, wintypes.HWND, ctypes.POINTER(ctypes.c_int))
            
            hwnd_found = [None]
            
            def enum_callback(hwnd, lParam):
                length = user32.GetWindowTextLengthW(hwnd)
                if length > 0:
                    buff = ctypes.create_unicode_buffer(length + 1)
                    user32.GetWindowTextW(hwnd, buff, length + 1)
                    if window_title.lower() in buff.value.lower():
                        hwnd_found[0] = hwnd
                        return False
                return True
            
            user32.EnumWindows(callback(enum_callback), 0)
            
            if hwnd_found[0]:
                user32.SetForegroundWindow(hwnd_found[0])
                return True
        else:
            # Just activate current
            user32.SetForegroundWindow(user32.GetForegroundWindow())
            return True
        return False
    except Exception:
        return False


def send_to_back(window_title: Optional[str] = None) -> bool:
    """Send a window to the back of the z-order.
    
    Args:
        window_title: Title of the window to send back.
                    If None, sends the frontmost window.
    
    Returns:
        True if successful.
    """
    if IS_MACOS:
        return _send_to_back_macos(window_title)
    elif IS_LINUX:
        return _send_to_back_linux(window_title)
    return False


def _send_to_back_macos(window_title: Optional[str]) -> bool:
    """Send window to back on macOS."""
    try:
        if window_title:
            script = f'''
            tell application "System Events"
                tell (first process whose name contains "{window_title}")
                    set miniaturized of first window to true
                    set miniaturized of first window to false
                end tell
            end tell
            '''
        else:
            script = '''
            tell application "System Events"
                tell (first process whose frontmost is true)
                    set miniaturized of first window to true
                    set miniaturized of first window to false
                end tell
            end tell
            '''
        result = subprocess.run(
            ["osascript", "-e", script],
            capture_output=True,
            timeout=5
        )
        return result.returncode == 0
    except Exception:
        return False


def _send_to_back_linux(window_title: Optional[str]) -> bool:
    """Send window to back on Linux."""
    try:
        if window_title:
            result = subprocess.run(
                ["xdotool", "search", "--name", window_title],
                capture_output=True,
                text=True,
                timeout=5
            )
            if result.returncode == 0:
                window_id = result.stdout.strip().split("\n")[0]
                # Lower the window
                subprocess.run(
                    ["xdotool", "windowlower", window_id],
                    capture_output=True,
                    timeout=5
                )
                return True
        return False
    except FileNotFoundError:
        return False


def get_window_list() -> list[dict]:
    """Get a list of all visible windows.
    
    Returns:
        List of window info dicts with keys: title, process, bounds.
    """
    if IS_MACOS:
        return _get_window_list_macos()
    elif IS_LINUX:
        return _get_window_list_linux()
    return []


def _get_window_list_macos() -> list[dict]:
    """Get window list on macOS."""
    windows = []
    try:
        script = '''
        tell application "System Events"
            set windowList to {}
            tell (every process)
                if visible is true then
                    set processName to name
                    tell (every window)
                        if exists then
                            set windowTitle to name
                            set windowBounds to bounds
                            copy {processName, windowTitle, windowBounds} to end of windowList
                        end if
                    end tell
                end if
            end tell
            return windowList
        end tell
        '''
        result = subprocess.run(
            ["osascript", "-e", script],
            capture_output=True,
            text=True,
            timeout=10
        )
        if result.returncode == 0:
            # Parse output (simplified)
            pass
    except Exception:
        pass
    return windows


def _get_window_list_linux() -> list[dict]:
    """Get window list on Linux using xdotool."""
    windows = []
    try:
        result = subprocess.run(
            ["xdotool", "search", "--onlyvisible", "--name", "."],
            capture_output=True,
            text=True,
            timeout=10
        )
        if result.returncode == 0:
            for window_id in result.stdout.strip().split("\n"):
                if window_id:
                    name_result = subprocess.run(
                        ["xdotool", "getwindowname", window_id],
                        capture_output=True,
                        text=True,
                        timeout=3
                    )
                    if name_result.returncode == 0:
                        windows.append({
                            "id": window_id,
                            "title": name_result.stdout.strip(),
                        })
    except FileNotFoundError:
        pass
    return windows
