"""
Window control utilities for automation window management.

Provides window manipulation including positioning,
sizing, and state control.
"""

from __future__ import annotations

import subprocess
from typing import Optional, Tuple, List
from dataclasses import dataclass
from enum import Enum


class WindowAction(Enum):
    """Window control actions."""
    MINIMIZE = "minimize"
    MAXIMIZE = "maximize"
    RESTORE = "restore"
    CLOSE = "close"
    HIDE = "hide"
    CENTER = "center"
    MOVE = "move"
    RESIZE = "resize"


@dataclass
class WindowPosition:
    """Window position and size."""
    x: int
    y: int
    width: int
    height: int
    
    @property
    def center_x(self) -> int:
        return self.x + self.width // 2
    
    @property
    def center_y(self) -> int:
        return self.y + self.height // 2
    
    @property
    def tuple(self) -> Tuple[int, int, int, int]:
        return (self.x, self.y, self.width, self.height)


@dataclass
class WindowActionResult:
    """Result of window action."""
    success: bool
    action: WindowAction
    message: str
    new_position: Optional[WindowPosition] = None


def get_frontmost_window_info() -> Optional[dict]:
    """
    Get frontmost window information.
    
    Returns:
        Dict with window info or None.
    """
    try:
        script = '''
        tell application "System Events"
            set frontProc to first process whose frontmost is true
            set procName to name of frontProc
            set pid to processID of frontProc
            tell first window of frontProc
                set winTitle to title
                set winPos to position
                set winSize to size
            end tell
            return {procName, pid, winTitle, winPos, winSize}
        end tell
        '''
        result = subprocess.run(
            ["osascript", "-e", script],
            capture_output=True,
            text=True,
            timeout=5
        )
        
        if result.stdout.strip():
            return parse_window_output(result.stdout)
    except Exception:
        pass
    return None


def parse_window_output(output: str) -> dict:
    """Parse AppleScript window output."""
    parts = output.strip().split(',')
    if len(parts) >= 5:
        return {
            'name': parts[0].strip(),
            'pid': int(parts[1].strip()),
            'title': parts[2].strip(),
            'position': parse_position(parts[3]),
            'size': parse_position(parts[4]),
        }
    return {}


def parse_position(s: str) -> Tuple[int, int]:
    """Parse position/size string."""
    cleaned = s.strip().replace('{', '').replace('}', '')
    parts = cleaned.split(',')
    return (int(parts[0].strip()), int(parts[1].strip()))


class WindowController:
    """Controls window actions."""
    
    def __init__(self, app_name: Optional[str] = None):
        """
        Initialize window controller.
        
        Args:
            app_name: Optional app name to target.
        """
        self.app_name = app_name
    
    def minimize(self) -> WindowActionResult:
        """Minimize frontmost window."""
        try:
            script = '''
            tell application "System Events"
                tell (first process whose frontmost is true)
                    set miniaturized of first window to true
                end tell
            end tell
            '''
            subprocess.run(["osascript", "-e", script], capture_output=True, timeout=5)
            return WindowActionResult(
                success=True,
                action=WindowAction.MINIMIZE,
                message="Window minimized"
            )
        except Exception as e:
            return WindowActionResult(
                success=False,
                action=WindowAction.MINIMIZE,
                message=f"Failed: {e}"
            )
    
    def maximize(self) -> WindowActionResult:
        """Maximize frontmost window."""
        try:
            script = '''
            tell application "System Events"
                tell (first process whose frontmost is true)
                    set bounds of first window to {0, 23, 1920, 1080}
                end tell
            end tell
            '''
            subprocess.run(["osascript", "-e", script], capture_output=True, timeout=5)
            return WindowActionResult(
                success=True,
                action=WindowAction.MAXIMIZE,
                message="Window maximized",
                new_position=WindowPosition(0, 23, 1920, 1080-23)
            )
        except Exception as e:
            return WindowActionResult(
                success=False,
                action=WindowAction.MAXIMIZE,
                message=f"Failed: {e}"
            )
    
    def close(self) -> WindowActionResult:
        """Close frontmost window."""
        try:
            script = '''
            tell application "System Events"
                tell (first process whose frontmost is true)
                    close first window
                end tell
            end tell
            '''
            subprocess.run(["osascript", "-e", script], capture_output=True, timeout=5)
            return WindowActionResult(
                success=True,
                action=WindowAction.CLOSE,
                message="Window closed"
            )
        except Exception as e:
            return WindowActionResult(
                success=False,
                action=WindowAction.CLOSE,
                message=f"Failed: {e}"
            )
    
    def hide(self) -> WindowActionResult:
        """Hide frontmost app."""
        try:
            script = '''
            tell application "System Events"
                set frontmost of (first process whose frontmost is true) to false
            end tell
            '''
            subprocess.run(["osascript", "-e", script], capture_output=True, timeout=5)
            return WindowActionResult(
                success=True,
                action=WindowAction.HIDE,
                message="App hidden"
            )
        except Exception as e:
            return WindowActionResult(
                success=False,
                action=WindowAction.HIDE,
                message=f"Failed: {e}"
            )
    
    def move(self, x: int, y: int) -> WindowActionResult:
        """Move window to position."""
        try:
            script = f'''
            tell application "System Events"
                tell (first process whose frontmost is true)
                    set position of first window to {{{x}, {y}}}
                end tell
            end tell
            '''
            subprocess.run(["osascript", "-e", script], capture_output=True, timeout=5)
            return WindowActionResult(
                success=True,
                action=WindowAction.MOVE,
                message=f"Window moved to ({x}, {y})",
                new_position=WindowPosition(x, y, 0, 0)
            )
        except Exception as e:
            return WindowActionResult(
                success=False,
                action=WindowAction.MOVE,
                message=f"Failed: {e}"
            )
    
    def resize(self, width: int, height: int) -> WindowActionResult:
        """Resize window."""
        try:
            script = f'''
            tell application "System Events"
                tell (first process whose frontmost is true)
                    set size of first window to {{{width}, {height}}}
                end tell
            end tell
            '''
            subprocess.run(["osascript", "-e", script], capture_output=True, timeout=5)
            return WindowActionResult(
                success=True,
                action=WindowAction.RESIZE,
                message=f"Window resized to {width}x{height}",
                new_position=WindowPosition(0, 0, width, height)
            )
        except Exception as e:
            return WindowActionResult(
                success=False,
                action=WindowAction.RESIZE,
                message=f"Failed: {e}"
            )
    
    def set_bounds(self, x: int, y: int, width: int, height: int) -> WindowActionResult:
        """Set window bounds."""
        try:
            script = f'''
            tell application "System Events"
                tell (first process whose frontmost is true)
                    set bounds of first window to {{{x}, {y}, {width}, {height}}}
                end tell
            end tell
            '''
            subprocess.run(["osascript", "-e", script], capture_output=True, timeout=5)
            return WindowActionResult(
                success=True,
                action=WindowAction.RESIZE,
                message=f"Bounds set to ({x}, {y}, {width}, {height})",
                new_position=WindowPosition(x, y, width, height)
            )
        except Exception as e:
            return WindowActionResult(
                success=False,
                action=WindowAction.RESIZE,
                message=f"Failed: {e}"
            )
    
    def center(self) -> WindowActionResult:
        """Center window on screen."""
        try:
            script = '''
            tell application "System Events"
                tell (first process whose frontmost is true)
                    set winBounds to bounds of first window
                    set winWidth to item 3 of winBounds
                    set winHeight to item 4 of winBounds
                    set screenWidth to 1920
                    set screenHeight to 1080
                    set newX to (screenWidth - winWidth) / 2
                    set newY to (screenHeight - winHeight) / 2
                    set bounds of first window to {newX, newY, newX + winWidth, newY + winHeight}
                end tell
            end tell
            '''
            subprocess.run(["osascript", "-e", script], capture_output=True, timeout=5)
            return WindowActionResult(
                success=True,
                action=WindowAction.CENTER,
                message="Window centered"
            )
        except Exception as e:
            return WindowActionResult(
                success=False,
                action=WindowAction.CENTER,
                message=f"Failed: {e}"
            )
