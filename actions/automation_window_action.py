"""
Window management automation module.

Provides window detection, positioning, sizing, and state management
for automated window manipulation workflows.

Author: Aito Auto Agent
"""

from __future__ import annotations

import subprocess
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Optional, Callable


class WindowState(Enum):
    """Window state enumeration."""
    NORMAL = auto()
    MINIMIZED = auto()
    MAXIMIZED = auto()
    FULLSCREEN = auto()
    HIDDEN = auto()
    UNKNOWN = auto()


class WindowAttribute(Enum):
    """Window attribute types."""
    TITLE = auto()
    PROCESS_ID = auto()
    POSITION = auto()
    SIZE = auto()
    STATE = auto()
    FRONT = auto()
    FOCUSED = auto()


@dataclass
class WindowPosition:
    """Window position (x, y coordinates)."""
    x: int
    y: int

    def to_tuple(self) -> tuple[int, int]:
        return (self.x, self.y)


@dataclass
class WindowSize:
    """Window dimensions."""
    width: int
    height: int

    def to_tuple(self) -> tuple[int, int]:
        return (self.width, self.height)


@dataclass
class WindowBounds:
    """Complete window bounds (position and size)."""
    x: int
    y: int
    width: int
    height: int

    @property
    def position(self) -> WindowPosition:
        return WindowPosition(self.x, self.y)

    @property
    def size(self) -> WindowSize:
        return WindowSize(self.width, self.height)

    def to_tuple(self) -> tuple[int, int, int, int]:
        return (self.x, self.y, self.width, self.height)

    @property
    def center(self) -> WindowPosition:
        """Get the center point of the window."""
        return WindowPosition(
            self.x + self.width // 2,
            self.y + self.height // 2
        )


@dataclass
class WindowInfo:
    """Complete window information."""
    window_id: str
    title: str
    app_name: str
    process_id: int
    bounds: WindowBounds
    state: WindowState = WindowState.NORMAL
    is_focused: bool = False
    is_front: bool = False


class WindowManager:
    """
    Window management automation.

    Provides operations for listing, positioning, sizing, and
    controlling window states across the desktop environment.

    Example:
        manager = WindowManager()
        windows = manager.list_windows()
        manager.move_window(windows[0].window_id, WindowPosition(100, 100))
        manager.resize_window(windows[0].window_id, WindowSize(800, 600))
    """

    def __init__(self, platform: str = "macos"):
        self._platform = platform
        self._window_cache: dict[str, WindowInfo] = {}

    def list_windows(self, app_name: Optional[str] = None) -> list[WindowInfo]:
        """
        List all windows, optionally filtered by application.

        Args:
            app_name: Optional application name filter

        Returns:
            List of WindowInfo objects
        """
        if self._platform == "macos":
            return self._list_windows_macos(app_name)
        elif self._platform == "windows":
            return self._list_windows_windows(app_name)
        else:
            return self._list_windows_x11(app_name)

    def _list_windows_macos(self, app_name: Optional[str] = None) -> list[WindowInfo]:
        """List windows on macOS using osascript."""
        windows = []

        try:
            script = '''
            tell application "System Events"
                set windowList to {}
                set appList to application processes
                repeat with theApp in appList
                    set appName to name of theApp
                    set winList to windows of theApp
                    repeat with theWin in winList
                        set winTitle to name of theWin
                        set winPos to position of theWin
                        set winSize to size of theWin
                        set winInfo to {appName, winTitle, item 1 of winPos, item 2 of winPos, item 1 of winSize, item 2 of winSize}
                        copy winInfo to end of windowList
                    end repeat
                end repeat
                return windowList
            end tell
            '''

            result = subprocess.run(
                ["osascript", "-e", script],
                capture_output=True,
                text=True,
                timeout=10
            )

            if result.returncode == 0 and result.stdout.strip():
                for line in result.stdout.strip().split("\n"):
                    if not line.strip():
                        continue
                    parts = [p.strip() for p in line.split(",")]
                    if len(parts) >= 6:
                        windows.append(WindowInfo(
                            window_id=f"macos_{parts[0]}_{parts[1]}",
                            title=parts[1],
                            app_name=parts[0],
                            process_id=0,
                            bounds=WindowBounds(
                                x=int(parts[2]),
                                y=int(parts[3]),
                                width=int(parts[4]),
                                height=int(parts[5])
                            )
                        ))

        except Exception as e:
            print(f"Error listing macOS windows: {e}")

        if app_name:
            windows = [w for w in windows if app_name.lower() in w.app_name.lower()]

        return windows

    def _list_windows_windows(self, app_name: Optional[str] = None) -> list[WindowInfo]:
        """List windows on Windows using PowerShell."""
        windows = []

        try:
            script = '''
            Add-Type @"
            using System;
            using System.Runtime.InteropServices;
            using System.Text;
            public class WindowHelper {
                [DllImport("user32.dll")]
                public static extern bool EnumWindows(EnumWindowsProc enumProc, IntPtr lParam);
                public delegate bool EnumWindowsProc(IntPtr hWnd, IntPtr lParam);
                [DllImport("user32.dll")]
                public static extern int GetWindowText(IntPtr hWnd, StringBuilder text, int count);
                [DllImport("user32.dll")]
                public static extern bool GetWindowRect(IntPtr hWnd, out RECT lpRect);
                [StructLayout(LayoutKind.Sequential)]
                public struct RECT { public int Left, Top, Right, Bottom; }
            }
"@
            $windows = @()
            $callback = [WindowHelper+EnumWindowsProc]{
                param($hwnd, $param)
                $len = [WindowHelper]::GetWindowText($hwnd, (New-Object Text.StringBuilder 256), 256)
                if ($len -gt 0) {
                    $title = (New-Object Text.StringBuilder 256) | ForEach-Object {
                        [void][WindowHelper]::GetWindowText($hwnd, $_, 256)
                        $_.ToString()
                    }
                    $rect = New-Object WindowHelper+RECT
                    [void][WindowHelper]::GetWindowRect($hwnd, [ref]$rect)
                    $script:windows += [PSCustomObject]@{
                        Id = $hwnd.ToString()
                        Title = $title
                        X = $rect.Left
                        Y = $rect.Top
                        Width = $rect.Right - $rect.Left
                        Height = $rect.Bottom - $rect.Top
                    }
                }
                return $true
            }
            [void][WindowHelper]::EnumWindows($callback, [IntPtr]::Zero)
            $windows | ConvertTo-Json -Compress
            '''

            result = subprocess.run(
                ["powershell", "-Command", script],
                capture_output=True,
                text=True,
                timeout=15
            )

        except Exception as e:
            print(f"Error listing Windows windows: {e}")

        return windows

    def _list_windows_x11(self, app_name: Optional[str] = None) -> list[WindowInfo]:
        """List windows on X11/Linux using xdotool."""
        windows = []

        try:
            result = subprocess.run(
                ["xdotool", "search", "--name", "."],
                capture_output=True,
                text=True,
                timeout=10
            )

            if result.returncode == 0:
                for window_id in result.stdout.strip().split("\n"):
                    if not window_id.strip():
                        continue

                    try:
                        name_result = subprocess.run(
                            ["xdotool", "getwindowname", window_id],
                            capture_output=True,
                            text=True,
                            timeout=5
                        )

                        geom_result = subprocess.run(
                            ["xdotool", "getwindowgeometry", "--shell", window_id],
                            capture_output=True,
                            text=True,
                            timeout=5
                        )

                        if name_result.returncode == 0:
                            title = name_result.stdout.strip()

                            bounds = WindowBounds(0, 0, 800, 600)
                            if geom_result.returncode == 0:
                                for line in geom_result.stdout.split("\n"):
                                    if line.startswith("Position="):
                                        parts = line.split("=")[1].split(",")
                                        bounds.x = int(parts[0])
                                        bounds.y = int(parts[1])
                                    elif line.startswith("Width="):
                                        bounds.width = int(line.split("=")[1])
                                    elif line.startswith("Height="):
                                        bounds.height = int(line.split("=")[1])

                            windows.append(WindowInfo(
                                window_id=f"x11_{window_id}",
                                title=title,
                                app_name=title.split(" - ")[-1] if " - " in title else title,
                                process_id=0,
                                bounds=bounds
                            ))

                    except Exception:
                        continue

        except Exception as e:
            print(f"Error listing X11 windows: {e}")

        return windows

    def move_window(
        self,
        window_id: str,
        position: WindowPosition,
        animate: bool = False
    ) -> bool:
        """
        Move window to specified position.

        Args:
            window_id: Window identifier
            position: Target position
            animate: Whether to animate the movement

        Returns:
            True if successful
        """
        if self._platform == "macos":
            return self._move_window_macos(window_id, position)
        elif self._platform == "windows":
            return self._move_window_windows(window_id, position)
        else:
            return self._move_window_x11(window_id, position)

    def _move_window_macos(self, window_id: str, position: WindowPosition) -> bool:
        """Move window on macOS."""
        try:
            script = f'''
            tell application "System Events"
                set position of window 1 of (first application process whose windows contains window "{window_id}") to {{{position.x}, {position.y}}}
            end tell
            '''
            subprocess.run(["osascript", "-e", script], capture_output=True, timeout=5)
            return True
        except Exception:
            return False

    def _move_window_windows(self, window_id: str, position: WindowPosition) -> bool:
        """Move window on Windows."""
        try:
            script = f'''
            Add-Type @"
            using System;
            using System.Runtime.InteropServices;
            public class WinMover {
                [DllImport("user32.dll")] public static extern bool MoveWindow(IntPtr hWnd, int X, int Y, int nWidth, int nHeight, bool bRepaint);
            }
"@
            $hwnd = [IntPtr]::new({window_id.replace("windows_", "0x")})
            [void][WinMover]::MoveWindow($hwnd, {position.x}, {position.y}, 800, 600, $true)
            '''
            subprocess.run(["powershell", "-Command", script], capture_output=True, timeout=5)
            return True
        except Exception:
            return False

    def _move_window_x11(self, window_id: str, position: WindowPosition) -> bool:
        """Move window on X11."""
        try:
            subprocess.run(
                ["xdotool", "windowmove", window_id.replace("x11_", ""),
                 str(position.x), str(position.y)],
                capture_output=True,
                timeout=5
            )
            return True
        except Exception:
            return False

    def resize_window(
        self,
        window_id: str,
        size: WindowSize,
        anchor: Optional[WindowPosition] = None
    ) -> bool:
        """
        Resize window to specified dimensions.

        Args:
            window_id: Window identifier
            size: Target size
            anchor: Optional anchor point for resize

        Returns:
            True if successful
        """
        if self._platform == "macos":
            return self._resize_window_macos(window_id, size)
        elif self._platform == "windows":
            return self._resize_window_windows(window_id, size)
        else:
            return self._resize_window_x11(window_id, size)

    def _resize_window_macos(self, window_id: str, size: WindowSize) -> bool:
        """Resize window on macOS."""
        try:
            script = f'''
            tell application "System Events"
                set size of window 1 of (first application process whose windows contains window "{window_id}") to {{{size.width}, {size.height}}}
            end tell
            '''
            subprocess.run(["osascript", "-e", script], capture_output=True, timeout=5)
            return True
        except Exception:
            return False

    def _resize_window_windows(self, window_id: str, size: WindowSize) -> bool:
        """Resize window on Windows."""
        try:
            script = f'''
            Add-Type @"
            using System;
            using System.Runtime.InteropServices;
            public class WinMover {{
                [DllImport("user32.dll")] public static extern bool MoveWindow(IntPtr hWnd, int X, int Y, int nWidth, int nHeight, bool bRepaint);
            }}
"@
            $hwnd = [IntPtr]::new({window_id.replace("windows_", "0x")})
            [void][WinMover]::MoveWindow($hwnd, 0, 0, {size.width}, {size.height}, $true)
            '''
            subprocess.run(["powershell", "-Command", script], capture_output=True, timeout=5)
            return True
        except Exception:
            return False

    def _resize_window_x11(self, window_id: str, size: WindowSize) -> bool:
        """Resize window on X11."""
        try:
            subprocess.run(
                ["xdotool", "windowsize", window_id.replace("x11_", ""),
                 str(size.width), str(size.height)],
                capture_output=True,
                timeout=5
            )
            return True
        except Exception:
            return False

    def set_window_state(self, window_id: str, state: WindowState) -> bool:
        """
        Set window state (minimize, maximize, restore).

        Args:
            window_id: Window identifier
            state: Target state

        Returns:
            True if successful
        """
        if self._platform == "macos":
            return self._set_state_macos(window_id, state)
        elif self._platform == "windows":
            return self._set_state_windows(window_id, state)
        else:
            return self._set_state_x11(window_id, state)

    def _set_state_macos(self, window_id: str, state: WindowState) -> bool:
        """Set window state on macOS."""
        try:
            if state == WindowState.MINIMIZED:
                script = f'''tell application "System Events" to set miniaturized of (first window of (first application process whose windows contains window "{window_id}")) to true'''
            elif state == WindowState.MAXIMIZED:
                script = f'''tell application "System Events" to setzoomed of (first window of (first application process whose windows contains window "{window_id}")) to true'''
            else:
                return False

            subprocess.run(["osascript", "-e", script], capture_output=True, timeout=5)
            return True
        except Exception:
            return False

    def _set_state_windows(self, window_id: str, state: WindowState) -> bool:
        """Set window state on Windows."""
        try:
            cmd_map = {
                WindowState.MINIMIZED: "minimize",
                WindowState.MAXIMIZED: "maximize",
                WindowState.NORMAL: "restore"
            }
            cmd = cmd_map.get(state)
            if not cmd:
                return False

            subprocess.run(
                ["powershell", "-Command",
                 f'Start-Process -FilePath "powershell" -ArgumentList "-Command Show-NativeWindow -Action {cmd} -WindowHandle {window_id.replace("windows_", "0x")}" -WindowStyle Hidden'],
                capture_output=True,
                timeout=5
            )
            return True
        except Exception:
            return False

    def _set_state_x11(self, window_id: str, state: WindowState) -> bool:
        """Set window state on X11."""
        try:
            action_map = {
                WindowState.MINIMIZED: "minimize",
                WindowState.MAXIMIZED: "maximize",
                WindowState.NORMAL: "restore"
            }
            action = action_map.get(state)
            if not action:
                return False

            subprocess.run(
                ["xdotool", "windowstate", action, window_id.replace("x11_", "")],
                capture_output=True,
                timeout=5
            )
            return True
        except Exception:
            return False

    def bring_to_front(self, window_id: str) -> bool:
        """Bring window to front."""
        try:
            if self._platform == "macos":
                subprocess.run(
                    ["osascript", "-e",
                     f'tell application "System Events" to perform action "AXRaise" of (first window of (first application process whose windows contains window "{window_id}"))'],
                    capture_output=True,
                    timeout=5
                )
            elif self._platform == "windows":
                subprocess.run(
                    ["powershell", "-Command",
                     f'(Add-Type -MemberDefinition "[DllImport(\\"user32.dll\\")] public static extern bool SetForegroundWindow(IntPtr hWnd);" -Name Win32 -Namespace W -PassThru)::SetForegroundWindow([IntPtr]::new({window_id.replace("windows_", "0x")}))'],
                    capture_output=True,
                    timeout=5
                )
            else:
                subprocess.run(
                    ["xdotool", "windowraise", window_id.replace("x11_", "")],
                    capture_output=True,
                    timeout=5
                )
            return True
        except Exception:
            return False

    def close_window(self, window_id: str) -> bool:
        """Close a window."""
        try:
            if self._platform == "macos":
                subprocess.run(
                    ["osascript", "-e",
                     f'tell application "System Events" to click button 1 of (first window of (first application process whose windows contains window "{window_id}"))'],
                    capture_output=True,
                    timeout=5
                )
            elif self._platform == "windows":
                subprocess.run(
                    ["powershell", "-Command",
                     f'Stop-Process -Id {window_id.replace("windows_", "")} -Force'],
                    capture_output=True,
                    timeout=5
                )
            else:
                subprocess.run(
                    ["xdotool", "windowclose", window_id.replace("x11_", "")],
                    capture_output=True,
                    timeout=5
                )
            return True
        except Exception:
            return False


def create_window_manager(platform: str = "macos") -> WindowManager:
    """Factory function to create a WindowManager."""
    return WindowManager(platform=platform)
