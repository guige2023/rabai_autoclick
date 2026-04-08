"""Multi-monitor utilities for handling multiple displays.

This module provides utilities for detecting and working with
multiple monitors in a multi-display setup.
"""

from __future__ import annotations

import platform
import subprocess
from dataclasses import dataclass
from typing import Optional


IS_MACOS = platform.system() == "Darwin"
IS_LINUX = platform.system() == "Linux"
IS_WINDOWS = platform.system() == "Windows"


@dataclass
class MonitorInfo:
    """Information about a connected monitor/display."""
    id: str
    name: str
    x: int
    y: int
    width: int
    height: int
    is_primary: bool = False
    
    @property
    def bounds(self) -> tuple[int, int, int, int]:
        """Get bounds as (x, y, width, height)."""
        return (self.x, self.y, self.width, self.height)
    
    @property
    def center(self) -> tuple[int, int]:
        """Get the center point of the monitor."""
        return (self.x + self.width // 2, self.y + self.height // 2)
    
    def contains_point(self, x: int, y: int) -> bool:
        """Check if a point is within this monitor's bounds."""
        return (self.x <= x < self.x + self.width and
                self.y <= y < self.y + self.height)


def get_all_monitors() -> list[MonitorInfo]:
    """Get information about all connected monitors.
    
    Returns:
        List of MonitorInfo for each connected display.
    """
    if IS_MACOS:
        return _get_monitors_macos()
    elif IS_LINUX:
        return _get_monitors_linux()
    elif IS_WINDOWS:
        return _get_monitors_windows()
    return [MonitorInfo(id="default", name="Primary", x=0, y=0, width=1920, height=1080, is_primary=True)]


def _get_monitors_macos() -> list[MonitorInfo]:
    """Get monitor information on macOS."""
    monitors = []
    try:
        result = subprocess.run(
            ["system_profiler", "SPDisplaysDataType", "-json"],
            capture_output=True,
            text=True,
            timeout=10
        )
        import json
        data = json.loads(result.stdout)
        displays = data.get("SPDisplaysDataType", [])
        
        if isinstance(displays, list):
            for i, display in enumerate(displays):
                if isinstance(display, dict):
                    # Extract resolution info
                    resolution = display.get("Resolution", "1920x1080")
                    if "x" in resolution:
                        parts = resolution.split("x")
                        if len(parts) == 2:
                            width = int(parts[0])
                            height = int(parts[1])
                        else:
                            width, height = 1920, 1080
                    else:
                        width, height = 1920, 1080
                    
                    monitors.append(MonitorInfo(
                        id=str(i),
                        name=display.get("DisplayType", f"Display {i+1}"),
                        x=0,  # Would need CGGetDisplays to get position
                        y=0,
                        width=width,
                        height=height,
                        is_primary=(i == 0),
                    ))
        elif isinstance(displays, dict):
            for i, (key, display) in enumerate(displays.items()):
                monitors.append(MonitorInfo(
                    id=key,
                    name=display.get("DisplayType", f"Display {i+1}"),
                    x=0, y=0,
                    width=1920, height=1080,
                    is_primary=(i == 0),
                ))
    except Exception:
        pass
    
    # Fallback
    if not monitors:
        monitors.append(MonitorInfo(
            id="main", name="Main Display", x=0, y=0, width=1920, height=1080, is_primary=True
        ))
    return monitors


def _get_monitors_linux() -> list[MonitorInfo]:
    """Get monitor information on Linux using xrandr."""
    monitors = []
    try:
        result = subprocess.run(
            ["xrandr"],
            capture_output=True,
            text=True,
            timeout=5
        )
        
        lines = result.stdout.split("\n")
        current_x = 0
        
        for line in lines:
            line = line.strip()
            if " connected " in line:
                parts = line.split()
                name = parts[0]
                # Parse resolution and position
                res_str = ""
                pos_x = 0
                pos_y = 0
                
                for part in parts[1:]:
                    if "x" in part and "+" in part:
                        res_str, pos_str = part.split("+")
                        pos_parts = pos_str.split("x")
                        if len(pos_parts) == 2:
                            pos_x = int(pos_parts[0])
                            pos_y = int(pos_parts[1])
                        dims = res_str.split("x")
                        if len(dims) == 2:
                            width, height = int(dims[0]), int(dims[1])
                            monitors.append(MonitorInfo(
                                id=name,
                                name=name,
                                x=pos_x,
                                y=pos_y,
                                width=width,
                                height=height,
                                is_primary=("primary" in line),
                            ))
                        break
    except Exception:
        pass
    
    if not monitors:
        monitors.append(MonitorInfo(
            id="default", name="Display", x=0, y=0, width=1920, height=1080, is_primary=True
        ))
    return monitors


def _get_monitors_windows() -> list[MonitorInfo]:
    """Get monitor information on Windows."""
    monitors = []
    try:
        import ctypes
        from ctypes import wintypes
        
        user32 = ctypes.windll.user32
        
        # EnumDisplayMonitors
        monitors_enum = []
        
        def callback(hMonitor, hdcMonitor, lprcMonitor, dwData):
            class RECT(ctypes.Structure):
                _fields_ = [("left", wintypes.LONG),
                            ("top", wintypes.LONG),
                            ("right", wintypes.LONG),
                            ("bottom", wintypes.LONG)]
            
            r = ctypes.cast(lprcMonitor, ctypes.POINTER(RECT)).contents
            monitors_enum.append({
                "left": r.left,
                "top": r.top,
                "right": r.right,
                "bottom": r.bottom,
            })
            return 1
        
        MONITORENUMPROC = ctypes.WINFUNCTYPE(
            wintypes.BOOL,
            wintypes.HMONITOR,
            wintypes.HDC,
            ctypes.POINTER(wintypes.RECT),
            wintypes.LPARAM
        )
        
        user32.EnumDisplayMonitors(None, None, MONITORENUMPROC(callback), 0)
        
        for i, m in enumerate(monitors_enum):
            monitors.append(MonitorInfo(
                id=str(i),
                name=f"Display {i+1}",
                x=m["left"],
                y=m["top"],
                width=m["right"] - m["left"],
                height=m["bottom"] - m["top"],
                is_primary=(i == 0),
            ))
    except Exception:
        pass
    
    if not monitors:
        monitors.append(MonitorInfo(
            id="default", name="Display", x=0, y=0, width=1920, height=1080, is_primary=True
        ))
    return monitors


def get_primary_monitor() -> MonitorInfo:
    """Get the primary monitor information."""
    monitors = get_all_monitors()
    for m in monitors:
        if m.is_primary:
            return m
    return monitors[0] if monitors else MonitorInfo(
        id="default", name="Default", x=0, y=0, width=1920, height=1080, is_primary=True
    )


def get_monitor_at(x: int, y: int) -> Optional[MonitorInfo]:
    """Get the monitor containing the given point.
    
    Args:
        x: X coordinate.
        y: Y coordinate.
    
    Returns:
        MonitorInfo for the monitor at the point, or None.
    """
    for monitor in get_all_monitors():
        if monitor.contains_point(x, y):
            return monitor
    return None
