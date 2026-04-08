"""Display information utilities for querying display properties.

This module provides utilities for querying detailed display/monitor
properties like resolution, scale factor, and color depth.
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
class DisplayMode:
    """Represents a display mode configuration."""
    width: int
    height: int
    refresh_rate: float
    scale_factor: float = 1.0
    bit_depth: int = 32


@dataclass
class DisplayInfo:
    """Comprehensive display information."""
    display_id: str
    name: str
    is_main: bool
    is_built_in: bool
    current_mode: DisplayMode
    available_modes: list[DisplayMode]
    
    @property
    def resolution(self) -> tuple[int, int]:
        """Get current resolution as (width, height)."""
        return (self.current_mode.width, self.current_mode.height)
    
    @property
    def dpi(self) -> int:
        """Estimate DPI based on scale factor (assumes 96 base DPI)."""
        return int(96 * self.current_mode.scale_factor)


def get_display_info(display_index: int = 0) -> Optional[DisplayInfo]:
    """Get information about a specific display.
    
    Args:
        display_index: Index of the display (0 = primary).
    
    Returns:
        DisplayInfo for the display, or None if not found.
    """
    if IS_MACOS:
        return _get_display_info_macos(display_index)
    elif IS_LINUX:
        return _get_display_info_linux(display_index)
    elif IS_WINDOWS:
        return _get_display_info_windows(display_index)
    return None


def _get_display_info_macos(display_index: int) -> Optional[DisplayInfo]:
    """Get display info on macOS."""
    try:
        import subprocess
        result = subprocess.run(
            ["system_profiler", "SPDisplaysDataType", "-json"],
            capture_output=True,
            text=True,
            timeout=10
        )
        import json
        data = json.loads(result.stdout)
        displays = data.get("SPDisplaysDataType", [])
        
        if isinstance(displays, list) and display_index < len(displays):
            display = displays[display_index]
            current = display.get("CurrentConfiguration", {})
            mode = DisplayMode(
                width=current.get("ResolutionWidth", 1920),
                height=current.get("ResolutionHeight", 1080),
                refresh_rate=60.0,
                scale_factor=current.get("ScaleFactor", 1.0),
            )
            return DisplayInfo(
                display_id=str(display_index),
                name=display.get("DisplayType", f"Display {display_index+1}"),
                is_main=(display_index == 0),
                is_built_in="Built-in" in str(display),
                current_mode=mode,
                available_modes=[mode],
            )
    except Exception:
        pass
    return None


def _get_display_info_linux(display_index: int) -> Optional[DisplayInfo]:
    """Get display info on Linux using xrandr."""
    try:
        result = subprocess.run(
            ["xrandr"],
            capture_output=True,
            text=True,
            timeout=5
        )
        
        displays_found = []
        for line in result.stdout.split("\n"):
            if "connected" in line:
                parts = line.split()
                name = parts[0]
                # Parse current mode
                mode_str = ""
                scale = 1.0
                for part in parts:
                    if "x" in part and "+" in part:
                        mode_str = part.split("+")[0]
                        break
                
                if mode_str:
                    dims = mode_str.split("x")
                    if len(dims) == 2:
                        width, height = int(dims[0]), int(dims[1])
                        mode = DisplayMode(width=width, height=height, refresh_rate=60.0, scale_factor=scale)
                        displays_found.append(DisplayInfo(
                            display_id=name,
                            name=name,
                            is_main=(display_index == len(displays_found)),
                            is_built_in=False,
                            current_mode=mode,
                            available_modes=[mode],
                        ))
        
        if display_index < len(displays_found):
            return displays_found[display_index]
    except Exception:
        pass
    return None


def _get_display_info_windows(display_index: int) -> Optional[DisplayInfo]:
    """Get display info on Windows."""
    try:
        import ctypes
        from ctypes import wintypes
        
        user32 = ctypes.windll.user32
        
        # Get device context
        hdc = user32.GetDC(0)
        width = user32.GetDeviceCaps(hdc, 8)  # HORZRES
        height = user32.GetDeviceCaps(hdc, 10)  # VERTRES
        refresh = user32.GetDeviceCaps(hdc, 116)  # VREFRESH
        user32.ReleaseDC(0, hdc)
        
        mode = DisplayMode(width=width, height=height, refresh_rate=float(refresh))
        return DisplayInfo(
            display_id=str(display_index),
            name=f"Display {display_index+1}",
            is_main=(display_index == 0),
            is_built_in=False,
            current_mode=mode,
            available_modes=[mode],
        )
    except Exception:
        pass
    return None


def get_all_displays() -> list[DisplayInfo]:
    """Get information about all connected displays.
    
    Returns:
        List of DisplayInfo for all displays.
    """
    displays = []
    for i in range(10):  # Check up to 10 displays
        info = get_display_info(i)
        if info is None:
            break
        displays.append(info)
    
    if not displays:
        # Return a default display
        displays.append(DisplayInfo(
            display_id="default",
            name="Default Display",
            is_main=True,
            is_built_in=False,
            current_mode=DisplayMode(width=1920, height=1080, refresh_rate=60.0),
            available_modes=[],
        ))
    
    return displays
