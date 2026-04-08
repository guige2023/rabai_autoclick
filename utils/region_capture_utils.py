"""Region capture utilities for screen region screenshots.

This module provides utilities for capturing specific regions of the
screen, which is useful for targeted image detection and processing.
"""

from __future__ import annotations

import platform
import subprocess
from typing import Optional
from dataclasses import dataclass


IS_MACOS = platform.system() == "Darwin"
IS_LINUX = platform.system() == "Linux"
IS_WINDOWS = platform.system() == "Windows"


@dataclass
class ScreenRegion:
    """Represents a rectangular screen region."""
    x: int
    y: int
    width: int
    height: int
    
    @property
    def tuple(self) -> tuple[int, int, int, int]:
        """Return region as (x, y, width, height) tuple."""
        return (self.x, self.y, self.width, self.height)
    
    @property
    def rect(self) -> tuple[int, int, int, int]:
        """Return region as (x1, y1, x2, y2) tuple."""
        return (self.x, self.y, self.x + self.width, self.y + self.height)
    
    def contains_point(self, x: int, y: int) -> bool:
        """Check if a point is within this region."""
        return (self.x <= x < self.x + self.width and
                self.y <= y < self.y + self.height)
    
    def intersect(self, other: "ScreenRegion") -> Optional["ScreenRegion"]:
        """Get the intersection of two regions."""
        x1 = max(self.x, other.x)
        y1 = max(self.y, other.y)
        x2 = min(self.x + self.width, other.x + other.width)
        y2 = min(self.y + self.height, other.y + other.height)
        
        if x1 < x2 and y1 < y2:
            return ScreenRegion(x1, y1, x2 - x1, y2 - y1)
        return None


def capture_region(
    region: ScreenRegion,
    output_path: Optional[str] = None,
) -> Optional[bytes]:
    """Capture a screen region and save to a file.
    
    Args:
        region: ScreenRegion to capture.
        output_path: Optional path to save the image. If None, returns PNG bytes.
    
    Returns:
        PNG image bytes if output_path is None, otherwise None on success.
    """
    x, y, w, h = region.tuple
    
    if IS_MACOS:
        return _capture_region_macos(x, y, w, h, output_path)
    elif IS_LINUX:
        return _capture_region_linux(x, y, w, h, output_path)
    elif IS_WINDOWS:
        return _capture_region_windows(x, y, w, h, output_path)
    return None


def _capture_region_macos(
    x: int,
    y: int,
    w: int,
    h: int,
    output_path: Optional[str],
) -> Optional[bytes]:
    """Capture region on macOS using screencapture."""
    try:
        path = output_path or "/tmp/_region_capture.png"
        cmd = [
            "screencapture",
            "-x",
            "-m",
            "-R", f"{x},{y},{w},{h}",
            path,
        ]
        result = subprocess.run(cmd, capture_output=True, timeout=10)
        if result.returncode != 0:
            return None
        
        if output_path:
            return None
        
        with open(path, "rb") as f:
            return f.read()
    except Exception:
        return None


def _capture_region_linux(
    x: int,
    y: int,
    w: int,
    h: int,
    output_path: Optional[str],
) -> Optional[bytes]:
    """Capture region on Linux using scrot or gnome-screenshot."""
    try:
        path = output_path or "/tmp/_region_capture.png"
        # Try scrot first
        cmd = ["scrot", "-a", f"{x},{y},{w},{h}", path]
        result = subprocess.run(cmd, capture_output=True, timeout=10)
        if result.returncode != 0:
            # Fallback to gnome-screenshot
            cmd = ["gnome-screenshot", "-a", "-f", path]
            result = subprocess.run(cmd, capture_output=True, timeout=10)
        
        if result.returncode != 0:
            return None
        
        if output_path:
            return None
        
        with open(path, "rb") as f:
            return f.read()
    except Exception:
        return None


def _capture_region_windows(
    x: int,
    y: int,
    w: int,
    h: int,
    output_path: Optional[str],
) -> Optional[bytes]:
    """Capture region on Windows using PIL/numpy."""
    try:
        from PIL import Image
        import numpy as np
        import ctypes
        
        # Get screen DC
        user32 = ctypes.windll.user32
        gdi32 = ctypes.windll.gdi32
        
        width = user32.GetSystemMetrics(0)
        height = user32.GetSystemMetrics(1)
        
        # Create compatible DC
        mem_dc = gdi32.CreateCompatibleDC(0)
        hwnd = user32.GetDesktopWindow()
        hwnd_dc = user32.GetDC(hwnd)
        comp_dc = gdi32.CreateCompatibleDC(hwnd_dc)
        
        # Create bitmap
        hbitmap = gdi32.CreateCompatibleBitmap(hwnd_dc, w, h)
        gdi32.SelectObject(comp_dc, hbitmap)
        
        # Copy region
        gdi32.BitBlt(comp_dc, 0, 0, w, h, hwnd_dc, x, y, 0x00CC0020)
        
        # Convert to PIL Image
        bmpinfo = ctypes.create_string_buffer(40)
        gdi32.GetObjectA(hbitmap, 40, bmpinfo)
        
        img = Image.frombuffer(
            'RGB',
            (w, h),
            ctypes.string_at(hbitmap, w * h * 3),
            'raw',
            'BGRX',
            0,
            1
        )
        
        path = output_path or "/tmp/_region_capture.png"
        img.save(path)
        
        # Cleanup
        gdi32.DeleteObject(hbitmap)
        gdi32.DeleteDC(comp_dc)
        user32.ReleaseDC(hwnd, hwnd_dc)
        
        if output_path:
            return None
        
        with open(path, "rb") as f:
            return f.read()
    except Exception:
        return None


def capture_all_monitors() -> list[ScreenRegion]:
    """Get screen regions for all connected monitors.
    
    Returns:
        List of ScreenRegion for each monitor.
    """
    regions = []
    
    if IS_MACOS:
        try:
            import subprocess
            script = '''
            tell application "Finder"
                get bounds of every window
            end tell
            '''
            # Use system_profiler for display info
            result = subprocess.run(
                ["system_profiler", "SPDisplaysDataType", "-json"],
                capture_output=True,
                timeout=10
            )
            import json
            data = json.loads(result.stdout)
            displays = data.get("SPDisplaysDataType", [])
            for display in displays:
                if isinstance(display, dict):
                    # Add logic to detect multiple displays
                    pass
        except Exception:
            pass
        
        # Default to main screen
        regions.append(ScreenRegion(0, 0, 1920, 1080))
    
    elif IS_LINUX:
        try:
            result = subprocess.run(
                ["xrandr"],
                capture_output=True,
                text=True,
                timeout=5
            )
            # Parse xrandr output for display info
            lines = result.stdout.split("\n")
            for line in lines:
                if " connected " in line:
                    parts = line.split()
                    if len(parts) >= 4:
                        # Parse resolution
                        res = parts[2].split("x")
                        if len(res) == 2:
                            w, h = int(res[0]), int(res[1])
                            regions.append(ScreenRegion(0, 0, w, h))
        except Exception:
            regions.append(ScreenRegion(0, 0, 1920, 1080))
    
    elif IS_WINDOWS:
        try:
            import ctypes
            user32 = ctypes.windll.user32
            width = user32.GetSystemMetrics(0)
            height = user32.GetSystemMetrics(1)
            regions.append(ScreenRegion(0, 0, width, height))
        except Exception:
            regions.append(ScreenRegion(0, 0, 1920, 1080))
    
    return regions
