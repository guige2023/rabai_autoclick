"""
Display and screen information utilities for UI automation.

Provides functions for querying display configuration,
screen metrics, and multi-monitor support.
"""

from __future__ import annotations

import sys
from typing import List, Optional, Tuple, NamedTuple
from dataclasses import dataclass


class DisplayInfo(NamedTuple):
    """Display configuration information."""
    id: int
    name: str
    x: int
    y: int
    width: int
    height: int
    is_primary: bool
    scale_factor: float
    orientation: str  # 'landscape', 'portrait', 'rotated'
    work_x: int
    work_y: int
    work_width: int
    work_height: int


@dataclass
class ScreenMetrics:
    """Comprehensive screen metrics."""
    width: int
    height: int
    diagonal: float  # inches
    dpi: int
    density: float
    ppi: float
    aspect_ratio: float
    
    @property
    def resolution(self) -> Tuple[int, int]:
        return (self.width, self.height)
    
    @property
    def is_4k(self) -> bool:
        return self.width >= 3840 or self.height >= 3840
    
    @property
    def is_retina(self) -> bool:
        return self.scale_factor > 1.0
    
    @property
    def scale_factor(self) -> float:
        return self.dpi / 96.0


def get_displays() -> List[DisplayInfo]:
    """Get information about all connected displays.
    
    Returns:
        List of DisplayInfo for each connected display
    """
    displays: List[DisplayInfo] = []
    
    if sys.platform == 'darwin':
        displays = _get_displays_macos()
    elif sys.platform == 'win32':
        displays = _get_displays_windows()
    else:
        displays = _get_displays_linux()
    
    return displays


def _get_displays_macos() -> List[DisplayInfo]:
    """Get display info using macOS APIs."""
    displays: List[DisplayInfo] = []
    
    try:
        import subprocess
        result = subprocess.run(
            ['system_profiler', 'SPDisplaysDataType', '-json'],
            capture_output=True,
            text=True,
            timeout=10,
        )
        
        if result.returncode == 0:
            import json
            data = json.loads(result.stdout)
            
            for idx, display in enumerate(data.get('SPDisplaysDataType', [])):
                info = display.get('spdisplays_main', display)
                
                resolution = info.get('spdisplays_resolution', '0x0').split('x')
                width = int(resolution[0]) if len(resolution) == 2 else 0
                height = int(resolution[1]) if len(resolution) == 2 else 0
                
                displays.append(DisplayInfo(
                    id=idx,
                    name=info.get('spdisplays_display_product_name', f'Display {idx}'),
                    x=0,
                    y=0,
                    width=width,
                    height=height,
                    is_primary=(idx == 0),
                    scale_factor=info.get('spdisplays_scale', 1.0),
                    orientation='landscape',
                    work_x=0,
                    work_y=0,
                    work_width=width,
                    work_height=height,
                ))
    except Exception:
        pass
    
    if not displays:
        displays.append(DisplayInfo(
            id=0,
            name='Built-in Display',
            x=0,
            y=0,
            width=1920,
            height=1080,
            is_primary=True,
            scale_factor=2.0,
            orientation='landscape',
            work_x=0,
            work_y=0,
            work_width=1920,
            work_height=1080,
        ))
    
    return displays


def _get_displays_windows() -> List[DisplayInfo]:
    """Get display info using Windows APIs."""
    displays: List[DisplayInfo] = []
    
    try:
        import ctypes
        from ctypes import wintypes
        
        user32 = ctypes.windll.user32
        
        def_enum = user32.EnumDisplayMonitors(None, None, None, 0)
        
        monitors: List[tuple] = []
        
        def callback(monitor, dc, rect, lparam):
            r = rect._asdict() if hasattr(rect, '_asdict') else {}
            monitors.append((
                monitor,
                r.get('left', 0),
                r.get('top', 0),
                r.get('right', 0) - r.get('left', 0),
                r.get('bottom', 0) - r.get('top', 0),
            ))
            return 1
        
        MONITORENUMPROC = ctypes.WINFUNCTYPE(
            ctypes.c_int,
            ctypes.c_void_p,
            ctypes.c_void_p,
            ctypes.POINTER(wintypes.RECT),
            wintypes.LPARAM,
        )
        
        user32.EnumDisplayMonitors(None, None, MONITORENUMPROC(callback), 0)
        
        for idx, (monitor, x, y, w, h) in enumerate(monitors):
            info = user32.GetMonitorInfoW(monitor)
            
            displays.append(DisplayInfo(
                id=idx,
                name=info.get('Device', f'Display {idx}'),
                x=x,
                y=y,
                width=w,
                height=h,
                is_primary=bool(info.get('dwFlags', 0) & 1),
                scale_factor=1.0,
                orientation='landscape' if w > h else 'portrait',
                work_x=x,
                work_y=y,
                work_width=w,
                work_height=h,
            ))
    except Exception:
        pass
    
    if not displays:
        displays.append(DisplayInfo(
            id=0,
            name='Primary Display',
            x=0,
            y=0,
            width=1920,
            height=1080,
            is_primary=True,
            scale_factor=1.0,
            orientation='landscape',
            work_x=0,
            work_y=0,
            work_width=1920,
            work_height=1080,
        ))
    
    return displays


def _get_displays_linux() -> List[DisplayInfo]:
    """Get display info for Linux/X11."""
    displays: List[DisplayInfo] = []
    
    try:
        import subprocess
        result = subprocess.run(
            ['xrandr', '--listactivemonitors'],
            capture_output=True,
            text=True,
            timeout=5,
        )
        
        if result.returncode == 0:
            lines = result.stdout.strip().split('\n')
            for i, line in enumerate(lines[1:], 0):
                parts = line.split()
                if len(parts) >= 4:
                    dims = parts[3].split('x')
                    offset = dims[1].split('+')
                    
                    width = int(dims[0])
                    height = int(offset[0])
                    x = int(offset[1])
                    y = int(offset[2])
                    
                    displays.append(DisplayInfo(
                        id=i,
                        name=f'Monitor {i}',
                        x=x,
                        y=y,
                        width=width,
                        height=height,
                        is_primary=(i == 0),
                        scale_factor=1.0,
                        orientation='landscape',
                        work_x=x,
                        work_y=y,
                        work_width=width,
                        work_height=height,
                    ))
    except Exception:
        pass
    
    if not displays:
        displays.append(DisplayInfo(
            id=0,
            name='Display 0',
            x=0,
            y=0,
            width=1920,
            height=1080,
            is_primary=True,
            scale_factor=1.0,
            orientation='landscape',
            work_x=0,
            work_y=0,
            work_width=1920,
            work_height=1080,
        ))
    
    return displays


def get_primary_display() -> Optional[DisplayInfo]:
    """Get the primary display.
    
    Returns:
        Primary DisplayInfo or None
    """
    displays = get_displays()
    for display in displays:
        if display.is_primary:
            return display
    return displays[0] if displays else None


def get_display_at(x: int, y: int) -> Optional[DisplayInfo]:
    """Get display containing the given point.
    
    Args:
        x: X coordinate
        y: Y coordinate
    
    Returns:
        DisplayInfo at point or None
    """
    displays = get_displays()
    
    for display in displays:
        if (display.x <= x < display.x + display.width and
            display.y <= y < display.y + display.height):
            return display
    
    return None


def get_display_count() -> int:
    """Get number of connected displays.
    
    Returns:
        Number of displays
    """
    return len(get_displays())


def is_multi_monitor() -> bool:
    """Check if multiple monitors are connected.
    
    Returns:
        True if more than one display
    """
    return get_display_count() > 1


def get_combined_screen_size() -> Tuple[int, int]:
    """Get combined size of all displays.
    
    Returns:
        (total_width, total_height)
    """
    displays = get_displays()
    
    if not displays:
        return (0, 0)
    
    min_x = min(d.x for d in displays)
    min_y = min(d.y for d in displays)
    max_x = max(d.x + d.width for d in displays)
    max_y = max(d.y + d.height for d in displays)
    
    return (max_x - min_x, max_y - min_y)


def get_screen_metrics(display: Optional[DisplayInfo] = None) -> ScreenMetrics:
    """Get detailed screen metrics.
    
    Args:
        display: Display to get metrics for (uses primary if None)
    
    Returns:
        ScreenMetrics object
    """
    if display is None:
        display = get_primary_display()
    
    if display is None:
        return ScreenMetrics(
            width=1920,
            height=1080,
            diagonal=24.0,
            dpi=96,
            density=1.0,
            ppi=96.0,
            aspect_ratio=16/9,
        )
    
    width = display.width
    height = display.height
    scale = display.scale_factor
    
    diag_pixels = (width ** 2 + height ** 2) ** 0.5
    diagonal = diag_pixels / (96 * scale)
    
    ppi = diag_pixels / diagonal
    
    aspect = width / height if height > 0 else 16/9
    
    return ScreenMetrics(
        width=width,
        height=height,
        diagonal=diagonal,
        dpi=int(96 * scale),
        density=scale,
        ppi=ppi,
        aspect_ratio=aspect,
    )


def get_safe_area_insets() -> Tuple[int, int, int, int]:
    """Get safe area insets (for devices with notches).
    
    Returns:
        (top, right, bottom, left) insets in pixels
    """
    if sys.platform != 'darwin':
        return (0, 0, 0, 0)
    
    try:
        import subprocess
        result = subprocess.run(
            ['defaults', 'read', '-g', 'AppleInterfaceStyle'],
            capture_output=True,
            text=True,
        )
        
        dark_mode = result.returncode == 0 and 'Dark' in result.stdout
        
        if dark_mode:
            return (47, 0, 34, 0)
        return (28, 0, 21, 0)
    except Exception:
        pass
    
    return (28, 0, 21, 0)


def pixels_to_display_coords(
    x: int,
    y: int,
    from_display: Optional[DisplayInfo] = None,
) -> Tuple[int, int]:
    """Convert pixels between displays with different scale factors.
    
    Args:
        x: X coordinate in source display
        y: Y coordinate in source display
        from_display: Source display (uses primary if None)
    
    Returns:
        (converted_x, converted_y)
    """
    if from_display is None:
        from_display = get_primary_display()
    
    if from_display is None:
        return (x, y)
    
    src_scale = from_display.scale_factor
    dst_display = get_display_at(x, y)
    dst_scale = dst_display.scale_factor if dst_display else 1.0
    
    if src_scale == dst_scale:
        return (x, y)
    
    factor = dst_scale / src_scale
    return (int(x * factor), int(y * factor))
