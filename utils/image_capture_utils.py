"""
Advanced image capture utilities for automation.

Provides high-quality screenshot capture with region selection,
multi-monitor support, and various capture modes.
"""

from __future__ import annotations

import subprocess
import os
import hashlib
import time
from typing import Optional, List, Tuple, Callable
from dataclasses import dataclass
from enum import Enum
from datetime import datetime


class CaptureMode(Enum):
    """Screenshot capture modes."""
    FULL_SCREEN = "full"
    REGION = "region"
    WINDOW = "window"
    DISPLAY = "display"
    SELECTION = "selection"


@dataclass
class CaptureOptions:
    """Screenshot capture options."""
    mode: CaptureMode = CaptureMode.FULL_SCREEN
    include_cursor: bool = False
    include_border: bool = True
    wait_for_key: bool = False
    output_format: str = "png"
    quality: int = 100
    display_index: int = 0
    window_id: Optional[int] = None
    region: Optional[Tuple[int, int, int, int]] = None


@dataclass
class CaptureResult:
    """Capture result."""
    path: str
    width: int
    height: int
    size_bytes: int
    format: str
    timestamp: datetime
    display_index: int
    hash_md5: str
    duration: float


def capture(options: CaptureOptions,
            output_dir: str = "/tmp") -> Optional[CaptureResult]:
    """
    Capture screenshot with options.
    
    Args:
        options: CaptureOptions configuration.
        output_dir: Output directory.
        
    Returns:
        CaptureResult or None.
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    output_path = os.path.join(output_dir, f"screenshot_{timestamp}.{options.output_format}")
    
    args = ["screencapture"]
    
    if not options.include_cursor:
        args.append("-x")
    else:
        args.append("-C")
    
    if options.include_border:
        args.append("-b")
    
    if options.wait_for_key:
        args.append("-w")
    
    if options.mode == CaptureMode.REGION and options.region:
        x, y, w, h = options.region
        args.extend(["-R", f"{x},{y},{w},{h}"])
    
    if options.mode == CaptureMode.DISPLAY:
        args.extend(["-d", str(options.display_index)])
    
    if options.mode == CaptureMode.WINDOW and options.window_id:
        args.extend(["-w", str(options.window_id)])
    
    args.extend(["-t", options.output_format])
    args.append(output_path)
    
    try:
        start = time.time()
        result = subprocess.run(args, check=True, capture_output=True)
        duration = time.time() - start
        
        if os.path.exists(output_path):
            return _process_captured(output_path, options, duration)
    except subprocess.CalledProcessError:
        pass
    
    return None


def _process_captured(path: str, options: CaptureOptions,
                      duration: float) -> Optional[CaptureResult]:
    """Process captured screenshot."""
    try:
        stat = os.stat(path)
        width, height = _get_image_dimensions(path)
        
        with open(path, 'rb') as f:
            md5_hash = hashlib.md5(f.read()).hexdigest()
        
        if options.output_format == "jpeg" and options.quality < 100:
            try:
                from PIL import Image
                img = Image.open(path)
                img.save(path, format='JPEG', quality=options.quality)
            except ImportError:
                pass
        
        return CaptureResult(
            path=path,
            width=width,
            height=height,
            size_bytes=stat.st_size,
            format=options.output_format,
            timestamp=datetime.now(),
            display_index=options.display_index,
            hash_md5=md5_hash,
            duration=duration
        )
    except Exception:
        return None


def _get_image_dimensions(path: str) -> Tuple[int, int]:
    """Get image dimensions."""
    try:
        import Quartz
        from PIL import Image
        with Image.open(path) as img:
            return img.size
    except ImportError:
        try:
            import struct
            with open(path, 'rb') as f:
                data = f.read()
                if data[:8] == b'\x89PNG\r\n\x1a\n':
                    w = struct.unpack('>I', data[16:20])[0]
                    h = struct.unpack('>I', data[20:24])[0]
                    return w, h
        except Exception:
            pass
    return (0, 0)


def capture_all_displays(output_dir: str = "/tmp") -> List[CaptureResult]:
    """
    Capture all connected displays.
    
    Args:
        output_dir: Output directory.
        
    Returns:
        List of CaptureResult for each display.
    """
    results = []
    
    try:
        import Quartz
        for i, _ in enumerate(Quartz.NSScreen.screens()):
            options = CaptureOptions(
                mode=CaptureMode.DISPLAY,
                display_index=i
            )
            result = capture(options, output_dir)
            if result:
                results.append(result)
    except Exception:
        options = CaptureOptions(mode=CaptureMode.FULL_SCREEN)
        result = capture(options, output_dir)
        if result:
            results.append(result)
    
    return results


def capture_with_overlay(callback: Callable[[str], None],
                         output_dir: str = "/tmp") -> Optional[str]:
    """
    Capture with interactive selection overlay.
    
    Args:
        callback: Called with capture path when complete.
        output_dir: Output directory.
        
    Returns:
        Path to captured image.
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = os.path.join(output_dir, f"screenshot_selection_{timestamp}.png")
    
    try:
        result = subprocess.run(
            ["screencapture", "-i", "-s", "-P", output_path],
            check=True,
            capture_output=True
        )
        
        if os.path.exists(output_path):
            callback(output_path)
            return output_path
    except subprocess.CalledProcessError:
        pass
    
    return None


def capture_window_by_id(window_id: int,
                         include_cursor: bool = False,
                         output_dir: str = "/tmp") -> Optional[CaptureResult]:
    """
    Capture specific window by ID.
    
    Args:
        window_id: Window ID.
        include_cursor: Include cursor.
        output_dir: Output directory.
        
    Returns:
        CaptureResult or None.
    """
    options = CaptureOptions(
        mode=CaptureMode.WINDOW,
        window_id=window_id,
        include_cursor=include_cursor
    )
    return capture(options, output_dir)


def capture_region(x: int, y: int, width: int, height: int,
                  output_dir: str = "/tmp") -> Optional[CaptureResult]:
    """
    Capture screen region.
    
    Args:
        x: X coordinate.
        y: Y coordinate.
        width: Region width.
        height: Region height.
        output_dir: Output directory.
        
    Returns:
        CaptureResult or None.
    """
    options = CaptureOptions(
        mode=CaptureMode.REGION,
        region=(x, y, width, height)
    )
    return capture(options, output_dir)


def capture_primary_display(output_dir: str = "/tmp") -> Optional[CaptureResult]:
    """
    Capture primary display.
    
    Args:
        output_dir: Output directory.
        
    Returns:
        CaptureResult or None.
    """
    options = CaptureOptions(mode=CaptureMode.DISPLAY, display_index=0)
    return capture(options, output_dir)
