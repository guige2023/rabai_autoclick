"""
Advanced screenshot utilities for automation workflows.

Provides screenshot capture with region selection, annotation,
and various output formats for macOS.
"""

from __future__ import annotations

import subprocess
import os
import hashlib
from typing import Optional, tuple
from dataclasses import dataclass
from enum import Enum
from datetime import datetime


class ScreenshotFormat(Enum):
    """Supported screenshot formats."""
    PNG = "png"
    TIFF = "tiff"
    JPEG = "jpeg"
    BMP = "bmp"


@dataclass
class ScreenshotResult:
    """Screenshot capture result."""
    path: str
    width: int
    height: int
    format: ScreenshotFormat
    size_bytes: int
    timestamp: datetime
    md5_hash: str


def capture_screen(output_path: Optional[str] = None) -> ScreenshotResult:
    """
    Capture full screen screenshot.
    
    Args:
        output_path: Optional output path. If None, uses /tmp with timestamp.
        
    Returns:
        ScreenshotResult with capture details.
    """
    if output_path is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_path = f"/tmp/screenshot_{timestamp}.png"
    
    try:
        subprocess.run(
            ["screencapture", "-x", output_path],
            check=True,
            capture_output=True
        )
        
        stat = os.stat(output_path)
        width, height = get_image_dimensions(output_path)
        
        with open(output_path, 'rb') as f:
            md5_hash = hashlib.md5(f.read()).hexdigest()
        
        return ScreenshotResult(
            path=output_path,
            width=width,
            height=height,
            format=ScreenshotFormat.PNG,
            size_bytes=stat.st_size,
            timestamp=datetime.now(),
            md5_hash=md5_hash
        )
    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"Screenshot failed: {e}")


def capture_region(x: int, y: int, width: int, height: int,
                   output_path: Optional[str] = None) -> ScreenshotResult:
    """
    Capture a specific screen region.
    
    Args:
        x: X coordinate of region top-left.
        y: Y coordinate of region top-left.
        width: Width of region.
        height: Height of region.
        output_path: Optional output path.
        
    Returns:
        ScreenshotResult with capture details.
    """
    if output_path is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_path = f"/tmp/screenshot_region_{timestamp}.png"
    
    try:
        subprocess.run(
            ["screencapture", "-x", "-R", f"{x},{y},{width},{height}", output_path],
            check=True,
            capture_output=True
        )
        
        stat = os.stat(output_path)
        actual_width, actual_height = get_image_dimensions(output_path)
        
        with open(output_path, 'rb') as f:
            md5_hash = hashlib.md5(f.read()).hexdigest()
        
        return ScreenshotResult(
            path=output_path,
            width=actual_width,
            height=actual_height,
            format=ScreenshotFormat.PNG,
            size_bytes=stat.st_size,
            timestamp=datetime.now(),
            md5_hash=md5_hash
        )
    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"Region screenshot failed: {e}")


def capture_window(window_id: int, output_path: Optional[str] = None) -> ScreenshotResult:
    """
    Capture a specific window by ID.
    
    Args:
        window_id: Window ID to capture.
        output_path: Optional output path.
        
    Returns:
        ScreenshotResult with capture details.
    """
    if output_path is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_path = f"/tmp/screenshot_window_{timestamp}.png"
    
    try:
        subprocess.run(
            ["screencapture", "-x", "-w", str(window_id), output_path],
            check=True,
            capture_output=True
        )
        
        stat = os.stat(output_path)
        width, height = get_image_dimensions(output_path)
        
        with open(output_path, 'rb') as f:
            md5_hash = hashlib.md5(f.read()).hexdigest()
        
        return ScreenshotResult(
            path=output_path,
            width=width,
            height=height,
            format=ScreenshotFormat.PNG,
            size_bytes=stat.st_size,
            timestamp=datetime.now(),
            md5_hash=md5_hash
        )
    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"Window screenshot failed: {e}")


def capture_with_selection() -> Optional[str]:
    """
    Capture screenshot with interactive region selection.
    
    Returns:
        Path to saved screenshot, or None if cancelled.
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = f"/tmp/screenshot_selection_{timestamp}.png"
    
    try:
        result = subprocess.run(
            ["screencapture", "-i", "-s", output_path],
            capture_output=True
        )
        if result.returncode == 0 and os.path.exists(output_path):
            return output_path
    except Exception:
        pass
    return None


def capture_to_clipboard() -> bool:
    """
    Capture screenshot directly to clipboard.
    
    Returns:
        True if successful, False otherwise.
    """
    try:
        subprocess.run(
            ["screencapture", "-c"],
            check=True,
            capture_output=True
        )
        return True
    except subprocess.CalledProcessError:
        return False


def capture_multi_monitor() -> list[ScreenshotResult]:
    """
    Capture screenshots of all connected monitors.
    
    Returns:
        List of ScreenshotResult for each monitor.
    """
    results = []
    
    try:
        import Quartz
        for screen in Quartz.NSScreen.screens():
            bounds = screen.frame()
            x, y = int(bounds.origin.x), int(bounds.origin.y)
            w, h = int(bounds.size.width), int(bounds.size.height)
            
            result = capture_region(x, y, w, h)
            results.append(result)
    except Exception:
        full = capture_screen()
        results.append(full)
    
    return results


def get_image_dimensions(image_path: str) -> tuple[int, int]:
    """
    Get image dimensions without loading full image.
    
    Args:
        image_path: Path to image file.
        
    Returns:
        Tuple of (width, height).
    """
    try:
        from PIL import Image
        with Image.open(image_path) as img:
            return img.size
    except ImportError:
        try:
            import struct
            with open(image_path, 'rb') as f:
                data = f.read()
                if data[:8] == b'\x89PNG\r\n\x1a\n':
                    w = struct.unpack('>I', data[16:20])[0]
                    h = struct.unpack('>I', data[20:24])[0]
                    return w, h
        except Exception:
            pass
    return 0, 0


def convert_screenshot(input_path: str, output_path: str,
                       target_format: ScreenshotFormat,
                       quality: int = 90) -> bool:
    """
    Convert screenshot to different format.
    
    Args:
        input_path: Source screenshot path.
        output_path: Destination path.
        target_format: Target format.
        quality: JPEG quality (1-100).
        
    Returns:
        True if successful, False otherwise.
    """
    try:
        from PIL import Image
        with Image.open(input_path) as img:
            if target_format == ScreenshotFormat.JPEG:
                img = img.convert('RGB')
                img.save(output_path, format='JPEG', quality=quality)
            else:
                img.save(output_path, format=target_format.value.upper())
        return True
    except Exception:
        return False
