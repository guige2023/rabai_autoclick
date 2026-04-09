"""Screen capture utilities for capturing and processing screenshots.

This module provides utilities for capturing screenshots from various sources,
including full screen, regions, specific windows, and multi-monitor setups,
useful for UI automation and testing workflows.

Author: AI Assistant
License: MIT
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum, auto
from typing import Optional, Tuple, List
import io


class CaptureSource(Enum):
    """Source for screen capture."""
    FULL_SCREEN = auto()
    PRIMARY_DISPLAY = auto()
    ALL_DISPLAYS = auto()
    WINDOW = auto()
    REGION = auto()


@dataclass
class CaptureConfig:
    """Configuration for screen capture."""
    source: CaptureSource = CaptureSource.FULL_SCREEN
    display_index: int = 0
    region: Optional[Tuple[int, int, int, int]] = None  # x, y, width, height
    window_id: Optional[int] = None
    include_cursor: bool = False
    include_border: bool = False
    format: str = "PNG"
    quality: int = 95


@dataclass
class DisplayCapture:
    """Information about a captured display."""
    display_index: int
    width: int
    height: int
    x_offset: int
    y_offset: int
    is_primary: bool
    image_data: bytes


def capture_full_screen(
    display_index: int = 0,
    include_cursor: bool = False,
) -> bytes:
    """Capture the full screen.
    
    Args:
        display_index: Display index to capture.
        include_cursor: Include cursor in capture.
    
    Returns:
        Screenshot bytes.
    """
    try:
        from PIL import Image
        import subprocess
        
        if include_cursor:
            return _capture_with_cursor()
        
        result = subprocess.run(
            ["screencapture", "-D", str(display_index + 1), "-x"],
            capture_output=True,
        )
        
        if result.returncode == 0 and result.stdout:
            return result.stdout
        
        return _fallback_capture(display_index)
    except Exception:
        return _fallback_capture(display_index)


def _capture_with_cursor() -> bytes:
    """Capture screen with cursor."""
    try:
        import subprocess
        result = subprocess.run(
            ["screencapture", "-C", "-x"],
            capture_output=True,
        )
        return result.stdout if result.returncode == 0 else b""
    except Exception:
        return b""


def _fallback_capture(display_index: int) -> bytes:
    """Fallback capture using PIL."""
    try:
        from PIL import ImageGrab
        import numpy as np
        import cv2
        
        if display_index == 0:
            img = ImageGrab.grab()
        else:
            return b""
        
        img_array = np.array(img)
        img_bgr = cv2.cvtColor(img_array, cv2.COLOR_RGB2BGR)
        
        _, encoded = cv2.imencode(".png", img_bgr)
        return encoded.tobytes()
    except ImportError:
        raise ImportError("PIL or numpy is required for fallback capture")


def capture_region(
    x: int,
    y: int,
    width: int,
    height: int,
) -> bytes:
    """Capture a screen region.
    
    Args:
        x: Left edge of region.
        y: Top edge of region.
        width: Width of region.
        height: Height of region.
    
    Returns:
        Screenshot bytes of the region.
    """
    try:
        import subprocess
        result = subprocess.run(
            ["screencapture", "-R", f"{x},{y},{width},{height}", "-x"],
            capture_output=True,
        )
        
        if result.returncode == 0 and result.stdout:
            return result.stdout
    except Exception:
        pass
    
    try:
        from PIL import ImageGrab
        import numpy as np
        import cv2
        
        img = ImageGrab.grab(bbox=(x, y, x + width, y + height))
        img_array = np.array(img)
        img_bgr = cv2.cvtColor(img_array, cv2.COLOR_RGB2BGR)
        
        _, encoded = cv2.imencode(".png", img_bgr)
        return encoded.tobytes()
    except Exception:
        return b""


def capture_primary_display() -> bytes:
    """Capture the primary display.
    
    Returns:
        Screenshot bytes.
    """
    return capture_full_screen(display_index=0)


def capture_all_displays() -> List[DisplayCapture]:
    """Capture all connected displays.
    
    Returns:
        List of DisplayCapture objects.
    """
    captures = []
    
    try:
        import subprocess
        import json
        
        result = subprocess.run(
            ["system_profiler", "SPDisplaysDataType", "-json"],
            capture_output=True,
            text=True,
        )
        
        if result.returncode == 0:
            data = json.loads(result.stdout)
            displays = data.get("SPDisplaysDataType", [])
            
            for i, display in enumerate(displays):
                try:
                    img_data = capture_full_screen(display_index=i)
                    if img_data:
                        captures.append(DisplayCapture(
                            display_index=i,
                            width=display.get("Width", 0),
                            height=display.get("Height", 0),
                            x_offset=0,
                            y_offset=0,
                            is_primary=(i == 0),
                            image_data=img_data,
                        ))
                except Exception:
                    continue
    except Exception:
        pass
    
    if not captures:
        img_data = capture_primary_display()
        if img_data:
            captures.append(DisplayCapture(
                display_index=0,
                width=1920,
                height=1080,
                x_offset=0,
                y_offset=0,
                is_primary=True,
                image_data=img_data,
            ))
    
    return captures


def capture_window(
    window_id: int,
    include_shadow: bool = False,
) -> bytes:
    """Capture a specific window by ID.
    
    Args:
        window_id: Window ID to capture.
        include_shadow: Include window shadow.
    
    Returns:
        Screenshot bytes of the window.
    """
    try:
        import subprocess
        args = ["screencapture", "-w", str(window_id), "-x"]
        
        result = subprocess.run(args, capture_output=True)
        
        if result.returncode == 0 and result.stdout:
            return result.stdout
    except Exception:
        pass
    
    return b""


def capture_with_config(config: CaptureConfig) -> bytes:
    """Capture screen using configuration.
    
    Args:
        config: Capture configuration.
    
    Returns:
        Screenshot bytes.
    """
    if config.source == CaptureSource.FULL_SCREEN:
        return capture_full_screen(
            display_index=config.display_index,
            include_cursor=config.include_cursor,
        )
    elif config.source == CaptureSource.REGION and config.region:
        x, y, w, h = config.region
        return capture_region(x, y, w, h)
    elif config.source == CaptureSource.PRIMARY_DISPLAY:
        return capture_primary_display()
    elif config.source == CaptureSource.WINDOW and config.window_id:
        return capture_window(config.window_id)
    else:
        return capture_full_screen(include_cursor=config.include_cursor)


def save_screenshot(
    image_data: bytes,
    filepath: str,
    format: str = "PNG",
    quality: int = 95,
) -> bool:
    """Save screenshot to file.
    
    Args:
        image_data: Screenshot bytes.
        filepath: Output file path.
        format: Image format (PNG, JPEG, etc.).
        quality: JPEG quality (1-100).
    
    Returns:
        True if successful.
    """
    try:
        from PIL import Image
        import io
        
        img = Image.open(io.BytesIO(image_data))
        
        if format.upper() == "JPEG" and img.mode in ("RGBA", "LA", "P"):
            img = img.convert("RGB")
        
        img.save(filepath, format=format.upper(), quality=quality, optimize=True)
        return True
    except Exception:
        return False
