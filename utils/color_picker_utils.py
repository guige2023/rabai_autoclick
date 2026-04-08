"""Color picker utilities for capturing colors from the screen.

This module provides utilities for picking colors from specific screen
coordinates, which is useful for color-based automation triggers.
"""

from __future__ import annotations

import platform
import subprocess
from typing import Optional, NamedTuple


IS_MACOS = platform.system() == "Darwin"


class RGBColor(NamedTuple):
    """RGB color representation."""
    r: int
    g: int
    b: int
    
    def to_hex(self) -> str:
        """Convert to hex color string."""
        return f"#{self.r:02X}{self.g:02X}{self.b:02X}"
    
    def to_hsl(self) -> tuple[float, float, float]:
        """Convert to HSL (Hue, Saturation, Lightness)."""
        r, g, b = self.r / 255.0, self.g / 255.0, self.b / 255.0
        max_c = max(r, g, b)
        min_c = min(r, g, b)
        l = (max_c + min_c) / 2
        
        if max_c == min_c:
            h = s = 0
        else:
            d = max_c - min_c
            s = l > 0.5 and d / (2 - max_c - min_c) or d / (max_c + min_c)
            if max_c == r:
                h = (g - b) / d + (g < b and 6 or 0)
            elif max_c == g:
                h = (b - r) / d + 2
            else:
                h = (r - g) / d + 4
            h /= 6
        return (h * 360, s * 100, l * 100)
    
    def luminance(self) -> float:
        """Calculate relative luminance."""
        r, g, b = self.r / 255.0, self.g / 255.0, self.b / 255.0
        return 0.299 * r + 0.587 * g + 0.114 * b


def pick_color_at(x: int, y: int) -> Optional[RGBColor]:
    """Pick the color at a specific screen coordinate.
    
    Args:
        x: X coordinate.
        y: Y coordinate.
    
    Returns:
        RGBColor at the coordinate, or None if unavailable.
    """
    if IS_MACOS:
        return _pick_color_macos(x, y)
    return _pick_color_pyautogui(x, y)


def _pick_color_macos(x: int, y: int) -> Optional[RGBColor]:
    """Pick color on macOS using screencapture."""
    try:
        # Capture a 1x1 pixel region
        result = subprocess.run(
            [
                "screencapture",
                "-x",
                "-m",
                "-R", f"{x},{y},1,1",
                "/tmp/_color_pick.png"
            ],
            capture_output=True,
            timeout=5
        )
        if result.returncode != 0:
            return None
        
        from PIL import Image
        img = Image.open("/tmp/_color_pick.png")
        rgb = img.convert("RGB")
        r, g, b = rgb.getpixel((0, 0))
        return RGBColor(r=int(r), g=int(g), b=int(b))
    except Exception:
        return None


def _pick_color_pyautogui(x: int, y: int) -> Optional[RGBColor]:
    """Pick color using pyautogui."""
    try:
        import pyautogui
        screenshot = pyautogui.screenshot(region=(x, y, 1, 1))
        r, g, b = screenshot.getpixel((0, 0))
        return RGBColor(r=r, g=g, b=b)
    except Exception:
        return None


def pick_color_interactive() -> Optional[RGBColor]:
    """Launch an interactive color picker.
    
    Returns:
        RGBColor selected by the user, or None if cancelled.
    """
    if IS_MACOS:
        try:
            # Use macOS built-in Digital Color Meter
            result = subprocess.run(
                ["osascript", "-e", 
                 'tell application "Finder" to activate'],
                capture_output=True,
                timeout=3
            )
            
            # Simulate opening Digital Color Meter
            subprocess.run(
                ["open", "-a", "Digital Color Meter"],
                capture_output=True,
                timeout=3
            )
            
            # Note: Full interactive picking would require
            # more complex automation
            return None
        except Exception:
            return None
    return None


def get_pixel_info(x: int, y: int) -> dict:
    """Get comprehensive pixel information at a coordinate.
    
    Args:
        x: X coordinate.
        y: Y coordinate.
    
    Returns:
        Dictionary with pixel info (color, luminance, etc.).
    """
    color = pick_color_at(x, y)
    if not color:
        return {}
    
    h, s, l = color.to_hsl()
    return {
        "rgb": color,
        "hex": color.to_hex(),
        "hsl": (round(h, 1), round(s, 1), round(l, 1)),
        "luminance": round(color.luminance(), 3),
    }
