"""Pixel detection and color matching utilities.

This module provides utilities for detecting pixels at specific screen
coordinates, matching colors with tolerance, and searching for pixels
matching criteria within screen regions.
"""

from __future__ import annotations

import time
from typing import Optional, NamedTuple

from .screenshot_utils import capture_screen


# Tolerance for color matching (0-255 per channel)
DEFAULT_COLOR_TOLERANCE = 10


class PixelColor(NamedTuple):
    """Represents a pixel color in RGB format."""
    r: int
    g: int
    b: int
    
    def distance_to(self, other: "PixelColor") -> float:
        """Calculate Euclidean distance to another color.
        
        Args:
            other: Another PixelColor to compare against.
        
        Returns:
            Distance between the two colors (0-441.67 max).
        """
        return ((self.r - other.r) ** 2 + 
                self.g - other.g) ** 2 + 
                (self.b - other.b) ** 2) ** 0.5
    
    def matches(
        self, 
        other: "PixelColor", 
        tolerance: int = DEFAULT_COLOR_TOLERANCE
    ) -> bool:
        """Check if this color matches another within tolerance.
        
        Args:
            other: Color to compare against.
            tolerance: Maximum per-channel difference allowed.
        
        Returns:
            True if all channels are within tolerance.
        """
        return (abs(self.r - other.r) <= tolerance and
                abs(self.g - other.g) <= tolerance and
                abs(self.b - other.b) <= tolerance)
    
    def to_hex(self) -> str:
        """Convert to hexadecimal color string.
        
        Returns:
            Hex color string like '#FF0000'.
        """
        return f"#{self.r:02X}{self.g:02X}{self.b:02X}"
    
    @classmethod
    def from_hex(cls, hex_color: str) -> "PixelColor":
        """Create PixelColor from hex string.
        
        Args:
            hex_color: Hex color string like '#FF0000' or 'FF0000'.
        
        Returns:
            New PixelColor instance.
        
        Raises:
            ValueError: If hex string is invalid.
        """
        hex_color = hex_color.lstrip("#")
        if len(hex_color) != 6:
            raise ValueError(f"Invalid hex color: {hex_color}")
        return cls(
            r=int(hex_color[0:2], 16),
            g=int(hex_color[2:4], 16),
            b=int(hex_color[4:6], 16),
        )


def get_pixel_color(x: int, y: int) -> PixelColor:
    """Get the color of a pixel at the given screen coordinates.
    
    Args:
        x: X coordinate.
        y: Y coordinate.
    
    Returns:
        PixelColor at the specified location.
    
    Raises:
        RuntimeError: If screenshot capture fails.
    """
    try:
        from PIL import Image
        import pyautogui
        
        # Capture a 1x1 region
        screenshot = pyautogui.screenshot(region=(x, y, 1, 1))
        r, g, b = screenshot.getpixel((0, 0))
        return PixelColor(r=r, g=g, b=b)
    except ImportError:
        # Fallback using PIL directly
        import subprocess
        # Use screencapture on macOS
        result = subprocess.run(
            ["screencapture", "-x", "-m", "-t", "png", "/tmp/_px.png"],
            capture_output=True
        )
        if result.returncode == 0:
            from PIL import Image
            img = Image.open("/tmp/_px.png")
            cropped = img.crop((x, y, x + 1, y + 1))
            r, g, b = cropped.getpixel((0, 0))
            return PixelColor(r=r, g=g, b=b)
        raise RuntimeError("Failed to capture pixel color")


def wait_for_pixel(
    x: int,
    y: int,
    expected_color: PixelColor,
    tolerance: int = DEFAULT_COLOR_TOLERANCE,
    timeout: float = 10.0,
    poll_interval: float = 0.1,
) -> bool:
    """Wait for a pixel to match an expected color.
    
    Args:
        x: X coordinate to monitor.
        y: Y coordinate to monitor.
        expected_color: Color to match.
        tolerance: Matching tolerance.
        timeout: Maximum time to wait in seconds.
        poll_interval: Time between checks in seconds.
    
    Returns:
        True if color matched, False if timeout occurred.
    """
    start = time.monotonic()
    while time.monotonic() - start < timeout:
        actual = get_pixel_color(x, y)
        if actual.matches(expected_color, tolerance):
            return True
        time.sleep(poll_interval)
    return False


def find_pixel_in_region(
    region: tuple[int, int, int, int],  # (x, y, width, height)
    target_color: PixelColor,
    tolerance: int = DEFAULT_COLOR_TOLERANCE,
) -> Optional[tuple[int, int]]:
    """Find a pixel matching target color within a screen region.
    
    Args:
        region: Region as (x, y, width, height).
        target_color: Color to search for.
        tolerance: Matching tolerance.
    
    Returns:
        Tuple of (x, y) if found, None otherwise.
    """
    x, y, w, h = region
    try:
        import pyautogui
        screenshot = pyautogui.screenshot(region=region)
        pixels = screenshot.load()
        
        for py in range(h):
            for px in range(w):
                r, g, b = pixels[px, py]
                color = PixelColor(r=r, g=g, b=b)
                if color.matches(target_color, tolerance):
                    return (x + px, y + py)
        return None
    except ImportError:
        return None


def find_all_pixels_in_region(
    region: tuple[int, int, int, int],
    target_color: PixelColor,
    tolerance: int = DEFAULT_COLOR_TOLERANCE,
) -> list[tuple[int, int]]:
    """Find all pixels matching target color within a region.
    
    Args:
        region: Region as (x, y, width, height).
        target_color: Color to search for.
        tolerance: Matching tolerance.
    
    Returns:
        List of (x, y) tuples for all matching pixels.
    """
    x, y, w, h = region
    matches = []
    
    try:
        import pyautogui
        screenshot = pyautogui.screenshot(region=region)
        pixels = screenshot.load()
        
        for py in range(h):
            for px in range(w):
                r, g, b = pixels[px, py]
                color = PixelColor(r=r, g=g, b=b)
                if color.matches(target_color, tolerance):
                    matches.append((x + px, y + py))
    except ImportError:
        pass
    
    return matches
