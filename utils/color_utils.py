"""
Color Utilities for UI Automation.

This module provides utilities for color manipulation, conversion,
and comparison for visual testing and UI automation.

Author: AI Assistant
License: MIT
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Tuple, Union


# Type alias for color values
ColorValue = Union[str, Tuple[int, int, int], Tuple[int, int, int, float]]


@dataclass
class RGB:
    """RGB color representation."""
    r: int  # 0-255
    g: int  # 0-255
    b: int  # 0-255
    a: float = 1.0  # 0.0-1.0
    
    def __post_init__(self):
        self.r = max(0, min(255, int(self.r)))
        self.g = max(0, min(255, int(self.g)))
        self.b = max(0, min(255, int(self.b)))
        self.a = max(0.0, min(1.0, float(self.a)))
    
    def to_hex(self) -> str:
        """Convert to hex color string."""
        if self.a < 1.0:
            return f"#{self.r:02x}{self.g:02x}{self.b:02x}{int(self.a*255):02x}"
        return f"#{self.r:02x}{self.g:02x}{self.b:02x}"
    
    def to_rgb_tuple(self) -> Tuple[int, int, int]:
        """Convert to RGB tuple."""
        return (self.r, self.g, self.b)
    
    def to_rgba_tuple(self) -> Tuple[int, int, int, float]:
        """Convert to RGBA tuple."""
        return (self.r, self.g, self.b, self.a)
    
    def to_hsl(self) -> 'HSL':
        """Convert to HSL color."""
        r, g, b = self.r / 255, self.g / 255, self.b / 255
        
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
        
        return HSL(h=h * 360, s=s, l=l, a=self.a)
    
    def distance_to(self, other: 'RGB') -> float:
        """Calculate Euclidean distance to another RGB color."""
        dr = self.r - other.r
        dg = self.g - other.g
        db = self.b - other.b
        return math.sqrt(dr*dr + dg*dg + db*db)
    
    def __eq__(self, other: object) -> bool:
        if not isinstance(other, RGB):
            return False
        return (
            self.r == other.r and
            self.g == other.g and
            self.b == other.b and
            abs(self.a - other.a) < 0.001
        )


@dataclass
class HSL:
    """HSL color representation."""
    h: float  # 0-360
    s: float  # 0.0-1.0
    l: float  # 0.0-1.0
    a: float = 1.0  # 0.0-1.0
    
    def __post_init__(self):
        self.h = self.h % 360
        self.s = max(0.0, min(1.0, float(self.s)))
        self.l = max(0.0, min(1.0, float(self.l)))
        self.a = max(0.0, min(1.0, float(self.a)))
    
    def to_rgb(self) -> RGB:
        """Convert to RGB color."""
        h, s, l = self.h / 360, self.s, self.l
        
        if s == 0:
            gray = int(l * 255)
            return RGB(r=gray, g=gray, b=gray, a=self.a)
        
        def hue_to_rgb(p: float, q: float, t: float) -> float:
            if t < 0: t += 1
            if t > 1: t -= 1
            if t < 1/6: return p + (q - p) * 6 * t
            if t < 1/2: return q
            if t < 2/3: return p + (q - p) * (2/3 - t) * 6
            return p
        
        q = l < 0.5 and l * (1 + s) or l + s - l * s
        p = 2 * l - q
        
        r = int(hue_to_rgb(p, q, h + 1/3) * 255)
        g = int(hue_to_rgb(p, q, h) * 255)
        b = int(hue_to_rgb(p, q, h - 1/3) * 255)
        
        return RGB(r=r, g=g, b=b, a=self.a)
    
    def to_hex(self) -> str:
        """Convert to hex color string."""
        return self.to_rgb().to_hex()


@dataclass
class HSV:
    """HSV color representation."""
    h: float  # 0-360
    s: float  # 0.0-1.0
    v: float  # 0.0-1.0
    a: float = 1.0  # 0.0-1.0
    
    def to_rgb(self) -> RGB:
        """Convert to RGB color."""
        h, s, v = self.h / 360, self.s, self.v
        
        if s == 0:
            gray = int(v * 255)
            return RGB(r=gray, g=gray, b=gray, a=self.a)
        
        i = int(h * 6)
        f = h * 6 - i
        p = v * (1 - s)
        q = v * (1 - f * s)
        t = v * (1 - (1 - f) * s)
        
        i = i % 6
        options = [
            (v, t, p),
            (q, v, p),
            (p, v, t),
            (p, q, v),
            (t, p, v),
            (v, p, q)
        ]
        
        r, g, b = options[i]
        return RGB(r=int(r*255), g=int(g*255), b=int(b*255), a=self.a)


def parse_color(color_str: str) -> RGB:
    """
    Parse a color string into an RGB object.
    
    Supports formats:
    - Hex: #RGB, #RRGGBB, #RRGGBBAA
    - rgb(r, g, b)
    - rgba(r, g, b, a)
    
    Args:
        color_str: Color string to parse
        
    Returns:
        RGB color object
        
    Raises:
        ValueError: If color format is invalid
    """
    color_str = color_str.strip().lower()
    
    # Hex format
    if color_str.startswith('#'):
        return _parse_hex(color_str)
    
    # rgb/rgba format
    if color_str.startswith('rgb'):
        return _parse_rgb(color_str)
    
    raise ValueError(f"Invalid color format: {color_str}")


def _parse_hex(hex_str: str) -> RGB:
    """Parse hex color string."""
    hex_str = hex_str.lstrip('#')
    
    if len(hex_str) == 3:
        r = int(hex_str[0] * 2, 16)
        g = int(hex_str[1] * 2, 16)
        b = int(hex_str[2] * 2, 16)
        return RGB(r=r, g=g, b=b)
    
    if len(hex_str) == 6:
        r = int(hex_str[0:2], 16)
        g = int(hex_str[2:4], 16)
        b = int(hex_str[4:6], 16)
        return RGB(r=r, g=g, b=b)
    
    if len(hex_str) == 8:
        r = int(hex_str[0:2], 16)
        g = int(hex_str[2:4], 16)
        b = int(hex_str[4:6], 16)
        a = int(hex_str[6:8], 16) / 255
        return RGB(r=r, g=g, b=b, a=a)
    
    raise ValueError(f"Invalid hex color: {hex_str}")


def _parse_rgb(rgb_str: str) -> RGB:
    """Parse rgb/rgba color string."""
    import re
    
    match = re.match(r'rgba?\s*\(\s*(\d+)\s*,\s*(\d+)\s*,\s*(\d+)\s*(?:,\s*([\d.]+))?\s*\)', rgb_str)
    if not match:
        raise ValueError(f"Invalid rgb/rgba color: {rgb_str}")
    
    r, g, b = int(match.group(1)), int(match.group(2)), int(match.group(3))
    a = float(match.group(4)) if match.group(4) else 1.0
    
    return RGB(r=r, g=g, b=b, a=a)


def blend_colors(color1: RGB, color2: RGB, factor: float = 0.5) -> RGB:
    """
    Blend two colors together.
    
    Args:
        color1: First color
        color2: Second color
        factor: Blend factor (0.0 = color1, 1.0 = color2)
        
    Returns:
        Blended RGB color
    """
    factor = max(0.0, min(1.0, factor))
    
    r = int(color1.r + (color2.r - color1.r) * factor)
    g = int(color1.g + (color2.g - color1.g) * factor)
    b = int(color1.b + (color2.b - color1.b) * factor)
    a = color1.a + (color2.a - color1.a) * factor
    
    return RGB(r=r, g=g, b=b, a=a)


def get_contrast_ratio(color1: RGB, color2: RGB) -> float:
    """
    Calculate WCAG contrast ratio between two colors.
    
    Returns:
        Contrast ratio (1.0 - 21.0)
    """
    def luminance(rgb: RGB) -> float:
        def adjust(c: int) -> float:
            c = c / 255
            return c / 12.92 if c <= 0.03928 else ((c + 0.055) / 1.055) ** 2.4
        return 0.2126 * adjust(rgb.r) + 0.7152 * adjust(rgb.g) + 0.0722 * adjust(rgb.b)
    
    l1 = luminance(color1)
    l2 = luminance(color2)
    
    lighter = max(l1, l2)
    darker = min(l1, l2)
    
    return (lighter + 0.05) / (darker + 0.05)


def is_readable(color1: RGB, color2: RGB, level: str = "AA") -> bool:
    """
    Check if color combination meets WCAG accessibility standards.
    
    Args:
        color1: First color
        color2: Second color
        level: WCAG level ("AA" or "AAA")
        
    Returns:
        True if contrast meets the standard
    """
    ratio = get_contrast_ratio(color1, color2)
    
    thresholds = {
        "AA": 4.5,   # Normal text
        "AAA": 7.0,  # Enhanced
        "AA_LARGE": 3.0  # Large text
    }
    
    threshold = thresholds.get(level, 4.5)
    return ratio >= threshold
