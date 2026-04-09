"""UI color utilities for automation.

Provides utilities for color manipulation, conversion,
comparison, and generation for UI automation.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple, Union


@dataclass
class RGBColor:
    """RGB color representation."""
    r: int
    g: int
    b: int
    a: int = 255
    
    def to_hex(self) -> str:
        """Convert to hex string.
        
        Returns:
            Hex color string.
        """
        return f"#{self.r:02X}{self.g:02X}{self.b:02X}"
    
    def to_hsl(self) -> "HSLColor":
        """Convert to HSL.
        
        Returns:
            HSLColor object.
        """
        r = self.r / 255.0
        g = self.g / 255.0
        b = self.b / 255.0
        
        max_c = max(r, g, b)
        min_c = min(r, g, b)
        l = (max_c + min_c) / 2
        
        if max_c == min_c:
            h = 0.0
            s = 0.0
        else:
            d = max_c - min_c
            s = d / (2 - max_c - min_c) if l > 0.5 else d / (max_c + min_c)
            
            if max_c == r:
                h = (g - b) / d + (6 if g < b else 0)
            elif max_c == g:
                h = (b - r) / d + 2
            else:
                h = (r - g) / d + 4
            
            h /= 6
        
        return HSLColor(int(h * 360), int(s * 100), int(l * 100))
    
    def luminance(self) -> float:
        """Calculate relative luminance.
        
        Returns:
            Luminance value.
        """
        def linearize(c: float) -> float:
            if c <= 0.03928:
                return c / 12.92
            return math.pow((c + 0.055) / 1.055, 2.4)
        
        r = linearize(self.r / 255.0)
        g = linearize(self.g / 255.0)
        b = linearize(self.b / 255.0)
        
        return 0.2126 * r + 0.7152 * g + 0.0722 * b


@dataclass
class HSLColor:
    """HSL color representation."""
    h: int
    s: int
    l: int
    
    def to_rgb(self) -> RGBColor:
        """Convert to RGB.
        
        Returns:
            RGBColor object.
        """
        h = self.h / 360.0
        s = self.s / 100.0
        l = self.l / 100.0
        
        if s == 0:
            gray = int(l * 255)
            return RGBColor(gray, gray, gray)
        
        def hue_to_rgb(p: float, q: float, t: float) -> float:
            if t < 0:
                t += 1
            if t > 1:
                t -= 1
            if t < 1/6:
                return p + (q - p) * 6 * t
            if t < 1/2:
                return q
            if t < 2/3:
                return p + (q - p) * (2/3 - t) * 6
            return p
        
        q = l * (1 + s) if l < 0.5 else l + s - l * s
        p = 2 * l - q
        
        r = int(hue_to_rgb(p, q, h + 1/3) * 255)
        g = int(hue_to_rgb(p, q, h) * 255)
        b = int(hue_to_rgb(p, q, h - 1/3) * 255)
        
        return RGBColor(r, g, b)


class ColorPalette:
    """Manages color palettes for UI automation.
    
    Provides utilities for creating, managing, and
    accessing color palettes.
    """
    
    MATERIAL_DESIGN = {
        "red": "#F44336",
        "pink": "#E91E63",
        "purple": "#9C27B0",
        "deep_purple": "#673AB7",
        "indigo": "#3F51B5",
        "blue": "#2196F3",
        "light_blue": "#03A9F4",
        "cyan": "#00BCD4",
        "teal": "#009688",
        "green": "#4CAF50",
        "light_green": "#8BC34A",
        "lime": "#CDDC39",
        "yellow": "#FFEB3B",
        "amber": "#FFC107",
        "orange": "#FF9800",
        "deep_orange": "#FF5722",
        "brown": "#795548",
        "grey": "#9E9E9E",
        "blue_grey": "#607D8B"
    }
    
    GRAY_SCALE = {
        "black": "#000000",
        "dark_gray": "#333333",
        "medium_gray": "#666666",
        "gray": "#999999",
        "light_gray": "#CCCCCC",
        "very_light_gray": "#EEEEEE",
        "white": "#FFFFFF"
    }
    
    def __init__(self) -> None:
        """Initialize the color palette."""
        self._custom_colors: Dict[str, str] = {}
    
    def get_color(self, name: str) -> Optional[str]:
        """Get a color by name.
        
        Args:
            name: Color name.
            
        Returns:
            Hex color string or None.
        """
        if name in self._custom_colors:
            return self._custom_colors[name]
        
        if name in self.MATERIAL_DESIGN:
            return self.MATERIAL_DESIGN[name]
        
        if name in self.GRAY_SCALE:
            return self.GRAY_SCALE[name]
        
        return None
    
    def add_color(self, name: str, hex_value: str) -> None:
        """Add a custom color.
        
        Args:
            name: Color name.
            hex_value: Hex color value.
        """
        self._custom_colors[name] = hex_value
    
    def get_contrast_color(self, background_hex: str) -> RGBColor:
        """Get appropriate contrast color for background.
        
        Args:
            background_hex: Background hex color.
            
        Returns:
            RGBColor (black or white).
        """
        bg = RGBColor.from_hex(background_hex)
        luminance = bg.luminance()
        
        if luminance > 0.5:
            return RGBColor(0, 0, 0)
        else:
            return RGBColor(255, 255, 255)
    
    def generate_shades(self, base_hex: str, count: int = 5) -> List[str]:
        """Generate shades of a color.
        
        Args:
            base_hex: Base hex color.
            count: Number of shades.
            
        Returns:
            List of hex colors.
        """
        base = RGBColor.from_hex(base_hex)
        hsl = base.to_hsl()
        
        shades = []
        for i in range(count):
            l = max(10, hsl.l - (i * 15))
            shade = HSLColor(hsl.h, hsl.s, l).to_rgb()
            shades.append(shade.to_hex())
        
        return shades


def parse_color(value: str) -> Optional[RGBColor]:
    """Parse a color value.
    
    Args:
        value: Color string (hex, rgb, etc).
        
    Returns:
        RGBColor or None.
    """
    value = value.strip()
    
    if value.startswith("#"):
        return RGBColor.from_hex(value)
    
    if value.startswith("rgb"):
        match = re.match(r"rgba?\((\d+),\s*(\d+),\s*(\d+)(?:,\s*([\d.]+))?\)", value)
        if match:
            r, g, b = int(match[1]), int(match[2]), int(match[3])
            a = int(float(match[4]) * 255) if match[4] else 255
            return RGBColor(r, g, b, a)
    
    return None


def blend_colors(color1: RGBColor, color2: RGBColor, ratio: float = 0.5) -> RGBColor:
    """Blend two colors.
    
    Args:
        color1: First color.
        color2: Second color.
        ratio: Blend ratio.
        
    Returns:
        Blended RGBColor.
    """
    r = int(color1.r * (1 - ratio) + color2.r * ratio)
    g = int(color1.g * (1 - ratio) + color2.g * ratio)
    b = int(color1.b * (1 - ratio) + color2.b * ratio)
    
    return RGBColor(r, g, b)


def calculate_contrast(color1: RGBColor, color2: RGBColor) -> float:
    """Calculate contrast ratio between two colors.
    
    Args:
        color1: First color.
        color2: Second color.
        
    Returns:
        Contrast ratio.
    """
    l1 = color1.luminance()
    l2 = color2.luminance()
    
    lighter = max(l1, l2)
    darker = min(l1, l2)
    
    return (lighter + 0.05) / (darker + 0.05)
