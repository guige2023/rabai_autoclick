"""
Color wheel and wheel-based color manipulation utilities.

Provides utilities for working with color wheels, including hue rotation,
complementary colors, analogous schemes, and color harmony calculations.
"""

from __future__ import annotations

import math
from typing import Tuple, List, Optional, Callable


def hex_to_rgb(hex_color: str) -> Tuple[int, int, int]:
    """Convert hex color string to RGB tuple."""
    hex_color = hex_color.lstrip('#')
    if len(hex_color) != 6:
        raise ValueError(f"Invalid hex color: {hex_color}")
    return (int(hex_color[0:2], 16), int(hex_color[2:4], 16), int(hex_color[4:6], 16))


def rgb_to_hex(r: int, g: int, b: int) -> str:
    """Convert RGB tuple to hex color string."""
    return f"#{r:02X}{g:02X}{b:02X}"


def rgb_to_hsv(r: int, g: int, b: int) -> Tuple[float, float, float]:
    """Convert RGB to HSV color space.
    
    Returns:
        Tuple of (hue: 0-360, saturation: 0-1, value: 0-1)
    """
    r, g, b = r / 255.0, g / 255.0, b / 255.0
    
    max_c = max(r, g, b)
    min_c = min(r, g, b)
    diff = max_c - min_c
    
    # Hue
    if max_c == min_c:
        h = 0.0
    elif max_c == r:
        h = (60 * ((g - b) / diff) + 360) % 360
    elif max_c == g:
        h = (60 * ((b - r) / diff) + 120) % 360
    else:
        h = (60 * ((r - g) / diff) + 240) % 360
    
    # Saturation
    s = 0.0 if max_c == 0 else diff / max_c
    
    # Value
    v = max_c
    
    return (h, s, v)


def hsv_to_rgb(h: float, s: float, v: float) -> Tuple[int, int, int]:
    """Convert HSV to RGB color space.
    
    Args:
        h: Hue (0-360)
        s: Saturation (0-1)
        v: Value (0-1)
        
    Returns:
        Tuple of (r, g, b) each 0-255
    """
    h = h % 360
    c = v * s
    x = c * (1 - abs((h / 60) % 2 - 1))
    m = v - c
    
    if 0 <= h < 60:
        r, g, b = c, x, 0
    elif 60 <= h < 120:
        r, g, b = x, c, 0
    elif 120 <= h < 180:
        r, g, b = 0, c, x
    elif 180 <= h < 240:
        r, g, b = 0, x, c
    elif 240 <= h < 300:
        r, g, b = x, 0, c
    else:
        r, g, b = c, 0, x
    
    return (int((r + m) * 255), int((g + m) * 255), int((b + m) * 255))


def rotate_hue(hex_color: str, degrees: float) -> str:
    """Rotate the hue of a color by degrees.
    
    Args:
        hex_color: Hex color string
        degrees: Degrees to rotate (positive = clockwise)
        
    Returns:
        New hex color string
    """
    rgb = hex_to_rgb(hex_color)
    h, s, v = rgb_to_hsv(*rgb)
    new_h = (h + degrees) % 360
    new_rgb = hsv_to_rgb(new_h, s, v)
    return rgb_to_hex(*new_rgb)


def get_complementary(hex_color: str) -> str:
    """Get the complementary color (180° opposite on wheel).
    
    Args:
        hex_color: Hex color string
        
    Returns:
        Complementary hex color
    """
    return rotate_hue(hex_color, 180)


def get_split_complementary(hex_color: str) -> Tuple[str, str]:
    """Get split complementary colors (150° and 210° from original).
    
    Args:
        hex_color: Hex color string
        
    Returns:
        Tuple of two hex colors
    """
    return (rotate_hue(hex_color, 150), rotate_hue(hex_color, 210))


def get_triadic(hex_color: str) -> Tuple[str, str]:
    """Get triadic colors (120° apart).
    
    Args:
        hex_color: Hex color string
        
    Returns:
        Tuple of two additional hex colors
    """
    return (rotate_hue(hex_color, 120), rotate_hue(hex_color, 240))


def get_tetradic(hex_color: str) -> Tuple[str, str, str]:
    """Get tetradic (rectangular) colors.
    
    Args:
        hex_color: Hex color string
        
    Returns:
        Tuple of three additional hex colors
    """
    return (rotate_hue(hex_color, 90), rotate_hue(hex_color, 180), rotate_hue(hex_color, 270))


def get_analogous(hex_color: str, count: int = 5, spread: float = 30.0) -> List[str]:
    """Get analogous colors.
    
    Args:
        hex_color: Hex color string
        count: Total number of colors to generate
        spread: Degrees between each color
        
    Returns:
        List of hex colors
    """
    rgb = hex_to_rgb(hex_color)
    h, s, v = rgb_to_hsv(*rgb)
    
    colors = []
    start = -((count - 1) / 2) * spread
    
    for i in range(count):
        new_h = (h + start + i * spread) % 360
        colors.append(rgb_to_hex(*hsv_to_rgb(new_h, s, v)))
    
    return colors


def get_monochromatic(hex_color: str, count: int = 5) -> List[str]:
    """Get monochromatic variations (same hue, different value/saturation).
    
    Args:
        hex_color: Hex color string
        count: Number of variations
        
    Returns:
        List of hex colors
    """
    rgb = hex_to_rgb(hex_color)
    h, s, v = rgb_to_hsv(*rgb)
    
    colors = []
    for i in range(count):
        new_v = (i + 1) / count
        colors.append(rgb_to_hex(*hsv_to_rgb(h, s, new_v)))
    
    return colors


def get_color_temperature(temp: float) -> str:
    """Get color from temperature (in Kelvin, approximated).
    
    Args:
        temp: Temperature in Kelvin (1000-40000)
        
    Returns:
        Approximate hex color
    """
    temp = max(1000, min(40000, temp)) / 100
    
    if temp <= 66:
        r = 255
    else:
        r = 329.698727446 * ((temp - 60) ** -0.1332047592)
        r = max(0, min(255, r))
    
    if temp <= 66:
        g = 99.4708025861 * math.log(temp) - 161.1195681661
        g = max(0, min(255, g))
    else:
        g = 288.1221695283 * ((temp - 60) ** -0.0755148492)
        g = max(0, min(255, g))
    
    if temp >= 66:
        b = 255
    elif temp <= 19:
        b = 0
    else:
        b = 138.5177312231 * math.log(temp - 10) - 305.0447927307
        b = max(0, min(255, b))
    
    return rgb_to_hex(int(r), int(g), int(b))


def get_warm_colors(count: int = 5) -> List[str]:
    """Get a range of warm colors (reds, oranges, yellows).
    
    Args:
        count: Number of colors
        
    Returns:
        List of hex colors
    """
    colors = []
    for i in range(count):
        hue = 0 + (30 * i * (60 / max(1, count - 1)))
        colors.append(rgb_to_hex(*hsv_to_rgb(hue, 0.8, 0.9)))
    return colors


def get_cool_colors(count: int = 5) -> List[str]:
    """Get a range of cool colors (blues, greens, purples).
    
    Args:
        count: Number of colors
        
    Returns:
        List of hex colors
    """
    colors = []
    for i in range(count):
        hue = 180 + (180 * i * (60 / max(1, count - 1)))
        colors.append(rgb_to_hex(*hsv_to_rgb(hue, 0.7, 0.85)))
    return colors


def get_rainbow_spectrum(count: int = 12) -> List[str]:
    """Get rainbow spectrum colors.
    
    Args:
        count: Number of colors
        
    Returns:
        List of hex colors
    """
    colors = []
    for i in range(count):
        hue = (360 / count) * i
        colors.append(rgb_to_hex(*hsv_to_rgb(hue, 0.9, 0.95)))
    return colors


class ColorWheel:
    """Represents a color wheel for interactive color selection."""
    
    def __init__(self, radius: int = 100):
        """Initialize color wheel.
        
        Args:
            radius: Wheel radius in pixels
        """
        self.radius = radius
        self.center = (radius, radius)
    
    def get_color_at_angle(self, angle: float, saturation: float = 1.0, value: float = 1.0) -> Tuple[int, int, int]:
        """Get color at a specific angle on the wheel.
        
        Args:
            angle: Angle in degrees
            saturation: Saturation (0-1)
            value: Value (0-1)
            
        Returns:
            RGB tuple
        """
        return hsv_to_rgb(angle % 360, saturation, value)
    
    def get_color_at_position(self, x: float, y: float) -> Tuple[int, int, int]:
        """Get color at a specific position.
        
        Args:
            x: X coordinate
            y: Y coordinate
            
        Returns:
            RGB tuple
        """
        dx = x - self.center[0]
        dy = y - self.center[1]
        
        angle = math.degrees(math.atan2(dy, dx)) % 360
        distance = math.sqrt(dx * dx + dy * dy)
        
        saturation = min(1.0, distance / self.radius)
        value = 1.0
        
        return hsv_to_rgb(angle, saturation, value)
    
    def get_angle_of_color(self, hex_color: str) -> float:
        """Get the angle of a color on the wheel.
        
        Args:
            hex_color: Hex color string
            
        Returns:
            Angle in degrees
        """
        rgb = hex_to_rgb(hex_color)
        h, _, _ = rgb_to_hsv(*rgb)
        return h
    
    def generate_wheel_pixels(self) -> List[Tuple[int, Tuple[int, int, int]]]:
        """Generate all pixels for this color wheel.
        
        Returns:
            List of ((x, y), rgb) tuples
        """
        pixels = []
        for y in range(self.radius * 2):
            for x in range(self.radius * 2):
                dx = x - self.center[0]
                dy = y - self.center[1]
                dist = math.sqrt(dx * dx + dy * dy)
                
                if dist <= self.radius:
                    color = self.get_color_at_position(x, y)
                    pixels.append(((x, y), color))
        
        return pixels


def create_color_palette(
    base_color: str,
    scheme: str = "analogous",
    count: int = 5
) -> List[str]:
    """Create a color palette based on a scheme.
    
    Args:
        base_color: Base hex color
        scheme: Color scheme name
        count: Number of colors
        
    Returns:
        List of hex colors
    """
    schemes = {
        "analogous": lambda c: get_analogous(c, count),
        "complementary": lambda c: [c, get_complementary(c)],
        "triadic": lambda c: [c] + list(get_triadic(c)),
        "tetradic": lambda c: [c] + list(get_tetradic(c)),
        "split-complementary": lambda c: [c] + list(get_split_complementary(c)),
        "monochromatic": lambda c: get_monochromatic(c, count),
        "rainbow": lambda c: get_rainbow_spectrum(count),
        "warm": lambda c: get_warm_colors(count),
        "cool": lambda c: get_cool_colors(count),
    }
    
    if scheme not in schemes:
        raise ValueError(f"Unknown scheme: {scheme}")
    
    return schemes[scheme](base_color)


def adjust_brightness(hex_color: str, factor: float) -> str:
    """Adjust brightness of a color.
    
    Args:
        hex_color: Hex color string
        factor: Brightness factor (>1 = brighter, <1 = darker)
        
    Returns:
        Adjusted hex color
    """
    rgb = hex_to_rgb(hex_color)
    h, s, v = rgb_to_hsv(*rgb)
    new_v = max(0, min(1, v * factor))
    return rgb_to_hex(*hsv_to_rgb(h, s, new_v))


def saturate(hex_color: str, factor: float) -> str:
    """Saturate or desaturate a color.
    
    Args:
        hex_color: Hex color string
        factor: Saturation factor (>1 = more saturated, <1 = less)
        
    Returns:
        Adjusted hex color
    """
    rgb = hex_to_rgb(hex_color)
    h, s, v = rgb_to_hsv(*rgb)
    new_s = max(0, min(1, s * factor))
    return rgb_to_hex(*hsv_to_rgb(h, new_s, v))
