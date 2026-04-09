"""
Color mixing and blending utilities for UI automation.

Provides functions for mixing colors, blending gradients,
and generating color palettes for visual feedback.
"""

from __future__ import annotations

import math
from typing import List, Tuple, Optional, Sequence


RGBA = Tuple[int, int, int, float]
RGB = Tuple[int, int, int]
HSL = Tuple[float, float, float]
HSV = Tuple[float, float, float]


def hex_to_rgb(hex_color: str) -> RGB:
    """Convert hex color string to RGB tuple.
    
    Args:
        hex_color: Hex color string (e.g., '#FF5733' or 'FF5733')
    
    Returns:
        RGB tuple (r, g, b) with values 0-255
    
    Raises:
        ValueError: If hex string is invalid
    """
    hex_color = hex_color.lstrip('#')
    if len(hex_color) not in (3, 6):
        raise ValueError(f"Invalid hex color: {hex_color}")
    
    if len(hex_color) == 3:
        hex_color = ''.join(c * 2 for c in hex_color)
    
    return tuple(int(hex_color[i:i+2], 16) for i in (0, 2, 4))


def rgb_to_hex(rgb: RGB, include_hash: bool = True) -> str:
    """Convert RGB tuple to hex color string.
    
    Args:
        rgb: RGB tuple (r, g, b) with values 0-255
        include_hash: Whether to include '#' prefix
    
    Returns:
        Hex color string
    """
    r, g, b = (max(0, min(255, int(c))) for c in rgb)
    result = f"{r:02X}{g:02X}{b:02X}"
    return f"#{result}" if include_hash else result


def rgb_to_hsl(rgb: RGB) -> HSL:
    """Convert RGB to HSL color space.
    
    Args:
        rgb: RGB tuple (r, g, b) with values 0-255
    
    Returns:
        HSL tuple (h, s, l) where h is degrees (0-360),
        s and l are percentages (0-1)
    """
    r, g, b = (c / 255.0 for c in rgb)
    max_c = max(r, g, b)
    min_c = min(r, g, b)
    l = (max_c + min_c) / 2.0
    
    if max_c == min_c:
        return (0.0, 0.0, l)
    
    d = max_c - min_c
    s = d / (2.0 - max_c - min_c) if l > 0.5 else d / (max_c + min_c)
    
    if max_c == r:
        h = ((g - b) / d + (6 if g < b else 0)) / 6.0
    elif max_c == g:
        h = ((b - r) / d + 2) / 6.0
    else:
        h = ((r - g) / d + 4) / 6.0
    
    return (h * 360.0, s, l)


def hsl_to_rgb(hsl: HSL) -> RGB:
    """Convert HSL to RGB color space.
    
    Args:
        hsl: HSL tuple (h, s, l) where h is degrees (0-360),
            s and l are percentages (0-1)
    
    Returns:
        RGB tuple (r, g, b) with values 0-255
    """
    h, s, l = hsl
    h = h / 360.0
    
    if s == 0:
        gray = int(l * 255)
        return (gray, gray, gray)
    
    def hue_to_rgb(p: float, q: float, t: float) -> float:
        if t < 0: t += 1
        if t > 1: t -= 1
        if t < 1/6: return p + (q - p) * 6 * t
        if t < 1/2: return q
        if t < 2/3: return p + (q - p) * (2/3 - t) * 6
        return p
    
    q = l * (1 + s) if l < 0.5 else l + s - l * s
    p = 2 * l - q
    
    r = int(hue_to_rgb(p, q, h + 1/3) * 255)
    g = int(hue_to_rgb(p, q, h) * 255)
    b = int(hue_to_rgb(p, q, h - 1/3) * 255)
    
    return (r, g, b)


def mix_colors(color1: RGB, color2: RGB, ratio: float = 0.5) -> RGB:
    """Mix two RGB colors together.
    
    Args:
        color1: First RGB color
        color2: Second RGB color
        ratio: Mix ratio (0.0 = all color1, 1.0 = all color2)
    
    Returns:
        Mixed RGB color
    """
    ratio = max(0.0, min(1.0, ratio))
    r = int(color1[0] * (1 - ratio) + color2[0] * ratio)
    g = int(color1[1] * (1 - ratio) + color2[1] * ratio)
    b = int(color1[2] * (1 - ratio) + color2[2] * ratio)
    return (r, g, b)


def blend_colors_linear(colors: Sequence[RGB], positions: Optional[Sequence[float]] = None) -> List[RGB]:
    """Blend multiple colors with linear interpolation.
    
    Args:
        colors: Sequence of RGB colors to blend
        positions: Optional positions (0.0 to 1.0) for each color
    
    Returns:
        List of blended RGB colors at interpolated positions
    """
    if not colors:
        return []
    if len(colors) == 1:
        return [colors[0]]
    
    if positions is None:
        positions = [i / (len(colors) - 1) for i in range(len(colors))]
    
    result = []
    for pos in positions:
        pos = max(0.0, min(1.0, pos))
        
        for i in range(len(colors) - 1):
            p1, p2 = positions[i], positions[i + 1]
            if p1 <= pos <= p2:
                if p2 == p1:
                    ratio = 0.0
                else:
                    ratio = (pos - p1) / (p2 - p1)
                blended = mix_colors(colors[i], colors[i + 1], ratio)
                result.append(blended)
                break
    
    return result


def generate_gradient(colors: Sequence[RGB], steps: int) -> List[RGB]:
    """Generate a gradient between multiple colors.
    
    Args:
        colors: Sequence of RGB colors
        steps: Number of color steps in output
    
    Returns:
        List of RGB colors forming the gradient
    """
    if steps < 1:
        return []
    if steps == 1:
        return [colors[0]] if colors else []
    
    positions = [i / (len(colors) - 1) for i in range(len(colors))]
    target_positions = [i / (steps - 1) for i in range(steps)]
    
    return blend_colors_linear(colors, target_positions)


def complementary_color(rgb: RGB) -> RGB:
    """Get the complementary (opposite) color.
    
    Args:
        rgb: RGB color tuple
    
    Returns:
        Complementary RGB color
    """
    return (255 - rgb[0], 255 - rgb[1], 255 - rgb[2])


def analogous_colors(rgb: RGB, angle: float = 30.0) -> List[RGB]:
    """Get analogous colors (adjacent on color wheel).
    
    Args:
        rgb: Source RGB color
        angle: Angle offset in degrees (default 30)
    
    Returns:
        List of analogous RGB colors
    """
    hsl = rgb_to_hsl(rgb)
    h = hsl[0]
    
    colors = []
    for offset in (-angle, 0, angle):
        new_h = (h + offset) % 360
        colors.append(hsl_to_rgb((new_h, hsl[1], hsl[2])))
    
    return colors


def triadic_colors(rgb: RGB) -> List[RGB]:
    """Get triadic colors (evenly spaced on color wheel).
    
    Args:
        rgb: Source RGB color
    
    Returns:
        List of three triadic RGB colors
    """
    hsl = rgb_to_hsl(rgb)
    h = hsl[0]
    
    return [
        hsl_to_rgb(((h + offset) % 360, hsl[1], hsl[2]))
        for offset in (0, 120, 240)
    ]


def adjust_brightness(rgb: RGB, factor: float) -> RGB:
    """Adjust color brightness.
    
    Args:
        rgb: Source RGB color
        factor: Brightness factor (0.0 = black, 1.0 = original, >1.0 = brighter)
    
    Returns:
        Adjusted RGB color
    """
    return tuple(max(0, min(255, int(c * factor))) for c in rgb)


def adjust_saturation(rgb: RGB, factor: float) -> RGB:
    """Adjust color saturation.
    
    Args:
        rgb: Source RGB color
        factor: Saturation factor (0.0 = grayscale, 1.0 = original)
    
    Returns:
        Adjusted RGB color
    """
    hsl = rgb_to_hsl(rgb)
    return hsl_to_rgb((hsl[0], hsl[1] * factor, hsl[2]))


def desaturate(rgb: RGB, amount: float = 0.5) -> RGB:
    """Desaturate a color (move towards gray).
    
    Args:
        rgb: Source RGB color
        amount: Desaturation amount (0.0 = no change, 1.0 = full gray)
    
    Returns:
        Desaturated RGB color
    """
    gray = int(0.299 * rgb[0] + 0.587 * rgb[1] + 0.114 * rgb[2])
    return mix_colors(rgb, (gray, gray, gray), amount)


def luminance(rgb: RGB) -> float:
    """Calculate relative luminance of RGB color.
    
    Uses the formula from WCAG 2.0.
    
    Args:
        rgb: RGB color tuple
    
    Returns:
        Luminance value between 0.0 and 1.0
    """
    def linearize(c: int) -> float:
        c = c / 255.0
        return c / 12.92 if c <= 0.03928 else ((c + 0.055) / 1.055) ** 2.4
    
    return (0.2126 * linearize(rgb[0]) + 
            0.7152 * linearize(rgb[1]) + 
            0.0722 * linearize(rgb[2]))


def contrast_ratio(rgb1: RGB, rgb2: RGB) -> float:
    """Calculate contrast ratio between two colors.
    
    Uses WCAG 2.0 formula.
    
    Args:
        rgb1: First RGB color
        rgb2: Second RGB color
    
    Returns:
        Contrast ratio (1.0 = no contrast, 21.0 = max contrast)
    """
    l1 = luminance(rgb1)
    l2 = luminance(rgb2)
    lighter = max(l1, l2)
    darker = min(l1, l2)
    return (lighter + 0.05) / (darker + 0.05)


def get_readable_text_color(bg_color: RGB) -> RGB:
    """Get a readable text color (black or white) for given background.
    
    Args:
        bg_color: Background RGB color
    
    Returns:
        Black (0,0,0) or white (255,255,255) for best contrast
    """
    return (0, 0, 0) if luminance(bg_color) > 0.179 else (255, 255, 255)
