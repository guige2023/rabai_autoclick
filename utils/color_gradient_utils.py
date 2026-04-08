"""
Color gradient generation utilities for automation workflows.

This module provides utilities for creating and manipulating color gradients,
useful for visual feedback, heatmaps, and UI highlighting in automation scripts.
"""

from __future__ import annotations

import math
from typing import List, Tuple, Optional, Callable


def hex_to_rgb(hex_color: str) -> Tuple[int, int, int]:
    """Convert hex color string to RGB tuple.
    
    Args:
        hex_color: Hex color string (e.g., '#FF5733' or 'FF5733')
        
    Returns:
        Tuple of (R, G, B) values (0-255)
        
    Raises:
        ValueError: If hex color format is invalid
    """
    hex_color = hex_color.lstrip('#')
    if len(hex_color) != 6:
        raise ValueError(f"Invalid hex color: {hex_color}")
    
    try:
        r = int(hex_color[0:2], 16)
        g = int(hex_color[2:4], 16)
        b = int(hex_color[4:6], 16)
        return (r, g, b)
    except ValueError as e:
        raise ValueError(f"Invalid hex color: {hex_color}") from e


def rgb_to_hex(r: int, g: int, b: int) -> str:
    """Convert RGB tuple to hex color string.
    
    Args:
        r: Red component (0-255)
        g: Green component (0-255)
        b: Blue component (0-255)
        
    Returns:
        Hex color string (e.g., '#FF5733')
    """
    return f"#{r:02X}{g:02X}{b:02X}"


def interpolate_color(
    color1: Tuple[int, int, int],
    color2: Tuple[int, int, int],
    factor: float
) -> Tuple[int, int, int]:
    """Interpolate between two RGB colors.
    
    Args:
        color1: Starting RGB color
        color2: Ending RGB color
        factor: Interpolation factor (0.0 = color1, 1.0 = color2)
        
    Returns:
        Interpolated RGB color
    """
    factor = max(0.0, min(1.0, factor))
    r = int(color1[0] + (color2[0] - color1[0]) * factor)
    g = int(color1[1] + (color2[1] - color1[1]) * factor)
    b = int(color1[2] + (color2[2] - color1[2]) * factor)
    return (r, g, b)


def generate_linear_gradient(
    start_color: str,
    end_color: str,
    steps: int
) -> List[str]:
    """Generate a linear gradient between two colors.
    
    Args:
        start_color: Starting hex color
        end_color: Ending hex color
        steps: Number of color steps in the gradient
        
    Returns:
        List of hex color strings representing the gradient
    """
    if steps < 2:
        raise ValueError("Steps must be at least 2")
    
    start_rgb = hex_to_rgb(start_color)
    end_rgb = hex_to_rgb(end_color)
    
    gradient = []
    for i in range(steps):
        factor = i / (steps - 1)
        rgb = interpolate_color(start_rgb, end_rgb, factor)
        gradient.append(rgb_to_hex(*rgb))
    
    return gradient


def generate_multi_stop_gradient(
    stops: List[Tuple[float, str]],
    steps: int
) -> List[str]:
    """Generate a gradient with multiple color stops.
    
    Args:
        stops: List of (position, hex_color) tuples where position is 0.0-1.0
        steps: Number of total color steps in the gradient
        
    Returns:
        List of hex color strings representing the gradient
    """
    if not stops:
        raise ValueError("At least one color stop is required")
    
    sorted_stops = sorted(stops, key=lambda x: x[0])
    gradient = []
    
    for i in range(steps):
        pos = i / (steps - 1)
        
        # Find surrounding stops
        lower_stop = sorted_stops[0]
        upper_stop = sorted_stops[-1]
        
        for j, (stop_pos, stop_color) in enumerate(sorted_stops):
            if pos >= stop_pos:
                lower_stop = (stop_pos, stop_color)
            if pos <= stop_pos:
                upper_stop = (stop_pos, stop_color)
                break
        
        # Interpolate between surrounding stops
        if lower_stop == upper_stop:
            rgb = hex_to_rgb(lower_stop[1])
        else:
            range_size = upper_stop[0] - lower_stop[0]
            if range_size == 0:
                factor = 0.0
            else:
                factor = (pos - lower_stop[0]) / range_size
            rgb = interpolate_color(
                hex_to_rgb(lower_stop[1]),
                hex_to_rgb(upper_stop[1]),
                factor
            )
        
        gradient.append(rgb_to_hex(*rgb))
    
    return gradient


def generate_heatmap_gradient(
    steps: int = 256,
    colormap: str = "viridis"
) -> List[str]:
    """Generate a heatmap-style gradient.
    
    Args:
        steps: Number of color steps
        colormap: Name of colormap ('viridis', 'plasma', 'inferno', 'magma', 'grayscale')
        
    Returns:
        List of hex color strings
    """
    colormaps = {
        "viridis": [
            (0.267, 0.004, 0.329),
            (0.282, 0.140, 0.458),
            (0.254, 0.265, 0.530),
            (0.206, 0.372, 0.553),
            (0.163, 0.471, 0.558),
            (0.128, 0.567, 0.551),
            (0.135, 0.659, 0.518),
            (0.267, 0.749, 0.441),
            (0.478, 0.821, 0.318),
            (0.741, 0.873, 0.150),
            (0.993, 0.906, 0.144),
        ],
        "plasma": [
            (0.050, 0.030, 0.280),
            (0.260, 0.070, 0.460),
            (0.440, 0.130, 0.500),
            (0.590, 0.220, 0.490),
            (0.720, 0.350, 0.430),
            (0.820, 0.510, 0.340),
            (0.890, 0.670, 0.260),
            (0.940, 0.820, 0.180),
            (0.990, 0.950, 0.090),
        ],
        "inferno": [
            (0.001, 0.000, 0.014),
            (0.150, 0.060, 0.360),
            (0.420, 0.070, 0.520),
            (0.620, 0.140, 0.540),
            (0.790, 0.270, 0.490),
            (0.910, 0.450, 0.370),
            (0.980, 0.650, 0.220),
            (0.990, 0.830, 0.090),
            (0.980, 0.980, 0.580),
        ],
        "magma": [
            (0.001, 0.000, 0.016),
            (0.110, 0.040, 0.280),
            (0.300, 0.090, 0.420),
            (0.480, 0.130, 0.480),
            (0.640, 0.210, 0.490),
            (0.770, 0.340, 0.470),
            (0.870, 0.500, 0.420),
            (0.950, 0.690, 0.340),
            (0.990, 0.890, 0.430),
        ],
        "grayscale": [
            (0.0, 0.0, 0.0),
            (1.0, 1.0, 1.0),
        ],
    }
    
    if colormap not in colormaps:
        raise ValueError(f"Unknown colormap: {colormap}")
    
    rgb_stops = [(pos, (r * 255, g * 255, b * 255)) 
                 for pos, (r, g, b) in enumerate(
                    .linspace(0, 1, len(colormaps[colormap])),
                     colormaps[colormap]
                 )]
    rgb_stops = list(zip(
       .linspace(0, 1, len(colormaps[colormap])),
        colormaps[colormap]
    ))
    rgb_stops = [(pos, (int(r * 255), int(g * 255), int(b * 255))) 
                 for pos, (r, g, b) in rgb_stops]
    
    stops = [(pos, rgb_to_hex(int(r), int(g), int(b))) 
             for pos, (r, g, b) in rgb_stops]
    
    return generate_multi_stop_gradient(stops, steps)


def linspace(start: float, end: float, num: int) -> List[float]:
    """Generate evenly spaced numbers over an interval."""
    if num < 2:
        return [start]
    step = (end - start) / (num - 1)
    return [start + i * step for i in range(num)]


def hsl_to_rgb(h: float, s: float, l: float) -> Tuple[int, int, int]:
    """Convert HSL color to RGB.
    
    Args:
        h: Hue (0.0-1.0)
        s: Saturation (0.0-1.0)
        l: Lightness (0.0-1.0)
        
    Returns:
        RGB tuple (0-255)
    """
    if s == 0:
        gray = int(l * 255)
        return (gray, gray, gray)
    
    def hue2rgb(p: float, q: float, t: float) -> float:
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
    r = hue2rgb(p, q, h + 1/3)
    g = hue2rgb(p, q, h)
    b = hue2rgb(p, q, h - 1/3)
    
    return (int(r * 255), int(g * 255), int(b * 255))


def rgb_to_hsl(r: int, g: int, b: int) -> Tuple[float, float, float]:
    """Convert RGB color to HSL.
    
    Args:
        r: Red (0-255)
        g: Green (0-255)
        b: Blue (0-255)
        
    Returns:
        HSL tuple (h: 0.0-1.0, s: 0.0-1.0, l: 0.0-1.0)
    """
    r /= 255
    g /= 255
    b /= 255
    
    max_c = max(r, g, b)
    min_c = min(r, g, b)
    l = (max_c + min_c) / 2
    
    if max_c == min_c:
        return (0.0, 0.0, l)
    
    d = max_c - min_c
    s = d / (2 - max_c - min_c) if l > 0.5 else d / (max_c + min_c)
    
    if max_c == r:
        h = (g - b) / d + (6 if g < b else 0)
    elif max_c == g:
        h = (b - r) / d + 2
    else:
        h = (r - g) / d + 4
    
    h /= 6
    return (h, s, l)


def shift_hue(rgb: Tuple[int, int, int], hue_shift: float) -> Tuple[int, int, int]:
    """Shift the hue of an RGB color.
    
    Args:
        rgb: RGB tuple
        hue_shift: Hue shift amount (-1.0 to 1.0)
        
    Returns:
        Shifted RGB tuple
    """
    h, s, l = rgb_to_hsl(*rgb)
    h = (h + hue_shift) % 1.0
    return hsl_to_rgb(h, s, l)


def adjust_saturation(
    rgb: Tuple[int, int, int],
    saturation_factor: float
) -> Tuple[int, int, int]:
    """Adjust saturation of an RGB color.
    
    Args:
        rgb: RGB tuple
        saturation_factor: Multiplier for saturation (0.0 = gray, 1.0 = original, 2.0 = double)
        
    Returns:
        Adjusted RGB tuple
    """
    h, s, l = rgb_to_hsl(*rgb)
    s = min(1.0, s * saturation_factor)
    return hsl_to_rgb(h, s, l)


def adjust_lightness(
    rgb: Tuple[int, int, int],
    lightness_factor: float
) -> Tuple[int, int, int]:
    """Adjust lightness of an RGB color.
    
    Args:
        rgb: RGB tuple
        lightness_factor: Multiplier for lightness
        
    Returns:
        Adjusted RGB tuple
    """
    h, s, l = rgb_to_hsl(*rgb)
    l = min(1.0, l * lightness_factor)
    return hsl_to_rgb(h, s, l)


def complementary_color(hex_color: str) -> str:
    """Get the complementary color (opposite on color wheel).
    
    Args:
        hex_color: Hex color string
        
    Returns:
        Complementary hex color string
    """
    rgb = hex_to_rgb(hex_color)
    h, s, l = rgb_to_hsl(*rgb)
    h = (h + 0.5) % 1.0
    return rgb_to_hex(*hsl_to_rgb(h, s, l))


def generate_analogous_colors(hex_color: str, count: int = 5) -> List[str]:
    """Generate analogous colors (colors adjacent on the color wheel).
    
    Args:
        hex_color: Base hex color
        count: Total number of colors to generate
        
    Returns:
        List of analogous hex color strings
    """
    rgb = hex_to_rgb(hex_color)
    h, s, l = rgb_to_hsl(*rgb)
    
    colors = []
    angle_step = 1.0 / 12.0  # 30 degrees
    
    start_angle = -((count - 1) / 2) * angle_step
    
    for i in range(count):
        shift = start_angle + i * angle_step
        new_h = (h + shift) % 1.0
        colors.append(rgb_to_hex(*hsl_to_rgb(new_h, s, l)))
    
    return colors


def generate_triadic_colors(hex_color: str) -> List[str]:
    """Generate triadic colors (three colors equally spaced on the wheel).
    
    Args:
        hex_color: Base hex color
        
    Returns:
        List of three hex color strings
    """
    rgb = hex_to_rgb(hex_color)
    h, s, l = rgb_to_hsl(*rgb)
    
    return [
        hex_color,
        rgb_to_hex(*hsl_to_rgb((h + 1/3) % 1.0, s, l)),
        rgb_to_hex(*hsl_to_rgb((h + 2/3) % 1.0, s, l)),
    ]


def generate_tetradic_colors(hex_color: str) -> List[str]:
    """Generate tetradic colors (four colors forming a rectangle).
    
    Args:
        hex_color: Base hex color
        
    Returns:
        List of four hex color strings
    """
    rgb = hex_to_rgb(hex_color)
    h, s, l = rgb_to_hsl(*rgb)
    
    return [
        hex_color,
        rgb_to_hex(*hsl_to_rgb((h + 0.25) % 1.0, s, l)),
        rgb_to_hex(*hsl_to_rgb((h + 0.5) % 1.0, s, l)),
        rgb_to_hex(*hsl_to_rgb((h + 0.75) % 1.0, s, l)),
    ]


def blend_multiply(color1: str, color2: str) -> str:
    """Blend two colors using multiply mode.
    
    Args:
        color1: First hex color
        color2: Second hex color
        
    Returns:
        Blended hex color
    """
    rgb1 = hex_to_rgb(color1)
    rgb2 = hex_to_rgb(color2)
    
    blended = (
        int(rgb1[0] * rgb2[0] / 255),
        int(rgb1[1] * rgb2[1] / 255),
        int(rgb1[2] * rgb2[2] / 255),
    )
    
    return rgb_to_hex(*blended)


def blend_screen(color1: str, color2: str) -> str:
    """Blend two colors using screen mode.
    
    Args:
        color1: First hex color
        color2: Second hex color
        
    Returns:
        Blended hex color
    """
    rgb1 = hex_to_rgb(color1)
    rgb2 = hex_to_rgb(color2)
    
    blended = (
        int(255 - (255 - rgb1[0]) * (255 - rgb2[0]) / 255),
        int(255 - (255 - rgb1[1]) * (255 - rgb2[1]) / 255),
        int(255 - (255 - rgb1[2]) * (255 - rgb2[2]) / 255),
    )
    
    return rgb_to_hex(*blended)


def blend_overlay(color1: str, color2: str) -> str:
    """Blend two colors using overlay mode.
    
    Args:
        color1: Base hex color
        color2: Overlay hex color
        
    Returns:
        Blended hex color
    """
    rgb1 = hex_to_rgb(color1)
    rgb2 = hex_to_rgb(color2)
    
    def overlay_channel(c1: int, c2: int) -> int:
        if c1 < 128:
            return int(2 * c1 * c2 / 255)
        else:
            return int(255 - 2 * (255 - c1) * (255 - c2) / 255)
    
    blended = (
        overlay_channel(rgb1[0], rgb2[0]),
        overlay_channel(rgb1[1], rgb2[1]),
        overlay_channel(rgb1[2], rgb2[2]),
    )
    
    return rgb_to_hex(*blended)
