"""
Image blending and compositing utilities for UI automation.

Provides functions for blending images, creating overlays,
and generating visual effects.
"""

from __future__ import annotations

from typing import Tuple, Optional, List, Union
from enum import Enum, auto


BlendMode = Enum('BlendMode', [
    'NORMAL', 'MULTIPLY', 'SCREEN', 'OVERLAY', 
    'DARKEN', 'LIGHTEN', 'SOFT_LIGHT', 'HARD_LIGHT',
    'COLOR_DODGE', 'COLOR_BURN', 'DIFFERENCE', 'EXCLUSION',
])


def blend_pixels(
    fg: Tuple[int, int, int],
    bg: Tuple[int, int, int],
    alpha: float,
    mode: BlendMode = BlendMode.NORMAL,
) -> Tuple[int, int, int]:
    """Blend foreground pixel over background.
    
    Args:
        fg: Foreground RGB
        bg: Background RGB
        alpha: Foreground alpha (0.0 to 1.0)
        mode: Blend mode
    
    Returns:
        Blended RGB pixel
    """
    if alpha <= 0:
        return bg
    
    if alpha >= 1:
        return fg
    
    t = alpha
    result = []
    
    for cf, cb in zip(fg, bg):
        blended = _blend_channel(cf, cb, mode)
        result.append(int(cb * (1 - t) + blended * t))
    
    return tuple(result)


def _blend_channel(fg: int, bg: int, mode: BlendMode) -> float:
    """Blend single channel."""
    fg_f = fg / 255.0
    bg_f = bg / 255.0
    
    if mode == BlendMode.NORMAL:
        return fg_f
    elif mode == BlendMode.MULTIPLY:
        return fg_f * bg_f
    elif mode == BlendMode.SCREEN:
        return 1 - (1 - fg_f) * (1 - bg_f)
    elif mode == BlendMode.OVERLAY:
        if bg_f < 0.5:
            return 2 * fg_f * bg_f
        return 1 - 2 * (1 - fg_f) * (1 - bg_f)
    elif mode == BlendMode.DARKEN:
        return min(fg_f, bg_f)
    elif mode == BlendMode.LIGHTEN:
        return max(fg_f, bg_f)
    elif mode == BlendMode.SOFT_LIGHT:
        if fg_f < 0.5:
            return bg_f - (1 - 2 * fg_f) * bg_f * (1 - bg_f)
        d = bg_f if bg_f < 0.25 else 1 - (1 - bg_f)
        return bg_f + (2 * fg_f - 1) * d
    elif mode == BlendMode.HARD_LIGHT:
        if fg_f < 0.5:
            return 2 * fg_f * bg_f
        return 1 - 2 * (1 - fg_f) * (1 - bg_f)
    elif mode == BlendMode.COLOR_DODGE:
        if fg_f >= 1:
            return 1.0
        return min(1.0, bg_f / (1 - fg_f))
    elif mode == BlendMode.COLOR_BURN:
        if fg_f <= 0:
            return 0.0
        return max(0.0, 1 - (1 - bg_f) / fg_f)
    elif mode == BlendMode.DIFFERENCE:
        return abs(fg_f - bg_f)
    elif mode == BlendMode.EXCLUSION:
        return fg_f + bg_f - 2 * fg_f * bg_f
    else:
        return fg_f


def alpha_composite(
    fg: Tuple[int, int, int, float],
    bg: Tuple[int, int, int, float],
) -> Tuple[int, int, int, float]:
    """Alpha composite two RGBA pixels.
    
    Args:
        fg: Foreground RGBA
        bg: Background RGBA
    
    Returns:
        Composited RGBA
    """
    rf, gf, bf, af = fg
    rb, gb, bb, ab = bg
    
    out_alpha = af + ab * (1 - af)
    
    if out_alpha == 0:
        return (0, 0, 0, 0)
    
    out_r = (rf * af + rb * ab * (1 - af)) / out_alpha
    out_g = (gf * af + gb * ab * (1 - af)) / out_alpha
    out_b = (bf * af + bb * ab * (1 - af)) / out_alpha
    
    return (int(out_r), int(out_g), int(out_b), out_alpha)


def gradient_blend(
    color1: Tuple[int, int, int],
    color2: Tuple[int, int, int],
    positions: List[float],
) -> List[Tuple[int, int, int]]:
    """Create gradient between two colors.
    
    Args:
        color1: Start color RGB
        color2: End color RGB
        positions: List of positions (0.0 to 1.0)
    
    Returns:
        List of blended colors
    """
    result = []
    
    for pos in positions:
        pos = max(0.0, min(1.0, pos))
        t = pos
        
        r = int(color1[0] * (1 - t) + color2[0] * t)
        g = int(color1[1] * (1 - t) + color2[1] * t)
        b = int(color1[2] * (1 - t) + color2[2] * t)
        
        result.append((r, g, b))
    
    return result


def linear_gradient(
    start: Tuple[int, int],
    end: Tuple[int, int],
    color1: Tuple[int, int, int],
    color2: Tuple[int, int, int],
    width: int,
    height: int,
) -> List[List[Tuple[int, int, int]]]:
    """Create linear gradient image.
    
    Args:
        start: Start point (x, y)
        end: End point (x, y)
        color1: Start color RGB
        color2: End color RGB
        width: Image width
        height: Image height
    
    Returns:
        2D list of pixel colors
    """
    from math import sqrt
    
    dx = end[0] - start[0]
    dy = end[1] - start[1]
    length = sqrt(dx * dx + dy * dy)
    
    if length == 0:
        return [[color1 for _ in range(width)] for _ in range(height)]
    
    image = []
    
    for y in range(height):
        row = []
        for x in range(width):
            px = x - start[0]
            py = y - start[1]
            
            t = max(0.0, min(1.0, (px * dx + py * dy) / (length * length)))
            
            r = int(color1[0] * (1 - t) + color2[0] * t)
            g = int(color1[1] * (1 - t) + color2[1] * t)
            b = int(color1[2] * (1 - t) + color2[2] * t)
            
            row.append((r, g, b))
        
        image.append(row)
    
    return image


def radial_gradient(
    center: Tuple[int, int],
    radius: float,
    color1: Tuple[int, int, int],
    color2: Tuple[int, int, int],
    width: int,
    height: int,
) -> List[List[Tuple[int, int, int]]]:
    """Create radial gradient image.
    
    Args:
        center: Center point (x, y)
        radius: Gradient radius
        color1: Center color RGB
        color2: Edge color RGB
        width: Image width
        height: Image height
    
    Returns:
        2D list of pixel colors
    """
    from math import sqrt
    
    image = []
    
    for y in range(height):
        row = []
        for x in range(width):
            dx = x - center[0]
            dy = y - center[1]
            dist = sqrt(dx * dx + dy * dy)
            
            t = max(0.0, min(1.0, dist / radius))
            
            r = int(color1[0] * (1 - t) + color2[0] * t)
            g = int(color1[1] * (1 - t) + color2[1] * t)
            b = int(color1[2] * (1 - t) + color2[2] * t)
            
            row.append((r, g, b))
        
        image.append(row)
    
    return image


def apply_opacity(
    rgb: Tuple[int, int, int],
    alpha: float,
    background: Optional[Tuple[int, int, int]] = (255, 255, 255),
) -> Tuple[int, int, int]:
    """Apply opacity to RGB color with background.
    
    Args:
        rgb: RGB color
        alpha: Opacity (0.0 to 1.0)
        background: Background color for blending
    
    Returns:
        Blended RGB
    """
    if background is None:
        background = (255, 255, 255)
    
    result = []
    for cf, cb in zip(rgb, background):
        result.append(int(cf * alpha + cb * (1 - alpha)))
    
    return tuple(result)


def multiply_blend(
    top: Tuple[int, int, int],
    bottom: Tuple[int, int, int],
) -> Tuple[int, int, int]:
    """Multiply blend two RGB colors.
    
    Args:
        top: Top layer RGB
        bottom: Bottom layer RGB
    
    Returns:
        Blended RGB
    """
    return tuple(int((t * b) / 255) for t, b in zip(top, bottom))


def screen_blend(
    top: Tuple[int, int, int],
    bottom: Tuple[int, int, int],
) -> Tuple[int, int, int]:
    """Screen blend two RGB colors.
    
    Args:
        top: Top layer RGB
        bottom: Bottom layer RGB
    
    Returns:
        Blended RGB
    """
    return tuple(
        int(255 - (255 - t) * (255 - b) / 255) 
        for t, b in zip(top, bottom)
    )


def overlay_blend(
    top: Tuple[int, int, int],
    bottom: Tuple[int, int, int],
) -> Tuple[int, int, int]:
    """Overlay blend two RGB colors.
    
    Args:
        top: Top layer RGB
        bottom: Bottom layer RGB
    
    Returns:
        Blended RGB
    """
    result = []
    for t, b in zip(top, bottom):
        t_f = t / 255.0
        b_f = b / 255.0
        
        if b_f < 0.5:
            val = 2 * t_f * b_f
        else:
            val = 1 - 2 * (1 - t_f) * (1 - b_f)
        
        result.append(int(val * 255))
    
    return tuple(result)


def create_vignette(
    width: int,
    height: int,
    center: Optional[Tuple[int, int]] = None,
    strength: float = 0.5,
) -> List[List[float]]:
    """Create vignette opacity map.
    
    Args:
        width: Image width
        height: Image height
        center: Vignette center (defaults to image center)
        strength: Vignette strength (0.0 to 1.0)
    
    Returns:
        2D list of opacity values
    """
    if center is None:
        cx, cy = width / 2, height / 2
    else:
        cx, cy = center
    
    max_dist = ((width / 2) ** 2 + (height / 2) ** 2) ** 0.5
    
    vignette = []
    
    for y in range(height):
        row = []
        for x in range(width):
            dx = x - cx
            dy = y - cy
            dist = (dx * dx + dy * dy) ** 0.5
            
            t = min(1.0, dist / max_dist)
            opacity = strength * (t * t)
            
            row.append(opacity)
        
        vignette.append(row)
    
    return vignette


def apply_vignette(
    image: List[List[Tuple[int, int, int]]],
    strength: float = 0.5,
    color: Optional[Tuple[int, int, int]] = None,
) -> List[List[Tuple[int, int, int]]]:
    """Apply vignette effect to image.
    
    Args:
        image: 2D image RGB list
        strength: Vignette strength
        color: Vignette color (defaults to black)
    
    Returns:
        Image with vignette applied
    """
    if color is None:
        color = (0, 0, 0)
    
    height = len(image)
    width = len(image[0]) if height > 0 else 0
    
    vignette_map = create_vignette(width, height, strength=strength)
    
    result = []
    
    for y in range(height):
        row = []
        for x in range(width):
            pixel = image[y][x]
            opacity = vignette_map[y][x]
            
            blended = []
            for cf, cv in zip(pixel, color):
                blended.append(int(cf * (1 - opacity) + cv * opacity))
            
            row.append(tuple(blended))
        
        result.append(row)
    
    return result


def color_dodge(fg: Tuple[int, int, int], bg: Tuple[int, int, int]) -> Tuple[int, int, int]:
    """Color dodge blend.
    
    Args:
        fg: Foreground RGB
        bg: Background RGB
    
    Returns:
        Blended RGB
    """
    return tuple(
        int(255 if f == 255 else min(255, b * 255 // (255 - f)))
        if f != 255 else 255
        for f, b in zip(fg, bg)
    )


def color_burn(fg: Tuple[int, int, int], bg: Tuple[int, int, int]) -> Tuple[int, int, int]:
    """Color burn blend.
    
    Args:
        fg: Foreground RGB
        bg: Background RGB
    
    Returns:
        Blended RGB
    """
    return tuple(
        int(0 if f == 0 else max(0, 255 - (255 - b) * 255 // f))
        if f != 0 else 0
        for f, b in zip(fg, bg)
    )


def difference_blend(fg: Tuple[int, int, int], bg: Tuple[int, int, int]) -> Tuple[int, int, int]:
    """Difference blend.
    
    Args:
        fg: Foreground RGB
        bg: Background RGB
    
    Returns:
        Blended RGB
    """
    return tuple(abs(f - b) for f, b in zip(fg, bg))


def exclusion_blend(fg: Tuple[int, int, int], bg: Tuple[int, int, int]) -> Tuple[int, int, int]:
    """Exclusion blend.
    
    Args:
        fg: Foreground RGB
        bg: Background RGB
    
    Returns:
        Blended RGB
    """
    return tuple(f + b - 2 * f * b // 255 for f, b in zip(fg, bg))
