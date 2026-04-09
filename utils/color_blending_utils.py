"""Color blending utilities for RabAI AutoClick.

Provides:
- Color blending modes (normal, multiply, screen, overlay, etc.)
- Color space conversions
- Color manipulation utilities
"""

from typing import Tuple, List, Optional
import math


RGB = Tuple[int, int, int]
RGBA = Tuple[int, int, int, int]
HSL = Tuple[float, float, float]
HSV = Tuple[float, float, float]


def rgb_to_hsv(r: int, g: int, b: int) -> HSV:
    """Convert RGB to HSV."""
    rf, gf, bf = r / 255.0, g / 255.0, b / 255.0
    max_c = max(rf, gf, bf)
    min_c = min(rf, gf, bf)
    delta = max_c - min_c

    if delta == 0:
        h = 0.0
    elif max_c == rf:
        h = 60 * (((gf - bf) / delta) % 6)
    elif max_c == gf:
        h = 60 * (((bf - rf) / delta) + 2)
    else:
        h = 60 * (((rf - gf) / delta) + 4)

    s = 0.0 if max_c == 0 else delta / max_c
    v = max_c
    return (h, s, v)


def hsv_to_rgb(h: float, s: float, v: float) -> RGB:
    """Convert HSV to RGB."""
    c = v * s
    x = c * (1 - abs((h / 60) % 2 - 1))
    m = v - c

    rf, gf, bf = 0.0, 0.0, 0.0
    if 0 <= h < 60:
        rf, gf, bf = c, x, 0
    elif 60 <= h < 120:
        rf, gf, bf = x, c, 0
    elif 120 <= h < 180:
        rf, gf, bf = 0, c, x
    elif 180 <= h < 240:
        rf, gf, bf = 0, x, c
    elif 240 <= h < 300:
        rf, gf, bf = x, 0, c
    else:
        rf, gf, bf = c, 0, x

    return (
        int((rf + m) * 255 + 0.5),
        int((gf + m) * 255 + 0.5),
        int((bf + m) * 255 + 0.5),
    )


def rgb_to_hsl(r: int, g: int, b: int) -> HSL:
    """Convert RGB to HSL."""
    rf, gf, bf = r / 255.0, g / 255.0, b / 255.0
    max_c = max(rf, gf, bf)
    min_c = min(rf, gf, bf)
    l = (max_c + min_c) / 2

    if max_c == min_c:
        return (0.0, 0.0, l)

    delta = max_c - min_c
    s = delta / (1 - abs(2 * l - 1))

    if max_c == rf:
        h = 60 * (((gf - bf) / delta) % 6)
    elif max_c == gf:
        h = 60 * (((bf - rf) / delta) + 2)
    else:
        h = 60 * (((rf - gf) / delta) + 4)

    return (h, s, l)


def blend_normal(base: RGB, blend: RGB, opacity: float = 1.0) -> RGB:
    """Normal blend mode."""
    a = opacity
    return (
        int(base[0] * (1 - a) + blend[0] * a),
        int(base[1] * (1 - a) + blend[1] * a),
        int(base[2] * (1 - a) + blend[2] * a),
    )


def blend_multiply(base: RGB, blend: RGB) -> RGB:
    """Multiply blend mode."""
    return (
        int(base[0] * blend[0] / 255),
        int(base[1] * blend[1] / 255),
        int(base[2] * blend[2] / 255),
    )


def blend_screen(base: RGB, blend: RGB) -> RGB:
    """Screen blend mode."""
    return (
        int(255 - (255 - base[0]) * (255 - blend[0]) / 255),
        int(255 - (255 - base[1]) * (255 - blend[1]) / 255),
        int(255 - (255 - base[2]) * (255 - blend[2]) / 255),
    )


def blend_overlay(base: RGB, blend: RGB) -> RGB:
    """Overlay blend mode."""
    def channel(b: int, c: int) -> int:
        if b < 128:
            return int(2 * b * c / 255)
        else:
            return int(255 - 2 * (255 - b) * (255 - c) / 255)

    return (channel(base[0], blend[0]), channel(base[1], blend[1]), channel(base[2], blend[2]))


def blend_soft_light(base: RGB, blend: RGB) -> RGB:
    """Soft light blend mode."""
    def channel(b: int, c: int) -> int:
        if c < 128:
            return int(b - (255 - 2 * c) * b * (255 - b) / (255 * 255))
        else:
            d = b
            if d < 64:
                val = int((16 * d / 255 - 12) * d / 255 + d * 4)
            else:
                val = int(math.sqrt(d / 255) * 255)
            return int(b + (2 * c - 255) * (val - b) / 255)

    return (channel(base[0], blend[0]), channel(base[1], blend[1]), channel(base[2], blend[2]))


def blend_hard_light(base: RGB, blend: RGB) -> RGB:
    """Hard light blend mode."""
    def channel(b: int, c: int) -> int:
        if c < 128:
            return int(2 * b * c / 255)
        else:
            return int(255 - 2 * (255 - b) * (255 - c) / 255)

    return (channel(base[0], blend[0]), channel(base[1], blend[1]), channel(base[2], blend[2]))


def blend_color_dodge(base: RGB, blend: RGB) -> RGB:
    """Color dodge blend mode."""
    def channel(b: int, c: int) -> int:
        if b >= 255:
            return 255
        return min(255, int(b * 255 / (255 - c))) if c < 255 else 255

    return (channel(base[0], blend[0]), channel(base[1], blend[1]), channel(base[2], blend[2]))


def blend_color_burn(base: RGB, blend: RGB) -> RGB:
    """Color burn blend mode."""
    def channel(b: int, c: int) -> int:
        if b <= 0:
            return 0
        return max(0, int(255 - (255 - b) * 255 / c)) if c > 0 else 0

    return (channel(base[0], blend[0]), channel(base[1], blend[1]), channel(base[2], blend[2]))


def blend_difference(base: RGB, blend: RGB) -> RGB:
    """Difference blend mode."""
    return (
        abs(base[0] - blend[0]),
        abs(base[1] - blend[1]),
        abs(base[2] - blend[2]),
    )


def blend_exclusion(base: RGB, blend: RGB) -> RGB:
    """Exclusion blend mode."""
    return (
        int((base[0] + blend[0]) / 2 - base[0] * blend[0] / 128),
        int((base[1] + blend[1]) / 2 - base[1] * blend[1] / 128),
        int((base[2] + blend[2]) / 2 - base[2] * blend[2] / 128),
    )


def blend_additive(base: RGB, blend: RGB, opacity: float = 1.0) -> RGB:
    """Additive blend (lighten)."""
    a = opacity
    return (
        min(255, int(base[0] + blend[0] * a)),
        min(255, int(base[1] + blend[1] * a)),
        min(255, int(base[2] + blend[2] * a)),
    )


BLEND_MODES: dict = {
    "normal": blend_normal,
    "multiply": blend_multiply,
    "screen": blend_screen,
    "overlay": blend_overlay,
    "soft_light": blend_soft_light,
    "hard_light": blend_hard_light,
    "color_dodge": blend_color_dodge,
    "color_burn": blend_color_burn,
    "difference": blend_difference,
    "exclusion": blend_exclusion,
    "additive": blend_additive,
}


def blend_images(
    base: List[List[RGB]],
    overlay: List[List[RGB]],
    mode: str = "normal",
    opacity: float = 1.0,
) -> List[List[RGB]]:
    """Blend two images pixel by pixel.

    Args:
        base: Base image as 2D RGB grid.
        overlay: Overlay image.
        mode: Blend mode name.
        opacity: Opacity of overlay.

    Returns:
        Blended image.
    """
    blend_fn = BLEND_MODES.get(mode, blend_normal)
    if not base or not overlay:
        return base[:] if base else overlay[:] if overlay else []

    h = min(len(base), len(overlay))
    w = min(len(base[0]), len(overlay[0])) if h > 0 else 0

    result: List[List[RGB]] = []
    for y in range(h):
        row: List[RGB] = []
        for x in range(w):
            row.append(blend_fn(base[y][x], overlay[y][x], opacity))
        result.append(row)

    return result


def lerp_color(c1: RGB, c2: RGB, t: float) -> RGB:
    """Linear interpolate between two colors."""
    return (
        int(c1[0] + (c2[0] - c1[0]) * t),
        int(c1[1] + (c2[1] - c1[1]) * t),
        int(c1[2] + (c2[2] - c1[2]) * t),
    )


def adjust_brightness(color: RGB, factor: float) -> RGB:
    """Adjust color brightness."""
    return (
        min(255, max(0, int(color[0] * factor))),
        min(255, max(0, int(color[1] * factor))),
        min(255, max(0, int(color[2] * factor))),
    )


def adjust_contrast(color: RGB, factor: float) -> RGB:
    """Adjust color contrast."""
    return (
        min(255, max(0, int(128 + (color[0] - 128) * factor))),
        min(255, max(0, int(128 + (color[1] - 128) * factor))),
        min(255, max(0, int(128 + (color[2] - 128) * factor))),
    )


def adjust_saturation(color: RGB, factor: float) -> RGB:
    """Adjust color saturation."""
    h, s, v = rgb_to_hsv(*color)
    s = max(0.0, min(1.0, s * factor))
    return hsv_to_rgb(h, s, v)


def grayscale(color: RGB) -> RGB:
    """Convert color to grayscale (luminance)."""
    gray = int(0.299 * color[0] + 0.587 * color[1] + 0.114 * color[2])
    return (gray, gray, gray)


def invert_color(color: RGB) -> RGB:
    """Invert color."""
    return (255 - color[0], 255 - color[1], 255 - color[2])


def color_distance(c1: RGB, c2: RGB) -> float:
    """Compute Euclidean distance between two colors."""
    dr = c1[0] - c2[0]
    dg = c1[1] - c2[1]
    db = c1[2] - c2[2]
    return math.sqrt(dr * dr + dg * dg + db * db)
