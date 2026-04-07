"""
Color conversion utilities between RGB, HSV, HSL, HEX, and more.
"""

import colorsys
from typing import Tuple, Union, Optional, List, Dict
from dataclasses import dataclass


RGB = Tuple[int, int, int]
HSV = Tuple[float, float, float]
HSL = Tuple[float, float, float]
ColorInput = Union[RGB, str]


@dataclass(frozen=True)
class Color:
    """Immutable color container with multiple format accessors."""
    r: int
    g: int
    b: int
    a: int = 255

    def __post_init__(self) -> None:
        for val in (self.r, self.g, self.b, self.a):
            if not 0 <= val <= 255:
                raise ValueError(f\"Color values must be in [0, 255], got {val}\")

    @property
    def rgb(self) -> RGB:
        return (self.r, self.g, self.b)

    @property
    def hex(self) -> str:
        return f\"#{self.r:02x}{self.g:02x}{self.b:02x}\"

    @property
    def hsv(self) -> HSV:
        r, g, b = self.r / 255, self.g / 255, self.b / 255
        h, s, v = colorsys.rgb_to_hsv(r, g, b)
        return (round(h, 6), round(s, 6), round(v, 6))

    @property
    def hsl(self) -> HSL:
        r, g, b = self.r / 255, self.g / 255, self.b / 255
        h, l, s = colorsys.rgb_to_hls(r, g, b)
        return (round(h, 6), round(s, 6), round(l, 6))

    def brightness(self) -> float:
        \"\"\"Perceived brightness [0, 255].\"\"\"\n        return 0.299 * self.r + 0.587 * self.g + 0.114 * self.b

    def is_dark(self, threshold: float = 127.5) -> bool:
        return self.brightness() < threshold

    def is_light(self, threshold: float = 127.5) -> bool:
        return self.brightness() >= threshold

    def contrast_color(self) -> \"Color\":
        \"\"\"Return black or white depending on which contrasts better.\"\"\"\n        return Color(0, 0, 0) if self.is_dark() else Color(255, 255, 255)

    def blend_with(self, other: \"Color\", factor: float = 0.5) -> \"Color\":
        \"\"\"Blend this color with another.\"\"\"\n        f = max(0, min(1, factor))\n        return Color(\n            r=int(self.r + (other.r - self.r) * f),\n            g=int(self.g + (other.g - self.g) * f),\n            b=int(self.b + (other.b - self.b) * f),\n            a=int(self.a + (other.a - self.a) * f),\n        )

    def to_dict(self) -> Dict[str, any]:
        return {\"rgb\": self.rgb, \"hex\": self.hex, \"hsv\": self.hsv, \"hsl\": self.hsl}

def parse_color(color: ColorInput) -> Color:
    """Parse various color formats into a Color object."""
    if isinstance(color, Color):
        return color
    if isinstance(color, tuple):
        return Color(color[0], color[1], color[2])
    if isinstance(color, str):
        color_str = color.strip().lstrip("#")
        if len(color_str) in (6, 3):
            if len(color_str) == 3:
                color_str = "".join(c * 2 for c in color_str)
            r = int(color_str[0:2], 16)
            g = int(color_str[2:4], 16)
            b = int(color_str[4:6], 16)
            return Color(r, g, b)
    raise ValueError(f"Cannot parse color: {color!r}")


def lighten(color: ColorInput, amount: float = 0.2) -> Color:
    """Lighten a color by blending with white."""
    c = parse_color(color)
    return c.blend_with(Color(255, 255, 255), amount)


def darken(color: ColorInput, amount: float = 0.2) -> Color:
    """Darken a color by blending with black."""
    c = parse_color(color)
    return c.blend_with(Color(0, 0, 0), amount)


def saturate(color: ColorInput, amount: float = 0.2) -> Color:
    """Increase saturation of a color."""
    c = parse_color(color)
    h, s, v = c.hsv
    s = min(1.0, s * (1 + amount))
    r, g, b = _hsv_to_rgb(h, s, v)
    return Color(r, g, b, c.a)


def desaturate(color: ColorInput, amount: float = 0.2) -> Color:
    """Decrease saturation of a color."""
    c = parse_color(color)
    h, s, v = c.hsv
    s = max(0.0, s * (1 - amount))
    r, g, b = _hsv_to_rgb(h, s, v)
    return Color(r, g, b, c.a)


def rotate_hue(color: ColorInput, degrees: float) -> Color:
    """Rotate the hue of a color."""
    c = parse_color(color)
    h, s, v = c.hsv
    h = (h + degrees / 360) % 1.0
    r, g, b = _hsv_to_rgb(h, s, v)
    return Color(r, g, b, c.a)


def complementary(color: ColorInput) -> Color:
    """Get the complementary color (180 degree hue rotation)."""
    return rotate_hue(color, 180)


def rgb_to_hsv(r: int, g: int, b: int) -> HSV:
    """Convert RGB to HSV."""
    h, s, v = colorsys.rgb_to_hsv(r / 255, g / 255, b / 255)
    return (round(h, 6), round(s, 6), round(v, 6))


def _hsv_to_rgb(h: float, s: float, v: float) -> RGB:
    """Convert HSV to RGB."""
    r, g, b = colorsys.hsv_to_rgb(h, s, v)
    return (round(r * 255), round(g * 255), round(b * 255))


def hsv_to_rgb(h: float, s: float, v: float) -> RGB:
    """Convert HSV to RGB."""
    return _hsv_to_rgb(h, s, v)


def generate_palette(
    base_color: ColorInput,
    count: int = 5,
    palette_type: str = "analogous"
) -> List[Color]:
    """Generate a color palette from a base color."""
    c = parse_color(base_color)
    h, s, v = c.hsv
    palette: List[Color] = []

    if palette_type == "analogous":
        for i in range(count):
            offset = (i - count // 2) * 30
            new_h = (h + offset / 360) % 1.0
            r, g, b = _hsv_to_rgb(new_h, s, v)
            palette.append(Color(r, g, b, c.a))

    elif palette_type == "complementary":
        comp_h = (h + 0.5) % 1.0
        r, g, b = _hsv_to_rgb(comp_h, s, v)
        palette.append(Color(r, g, b, c.a))
        for i in range(1, count // 2 + 1):
            factor = i / (count // 2 + 1)
            palette.append(lighten(c, factor * 0.3))
            palette.append(darken(c, factor * 0.3))

    elif palette_type == "monochromatic":
        for i in range(count):
            new_v = min(1.0, max(0.0, v - 0.3 + i * 0.15))
            r, g, b = _hsv_to_rgb(h, s, new_v)
            palette.append(Color(r, g, b, c.a))

    return palette[:count]
