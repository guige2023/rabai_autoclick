"""
Color conversion and manipulation utilities.

Provides:
- RGB/HEX/HSV/HSL/CMYK conversion
- Color distance (Delta E, CIE76, CIE94)
- Color palette generation
- Color blending/interpolation
- Color naming and parsing
"""

from __future__ import annotations

import math
import random
from dataclasses import dataclass
from typing import Iterator


@dataclass(frozen=True, slots=True)
class RGB:
    """Red-Green-Blue color representation."""

    red: float  # 0-255
    green: float  # 0-255
    blue: float  # 0-255
    alpha: float = 255.0  # 0-255

    def __post_init__(self) -> None:
        for name in ("red", "green", "blue", "alpha"):
            val = getattr(self, name)
            if not 0 <= val <= 255:
                raise ValueError(f"{name} must be in [0, 255], got {val}")

    def to_hex(self, include_alpha: bool = False) -> str:
        """Convert to HEX string."""
        if include_alpha and self.alpha < 255:
            return f"#{self.red:02x}{self.green:02x}{self.blue:02x}{self.alpha:02x}"
        return f"#{self.red:02x}{self.green:02x}{self.blue:02x}"

    def to_hsv(self) -> HSV:
        """Convert to HSV."""
        r, g, b = self.red / 255, self.green / 255, self.blue / 255
        max_c = max(r, g, b)
        min_c = min(r, g, b)
        delta = max_c - min_c

        if delta == 0:
            hue = 0.0
        elif max_c == r:
            hue = 60 * (((g - b) / delta) % 6)
        elif max_c == g:
            hue = 60 * ((b - r) / delta + 2)
        else:
            hue = 60 * ((r - g) / delta + 4)

        saturation = 0 if max_c == 0 else delta / max_c
        value = max_c

        return HSV(hue % 360, saturation, value)

    def to_hsl(self) -> HSL:
        """Convert to HSL."""
        r, g, b = self.red / 255, self.green / 255, self.blue / 255
        max_c = max(r, g, b)
        min_c = min(r, g, b)
        delta = max_c - min_c

        lightness = (max_c + min_c) / 2

        if delta == 0:
            hue = 0.0
            saturation = 0.0
        else:
            saturation = delta / (1 - abs(2 * lightness - 1))

            if max_c == r:
                hue = 60 * (((g - b) / delta) % 6)
            elif max_c == g:
                hue = 60 * ((b - r) / delta + 2)
            else:
                hue = 60 * ((r - g) / delta + 4)

        return HSL(hue % 360, saturation, lightness)

    def to_cmyk(self) -> CMYK:
        """Convert to CMYK."""
        r, g, b = self.red / 255, self.green / 255, self.blue / 255
        k = 1 - max(r, g, b)

        if k == 1:
            return CMYK(0, 0, 0, 1)

        c = (1 - r - k) / (1 - k)
        m = (1 - g - k) / (1 - k)
        y = (1 - b - k) / (1 - k)

        return CMYK(c, m, y, k)

    def to_tuple(self, include_alpha: bool = False) -> tuple[int, int, int] | tuple[int, int, int, int]:
        """Convert to tuple."""
        if include_alpha:
            return (int(self.red), int(self.green), int(self.blue), int(self.alpha))
        return (int(self.red), int(self.green), int(self.blue))

    @classmethod
    def from_hex(cls, hex_color: str) -> RGB:
        """Parse HEX string to RGB."""
        hex_color = hex_color.lstrip("#")
        if len(hex_color) == 6:
            r = int(hex_color[0:2], 16)
            g = int(hex_color[2:4], 16)
            b = int(hex_color[4:6], 16)
            return cls(r, g, b)
        elif len(hex_color) == 8:
            r = int(hex_color[0:2], 16)
            g = int(hex_color[2:4], 16)
            b = int(hex_color[4:6], 16)
            a = int(hex_color[6:8], 16)
            return cls(r, g, b, a)
        raise ValueError(f"Invalid HEX color: {hex_color}")


@dataclass(frozen=True, slots=True)
class HSV:
    """Hue-Saturation-Value color representation."""

    hue: float  # 0-360
    saturation: float  # 0-1
    value: float  # 0-1

    def __post_init__(self) -> None:
        if not 0 <= self.hue < 360:
            raise ValueError(f"Hue must be in [0, 360), got {self.hue}")
        if not 0 <= self.saturation <= 1:
            raise ValueError(f"Saturation must be in [0, 1], got {self.saturation}")
        if not 0 <= self.value <= 1:
            raise ValueError(f"Value must be in [0, 1], got {self.value}")

    def to_rgb(self) -> RGB:
        """Convert to RGB."""
        h = self.hue
        s = self.saturation
        v = self.value

        if s == 0:
            val = int(v * 255)
            return RGB(val, val, val)

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

        return RGB(int((r + m) * 255), int((g + m) * 255), int((b + m) * 255))

    def to_hex(self) -> str:
        """Convert to HEX."""
        return self.to_rgb().to_hex()


@dataclass(frozen=True, slots=True)
class HSL:
    """Hue-Saturation-Lightness color representation."""

    hue: float  # 0-360
    saturation: float  # 0-1
    lightness: float  # 0-1

    def __post_init__(self) -> None:
        if not 0 <= self.hue < 360:
            raise ValueError(f"Hue must be in [0, 360), got {self.hue}")
        if not 0 <= self.saturation <= 1:
            raise ValueError(f"Saturation must be in [0, 1], got {self.saturation}")
        if not 0 <= self.lightness <= 1:
            raise ValueError(f"Lightness must be in [0, 1], got {self.lightness}")

    def to_rgb(self) -> RGB:
        """Convert to RGB."""
        h = self.hue
        s = self.saturation
        l = self.lightness

        if s == 0:
            val = int(l * 255)
            return RGB(val, val, val)

        c = (1 - abs(2 * l - 1)) * s
        x = c * (1 - abs((h / 60) % 2 - 1))
        m = l - c / 2

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

        return RGB(int((r + m) * 255), int((g + m) * 255), int((b + m) * 255))


@dataclass(frozen=True, slots=True)
class CMYK:
    """Cyan-Magenta-Yellow-Key color representation."""

    cyan: float  # 0-1
    magenta: float  # 0-1
    yellow: float  # 0-1
    key: float  # 0-1

    def to_rgb(self) -> RGB:
        """Convert to RGB."""
        r = int((1 - self.cyan) * (1 - self.key) * 255)
        g = int((1 - self.magenta) * (1 - self.key) * 255)
        b = int((1 - self.yellow) * (1 - self.key) * 255)
        return RGB(r, g, b)


def color_distance_CIE76(c1: RGB, c2: RGB) -> float:
    """
    Calculate color distance using CIE76 formula.

    Args:
        c1: First color
        c2: Second color

    Returns:
        Delta E value (lower = more similar)
    """
    lab1 = _rgb_to_lab(c1)
    lab2 = _rgb_to_lab(c2)

    return math.sqrt(sum((a - b) ** 2 for a, b in zip(lab1, lab2)))


def color_distance_CIE94(c1: RGB, c2: RGB) -> float:
    """
    Calculate color distance using CIE94 formula.

    Args:
        c1: First color
        c2: Second color

    Returns:
        Delta E value
    """
    lab1 = _rgb_to_lab(c1)
    lab2 = _rgb_to_lab(c2)

    L1, a1, b1 = lab1
    L2, a2, b2 = lab2

    delta_L = L1 - L2
    delta_a = a1 - a2
    delta_b = b1 - b2

    C1 = math.sqrt(a1**2 + b1**2)
    C2 = math.sqrt(a2**2 + b2**2)
    delta_C = C1 - C2
    delta_H_sq = delta_a**2 + delta_b**2 - delta_C**2

    k_L = 1
    k_C = 1
    k_H = 1
    K1 = 0.045
    K2 = 0.015

    S_L = 1
    S_C = 1 + K1 * C1
    S_H = 1 + K2 * C1

    L_term = (delta_L / (k_L * S_L)) ** 2
    C_term = (delta_C / (k_C * S_C)) ** 2
    H_term = (delta_H_sq / (k_H * S_H)) ** 2 if delta_H_sq > 0 else 0

    return math.sqrt(L_term + C_term + H_term)


def _rgb_to_lab(rgb: RGB) -> tuple[float, float, float]:
    """Convert RGB to LAB color space."""
    r, g, b = rgb.red / 255, rgb.green / 255, rgb.blue / 255

    def f(t: float) -> float:
        if t > 0.008856:
            return t ** (1 / 3)
        return 7.787 * t + 16 / 116

    r = f(r)
    g = f(g)
    b = f(b)

    X = 0.4124 * r + 0.3576 * g + 0.1805 * b
    Y = 0.2126 * r + 0.7152 * g + 0.0722 * b
    Z = 0.0193 * r + 0.1192 * g + 0.9505 * b

    X, Y, Z = X * 100, Y * 100, Z * 100

    X_ref, Y_ref, Z_ref = 95.047, 100.0, 108.883

    x = X / X_ref
    y = Y / Y_ref
    z = Z / Z_ref

    fx = f(x)
    fy = f(y)
    fz = f(z)

    L = 116 * fy - 16
    a = 500 * (fx - fy)
    b_lab = 200 * (fy - fz)

    return (L, a, b_lab)


def blend_colors(c1: RGB, c2: RGB, factor: float = 0.5) -> RGB:
    """
    Blend two colors.

    Args:
        c1: First color
        c2: Second color
        factor: Blend factor (0 = c1, 1 = c2)

    Returns:
        Blended RGB color
    """
    factor = max(0, min(1, factor))
    return RGB(
        int(c1.red + (c2.red - c1.red) * factor),
        int(c1.green + (c2.green - c1.green) * factor),
        int(c1.blue + (c2.blue - c1.blue) * factor),
        int(c1.alpha + (c2.alpha - c1.alpha) * factor),
    )


def generate_palette(base_color: RGB, count: int = 5, palette_type: str = "analogous") -> list[RGB]:
    """
    Generate a color palette from a base color.

    Args:
        base_color: Starting color
        count: Number of colors to generate
        palette_type: Type of palette - 'analogous', 'complementary', 'triadic', 'split', 'monochromatic'

    Returns:
        List of RGB colors
    """
    hsv = base_color.to_hsv()
    palette = [base_color]

    if palette_type == "analogous":
        step = 30
        for i in range(1, count):
            new_hue = (hsv.hue + i * step) % 360
            palette.append(HSV(new_hue, hsv.saturation, hsv.value).to_rgb())

    elif palette_type == "complementary":
        for i in range(1, count):
            new_hue = (hsv.hue + 180) % 360
            palette.append(HSV(new_hue, hsv.saturation if i % 2 else hsv.saturation * 0.5, hsv.value).to_rgb())

    elif palette_type == "triadic":
        hues = [(hsv.hue + i * 120) % 360 for i in range(3)]
        for hue in hues[1:]:
            palette.append(HSV(hue, hsv.saturation, hsv.value).to_rgb())

    elif palette_type == "split":
        split_angles = [150, 210]
        for angle in split_angles:
            palette.append(HSV((hsv.hue + angle) % 360, hsv.saturation, hsv.value).to_rgb())

    elif palette_type == "monochromatic":
        for i in range(1, count):
            new_value = max(0.1, hsv.value - i * 0.15)
            palette.append(HSV(hsv.hue, hsv.saturation, new_value).to_rgb())

    return palette[:count]


def lerp_color(c1: RGB, c2: RGB, t: float) -> RGB:
    """Linear interpolation between two colors."""
    return blend_colors(c1, c2, t)


def random_color(saturation_range: tuple[float, float] = (0.5, 1.0), lightness_range: tuple[float, float] = (0.3, 0.7)) -> RGB:
    """Generate a random vibrant color."""
    h = random.uniform(0, 360)
    s = random.uniform(*saturation_range)
    l = random.uniform(*lightness_range)
    return HSL(h, s, l).to_rgb()


def adjust_brightness(color: RGB, factor: float) -> RGB:
    """Adjust color brightness by a factor."""
    hsv = color.to_hsv()
    return HSV(hsv.hue, hsv.saturation, max(0, min(1, hsv.value * factor))).to_rgb()


def adjust_saturation(color: RGB, factor: float) -> RGB:
    """Adjust color saturation by a factor."""
    hsv = color.to_hsv()
    return HSV(hsv.hue, max(0, min(1, hsv.saturation * factor)), hsv.value).to_rgb()


def desaturate(color: RGB, amount: float = 0.5) -> RGB:
    """Desaturate a color by blending with gray."""
    gray = RGB(128, 128, 128)
    return blend_colors(color, gray, amount)


def invert_color(color: RGB) -> RGB:
    """Invert a color."""
    return RGB(255 - int(color.red), 255 - int(color.green), 255 - int(color.blue), color.alpha)


def get_contrast_color(color: RGB) -> RGB:
    """Get a high-contrast text color (black or white) for a background."""
    luminance = (0.299 * color.red + 0.587 * color.green + 0.114 * color.blue) / 255
    return RGB(0, 0, 0) if luminance > 0.5 else RGB(255, 255, 255)


def color_range(start: RGB, end: RGB, steps: int) -> Iterator[RGB]:
    """Generate a range of colors between two colors."""
    for i in range(steps):
        t = i / max(1, steps - 1)
        yield blend_colors(start, end, t)


WEB_SAFE_COLORS = [
    RGB(255, 255, 255),
    RGB(255, 0, 0),
    RGB(0, 255, 0),
    RGB(0, 0, 255),
    RGB(255, 255, 0),
    RGB(0, 255, 255),
    RGB(255, 0, 255),
    RGB(192, 192, 192),
    RGB(128, 128, 128),
    RGB(128, 0, 0),
    RGB(128, 128, 0),
    RGB(0, 128, 0),
    RGB(128, 0, 128),
    RGB(0, 128, 128),
    RGB(0, 0, 128),
    RGB(255, 128, 0),
]


def nearest_web_safe_color(color: RGB) -> RGB:
    """Find the nearest web-safe color."""
    return min(WEB_SAFE_COLORS, key=lambda c: color_distance_CIE94(c, color))
