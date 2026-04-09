"""Color mixing and blending utilities for UI automation.

This module provides utilities for mixing colors, creating gradients,
and applying color transformations for UI visualization and feedback.
"""

from __future__ import annotations

from typing import Sequence
from dataclasses import dataclass
import math


@dataclass
class Color:
    """RGBA color representation.

    Attributes:
        r: Red component (0-255).
        g: Green component (0-255).
        b: Blue component (0-255).
        a: Alpha component (0-255, default 255).
    """
    r: int
    g: int
    b: int
    a: int = 255

    def __post_init__(self) -> None:
        """Clamp values to valid range."""
        self.r = max(0, min(255, self.r))
        self.g = max(0, min(255, self.g))
        self.b = max(0, min(255, self.b))
        self.a = max(0, min(255, self.a))

    @property
    def rgb(self) -> tuple[int, int, int]:
        """RGB tuple (without alpha)."""
        return (self.r, self.g, self.b)

    @property
    def rgba(self) -> tuple[int, int, int, int]:
        """RGBA tuple."""
        return (self.r, self.g, self.b, self.a)

    @property
    def hex(self) -> str:
        """Hex string representation (#RRGGBB)."""
        return f"#{self.r:02x}{self.g:02x}{self.b:02x}"

    @property
    def hex_alpha(self) -> str:
        """Hex string with alpha (#RRGGBBAA)."""
        return f"#{self.r:02x}{self.g:02x}{self.b:02x}{self.a:02x}"

    @property
    def luminance(self) -> float:
        """Compute relative luminance (0.0 to 1.0)."""
        def linearize(c: int) -> float:
            c_norm = c / 255.0
            if c_norm <= 0.03928:
                return c_norm / 12.92
            return math.pow((c_norm + 0.055) / 1.055, 2.4)

        r_lin = linearize(self.r)
        g_lin = linearize(self.g)
        b_lin = linearize(self.b)

        return 0.2126 * r_lin + 0.7152 * g_lin + 0.0722 * b_lin

    @property
    def is_dark(self) -> bool:
        """Check if color is considered dark."""
        return self.luminance < 0.5

    @property
    def is_light(self) -> bool:
        """Check if color is considered light."""
        return self.luminance >= 0.5

    def with_alpha(self, alpha: int) -> Color:
        """Create new color with different alpha.

        Args:
            alpha: New alpha value (0-255).

        Returns:
            New Color with modified alpha.
        """
        return Color(self.r, self.g, self.b, alpha)

    def to_grayscale(self) -> Color:
        """Convert to grayscale using luminance.

        Returns:
            Grayscale Color.
        """
        gray = int(0.299 * self.r + 0.587 * self.g + 0.114 * self.b)
        return Color(gray, gray, gray, self.a)

    def blend(self, other: Color, t: float) -> Color:
        """Blend with another color.

        Args:
            other: Color to blend with.
            t: Blend factor (0.0 = self, 1.0 = other).

        Returns:
            Blended Color.
        """
        t = max(0.0, min(1.0, t))
        return Color(
            r=int(self.r + (other.r - self.r) * t),
            g=int(self.g + (other.g - self.g) * t),
            b=int(self.b + (other.b - self.b) * t),
            a=int(self.a + (other.a - self.a) * t)
        )


def from_hex(hex_str: str) -> Color:
    """Parse color from hex string.

    Supports formats: #RGB, #RRGGBB, #RRGGBBAA

    Args:
        hex_str: Hex color string.

    Returns:
        Parsed Color.

    Raises:
        ValueError: If hex string is invalid.
    """
    hex_str = hex_str.lstrip("#")

    if len(hex_str) == 3:
        hex_str = "".join(c * 2 for c in hex_str)

    if len(hex_str) == 6:
        hex_str += "FF"

    if len(hex_str) != 8:
        raise ValueError(f"Invalid hex color: #{hex_str}")

    try:
        return Color(
            r=int(hex_str[0:2], 16),
            g=int(hex_str[2:4], 16),
            b=int(hex_str[4:6], 16),
            a=int(hex_str[6:8], 16)
        )
    except ValueError as e:
        raise ValueError(f"Invalid hex color: #{hex_str}") from e


def from_hsv(h: float, s: float, v: float, a: int = 255) -> Color:
    """Create color from HSV values.

    Args:
        h: Hue (0.0 to 360.0).
        s: Saturation (0.0 to 1.0).
        v: Value (0.0 to 1.0).
        a: Alpha (0-255).

    Returns:
        Color in RGBA.
    """
    h = h % 360.0
    s = max(0.0, min(1.0, s))
    v = max(0.0, min(1.0, v))

    c = v * s
    x = c * (1 - abs((h / 60.0) % 2 - 1))
    m = v - c

    if 0 <= h < 60:
        r, g, b = c, x, 0.0
    elif 60 <= h < 120:
        r, g, b = x, c, 0.0
    elif 120 <= h < 180:
        r, g, b = 0.0, c, x
    elif 180 <= h < 240:
        r, g, b = 0.0, x, c
    elif 240 <= h < 300:
        r, g, b = x, 0.0, c
    else:
        r, g, b = c, 0.0, x

    return Color(
        r=int((r + m) * 255),
        g=int((g + m) * 255),
        b=int((b + m) * 255),
        a=a
    )


def to_hsv(color: Color) -> tuple[float, float, float]:
    """Convert Color to HSV.

    Args:
        color: Color to convert.

    Returns:
        Tuple of (hue, saturation, value).
    """
    r = color.r / 255.0
    g = color.g / 255.0
    b = color.b / 255.0

    max_c = max(r, g, b)
    min_c = min(r, g, b)
    delta = max_c - min_c

    if delta == 0:
        h = 0.0
    elif max_c == r:
        h = 60.0 * (((g - b) / delta) % 6)
    elif max_c == g:
        h = 60.0 * (((b - r) / delta) + 2)
    else:
        h = 60.0 * (((r - g) / delta) + 4)

    s = 0.0 if max_c == 0 else delta / max_c
    v = max_c

    return (h, s, v)


def mix_colors(colors: Sequence[Color], weights: Sequence[float] | None = None) -> Color:
    """Mix multiple colors together.

    Args:
        colors: Sequence of colors to mix.
        weights: Optional weights for each color (must sum to 1).

    Returns:
        Mixed Color.
    """
    if not colors:
        return Color(0, 0, 0)

    if len(colors) == 1:
        return colors[0]

    if weights is None:
        weights = [1.0 / len(colors)] * len(colors)
    else:
        total = sum(weights)
        if total > 0:
            weights = [w / total for w in weights]
        else:
            weights = [1.0 / len(colors)] * len(colors)

    r = sum(c.r * w for c, w in zip(colors, weights))
    g = sum(c.g * w for c, w in zip(colors, weights))
    b = sum(c.b * w for c, w in zip(colors, weights))
    a = sum(c.a * w for c, w in zip(colors, weights))

    return Color(int(r), int(g), int(b), int(a))


def generate_gradient(
    start_color: Color,
    end_color: Color,
    steps: int
) -> list[Color]:
    """Generate a linear gradient between two colors.

    Args:
        start_color: Starting color.
        end_color: Ending color.
        steps: Number of color steps in gradient.

    Returns:
        List of Colors forming the gradient.
    """
    if steps < 2:
        return [start_color]

    gradient: list[Color] = []

    for i in range(steps):
        t = i / (steps - 1)
        gradient.append(start_color.blend(end_color, t))

    return gradient


def generate_radial_gradient(
    center_color: Color,
    edge_color: Color,
    radius: float
) -> list[tuple[tuple[float, float], Color]]:
    """Generate radial gradient points.

    Args:
        center_color: Color at center.
        edge_color: Color at edge.
        radius: Radius of gradient.

    Returns:
        List of (position, color) tuples.
    """
    return [
        ((0.0, 0.0), center_color),
        ((radius * 0.5, 0.0), center_color.blend(edge_color, 0.5)),
        ((radius, 0.0), edge_color),
    ]


def complementary(color: Color) -> Color:
    """Get complementary color (opposite on color wheel).

    Args:
        color: Source color.

    Returns:
        Complementary Color.
    """
    h, s, v = to_hsv(color)
    new_h = (h + 180.0) % 360.0
    return from_hsv(new_h, s, v, color.a)


def analogous(color: Color, angle: float = 30.0) -> tuple[Color, Color]:
    """Get analogous colors (adjacent on color wheel).

    Args:
        color: Source color.
        angle: Angle offset (default 30 degrees).

    Returns:
        Tuple of (rotated_negative, rotated_positive).
    """
    h, s, v = to_hsv(color)

    h1 = (h - angle) % 360.0
    h2 = (h + angle) % 360.0

    return (
        from_hsv(h1, s, v, color.a),
        from_hsv(h2, s, v, color.a)
    )


def triadic(color: Color) -> tuple[Color, Color]:
    """Get triadic colors (120 degrees apart).

    Args:
        color: Source color.

    Returns:
        Tuple of two triadic colors.
    """
    h, s, v = to_hsv(color)

    return (
        from_hsv((h + 120.0) % 360.0, s, v, color.a),
        from_hsv((h + 240.0) % 360.0, s, v, color.a)
    )


def saturate(color: Color, amount: float) -> Color:
    """Adjust color saturation.

    Args:
        color: Source color.
        amount: Saturation adjustment (-1.0 to 1.0).

    Returns:
        Adjusted Color.
    """
    h, s, v = to_hsv(color)
    new_s = max(0.0, min(1.0, s + amount))
    return from_hsv(h, new_s, v, color.a)


def lighten(color: Color, amount: float) -> Color:
    """Lighten color.

    Args:
        color: Source color.
        amount: Lightness adjustment (-1.0 to 1.0).

    Returns:
        Adjusted Color.
    """
    h, s, v = to_hsv(color)
    new_v = max(0.0, min(1.0, v + amount))
    return from_hsv(h, s, new_v, color.a)


def darken(color: Color, amount: float) -> Color:
    """Darken color.

    Args:
        color: Source color.
        amount: Darken amount (-1.0 to 1.0).

    Returns:
        Adjusted Color.
    """
    return lighten(color, -amount)


# Predefined color palettes
MATERIAL_COLORS = {
    "red": Color(244, 67, 54),
    "pink": Color(233, 30, 99),
    "purple": Color(156, 39, 176),
    "deep_purple": Color(103, 58, 183),
    "indigo": Color(63, 81, 181),
    "blue": Color(33, 150, 243),
    "cyan": Color(0, 188, 212),
    "teal": Color(0, 150, 136),
    "green": Color(76, 175, 80),
    "light_green": Color(139, 195, 74),
    "lime": Color(205, 220, 57),
    "yellow": Color(255, 235, 59),
    "amber": Color(255, 193, 7),
    "orange": Color(255, 152, 0),
    "deep_orange": Color(255, 87, 34),
    "brown": Color(121, 85, 72),
    "grey": Color(158, 158, 158),
    "blue_grey": Color(96, 125, 139),
}
