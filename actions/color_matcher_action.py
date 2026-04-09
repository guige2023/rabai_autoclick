"""
Color Matcher Action Module.

Matches colors for visual element detection and comparison,
with support for CSS color formats and tolerance matching.
"""

import re
from typing import Optional, Tuple, Union


class ColorParser:
    """Parses color strings into RGBA tuples."""

    HEX_PATTERN = re.compile(r"#([0-9a-fA-F]{2})([0-9a-fA-F]{2})([0-9a-fA-F]{2})(?:([0-9a-fA-F]{2}))?")
    RGB_PATTERN = re.compile(r"rgb\(\s*(\d+)\s*,\s*(\d+)\s*,\s*(\d+)\s*\)")
    RGBA_PATTERN = re.compile(r"rgba\(\s*(\d+)\s*,\s*(\d+)\s*,\s*(\d+)\s*,\s*([\d.]+)\s*\)")
    HSL_PATTERN = re.compile(r"hsl\(\s*(\d+)\s*,\s*(\d+)%\s*,\s*(\d+)%\s*\)")

    @classmethod
    def parse(cls, color_str: str) -> Optional[Tuple[int, int, int, float]]:
        """
        Parse a color string into RGBA tuple.

        Args:
            color_str: Color in hex, rgb(), rgba(), or hsl() format.

        Returns:
            RGBA tuple (r, g, b, a) or None if unparseable.
        """
        color_str = color_str.strip().lower()

        if color_str in NAMED_COLORS:
            return NAMED_COLORS[color_str]

        hex_match = cls.HEX_PATTERN.match(color_str)
        if hex_match:
            r = int(hex_match.group(1), 16)
            g = int(hex_match.group(2), 16)
            b = int(hex_match.group(3), 16)
            a = int(hex_match.group(4), 16) / 255.0 if hex_match.group(4) else 1.0
            return (r, g, b, a)

        rgba_match = cls.RGBA_PATTERN.match(color_str)
        if rgba_match:
            return (
                int(rgba_match.group(1)),
                int(rgba_match.group(2)),
                int(rgba_match.group(3)),
                float(rgba_match.group(4)),
            )

        rgb_match = cls.RGB_PATTERN.match(color_str)
        if rgb_match:
            return (
                int(rgb_match.group(1)),
                int(rgb_match.group(2)),
                int(rgb_match.group(3)),
                1.0,
            )

        hsl_match = cls.HSL_PATTERN.match(color_str)
        if hsl_match:
            r, g, b = cls._hsl_to_rgb(
                int(hsl_match.group(1)),
                int(hsl_match.group(2)),
                int(hsl_match.group(3)),
            )
            return (r, g, b, 1.0)

        return None

    @staticmethod
    def _hsl_to_rgb(h: int, s: int, l: int) -> Tuple[int, int, int]:
        """Convert HSL to RGB."""
        s /= 100.0
        l /= 100.0
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

        return (int((r + m) * 255), int((g + m) * 255), int((b + m) * 255))


class ColorMatcher:
    """Matches colors with tolerance support."""

    def __init__(self, tolerance: int = 10):
        """
        Initialize color matcher.

        Args:
            tolerance: RGB component difference tolerance (0-255).
        """
        self.tolerance = tolerance

    def are_similar(
        self,
        color1: Union[str, Tuple[int, int, int, float]],
        color2: Union[str, Tuple[int, int, int, float]],
    ) -> bool:
        """
        Check if two colors are similar within tolerance.

        Args:
            color1: First color (string or RGBA tuple).
            color2: Second color.

        Returns:
            True if similar, False otherwise.
        """
        c1 = self._normalize(color1)
        c2 = self._normalize(color2)
        if c1 is None or c2 is None:
            return False
        return self._color_distance(c1, c2) <= self.tolerance

    def match_distance(
        self,
        color1: Union[str, Tuple[int, int, int, float]],
        color2: Union[str, Tuple[int, int, int, float]],
    ) -> float:
        """
        Compute color distance between two colors.

        Args:
            color1: First color.
            color2: Second color.

        Returns:
            Euclidean distance in RGB space.
        """
        c1 = self._normalize(color1)
        c2 = self._normalize(color2)
        if c1 is None or c2 is None:
            return 256.0
        return self._color_distance(c1, c2)

    def find_closest_named(
        self,
        color: Union[str, Tuple[int, int, int, float]],
    ) -> Optional[str]:
        """
        Find the closest named color.

        Args:
            color: Color to match.

        Returns:
            Name of closest named color or None.
        """
        c = self._normalize(color)
        if c is None:
            return None

        closest = None
        min_dist = float("inf")
        for name, named_c in NAMED_COLORS.items():
            dist = self._color_distance(c, named_c)
            if dist < min_dist:
                min_dist = dist
                closest = name
        return closest

    def _normalize(
        self, color: Union[str, Tuple[int, int, int, float]]
    ) -> Optional[Tuple[int, int, int, float]]:
        """Normalize color to RGBA tuple."""
        if isinstance(color, str):
            return ColorParser.parse(color)
        return color

    @staticmethod
    def _color_distance(c1: Tuple, c2: Tuple) -> float:
        """Compute Euclidean distance in RGB space."""
        return sum((a - b) ** 2 for a, b in zip(c1[:3], c2[:3])) ** 0.5


NAMED_COLORS = {
    "black": (0, 0, 0, 1.0),
    "white": (255, 255, 255, 1.0),
    "red": (255, 0, 0, 1.0),
    "green": (0, 128, 0, 1.0),
    "blue": (0, 0, 255, 1.0),
    "yellow": (255, 255, 0, 1.0),
    "cyan": (0, 255, 255, 1.0),
    "magenta": (255, 0, 255, 1.0),
    "gray": (128, 128, 128, 1.0),
    "grey": (128, 128, 128, 1.0),
    "orange": (255, 165, 0, 1.0),
    "purple": (128, 0, 128, 1.0),
    "pink": (255, 192, 203, 1.0),
    "brown": (165, 42, 42, 1.0),
    "navy": (0, 0, 128, 1.0),
    "teal": (0, 128, 128, 1.0),
    "olive": (128, 128, 0, 1.0),
    "maroon": (128, 0, 0, 1.0),
    "silver": (192, 192, 192, 1.0),
    "lime": (0, 255, 0, 1.0),
    "aqua": (0, 255, 255, 1.0),
    "transparent": (0, 0, 0, 0.0),
}
