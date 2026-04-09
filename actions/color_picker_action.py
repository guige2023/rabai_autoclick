"""
Color Picker Action Module

Picks colors from screen, manages color palettes,
and provides color conversion for UI automation.

MIT License - Copyright (c) 2025 RabAi Research
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

logger = logging.getLogger(__name__)


class ColorFormat(Enum):
    """Color format types."""

    HEX = "hex"
    RGB = "rgb"
    RGBA = "rgba"
    HSL = "hsl"
    HSLA = "hsla"
    HSV = "hsv"


@dataclass
class Color:
    """Represents a color."""

    r: int
    g: int
    b: int
    a: float = 1.0

    def to_hex(self) -> str:
        """Convert to hex string."""
        return f"#{self.r:02x}{self.g:02x}{self.b:02x}"

    def to_rgb(self) -> Tuple[int, int, int]:
        """Convert to RGB tuple."""
        return (self.r, self.g, self.b)

    def to_rgba(self) -> Tuple[int, int, int, float]:
        """Convert to RGBA tuple."""
        return (self.r, self.g, self.b, self.a)

    def to_hsl(self) -> Tuple[float, float, float]:
        """Convert to HSL."""
        r, g, b = self.r / 255, self.g / 255, self.b / 255
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

        return (h / 6, s, l)

    @classmethod
    def from_hex(cls, hex_str: str) -> "Color":
        """Create Color from hex string."""
        hex_str = hex_str.lstrip("#")
        if len(hex_str) == 6:
            return cls(
                r=int(hex_str[0:2], 16),
                g=int(hex_str[2:4], 16),
                b=int(hex_str[4:6], 16),
            )
        elif len(hex_str) == 8:
            return cls(
                r=int(hex_str[0:2], 16),
                g=int(hex_str[2:4], 16),
                b=int(hex_str[4:6], 16),
                a=int(hex_str[6:8], 16) / 255,
            )
        return cls(0, 0, 0)


@dataclass
class ColorPickerConfig:
    """Configuration for color picker."""

    default_format: ColorFormat = ColorFormat.HEX
    pick_on_click: bool = True
    magnify: bool = True
    magnify_scale: int = 8


class ColorPicker:
    """
    Picks colors from screen and manages palettes.

    Supports various color formats, palette management,
    and color comparison.
    """

    def __init__(
        self,
        config: Optional[ColorPickerConfig] = None,
        screen_capture: Optional[Callable[[int, int], Optional[Color]]] = None,
    ):
        self.config = config or ColorPickerConfig()
        self.screen_capture = screen_capture or self._default_capture
        self._palette: List[Color] = []
        self._recent: List[Color] = []

    def _default_capture(self, x: int, y: int) -> Optional[Color]:
        """Default screen color capture."""
        return None

    def pick_at(self, x: int, y: int) -> Optional[Color]:
        """
        Pick color at screen coordinates.

        Args:
            x: X coordinate
            y: Y coordinate

        Returns:
            Color at position or None
        """
        color = self.screen_capture(x, y)

        if color:
            self._add_recent(color)
            self._add_to_palette(color)

        return color

    def _add_recent(self, color: Color) -> None:
        """Add color to recent list."""
        self._recent.insert(0, color)
        if len(self._recent) > 10:
            self._recent = self._recent[:10]

    def _add_to_palette(self, color: Color) -> None:
        """Add color to palette if not duplicate."""
        for existing in self._palette:
            if self._colors_match(existing, color):
                return
        self._palette.append(color)

    def _colors_match(self, c1: Color, c2: Color, tolerance: int = 5) -> bool:
        """Check if two colors match within tolerance."""
        return (
            abs(c1.r - c2.r) <= tolerance
            and abs(c1.g - c2.g) <= tolerance
            and abs(c1.b - c2.b) <= tolerance
        )

    def add_to_palette(self, color: Color) -> None:
        """Manually add color to palette."""
        self._add_to_palette(color)

    def get_palette(self) -> List[Color]:
        """Get all palette colors."""
        return self._palette.copy()

    def get_recent(self) -> List[Color]:
        """Get recent colors."""
        return self._recent.copy()

    def clear_palette(self) -> None:
        """Clear the palette."""
        self._palette.clear()

    def find_closest(
        self,
        target: Color,
        colors: Optional[List[Color]] = None,
    ) -> Optional[Color]:
        """
        Find closest color in list.

        Args:
            target: Target color
            colors: List to search (uses palette if None)

        Returns:
            Closest color or None
        """
        search_list = colors or self._palette
        if not search_list:
            return None

        closest = None
        min_distance = float("inf")

        for color in search_list:
            distance = self._color_distance(target, color)
            if distance < min_distance:
                min_distance = distance
                closest = color

        return closest

    def _color_distance(self, c1: Color, c2: Color) -> float:
        """Calculate color distance (Euclidean)."""
        dr = c1.r - c2.r
        dg = c1.g - c2.g
        db = c1.b - c2.b
        return (dr * dr + dg * dg + db * db) ** 0.5

    def format_color(
        self,
        color: Color,
        format: ColorFormat = ColorFormat.HEX,
    ) -> str:
        """Format color to string."""
        if format == ColorFormat.HEX:
            return color.to_hex()
        elif format == ColorFormat.RGB:
            return f"rgb({color.r}, {color.g}, {color.b})"
        elif format == ColorFormat.RGBA:
            return f"rgba({color.r}, {color.g}, {color.b}, {color.a})"
        elif format == ColorFormat.HSL:
            h, s, l = color.to_hsl()
            return f"hsl({h * 360:.1f}, {s * 100:.1f}%, {l * 100:.1f}%)"
        return color.to_hex()


def create_color_picker(
    config: Optional[ColorPickerConfig] = None,
) -> ColorPicker:
    """Factory function."""
    return ColorPicker(config=config)
