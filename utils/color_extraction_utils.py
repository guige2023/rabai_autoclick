"""
Color extraction utilities for UI element color analysis.

Provides color extraction from screenshots, dominant color detection,
palette generation, and color comparison.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


@dataclass
class Color:
    """RGB color representation."""
    r: int
    g: int
    b: int

    @property
    def rgb(self) -> tuple[int, int, int]:
        return (self.r, self.g, self.b)

    @property
    def hex(self) -> str:
        return f"#{self.r:02x}{self.g:02x}{self.b:02x}"

    @property
    def luminance(self) -> float:
        """Compute relative luminance."""
        def to_linear(c: int) -> float:
            v = c / 255.0
            return v / 12.92 if v <= 0.04045 else ((v + 0.055) / 1.055) ** 2.4
        return 0.2126 * to_linear(self.r) + 0.7152 * to_linear(self.g) + 0.0722 * to_linear(self.b)

    def is_light(self) -> bool:
        return self.luminance > 0.5

    def contrast_with(self, other: Color) -> float:
        """Compute contrast ratio with another color."""
        l1 = self.luminance
        l2 = other.luminance
        lighter = max(l1, l2)
        darker = min(l1, l2)
        return (lighter + 0.05) / (darker + 0.05)

    def distance_to(self, other: Color) -> float:
        """Compute Euclidean distance to another color."""
        return ((self.r - other.r) ** 2 + (self.g - other.g) ** 2 + (self.b - other.b) ** 2) ** 0.5


class ColorExtractor:
    """Extracts colors from image data."""

    def extract_dominant(
        self,
        pixels: list[tuple[int, int, int]],
        k: int = 5,
    ) -> list[Color]:
        """Extract k dominant colors from pixel list using simple clustering."""
        if not pixels:
            return []

        # Simple k-means-like approach
        colors = [Color(p[0], p[1], p[2]) for p in pixels[:1000]]  # Sample

        # Quantize to reduce color space
        quantized = {}
        for c in colors:
            qr, qg, qb = c.r // 32 * 32, c.g // 32 * 32, c.b // 32 * 32
            key = (qr, qg, qb)
            quantized[key] = quantized.get(key, 0) + 1

        # Sort by frequency
        sorted_colors = sorted(quantized.items(), key=lambda x: x[1], reverse=True)
        result = []
        for (r, g, b), count in sorted_colors[:k]:
            result.append(Color(r, g, b))
        return result

    def extract_from_region(
        self,
        pixels: list[tuple[int, int, int]],
    ) -> Color:
        """Extract average color from a region."""
        if not pixels:
            return Color(0, 0, 0)

        total_r = sum(p[0] for p in pixels)
        total_g = sum(p[1] for p in pixels)
        total_b = sum(p[2] for p in pixels)
        count = len(pixels)

        return Color(total_r // count, total_g // count, total_b // count)

    def extract_palette(
        self,
        pixels: list[tuple[int, int, int]],
        palette_size: int = 8,
    ) -> list[Color]:
        """Extract a color palette from pixels."""
        return self.extract_dominant(pixels, k=palette_size)


def hex_to_color(hex_str: str) -> Color:
    """Parse hex color string to Color object."""
    hex_str = hex_str.lstrip("#")
    if len(hex_str) == 6:
        return Color(
            r=int(hex_str[0:2], 16),
            g=int(hex_str[2:4], 16),
            b=int(hex_str[4:6], 16),
        )
    return Color(0, 0, 0)


def color_from_rgb(r: int, g: int, b: int) -> Color:
    return Color(r, g, b)


def color_distance(c1: Color, c2: Color) -> float:
    return c1.distance_to(c2)


def best_text_color(bg_color: Color) -> Color:
    """Determine best text color (black or white) for given background."""
    if bg_color.is_light():
        return Color(0, 0, 0)
    return Color(255, 255, 255)


__all__ = ["Color", "ColorExtractor", "hex_to_color", "color_from_rgb", "color_distance", "best_text_color"]
