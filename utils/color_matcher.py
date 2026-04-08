"""
Color Matcher Utility

Matches and compares colors from screenshots for visual automation.
Supports RGB, HSV, and hex color formats.

Example:
    >>> matcher = ColorMatcher()
    >>> match = matcher.find_color_in_region(screenshot, (255, 0, 0), region=(0, 0, 800, 600))
    >>> print(match.point)
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Callable
import math


@dataclass
class Color:
    """Represents an RGB color with optional alpha."""
    r: int
    g: int
    b: int
    a: int = 255

    def to_hex(self) -> str:
        """Return hex string like '#FF0000'."""
        return f"#{self.r:02X}{self.g:02X}{self.b:02X}"

    def to_rgb_tuple(self) -> tuple[int, int, int]:
        """Return as (r, g, b) tuple."""
        return (self.r, self.g, self.b)

    def to_hsv(self) -> tuple[float, float, float]:
        """Return HSV representation (h: 0-360, s: 0-1, v: 0-1)."""
        r, g, b = self.r / 255.0, self.g / 255.0, self.b / 255.0
        max_c = max(r, g, b)
        min_c = min(r, g, b)
        delta = max_c - min_c

        if delta == 0:
            h = 0.0
        elif max_c == r:
            h = 60 * (((g - b) / delta) % 6)
        elif max_c == g:
            h = 60 * (((b - r) / delta) + 2)
        else:
            h = 60 * (((r - g) / delta) + 4)

        s = 0.0 if max_c == 0 else delta / max_c
        v = max_c
        return (h, s, v)

    @classmethod
    def from_hex(cls, hex_str: str) -> Color:
        """Parse color from hex string like '#FF0000' or 'FF0000'."""
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
                a=int(hex_str[6:8], 16),
            )
        return cls(0, 0, 0)

    def distance_rgb(self, other: Color) -> float:
        """Euclidean distance in RGB space."""
        return math.sqrt(
            (self.r - other.r) ** 2
            + (self.g - other.g) ** 2
            + (self.b - other.b) ** 2
        )

    def distance_hsv(self, other: Color) -> float:
        """Weighted distance in HSV space."""
        h1, s1, v1 = self.to_hsv()
        h2, s2, v2 = other.to_hsv()
        # Hue wraps around, take shortest path
        dh = min(abs(h1 - h2), 360 - abs(h1 - h2)) / 180.0
        ds = abs(s1 - s2)
        dv = abs(v1 - v2)
        return math.sqrt(dh ** 2 + ds ** 2 + dv ** 2)

    def matches(
        self,
        other: Color,
        threshold: float = 30.0,
        metric: str = "rgb",
    ) -> bool:
        """
        Check if two colors match within a threshold.

        Args:
            other: Color to compare against.
            threshold: Maximum allowed distance.
            metric: 'rgb' or 'hsv' distance metric.
        """
        if metric == "hsv":
            return self.distance_hsv(other) <= threshold / 255.0
        return self.distance_rgb(other) <= threshold


@dataclass
class ColorMatch:
    """Result of a color search operation."""
    point: tuple[int, int]
    matched_color: Color
    distance: float
    confidence: float  # 0.0 to 1.0


class ColorMatcher:
    """
    Matches colors in images for visual automation.

    Args:
        tolerance: Default color matching tolerance (0-255).
    """

    def __init__(self, tolerance: float = 30.0) -> None:
        self.tolerance = tolerance
        self._image_cache: dict[str, any] = {}

    def match_color_in_image(
        self,
        image_path: str,
        target: Color,
        tolerance: Optional[float] = None,
    ) -> list[ColorMatch]:
        """
        Find all occurrences of a color in an image.

        Args:
            image_path: Path to image file.
            target: Target color to find.
            tolerance: Matching threshold (overrides default).

        Returns:
            List of ColorMatch objects.
        """
        from PIL import Image
        import numpy as np

        tol = tolerance if tolerance is not None else self.tolerance
        matches: list[ColorMatch] = []

        try:
            img = Image.open(image_path).convert("RGB")
            arr = np.array(img)
            target_rgb = target.to_rgb_tuple()

            # Compare each pixel
            diff = np.sqrt(
                (arr[:, :, 0].astype(int) - target_rgb[0]) ** 2
                + (arr[:, :, 1].astype(int) - target_rgb[1]) ** 2
                + (arr[:, :, 2].astype(int) - target_rgb[2]) ** 2
            )

            # Find pixels within tolerance
            rows, cols = np.where(diff <= tol)
            for y, x in zip(rows, cols):
                matched_color = Color(
                    r=int(arr[y, x, 0]),
                    g=int(arr[y, x, 1]),
                    b=int(arr[y, x, 2]),
                )
                dist = diff[y, x]
                confidence = 1.0 - (dist / 255.0)
                matches.append(ColorMatch(
                    point=(x, y),
                    matched_color=matched_color,
                    distance=float(dist),
                    confidence=float(confidence),
                ))
        except Exception:
            pass

        return matches

    def find_dominant_colors(
        self,
        image_path: str,
        count: int = 5,
    ) -> list[tuple[Color, float]]:
        """
        Find dominant colors in an image using k-means-like clustering.

        Args:
            image_path: Path to image file.
            count: Number of dominant colors to find.

        Returns:
            List of (Color, percentage) tuples.
        """
        from PIL import Image
        import numpy as np

        try:
            img = Image.open(image_path).convert("RGB")
            arr = np.array(img)
            pixels = arr.reshape(-1, 3)

            # Simple quantization approach
            quantized = (pixels // 32) * 32
            unique, counts = np.unique(quantized, axis=0, return_counts=True)

            sorted_indices = np.argsort(-counts)[:count]
            total = len(pixels)

            results: list[tuple[Color, float]] = []
            for idx in sorted_indices:
                rgb = unique[idx]
                pct = counts[idx] / total
                results.append((Color(int(rgb[0]), int(rgb[1]), int(rgb[2])), pct))

            return results
        except Exception:
            return []

    def get_pixel_color(
        self,
        image_path: str,
        point: tuple[int, int],
    ) -> Optional[Color]:
        """
        Get the color of a specific pixel.

        Args:
            image_path: Path to image file.
            point: (x, y) pixel coordinates.

        Returns:
            Color at the specified point, or None on error.
        """
        from PIL import Image

        try:
            img = Image.open(image_path).convert("RGB")
            x, y = point
            if 0 <= x < img.width and 0 <= y < img.height:
                r, g, b = img.getpixel((x, y))
                return Color(int(r), int(g), int(b))
        except Exception:
            pass
        return None
