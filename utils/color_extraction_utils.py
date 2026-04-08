"""Color Extraction Utilities.

Extract dominant colors, palettes, and color histograms from UI screenshots.
Useful for visual comparison, theme detection, and color-based element identification.
"""

from __future__ import annotations

import math
from collections import Counter
from dataclasses import dataclass
from typing import Optional


@dataclass
class Color:
    """Represents an RGB color with utility methods.

    Attributes:
        r: Red component (0-255).
        g: Green component (0-255).
        b: Blue component (0-255).
        count: Pixel count for this color in an image.
    """

    r: int
    g: int
    b: int
    count: int = 1

    def __post_init__(self) -> None:
        """Clamp values to valid range."""
        self.r = max(0, min(255, self.r))
        self.g = max(0, min(255, self.g))
        self.b = max(0, min(255, self.b))

    @property
    def rgb(self) -> tuple[int, int, int]:
        """Return as RGB tuple."""
        return (self.r, self.g, self.b)

    @property
    def hex(self) -> str:
        """Return as hex string."""
        return f"#{self.r:02x}{self.g:02x}{self.b:02x}"

    @property
    def luminance(self) -> float:
        """Calculate perceived luminance (0.0 to 1.0)."""
        return 0.299 * self.r + 0.587 * self.g + 0.114 * self.b

    @property
    def is_dark(self) -> bool:
        """Check if color is considered dark."""
        return self.luminance < 128

    @property
    def is_light(self) -> bool:
        """Check if color is considered light."""
        return self.luminance >= 128

    def distance_to(self, other: "Color") -> float:
        """Calculate Euclidean distance to another color.

        Args:
            other: Color to measure distance to.

        Returns:
            Distance value (0.0 to ~441.67 for max distance).
        """
        dr = self.r - other.r
        dg = self.g - other.g
        db = self.b - other.b
        return math.sqrt(dr * dr + dg * dg + db * db)

    def is_similar_to(self, other: "Color", threshold: float = 30.0) -> bool:
        """Check if another color is within threshold distance.

        Args:
            other: Color to compare.
            threshold: Maximum distance to consider similar.

        Returns:
            True if colors are similar.
        """
        return self.distance_to(other) <= threshold

    def to_hsv(self) -> tuple[float, float, float]:
        """Convert to HSV representation.

        Returns:
            Tuple of (hue 0-360, saturation 0-1, value 0-1).
        """
        r, g, b = self.r / 255.0, self.g / 255.0, self.b / 255.0
        max_c = max(r, g, b)
        min_c = min(r, g, b)
        diff = max_c - min_c

        if diff == 0:
            hue = 0.0
        elif max_c == r:
            hue = 60 * (((g - b) / diff) % 6)
        elif max_c == g:
            hue = 60 * (((b - r) / diff) + 2)
        else:
            hue = 60 * (((r - g) / diff) + 4)

        saturation = 0.0 if max_c == 0 else diff / max_c
        value = max_c
        return (hue, saturation, value)


@dataclass
class ColorPalette:
    """Collection of colors forming a palette.

    Attributes:
        dominant: The most dominant color.
        colors: All colors in the palette.
    """

    dominant: Color
    colors: list[Color]

    def find_similar(self, color: Color, threshold: float = 30.0) -> Optional[Color]:
        """Find a color in the palette similar to the given color.

        Args:
            color: Color to match.
            threshold: Maximum distance for similarity.

        Returns:
            Matching Color or None.
        """
        for c in self.colors:
            if c.is_similar_to(color, threshold):
                return c
        return None


class DominantColorExtractor:
    """Extracts dominant colors from image data.

    Uses k-means-style clustering for color quantization.

    Example:
        extractor = DominantColorExtractor()
        pixels = load_image_pixels("screenshot.png")
        palette = extractor.extract(pixels, num_colors=5)
    """

    def __init__(self):
        """Initialize the extractor."""
        pass

    def extract(
        self,
        pixels: list[tuple[int, int, int]],
        num_colors: int = 5,
        min_threshold: float = 0.01,
    ) -> ColorPalette:
        """Extract dominant colors from pixel data.

        Args:
            pixels: List of (r, g, b) tuples.
            num_colors: Target number of dominant colors.
            min_threshold: Minimum population ratio for a color to be included.

        Returns:
            ColorPalette with extracted colors.
        """
        if not pixels:
            return ColorPalette(
                dominant=Color(0, 0, 0),
                colors=[Color(0, 0, 0)],
            )

        # Count color frequencies
        color_counts: dict[tuple[int, int, int], int] = Counter(pixels)

        # Quantize colors to reduce palette
        quantized = self._quantize_colors(color_counts, num_colors * 2)

        # Cluster similar colors
        clusters = self._cluster_colors(quantized, num_colors)

        # Sort by population
        sorted_colors = sorted(clusters, key=lambda c: c.count, reverse=True)

        total_pixels = len(pixels)
        filtered = [c for c in sorted_colors if c.count / total_pixels >= min_threshold]

        if not filtered:
            filtered = sorted_colors[:num_colors]

        dominant = filtered[0] if filtered else Color(0, 0, 0)
        return ColorPalette(dominant=dominant, colors=filtered)

    def _quantize_colors(
        self,
        color_counts: dict[tuple[int, int, int], int],
        levels: int = 8,
    ) -> dict[tuple[int, int, int], int]:
        """Reduce color precision for clustering.

        Args:
            color_counts: Color frequency map.
            levels: Number of quantization levels per channel.

        Returns:
            Quantized color counts.
        """
        step = 256 // levels
        quantized: dict[tuple[int, int, int], int] = {}

        for (r, g, b), count in color_counts.items():
            qr = (r // step) * step + step // 2
            qg = (g // step) * step + step // 2
            qb = (b // step) * step + step // 2
            qcolor = (max(0, min(255, qr)), max(0, min(255, qg)), max(0, min(255, qb)))
            quantized[qcolor] = quantized.get(qcolor, 0) + count

        return quantized

    def _cluster_colors(
        self,
        color_counts: dict[tuple[int, int, int], int],
        target_count: int,
    ) -> list[Color]:
        """Cluster similar colors together.

        Args:
            color_counts: Quantized color counts.
            target_count: Target number of clusters.

        Returns:
            List of Color objects representing clusters.
        """
        if len(color_counts) <= target_count:
            return [Color(r, g, b, c) for (r, g, b), c in color_counts.items()]

        # Simple greedy clustering
        sorted_colors = sorted(
            color_counts.keys(), key=lambda c: color_counts[c], reverse=True
        )
        centroids = sorted_colors[:target_count]

        clusters: dict[int, list[tuple[int, int, int]]] = {i: [] for i in range(len(centroids))}

        for color in color_counts.keys():
            min_dist = float("inf")
            closest = 0
            for i, centroid in enumerate(centroids):
                dist = math.sqrt(sum((a - b) ** 2 for a, b in zip(color, centroid)))
                if dist < min_dist:
                    min_dist = dist
                    closest = i
            clusters[closest].append(color)

        result = []
        for i, centroid in enumerate(centroids):
            cluster_colors = clusters[i]
            if cluster_colors:
                total_count = sum(color_counts[c] for c in cluster_colors)
                avg_r = sum(c[0] * color_counts[c] for c in cluster_colors) // total_count
                avg_g = sum(c[1] * color_counts[c] for c in cluster_colors) // total_count
                avg_b = sum(c[2] * color_counts[c] for c in cluster_colors) // total_count
                result.append(Color(avg_r, avg_g, avg_b, total_count))
            else:
                result.append(Color(*centroid, color_counts[centroid]))

        return result


class ColorHistogram:
    """Computes color histograms for images."""

    def __init__(self, bins_per_channel: int = 16):
        """Initialize histogram builder.

        Args:
            bins_per_channel: Number of bins per color channel.
        """
        self.bins_per_channel = bins_per_channel

    def compute(self, pixels: list[tuple[int, int, int]]) -> dict[str, list[int]]:
        """Compute color histogram from pixel data.

        Args:
            pixels: List of (r, g, b) tuples.

        Returns:
            Dictionary with 'r', 'g', 'b' keys containing bin counts.
        """
        bin_size = 256 // self.bins_per_channel
        hist_r = [0] * self.bins_per_channel
        hist_g = [0] * self.bins_per_channel
        hist_b = [0] * self.bins_per_channel

        for r, g, b in pixels:
            hist_r[min(r // bin_size, self.bins_per_channel - 1)] += 1
            hist_g[min(g // bin_size, self.bins_per_channel - 1)] += 1
            hist_b[min(b // bin_size, self.bins_per_channel - 1)] += 1

        return {"r": hist_r, "g": hist_g, "b": hist_b}

    def compare(
        self,
        hist1: dict[str, list[int]],
        hist2: dict[str, list[int]],
    ) -> float:
        """Compare two histograms using correlation.

        Args:
            hist1: First histogram dict.
            hist2: Second histogram dict.

        Returns:
            Correlation coefficient (-1 to 1).
        """
        def correlate(a: list[int], b: list[int]) -> float:
            n = len(a)
            if n == 0:
                return 0.0
            mean_a = sum(a) / n
            mean_b = sum(b) / n
            numerator = sum((x - mean_a) * (y - mean_b) for x, y in zip(a, b))
            denom_a = math.sqrt(sum((x - mean_a) ** 2 for x in a))
            denom_b = math.sqrt(sum((y - mean_b) ** 2 for y in b))
            if denom_a == 0 or denom_b == 0:
                return 0.0
            return numerator / (denom_a * denom_b)

        r_corr = correlate(hist1["r"], hist2["r"])
        g_corr = correlate(hist1["g"], hist2["g"])
        b_corr = correlate(hist1["b"], hist2["b"])
        return (r_corr + g_corr + b_corr) / 3.0
