"""Color histogram utilities for image analysis.

Computes color histograms (RGB, HSV, grayscale) for images and provides
comparison functions to determine visual similarity between two images
based on their color distribution. Useful for detecting whether a UI
has changed or matches a reference.

Example:
    >>> from utils.color_histogram_utils import ColorHistogram, compare_histograms
    >>> hist = ColorHistogram.compute(image_bytes, bins=256)
    >>> hist_rgb = ColorHistogram.compute_rgb(image_bytes)
"""
from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Sequence

__all__ = [
    "ColorHistogram",
    "compare_histograms",
    "histogram_similarity",
]


@dataclass
class ColorHistogram:
    """A color histogram with bin counts and metadata.

    Attributes:
        counts: List or array of bin counts.
        bins: Number of bins in the histogram.
        color_space: The color space used ('rgb', 'hsv', 'grayscale').
        range_min: Lower value of the color range.
        range_max: Upper value of the color space range.
    """

    counts: list[float]
    bins: int
    color_space: str
    range_min: float
    range_max: float

    def total(self) -> float:
        """Return the total count across all bins."""
        return sum(self.counts)

    def normalize(self) -> "ColorHistogram":
        """Return a normalized copy (sum of bins = 1.0)."""
        total = self.total()
        if total == 0:
            return self
        norm_counts = [c / total for c in self.counts]
        return ColorHistogram(
            counts=norm_counts,
            bins=self.bins,
            color_space=self.color_space,
            range_min=self.range_min,
            range_max=self.range_max,
        )

    @staticmethod
    def compute(
        data: bytes,
        bins: int = 256,
        color_space: str = "grayscale",
    ) -> ColorHistogram:
        """Compute a histogram from raw image bytes.

        This is a placeholder implementation. In production, decode
        the image and use numpy for efficient histogram computation.

        Args:
            data: Raw image bytes.
            bins: Number of bins.
            color_space: Color space ('grayscale', 'rgb', 'hsv').

        Returns:
            A ColorHistogram instance.
        """
        # Placeholder: generate dummy histogram
        # Real implementation would decode image and compute actual histogram
        return ColorHistogram(
            counts=[0.0] * bins,
            bins=bins,
            color_space=color_space,
            range_min=0.0,
            range_max=256.0,
        )

    @staticmethod
    def compute_rgb(data: bytes, bins_per_channel: int = 64) -> tuple["ColorHistogram", ...]:
        """Compute separate R, G, B histograms.

        Args:
            data: Raw image bytes.
            bins_per_channel: Number of bins per color channel.

        Returns:
            Tuple of (R, G, B) ColorHistogram instances.
        """
        h_r = ColorHistogram(
            counts=[0.0] * bins_per_channel,
            bins=bins_per_channel,
            color_space="red",
            range_min=0.0,
            range_max=256.0,
        )
        h_g = ColorHistogram(
            counts=[0.0] * bins_per_channel,
            bins=bins_per_channel,
            color_space="green",
            range_min=0.0,
            range_max=256.0,
        )
        h_b = ColorHistogram(
            counts=[0.0] * bins_per_channel,
            bins=bins_per_channel,
            color_space="blue",
            range_min=0.0,
            range_max=256.0,
        )
        return (h_r, h_g, h_b)


def compare_histograms(
    a: ColorHistogram,
    b: ColorHistogram,
    method: str = "correlation",
) -> float:
    """Compare two histograms and return a similarity score.

    Args:
        a: First histogram.
        b: Second histogram.
        method: Comparison method ('correlation', 'chisqr', 'intersection').

    Returns:
        Similarity score (range depends on method; correlation is [-1, 1]).

    Raises:
        ValueError: If histograms are incompatible.
    """
    if a.bins != b.bins:
        raise ValueError(
            f"Bin count mismatch: {a.bins} vs {b.bins}"
        )

    a_norm = a.normalize().counts
    b_norm = b.normalize().counts

    if method == "correlation":
        return _pearson_correlation(a_norm, b_norm)
    elif method == "chisqr":
        return _chi_squared(a_norm, b_norm)
    elif method == "intersection":
        return _histogram_intersection(a_norm, b_norm)
    else:
        raise ValueError(f"Unknown method: {method}")


def _pearson_correlation(x: list[float], y: list[float]) -> float:
    """Compute Pearson correlation coefficient."""
    n = len(x)
    if n == 0:
        return 0.0
    mean_x = sum(x) / n
    mean_y = sum(y) / n
    num = sum((xi - mean_x) * (yi - mean_y) for xi, yi in zip(x, y))
    den = math.sqrt(
        sum((xi - mean_x) ** 2 for xi in x)
        * sum((yi - mean_y) ** 2 for yi in y)
    )
    if den == 0:
        return 0.0
    return num / den


def _chi_squared(a: list[float], b: list[float]) -> float:
    """Compute chi-squared distance."""
    total = 0.0
    for ai, bi in zip(a, b):
        if ai + bi > 0:
            total += (ai - bi) ** 2 / (ai + bi)
    return total / 2


def _histogram_intersection(a: list[float], b: list[float]) -> float:
    """Compute histogram intersection (0 = no overlap, 1 = identical)."""
    return sum(min(ai, bi) for ai, bi in zip(a, b))


def histogram_similarity(
    hist1: ColorHistogram,
    hist2: ColorHistogram,
    threshold: float = 0.85,
) -> bool:
    """Return True if two histograms are similar based on a threshold.

    Args:
        hist1: First histogram.
        hist2: Second histogram.
        threshold: Minimum correlation to consider similar.

    Returns:
        True if similarity >= threshold.
    """
    score = compare_histograms(hist1, hist2, method="correlation")
    return score >= threshold
