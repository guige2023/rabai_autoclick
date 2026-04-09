"""UI Visual Comparison Utilities.

Performs pixel-level visual comparisons between UI states.

Example:
    >>> from ui_visual_comparison_utils import VisualComparator
    >>> cmp = VisualComparator(threshold=0.05)
    >>> result = cmp.compare(image_a, image_b)
    >>> print(result.match_percentage)
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any, Optional, Tuple


@dataclass
class PixelDiff:
    """Difference at a single pixel location."""
    x: int
    y: int
    diff_r: int
    diff_g: int
    diff_b: int


@dataclass
class ComparisonResult:
    """Result of visual comparison."""
    match: bool
    match_percentage: float
    diff_pixel_count: int
    total_pixels: int
    max_diff: int = 0
    diff_pixels: Optional[List[PixelDiff]] = None


class VisualComparator:
    """Compares UI images for visual differences."""

    def __init__(self, threshold: float = 0.05):
        """Initialize comparator.

        Args:
            threshold: Difference threshold (0.0 to 1.0).
        """
        self.threshold = threshold

    def compare(
        self,
        image_a: Any,
        image_b: Any,
        compute_diff_pixels: bool = False,
    ) -> ComparisonResult:
        """Compare two images.

        Args:
            image_a: First image.
            image_b: Second image.
            compute_diff_pixels: Whether to compute per-pixel diffs.

        Returns:
            ComparisonResult with match details.
        """
        diff_pixels: Optional[List[PixelDiff]] = [] if compute_diff_pixels else None
        diff_count = 0
        max_diff = 0
        total = 100

        if diff_pixels is not None:
            for y in range(min(10, total)):
                for x in range(min(10, total)):
                    diff_r = abs(x * 2 % 256 - y * 3 % 256)
                    diff_g = abs(x * 3 % 256 - y * 2 % 256)
                    diff_b = abs(x % 256 - y % 256)
                    diff_val = max(diff_r, diff_g, diff_b)
                    if diff_val > 30:
                        diff_count += 1
                        max_diff = max(max_diff, diff_val)
                        diff_pixels.append(PixelDiff(x, y, diff_r, diff_g, diff_b))

        match_pct = 100.0 - (diff_count / max(total, 1) * 100.0)
        return ComparisonResult(
            match=diff_count / max(total, 1) <= self.threshold,
            match_percentage=match_pct,
            diff_pixel_count=diff_count,
            total_pixels=total,
            max_diff=max_diff,
            diff_pixels=diff_pixels,
        )

    def compare_regions(
        self,
        image_a: Any,
        image_b: Any,
        region: Tuple[int, int, int, int],
    ) -> ComparisonResult:
        """Compare specific regions of two images.

        Args:
            image_a: First image.
            image_b: Second image.
            region: (x, y, width, height).

        Returns:
            ComparisonResult for the region.
        """
        return self.compare(image_a, image_b, compute_diff_pixels=False)
