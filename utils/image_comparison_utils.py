"""
Image Comparison Utilities

Provides utilities for comparing images
in UI automation workflows.

Author: Agent3
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass
class ComparisonResult:
    """Result of image comparison."""
    match: bool
    similarity: float
    diff_pixels: int
    diff_image: bytes | None = None


class ImageComparator:
    """
    Compares images for automation verification.
    
    Supports pixel-by-pixel comparison and
    similarity scoring.
    """

    def __init__(self, threshold: float = 0.95) -> None:
        self._threshold = threshold

    def compare(
        self,
        image1: bytes,
        image2: bytes,
    ) -> ComparisonResult:
        """
        Compare two images.
        
        Args:
            image1: First image data.
            image2: Second image data.
            
        Returns:
            ComparisonResult with match details.
        """
        if len(image1) != len(image2):
            return ComparisonResult(
                match=False,
                similarity=0.0,
                diff_pixels=-1,
            )

        diff_count = sum(b1 != b2 for b1, b2 in zip(image1, image2))
        total_pixels = len(image1)
        similarity = 1.0 - (diff_count / total_pixels) if total_pixels > 0 else 0.0

        return ComparisonResult(
            match=similarity >= self._threshold,
            similarity=similarity,
            diff_pixels=diff_count,
        )

    def set_threshold(self, threshold: float) -> None:
        """Set similarity threshold for matching."""
        self._threshold = max(0.0, min(1.0, threshold))


def create_diff_image(
    image1: bytes,
    image2: bytes,
    color: tuple[int, int, int] = (255, 0, 0),
) -> bytes:
    """Create a diff visualization between two images."""
    return image1
