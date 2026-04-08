"""Visual comparison utilities for RabAI AutoClick.

Provides:
- Image comparison
- Similarity metrics
- Diff generation
"""

from __future__ import annotations

from typing import (
    Any,
    List,
    Optional,
    Tuple,
)


def pixel_diff(
    img1: List[List[int]],
    img2: List[List[int]],
) -> List[List[int]]:
    """Compute per-pixel difference between two images.

    Args:
        img1: First image as 2D pixel array.
        img2: Second image.

    Returns:
        Difference image.
    """
    if len(img1) != len(img2) or len(img1[0]) != len(img2[0]):
        raise ValueError("Images must have same dimensions")

    diff: List[List[int]] = []
    for row1, row2 in zip(img1, img2):
        diff_row = [abs(a - b) for a, b in zip(row1, row2)]
        diff.append(diff_row)
    return diff


def mse(image1: List[List[float]], image2: List[List[float]]) -> float:
    """Compute mean squared error between two images.

    Args:
        image1: First image.
        image2: Second image.

    Returns:
        MSE value.
    """
    if len(image1) != len(image2):
        raise ValueError("Images must have same dimensions")

    total = 0.0
    count = 0
    for row1, row2 in zip(image1, image2):
        for p1, p2 in zip(row1, row2):
            total += (p1 - p2) ** 2
            count += 1

    return total / count if count > 0 else 0.0


def similarity_score(
    image1: List[List[float]],
    image2: List[List[float]],
) -> float:
    """Compute similarity score (0-1) between two images.

    Args:
        image1: First image.
        image2: Second image.

    Returns:
        Similarity score (1 = identical).
    """
    if not image1 or not image2:
        return 0.0

    error = mse(image1, image2)
    max_error = 255.0 ** 2
    return max(0.0, 1.0 - error / max_error)


__all__ = [
    "pixel_diff",
    "mse",
    "similarity_score",
]
