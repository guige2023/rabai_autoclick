"""Screenshot comparison utilities.

This module provides utilities for comparing screenshots,
computing diffs, and detecting visual changes.
"""

from __future__ import annotations

from typing import List, Optional, Tuple
from dataclasses import dataclass
import math


@dataclass
class DiffRegion:
    """A region where screenshots differ."""
    x: int
    y: int
    width: int
    height: int
    similarity: float  # 0.0 = identical, 1.0 = completely different


@dataclass
class ComparisonResult:
    """Result of screenshot comparison."""
    identical: bool
    similarity: float  # Overall similarity 0.0-1.0
    diff_regions: List[DiffRegion] = None
    diff_pixel_count: int = 0
    total_pixels: int = 0

    def __post_init__(self) -> None:
        if self.diff_regions is None:
            self.diff_regions = []


def compute_pixel_similarity(
    pixels1: List[List[int]],
    pixels2: List[List[int]],
) -> float:
    """Compute per-pixel similarity between two images.

    Args:
        pixels1: First image as RGB pixel grid.
        pixels2: Second image as RGB pixel grid.

    Returns:
        Similarity score 0.0-1.0.
    """
    if len(pixels1) != len(pixels2) or len(pixels1[0]) != len(pixels2[0]):
        return 0.0

    total = 0
    diff_count = 0
    for row1, row2 in zip(pixels1, pixels2):
        for p1, p2 in zip(row1, row2):
            total += 1
            # Per-channel difference
            diff = sum(abs(a - b) for a, b in zip(p1[:3], p2[:3])) / (3 * 255)
            diff_count += diff

    return 1.0 - (diff_count / total) if total > 0 else 1.0


def find_diff_regions(
    pixels1: List[List[int]],
    pixels2: List[List[int]],
    threshold: float = 0.1,
    min_region_size: int = 5,
) -> List[DiffRegion]:
    """Find rectangular regions that differ.

    Args:
        pixels1: First image pixels.
        pixels2: Second image pixels.
        threshold: Difference threshold (0.0-1.0).
        min_region_size: Minimum region size in pixels.

    Returns:
        List of DiffRegion.
    """
    if len(pixels1) != len(pixels2) or len(pixels1[0]) != len(pixels2[0]):
        return []

    height = len(pixels1)
    width = len(pixels1[0])
    diff_map = [[0] * width for _ in range(height)]

    for y in range(height):
        for x in range(width):
            p1 = pixels1[y][x]
            p2 = pixels2[y][x]
            diff = sum(abs(a - b) for a, b in zip(p1[:3], p2[:3])) / (3 * 255)
            if diff > threshold:
                diff_map[y][x] = 1

    regions = []
    visited = [[False] * width for _ in range(height)]

    for y in range(height):
        for x in range(width):
            if diff_map[y][x] and not visited[y][x]:
                region = _flood_fill_diff(diff_map, visited, x, y, width, height)
                if region.width >= min_region_size or region.height >= min_region_size:
                    regions.append(region)

    return regions


def _flood_fill_diff(
    diff_map: List[List[int]],
    visited: List[List[bool]],
    start_x: int,
    start_y: int,
    width: int,
    height: int,
) -> DiffRegion:
    """Flood fill to find connected diff region."""
    min_x, min_y = start_x, start_y
    max_x, max_y = start_x, start_y
    stack = [(start_x, start_y)]

    while stack:
        x, y = stack.pop()
        if x < 0 or x >= width or y < 0 or y >= height:
            continue
        if visited[y][x] or not diff_map[y][x]:
            continue
        visited[y][x] = True
        min_x = min(min_x, x)
        max_x = max(max_x, x)
        min_y = min(min_y, y)
        max_y = max(max_y, y)
        stack.extend([
            (x + 1, y), (x - 1, y),
            (x, y + 1), (x, y - 1),
        ])

    return DiffRegion(
        x=min_x,
        y=min_y,
        width=max_x - min_x + 1,
        height=max_y - min_y + 1,
        similarity=0.5,
    )


def compute_histogram_similarity(
    hist1: List[int],
    hist2: List[int],
) -> float:
    """Compute similarity between two histograms.

    Args:
        hist1: First histogram.
        hist2: Second histogram.

    Returns:
        Similarity score 0.0-1.0.
    """
    if len(hist1) != len(hist2):
        return 0.0

    sum_sq_diff = sum((a - b) ** 2 for a, b in zip(hist1, hist2))
    sum_sq_1 = sum(a ** 2 for a in hist1)
    sum_sq_2 = sum(b ** 2 for b in hist2)

    if sum_sq_1 == 0 and sum_sq_2 == 0:
        return 1.0

    return 1.0 / (1.0 + math.sqrt(sum_sq_diff / (sum_sq_1 + sum_sq_2 + 1e-10)))


__all__ = [
    "DiffRegion",
    "ComparisonResult",
    "compute_pixel_similarity",
    "find_diff_regions",
    "compute_histogram_similarity",
]
