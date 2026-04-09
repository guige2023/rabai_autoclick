"""
Gesture Similarity Utilities

Compute similarity scores between gesture templates and observed
gesture paths. Used for gesture recognition and matching.

Author: rabai_autoclick-agent3
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import List, Tuple


@dataclass
class Point:
    """A 2D point with optional timestamp."""
    x: float
    y: float
    t: float = 0.0


@dataclass
class GestureSimilarityResult:
    """Similarity score between two gestures."""
    score: float  # 0.0 (dissimilar) to 1.0 (identical)
    normalized_score: float
    point_error: float


def resample_path(points: List[Point], target_count: int) -> List[Point]:
    """Resample a gesture path to a fixed number of evenly-spaced points."""
    if len(points) < 2 or target_count < 2:
        return points[:target_count] if points else []

    total_length = sum(
        math.sqrt((points[i + 1].x - points[i].x) ** 2 +
                   (points[i + 1].y - points[i].y) ** 2)
        for i in range(len(points) - 1)
    )
    interval = total_length / (target_count - 1)
    resampled = [points[0]]
    accumulated = 0.0
    j = 0

    for i in range(len(points) - 1):
        p1, p2 = points[i], points[i + 1]
        segment_length = math.sqrt((p2.x - p1.x) ** 2 + (p2.y - p1.y) ** 2)
        while accumulated + segment_length >= interval * len(resampled):
            ratio = (interval * len(resampled) - accumulated) / segment_length
            mid_x = p1.x + ratio * (p2.x - p1.x)
            mid_y = p1.y + ratio * (p2.y - p1.y)
            mid_t = p1.t + ratio * (p2.t - p1.t)
            resampled.append(Point(mid_x, mid_y, mid_t))
            if len(resampled) >= target_count:
                break
            accumulated += segment_length
            p1, p2 = Point(mid_x, mid_y, mid_t), p2
        accumulated += segment_length

    while len(resampled) < target_count:
        resampled.append(points[-1])

    return resampled[:target_count]


def compute_path_similarity(
    path_a: List[Point],
    path_b: List[Point],
    normalize: bool = True,
) -> GestureSimilarityResult:
    """
    Compute similarity between two gesture paths using point-wise
    Euclidean distance after resampling to the same resolution.

    Args:
        path_a: First gesture path.
        path_b: Second gesture path.
        normalize: Whether to normalize score to [0, 1].

    Returns:
        GestureSimilarityResult with raw and normalized scores.
    """
    n = max(len(path_a), len(path_b), 10)
    ra = resample_path(path_a, n)
    rb = resample_path(path_b, n)

    errors = [
        math.sqrt((ra[i].x - rb[i].x) ** 2 + (ra[i].y - rb[i].y) ** 2)
        for i in range(n)
    ]
    point_error = sum(errors) / n

    # Scale error to a [0, 1] score
    max_acceptable_error = 50.0  # pixels
    score = max(0.0, 1.0 - point_error / max_acceptable_error)

    if normalize:
        score = min(1.0, max(0.0, score))

    return GestureSimilarityResult(
        score=score,
        normalized_score=score,
        point_error=point_error,
    )
