"""
Input matching score utilities.

This module provides utilities for computing similarity scores
between input sequences and reference patterns.
"""

from __future__ import annotations

import math
from typing import List, Tuple, Optional, Dict, Any
from dataclasses import dataclass, field


# Type aliases
Point2D = Tuple[float, float]
Sequence = List[float]


@dataclass
class MatchScore:
    """Result of matching two input sequences."""
    overall_score: float
    normalized_score: float
    confidence: float
    matched_indices: List[int]
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class DTWResult:
    """Result of Dynamic Time Warping alignment."""
    distance: float
    path: List[Tuple[int, int]]
    normalized_distance: float


def euclidean_distance(p1: Point2D, p2: Point2D) -> float:
    """Compute Euclidean distance between two points."""
    dx = p1[0] - p2[0]
    dy = p1[1] - p2[1]
    return math.sqrt(dx * dx + dy * dy)


def compute_mae(predicted: Sequence, actual: Sequence) -> float:
    """Compute Mean Absolute Error between two sequences."""
    if len(predicted) != len(actual):
        min_len = min(len(predicted), len(actual))
        predicted = predicted[:min_len]
        actual = actual[:min_len]
    if not predicted:
        return 0.0
    return sum(abs(p - a) for p, a in zip(predicted, actual)) / len(predicted)


def compute_mse(predicted: Sequence, actual: Sequence) -> float:
    """Compute Mean Squared Error between two sequences."""
    if len(predicted) != len(actual):
        min_len = min(len(predicted), len(actual))
        predicted = predicted[:min_len]
        actual = actual[:min_len]
    if not predicted:
        return 0.0
    return sum((p - a) ** 2 for p, a in zip(predicted, actual)) / len(predicted)


def compute_correlation(predicted: Sequence, actual: Sequence) -> float:
    """Compute Pearson correlation coefficient."""
    if len(predicted) != len(actual):
        min_len = min(len(predicted), len(actual))
        predicted = predicted[:min_len]
        actual = actual[:min_len]
    if len(predicted) < 2:
        return 1.0 if predicted == actual else 0.0

    mean_p = sum(predicted) / len(predicted)
    mean_a = sum(actual) / len(actual)

    cov = sum((p - mean_p) * (a - mean_a) for p, a in zip(predicted, actual))
    std_p = math.sqrt(sum((p - mean_p) ** 2 for p in predicted))
    std_a = math.sqrt(sum((a - mean_a) ** 2 for a in actual))

    if std_p < 1e-10 or std_a < 1e-10:
        return 0.0

    return cov / (std_p * std_a)


def compute_dtw_distance(seq1: Sequence, seq2: Sequence, window: Optional[int] = None) -> DTWResult:
    """
    Compute Dynamic Time Warping distance between two sequences.

    Args:
        seq1: First sequence.
        seq2: Second sequence.
        window: Sakoe-Chiba window width.

    Returns:
        DTWResult with distance and alignment path.
    """
    n, m = len(seq1), len(seq2)
    if n == 0 or m == 0:
        return DTWResult(distance=float('inf'), path=[], normalized_distance=float('inf'))

    if window is None:
        window = max(n, m)

    # Initialize cost matrix
    INF = float('inf')
    dtw: List[List[float]] = [[INF] * (m + 1) for _ in range(n + 1)]
    dtw[0][0] = 0.0

    # Fill cost matrix
    for i in range(1, n + 1):
        for j in range(1, m + 1):
            cost = abs(seq1[i - 1] - seq2[j - 1])
            dtw[i][j] = cost + min(dtw[i - 1][j], dtw[i][j - 1], dtw[i - 1][j - 1])

    # Backtrack to find path
    path: List[Tuple[int, int]] = []
    i, j = n, m
    while i > 0 and j > 0:
        path.append((i - 1, j - 1))
        candidates = [
            (dtw[i - 1][j - 1], i - 1, j - 1),
            (dtw[i - 1][j], i - 1, j),
            (dtw[i][j - 1], i, j - 1),
        ]
        _, i, j = min(candidates, key=lambda x: x[0])

    path.reverse()

    distance = dtw[n][m]
    normalized = distance / max(n, m)

    return DTWResult(distance=distance, path=path, normalized_distance=normalized)


def compute_similarity_score(
    predicted: Sequence,
    actual: Sequence,
    method: str = "combined",
) -> MatchScore:
    """
    Compute overall similarity score between sequences.

    Args:
        predicted: Predicted/observed sequence.
        actual: Reference/expected sequence.
        method: Scoring method ("mae", "mse", "dtw", "combined").

    Returns:
        MatchScore with comprehensive scoring.
    """
    if not predicted or not actual:
        return MatchScore(overall_score=0.0, normalized_score=0.0, confidence=0.0, matched_indices=[])

    mae = compute_mae(predicted, actual)
    mse = compute_mse(predicted, actual)
    dtw_result = compute_dtw_distance(predicted, actual)

    # Normalize scores to 0-1 (higher is better)
    mae_score = max(0.0, 1.0 - min(1.0, mae / 100.0))
    mse_score = max(0.0, 1.0 - min(1.0, math.sqrt(mse) / 100.0))
    dtw_score = max(0.0, 1.0 - min(1.0, dtw_result.normalized_distance / 50.0))
    correlation = compute_correlation(predicted, actual)
    correlation_score = (correlation + 1.0) / 2.0  # Normalize to 0-1

    if method == "mae":
        overall = mae_score
    elif method == "mse":
        overall = mse_score
    elif method == "dtw":
        overall = dtw_score
    elif method == "combined":
        overall = (mae_score * 0.3 + dtw_score * 0.3 + correlation_score * 0.4)
    else:
        overall = (mae_score + dtw_score + correlation_score) / 3.0

    # Confidence based on sequence lengths
    confidence = min(1.0, min(len(predicted), len(actual)) / 10.0)

    # Matched indices from DTW path
    matched = [p[0] for p in dtw_result.path]

    return MatchScore(
        overall_score=overall,
        normalized_score=overall,
        confidence=confidence,
        matched_indices=matched,
        metadata={
            "mae": mae,
            "mse": mse,
            "dtw_distance": dtw_result.distance,
            "correlation": correlation,
            "mae_score": mae_score,
            "dtw_score": dtw_score,
            "correlation_score": correlation_score,
        },
    )
