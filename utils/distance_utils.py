"""
Distance and metric utilities.

Provides various distance metrics: Euclidean, Manhattan, Chebyshev,
Hamming, edit distance, DTW, Mahalanobis, and Minkowski.
"""

from __future__ import annotations

import math
from typing import Callable, Sequence


def euclidean(a: Sequence[float], b: Sequence[float]) -> float:
    """Euclidean (L2) distance."""
    return math.sqrt(sum((x - y) ** 2 for x, y in zip(a, b)))


def manhattan(a: Sequence[float], b: Sequence[float]) -> float:
    """Manhattan (L1/cityblock) distance."""
    return sum(abs(x - y) for x, y in zip(a, b))


def chebyshev(a: Sequence[float], b: Sequence[float]) -> float:
    """Chebyshev (L-infinity) distance."""
    return max(abs(x - y) for x, y in zip(a, b))


def minkowski(a: Sequence[float], b: Sequence[float], p: float = 3.0) -> float:
    """
    Minkowski distance (generalization of L1, L2, L-infinity).

    Args:
        p: Order (1=Manhattan, 2=Euclidean, inf=Chebyshev)
    """
    if p == float("inf"):
        return chebyshev(a, b)
    if p <= 0:
        raise ValueError("p must be positive")
    if p == 1:
        return manhattan(a, b)
    if p == 2:
        return euclidean(a, b)
    return sum(abs(x - y) ** p for x, y in zip(a, b)) ** (1.0 / p)


def cosine_distance(a: Sequence[float], b: Sequence[float]) -> float:
    """Cosine distance = 1 - cosine similarity."""
    dot = sum(x * y for x, y in zip(a, b))
    norm_a = math.sqrt(sum(x * x for x in a))
    norm_b = math.sqrt(sum(y * y for y in b))
    if norm_a == 0 or norm_b == 0:
        return 1.0
    return 1.0 - dot / (norm_a * norm_b)


def cosine_similarity(a: Sequence[float], b: Sequence[float]) -> float:
    """Cosine similarity = 1 - cosine_distance."""
    return 1.0 - cosine_distance(a, b)


def hamming_distance(a: str | list, b: str | list) -> int:
    """Hamming distance (number of positions that differ)."""
    return sum(1 for x, y in zip(a, b) if x != y)


def jaccard_distance(a: frozenset, b: frozenset) -> float:
    """Jaccard distance = 1 - Jaccard similarity."""
    if not a and not b:
        return 0.0
    intersection = len(a & b)
    union = len(a | b)
    return 1.0 - intersection / union if union > 0 else 0.0


def jaccard_similarity(a: frozenset, b: frozenset) -> float:
    """Jaccard similarity coefficient."""
    if not a and not b:
        return 1.0
    union = len(a | b)
    return len(a & b) / union if union > 0 else 0.0


def mahalanobis_distance(
    x: Sequence[float],
    mean: Sequence[float],
    cov_inv: Sequence[Sequence[float]],
) -> float:
    """
    Mahalanobis distance accounting for correlation.

    Args:
        x: Point
        mean: Distribution mean
        cov_inv: Inverse covariance matrix

    Returns:
        Mahalanobis distance.
    """
    diff = [xi - mi for xi, mi in zip(x, mean)]
    # diff^T * cov_inv * diff
    result = 0.0
    for i, di in enumerate(diff):
        for j, dj in enumerate(diff):
            result += diff[i] * cov_inv[i][j] * diff[j]
    return math.sqrt(result)


def dynamic_time_warping(
    seq1: list[float],
    seq2: list[float],
    metric: Callable[[float, float], float] | None = None,
) -> float:
    """
    Dynamic Time Warping distance.

    Args:
        seq1: First sequence
        seq2: Second sequence
        metric: Point-wise distance function (default: squared diff)

    Returns:
        DTW distance.
    """
    if metric is None:
        metric = lambda a, b: (a - b) ** 2

    n, m = len(seq1), len(seq2)
    dp: list[list[float]] = [[float("inf")] * (m + 1) for _ in range(n + 1)]
    dp[0][0] = 0.0

    for i in range(1, n + 1):
        for j in range(1, m + 1):
            cost = metric(seq1[i - 1], seq2[j - 1])
            dp[i][j] = cost + min(dp[i - 1][j], dp[i][j - 1], dp[i - 1][j - 1])

    return math.sqrt(dp[n][m])


def edit_distance(s: str, t: str) -> int:
    """Levenshtein edit distance (recursive with memoization)."""
    m, n = len(s), len(t)
    dp: list[list[int]] = [[0] * (n + 1) for _ in range(m + 1)]
    for i in range(m + 1):
        dp[i][0] = i
    for j in range(n + 1):
        dp[0][j] = j
    for i in range(1, m + 1):
        for j in range(1, n + 1):
            if s[i - 1] == t[j - 1]:
                dp[i][j] = dp[i - 1][j - 1]
            else:
                dp[i][j] = 1 + min(dp[i - 1][j], dp[i][j - 1], dp[i - 1][j - 1])
    return dp[m][n]


def sorensen_dice_distance(s1: str, s2: str) -> float:
    """Sorensen-Dice distance = 1 - Dice coefficient."""
    def bigrams(s: str) -> set[str]:
        return {s[i:i+2] for i in range(len(s) - 1)} if len(s) >= 2 else set()
    bi1, bi2 = bigrams(s1), bigrams(s2)
    if not bi1 and not bi2:
        return 0.0
    intersection = len(bi1 & bi2)
    return 1.0 - 2 * intersection / (len(bi1) + len(bi2)) if (len(bi1) + len(bi2)) > 0 else 0.0


def canberra_distance(a: Sequence[float], b: Sequence[float]) -> float:
    """Canberra distance (weighted Manhattan)."""
    return sum(
        abs(a[i] - b[i]) / (abs(a[i]) + abs(b[i]))
        for i in range(len(a))
        if a[i] != 0 or b[i] != 0
    )


def kl_divergence(p: list[float], q: list[float]) -> float:
    """Kullback-Leibler divergence D(P||Q)."""
    d = 0.0
    for pi, qi in zip(p, q):
        if pi > 0:
            if qi <= 0:
                return float("inf")
            d += pi * math.log(pi / qi)
    return d


def wasserstein_distance_1d(a: list[float], b: list[float]) -> float:
    """
    1D Earth Mover's Distance (Wasserstein-1).

    Args:
        a: First distribution (sorted)
        b: Second distribution (sorted)

    Returns:
        Wasserstein-1 distance.
    """
    a_sorted = sorted(a)
    b_sorted = sorted(b)
    return sum(abs(x - y) for x, y in zip(a_sorted, b_sorted)) / len(a_sorted)


def haversine_distance(
    lat1: float, lon1: float,
    lat2: float, lon2: float,
) -> float:
    """
    Great-circle distance between two points on Earth (km).

    Args:
        lat1, lon1: First point coordinates (degrees)
        lat2, lon2: Second point coordinates (degrees)

    Returns:
        Distance in kilometers.
    """
    R = 6371.0  # Earth radius in km
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)

    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return R * c
