"""
Statistics utilities v2 — advanced statistical methods.

Companion to statistics_utils.py. Adds hypothesis testing,
regression analysis, and distribution fitting.
"""

from __future__ import annotations

import math
import random
from typing import Callable


def pearson_correlation(xs: list[float], ys: list[float]) -> float:
    """
    Compute Pearson correlation coefficient.

    Args:
        xs: First variable samples
        ys: Second variable samples

    Returns:
        Correlation coefficient in [-1, 1]
    """
    n = len(xs)
    if n != len(ys) or n < 2:
        return 0.0
    mean_x = sum(xs) / n
    mean_y = sum(ys) / n
    num = sum((xs[i] - mean_x) * (ys[i] - mean_y) for i in range(n))
    den_x = math.sqrt(sum((x - mean_x) ** 2 for x in xs))
    den_y = math.sqrt(sum((y - mean_y) ** 2 for y in ys))
    if den_x * den_y == 0:
        return 0.0
    return num / (den_x * den_y)


def spearman_rank_correlation(xs: list[float], ys: list[float]) -> float:
    """
    Compute Spearman's rank correlation coefficient.

    Args:
        xs: First variable samples
        ys: Second variable samples

    Returns:
        Rank correlation in [-1, 1]
    """
    def rank(data: list[float]) -> list[float]:
        sorted_idx = sorted(range(len(data)), key=lambda i: data[i])
        ranks = [0.0] * len(data)
        i = 0
        while i < len(data):
            j = i
            while j < len(data) - 1 and data[sorted_idx[j]] == data[sorted_idx[j + 1]]:
                j += 1
            avg_rank = (i + j) / 2
            for k in range(i, j + 1):
                ranks[sorted_idx[k]] = avg_rank
            i = j + 1
        return ranks
    rx, ry = rank(xs), rank(ys)
    return pearson_correlation(rx, ry)


def t_test(sample1: list[float], sample2: list[float]) -> tuple[float, float]:
    """
    Two-sample independent t-test.

    Returns:
        Tuple of (t-statistic, p-value, degrees of freedom)
    """
    n1, n2 = len(sample1), len(sample2)
    m1, m2 = sum(sample1) / n1, sum(sample2) / n2
    v1 = sum((x - m1) ** 2 for x in sample1) / (n1 - 1)
    v2 = sum((x - m2) ** 2 for x in sample2) / (n2 - 1)
    pooled_se = math.sqrt(v1 / n1 + v2 / n2)
    if pooled_se == 0:
        return 0.0, 1.0
    t = (m1 - m2) / pooled_se
    df = min(n1 - 1, n2 - 1)
    p = 2 * (1 - student_t_cdf(abs(t), df))
    return t, p


def student_t_cdf(t: float, df: int) -> float:
    """Approximate Student's t CDF."""
    from math import gamma
    x = df / (df + t * t)
    return 1 - 0.5 * student_t_beta(df / 2, 0.5, x)


def student_t_beta(a: float, b: float, x: float) -> float:
    """Incomplete beta function approximation."""
    import math
    return math.beta(a, b) * student_t_betainc(a, b, x)


def student_t_betainc(a: float, b: float, x: float) -> float:
    """Continued fraction approximation of incomplete beta."""
    bt = math.exp(math.lgamma(a + b) - math.lgamma(a) - math.lgamma(b) + a * math.log(x) + b * math.log(1 - x))
    if x < (a + 1) / (a + b + 2):
        return bt * student_t_betacf(a, b, x) / a
    return 1 - bt * student_t_betacf(b, a, 1 - x) / b


def student_t_betacf(a: float, b: float, x: float) -> float:
    """Continued fraction for incomplete beta."""
    m, aa, c, d = 0, 1.0, 1.0, 0.0
    for i in range(200):
        m2 = 2 * m
        aa = (a + m) * (a + b + m) * x / ((a + 2 * m) * (a + 2 * m + 1))
        d = 1 + aa * d
        if abs(d) < 1e-30:
            d = 1e-30
        c = 1 + aa / c
        if abs(c) < 1e-30:
            c = 1e-30
        d = 1 / d
        del_ = c * d
        aa = -(a + m) * (a + b + m) * x / ((a + 2 * m - 1) * (a + 2 * m))
        c = 1 + aa / c
        d = 1 / d
        del_ = c * d
        if abs(del_ - 1) < 1e-7:
            break
    return del_


def chi_square_test(observed: list[list[float]]) -> tuple[float, float]:
    """
    Chi-square test for independence.

    Args:
        observed: Contingency table

    Returns:
        Tuple of (chi-square statistic, p-value)
    """
    rows = len(observed)
    cols = len(observed[0])
    row_totals = [sum(observed[i]) for i in range(rows)]
    col_totals = [sum(observed[i][j] for i in range(rows)) for j in range(cols)]
    total = sum(row_totals)
    if total == 0:
        return 0.0, 1.0
    chi_sq = 0.0
    for i in range(rows):
        for j in range(cols):
            expected = row_totals[i] * col_totals[j] / total
            if expected > 0:
                chi_sq += (observed[i][j] - expected) ** 2 / expected
    df = (rows - 1) * (cols - 1)
    p = 1 - chi_square_cdf(chi_sq, df)
    return chi_sq, p


def chi_square_cdf(x: float, df: int) -> float:
    """Chi-square CDF approximation."""
    import math
    if x <= 0:
        return 0.0
    k = df / 2
    return math.gammainc(k, x / 2)


def linear_regression(xs: list[float], ys: list[float]) -> tuple[float, float, float]:
    """
    Simple linear regression: y = mx + b.

    Returns:
        Tuple of (slope, intercept, r_squared)
    """
    n = len(xs)
    if n < 2:
        return 0.0, 0.0, 0.0
    mean_x, mean_y = sum(xs) / n, sum(ys) / n
    ss_xy = sum((xs[i] - mean_x) * (ys[i] - mean_y) for i in range(n))
    ss_xx = sum((x - mean_x) ** 2 for x in xs)
    if ss_xx == 0:
        return 0.0, mean_y, 0.0
    m = ss_xy / ss_xx
    b = mean_y - m * mean_x
    ss_res = sum((ys[i] - m * xs[i] - b) ** 2 for i in range(n))
    ss_tot = sum((y - mean_y) ** 2 for y in ys)
    r_sq = 1 - ss_res / ss_tot if ss_tot > 0 else 0.0
    return m, b, r_sq


def bootstrap_ci(
    data: list[float],
    statistic: Callable[[list[float]], float],
    n_resamples: int = 10000,
    confidence: float = 0.95,
    seed: int | None = None,
) -> tuple[float, float]:
    """
    Bootstrap confidence interval.

    Args:
        data: Sample data
        statistic: Function to compute statistic
        n_resamples: Number of bootstrap samples
        confidence: Confidence level
        seed: Random seed

    Returns:
        Tuple of (lower, upper) bounds
    """
    if seed is not None:
        random.seed(seed)
    n = len(data)
    obs_stat = statistic(data)
    resampled = []
    for _ in range(n_resamples):
        sample = random.choices(data, k=n)
        resampled.append(statistic(sample))
    alpha = (1 - confidence) / 2
    lower = sorted(resampled)[int(alpha * n_resamples)]
    upper = sorted(resampled)[int((1 - alpha) * n_resamples)]
    return lower, upper


def kmeans(
    points: list[tuple[float, float]],
    k: int,
    max_iter: int = 100,
    seed: int | None = None,
) -> tuple[list[tuple[float, float]], list[int]]:
    """
    K-means clustering.

    Returns:
        Tuple of (centroids, assignments)
    """
    if seed is not None:
        random.seed(seed)
    n = len(points)
    centroids = random.sample(points, k)
    assignments = [0] * n
    for _ in range(max_iter):
        for i, p in enumerate(points):
            assignments[i] = min(range(k), key=lambda j: (p[0] - centroids[j][0])**2 + (p[1] - centroids[j][1])**2)
        new_centroids = [(0.0, 0.0)] * k
        counts = [0] * k
        for i, c in enumerate(assignments):
            new_centroids[c] = (new_centroids[c][0] + points[i][0], new_centroids[c][1] + points[i][1])
            counts[c] += 1
        for j in range(k):
            if counts[j] > 0:
                new_centroids[j] = (new_centroids[j][0] / counts[j], new_centroids[j][1] / counts[j])
            else:
                new_centroids[j] = points[random.randint(0, n - 1)]
        if new_centroids == centroids:
            break
        centroids = new_centroids
    return centroids, assignments
