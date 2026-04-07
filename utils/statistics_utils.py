"""
Statistical analysis utilities.

Provides descriptive statistics, hypothesis testing, correlation,
ANOVA, and distribution fitting.
"""

from __future__ import annotations

import math
from typing import Callable


def mean(values: list[float]) -> float:
    """Arithmetic mean."""
    n = len(values)
    if n == 0:
        return 0.0
    return sum(values) / n


def median(values: list[float]) -> float:
    """Median value."""
    n = len(values)
    if n == 0:
        return 0.0
    sorted_vals = sorted(values)
    mid = n // 2
    if n % 2 == 0:
        return (sorted_vals[mid - 1] + sorted_vals[mid]) / 2
    return sorted_vals[mid]


def mode(values: list[float], tol: float = 1e-9) -> float | None:
    """Most frequent value (first one if multiple ties)."""
    if not values:
        return None
    counts: dict[float, int] = {}
    for v in values:
        rounded = round(v, 9)
        counts[rounded] = counts.get(rounded, 0) + 1
    return max(counts, key=counts.get)


def variance(values: list[float], ddof: int = 0) -> float:
    """Population variance (ddof=0) or sample variance (ddof=1)."""
    n = len(values)
    if n <= ddof:
        return 0.0
    m = mean(values)
    return sum((x - m) ** 2 for x in values) / (n - ddof)


def std(values: list[float], ddof: int = 0) -> float:
    """Standard deviation."""
    return math.sqrt(variance(values, ddof))


def sem(values: list[float]) -> float:
    """Standard error of the mean (sample)."""
    n = len(values)
    if n < 2:
        return 0.0
    return std(values, ddof=1) / math.sqrt(n)


def percentile(values: list[float], p: float) -> float:
    """Percentile value (0 <= p <= 100)."""
    if not values:
        return 0.0
    sorted_vals = sorted(values)
    idx = (p / 100.0) * (len(sorted_vals) - 1)
    lo = int(math.floor(idx))
    hi = int(math.ceil(idx))
    if lo == hi:
        return sorted_vals[lo]
    frac = idx - lo
    return sorted_vals[lo] * (1 - frac) + sorted_vals[hi] * frac


def iqr(values: list[float]) -> float:
    """Interquartile range (Q3 - Q1)."""
    return percentile(values, 75) - percentile(values, 25)


def skewness(values: list[float]) -> float:
    """Sample skewness (Fisher-Pearson)."""
    n = len(values)
    if n < 3:
        return 0.0
    m = mean(values)
    s = std(values, ddof=1)
    if s == 0:
        return 0.0
    return (sum((x - m) ** 3 for x in values) / n) / (s ** 3)


def kurtosis(values: list[float]) -> float:
    """Excess kurtosis (Fisher's definition, normal = 0)."""
    n = len(values)
    if n < 4:
        return 0.0
    m = mean(values)
    s = std(values, ddof=1)
    if s == 0:
        return 0.0
    return (sum((x - m) ** 4 for x in values) / n) / (s ** 4) - 3.0


def covariance(xs: list[float], ys: list[float], ddof: int = 0) -> float:
    """Covariance between two variables."""
    n = min(len(xs), len(ys))
    if n <= ddof:
        return 0.0
    mx = mean(xs[:n])
    my = mean(ys[:n])
    return sum((xs[i] - mx) * (ys[i] - my) for i in range(n)) / (n - ddof)


def pearson_correlation(xs: list[float], ys: list[float]) -> float:
    """Pearson correlation coefficient [-1, 1]."""
    n = min(len(xs), len(ys))
    if n < 2:
        return 0.0
    cov = covariance(xs[:n], ys[:n], ddof=1)
    sx = std(xs[:n], ddof=1)
    sy = std(ys[:n], ddof=1)
    if sx == 0 or sy == 0:
        return 0.0
    return cov / (sx * sy)


def spearman_correlation(xs: list[float], ys: list[float]) -> float:
    """Spearman rank correlation coefficient."""
    n = min(len(xs), len(ys))
    if n < 2:
        return 0.0

    def rank(vals: list[float]) -> list[float]:
        sorted_pairs = sorted(enumerate(vals), key=lambda x: x[1])
        ranks = [0.0] * len(vals)
        i = 0
        while i < n:
            j = i
            while j < n - 1 and sorted_pairs[j + 1][1] == sorted_pairs[i][1]:
                j += 1
            avg_rank = (i + j) / 2.0
            for k in range(i, j + 1):
                ranks[sorted_pairs[k][0]] = avg_rank
            i = j + 1
        return ranks

    rx = rank(xs[:n])
    ry = rank(ys[:n])
    return pearson_correlation(rx, ry)


def z_score(values: list[float], x: float) -> float:
    """Z-score of x relative to the dataset."""
    m = mean(values)
    s = std(values, ddof=1)
    if s == 0:
        return 0.0
    return (x - m) / s


def standardize(values: list[float]) -> list[float]:
    """Z-score standardization (mean=0, std=1)."""
    n = len(values)
    if n < 2:
        return [0.0] * n
    m = mean(values)
    s = std(values, ddof=1)
    if s == 0:
        return [0.0] * n
    return [(x - m) / s for x in values]


def min_max_scale(values: list[float], lo: float = 0.0, hi: float = 1.0) -> list[float]:
    """Min-max scaling to [lo, hi]."""
    mn = min(values)
    mx = max(values)
    if abs(mx - mn) < 1e-12:
        return [(lo + hi) / 2.0] * len(values)
    return [lo + (hi - lo) * (x - mn) / (mx - mn) for x in values]


def t_test_paired(xs: list[float], ys: list[float]) -> float:
    """
    Two-sided paired t-test p-value (approximation).

    Returns:
        p-value for the null hypothesis that the paired differences have mean zero.
    """
    n = min(len(xs), len(ys))
    if n < 2:
        return 1.0
    diffs = [xs[i] - ys[i] for i in range(n)]
    d_mean = mean(diffs)
    d_std = std(diffs, ddof=1)
    if d_std == 0:
        return 1.0 if abs(d_mean) < 1e-12 else 0.0
    t_stat = d_mean / (d_std / math.sqrt(n))
    # Approximate p-value using normal distribution
    from probability_utils import normal_cdf
    p_val = 2.0 * (1.0 - normal_cdf(abs(t_stat)))
    return p_val


def chi_square_test(observed: list[float], expected: list[float]) -> float:
    """
    Chi-square statistic for goodness of fit.

    Returns:
        Chi-square test statistic.
    """
    if len(observed) != len(expected):
        raise ValueError("observed and expected must have same length")
    return sum((o - e) ** 2 / e for o, e in zip(observed, expected) if e != 0)


def anova_one_way(groups: list[list[float]]) -> tuple[float, float]:
    """
    One-way ANOVA.

    Args:
        groups: List of groups, each group is a list of values

    Returns:
        Tuple of (F-statistic, p-value approximation).
    """
    k = len(groups)
    if k < 2:
        return 0.0, 1.0
    all_vals = [v for g in groups for v in g]
    N = len(all_vals)
    if N < k:
        return 0.0, 1.0

    grand_mean = mean(all_vals)
    ss_between = sum(len(g) * (mean(g) - grand_mean) ** 2 for g in groups)
    ss_within = sum(sum((x - mean(g)) ** 2 for x in g) for g in groups)

    df_between = k - 1
    df_within = N - k
    if df_within == 0:
        return 0.0, 1.0

    ms_between = ss_between / df_between
    ms_within = ss_within / df_within
    f_stat = ms_between / ms_within if ms_within > 0 else 0.0

    # Approximate p-value using F distribution approximation
    # Using normal approximation for large df
    if df_between > 0 and df_within > 30:
        import probability_utils as pu
        # Convert F to approximate z-score
        f_mean = df_within / (df_within - 2) if df_within > 2 else 1.0
        z = (f_stat - f_mean) / math.sqrt(2 * f_mean ** 2 / df_within)
        p_val = 2.0 * (1.0 - pu.normal_cdf(abs(z)))
    else:
        p_val = 0.05  # fallback

    return f_stat, p_val


def sample_size_needed(
    sigma: float,
    margin: float,
    confidence: float = 0.95,
) -> int:
    """
    Required sample size for estimating a mean.

    Args:
        sigma: Estimated standard deviation
        margin: Desired margin of error
        confidence: Confidence level (default 0.95)

    Returns:
        Required sample size.
    """
    if margin <= 0 or sigma <= 0:
        raise ValueError("margin and sigma must be positive")
    z_scores = {0.90: 1.645, 0.95: 1.96, 0.99: 2.576}
    z = z_scores.get(confidence, 1.96)
    return math.ceil((z * sigma / margin) ** 2)
