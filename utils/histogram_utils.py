"""Histogram and statistics utilities for RabAI AutoClick.

Provides:
- Histogram computation and analysis
- Statistical moments (mean, variance, skew, kurtosis)
- Distribution fitting
- Quantile computation
"""

from typing import List, Tuple, Optional, Callable
import math


def compute_histogram(
    values: List[float],
    num_bins: int = 10,
    bin_min: Optional[float] = None,
    bin_max: Optional[float] = None,
) -> Tuple[List[float], List[float], List[float]]:
    """Compute histogram of values.

    Args:
        values: Input data.
        num_bins: Number of bins.
        bin_min: Optional minimum (default: min(values)).
        bin_max: Optional maximum (default: max(values)).

    Returns:
        (bin_edges, bin_centers, bin_counts).
    """
    if not values:
        return ([], [], [])

    mn = bin_min if bin_min is not None else min(values)
    mx = bin_max if bin_max is not None else max(values)

    if abs(mx - mn) < 1e-10:
        return ([mn, mx], [(mn + mx) / 2], [len(values)])

    bin_width = (mx - mn) / num_bins
    edges = [mn + i * bin_width for i in range(num_bins + 1)]
    centers = [(edges[i] + edges[i + 1]) / 2 for i in range(num_bins)]
    counts = [0] * num_bins

    for v in values:
        bin_idx = int((v - mn) / bin_width)
        bin_idx = max(0, min(num_bins - 1, bin_idx))
        counts[bin_idx] += 1

    return (edges, centers, counts)


def histogram_pdf(
    counts: List[float],
    total: Optional[float] = None,
) -> List[float]:
    """Convert histogram counts to probability densities.

    Args:
        counts: Bin counts.
        total: Optional total (default: sum(counts)).

    Returns:
        Probability density per bin.
    """
    if total is None:
        total = sum(counts)
    if total < 1e-10:
        return [0.0] * len(counts)
    return [c / total for c in counts]


def histogram_cdf(
    counts: List[float],
) -> List[float]:
    """Compute cumulative distribution from histogram.

    Args:
        counts: Bin counts.

    Returns:
        Cumulative probabilities.
    """
    total = sum(counts)
    if total < 1e-10:
        return [0.0] * len(counts)
    cumsum = 0.0
    result: List[float] = []
    for c in counts:
        cumsum += c
        result.append(cumsum / total)
    return result


def percentile(
    values: List[float],
    p: float,
) -> float:
    """Compute percentile of values.

    Args:
        values: Input data.
        p: Percentile [0, 100].

    Returns:
        Percentile value.
    """
    if not values:
        return 0.0
    sorted_vals = sorted(values)
    idx = (p / 100.0) * (len(sorted_vals) - 1)
    lo = int(idx)
    hi = min(lo + 1, len(sorted_vals) - 1)
    frac = idx - lo
    return sorted_vals[lo] * (1 - frac) + sorted_vals[hi] * frac


def quartiles(values: List[float]) -> Tuple[float, float, float]:
    """Compute Q1, Q2 (median), Q3."""
    return (percentile(values, 25), percentile(values, 50), percentile(values, 75))


def mean(values: List[float]) -> float:
    """Compute arithmetic mean."""
    return sum(values) / len(values) if values else 0.0


def variance(values: List[float]) -> float:
    """Compute variance."""
    if len(values) < 2:
        return 0.0
    m = mean(values)
    return sum((v - m) ** 2 for v in values) / (len(values) - 1)


def std_dev(values: List[float]) -> float:
    """Compute standard deviation."""
    return math.sqrt(variance(values))


def skewness(values: List[float]) -> float:
    """Compute skewness (3rd standardized moment)."""
    if len(values) < 3:
        return 0.0
    m = mean(values)
    s = std_dev(values)
    if s < 1e-10:
        return 0.0
    n = len(values)
    return (n / ((n - 1) * (n - 2))) * sum(((v - m) / s) ** 3 for v in values)


def kurtosis(values: List[float]) -> float:
    """Compute kurtosis (4th standardized moment - 3)."""
    if len(values) < 4:
        return 0.0
    m = mean(values)
    s = std_dev(values)
    if s < 1e-10:
        return 0.0
    n = len(values)
    avg = sum(((v - m) / s) ** 4 for v in values) / n
    return avg - 3


def median_absolute_deviation(values: List[float]) -> float:
    """Compute MAD for outlier detection."""
    med = percentile(values, 50)
    return percentile([abs(v - med) for v in values], 50)


def iqr(values: List[float]) -> float:
    """Interquartile range."""
    q1, _, q3 = quartiles(values)
    return q3 - q1


def detect_outliers_iqr(
    values: List[float],
    multiplier: float = 1.5,
) -> List[int]:
    """Detect outliers using IQR method.

    Returns:
        List of outlier indices.
    """
    q1, q2, q3 = quartiles(values)
    iqr_val = q3 - q1
    lower = q1 - multiplier * iqr_val
    upper = q3 + multiplier * iqr_val
    return [i for i, v in enumerate(values) if v < lower or v > upper]


def detect_outliers_zscore(
    values: List[float],
    threshold: float = 3.0,
) -> List[int]:
    """Detect outliers using Z-score method."""
    m = mean(values)
    s = std_dev(values)
    if s < 1e-10:
        return []
    return [i for i, v in enumerate(values) if abs((v - m) / s) > threshold]


def mode(values: List[float], tolerance: float = 1e-6) -> Optional[float]:
    """Find most common value (mode)."""
    if not values:
        return None
    sorted_vals = sorted(values)
    best_val = sorted_vals[0]
    best_count = 1
    current_val = sorted_vals[0]
    current_count = 1
    for v in sorted_vals[1:]:
        if abs(v - current_val) < tolerance:
            current_count += 1
        else:
            if current_count > best_count:
                best_count = current_count
                best_val = current_val
            current_val = v
            current_count = 1
    if current_count > best_count:
        best_val = current_val
    return best_val


def histogram_entropy(counts: List[float]) -> float:
    """Compute entropy of histogram distribution."""
    total = sum(counts)
    if total < 1e-10:
        return 0.0
    h = 0.0
    for c in counts:
        if c > 0:
            p = c / total
            h -= p * math.log2(p)
    return h


def histogram_intersection(
    hist1: List[float],
    hist2: List[float],
) -> float:
    """Compute histogram intersection similarity."""
    if len(hist1) != len(hist2):
        return 0.0
    total1 = sum(hist1)
    total2 = sum(hist2)
    if total1 < 1e-10 or total2 < 1e-10:
        return 0.0
    return sum(min(h1 / total1, h2 / total2) for h1, h2 in zip(hist1, hist2))


def bhattacharyya_distance(
    hist1: List[float],
    hist2: List[float],
) -> float:
    """Compute Bhattacharyya distance between histograms."""
    if len(hist1) != len(hist2):
        return float("inf")
    total1 = sum(hist1)
    total2 = sum(hist2)
    if total1 < 1e-10 or total2 < 1e-10:
        return 0.0
    sum_sq = sum(math.sqrt((h1 / total1) * (h2 / total2)) for h1, h2 in zip(hist1, hist2))
    return -math.log(sum_sq) if sum_sq > 0 else float("inf")
