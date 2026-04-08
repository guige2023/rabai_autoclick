"""Data Statistics Action Module.

Provides statistical analysis: descriptive statistics,
distributions, correlations, and outlier detection.
"""
from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Tuple, TypeVar
import math

T = TypeVar("T")


@dataclass
class DescriptiveStats:
    """Descriptive statistics."""
    count: int = 0
    sum: float = 0.0
    mean: float = 0.0
    median: float = 0.0
    mode: Any = None
    stddev: float = 0.0
    variance: float = 0.0
    min: float = 0.0
    max: float = 0.0
    q1: float = 0.0
    q3: float = 0.0


@dataclass
class CorrelationResult:
    """Correlation result between two variables."""
    variable_x: str
    variable_y: str
    pearson: float = 0.0
    spearman: float = 0.0


class DataStatisticsAction:
    """Statistical analysis for data.

    Example:
        stats = DataStatisticsAction()

        result = stats.describe([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
        print(result.mean)  # 5.5
        print(result.stddev)
    """

    def __init__(self) -> None:
        pass

    def describe(self, data: List[float]) -> DescriptiveStats:
        """Calculate descriptive statistics.

        Args:
            data: List of numeric values

        Returns:
            DescriptiveStats object
        """
        if not data:
            return DescriptiveStats()

        sorted_data = sorted(data)
        n = len(data)
        total = sum(data)
        mean_val = total / n

        variance_val = sum((x - mean_val) ** 2 for x in data) / n
        stddev_val = math.sqrt(variance_val)

        median_val = self._median(sorted_data)
        mode_val = self._mode(data)
        q1_val = self._percentile(sorted_data, 25)
        q3_val = self._percentile(sorted_data, 75)

        return DescriptiveStats(
            count=n,
            sum=total,
            mean=mean_val,
            median=median_val,
            mode=mode_val,
            stddev=stddev_val,
            variance=variance_val,
            min=min(data),
            max=max(data),
            q1=q1_val,
            q3=q3_val,
        )

    def _median(self, sorted_data: List[float]) -> float:
        """Calculate median."""
        n = len(sorted_data)
        mid = n // 2
        if n % 2 == 0:
            return (sorted_data[mid - 1] + sorted_data[mid]) / 2
        return sorted_data[mid]

    def _mode(self, data: List[float]) -> Any:
        """Calculate mode."""
        if not data:
            return None
        counter = Counter(data)
        most_common = counter.most_common(1)
        return most_common[0][0] if most_common else None

    def _percentile(self, sorted_data: List[float], p: float) -> float:
        """Calculate percentile."""
        n = len(sorted_data)
        index = (p / 100) * (n - 1)
        lower = int(index)
        upper = lower + 1
        weight = index - lower

        if upper >= n:
            return sorted_data[-1]
        return sorted_data[lower] * (1 - weight) + sorted_data[upper] * weight

    def correlate(
        self,
        x: List[float],
        y: List[float],
    ) -> CorrelationResult:
        """Calculate correlation between two variables.

        Args:
            x: First variable
            y: Second variable

        Returns:
            CorrelationResult with pearson and spearman correlations
        """
        if len(x) != len(y) or len(x) < 2:
            return CorrelationResult(
                variable_x="x",
                variable_y="y",
                pearson=0.0,
                spearman=0.0,
            )

        n = len(x)
        mean_x = sum(x) / n
        mean_y = sum(y) / n

        cov = sum((x[i] - mean_x) * (y[i] - mean_y) for i in range(n)) / n

        std_x = math.sqrt(sum((xi - mean_x) ** 2 for xi in x) / n)
        std_y = math.sqrt(sum((yi - mean_y) ** 2 for yi in y) / n)

        pearson = cov / (std_x * std_y) if std_x * std_y != 0 else 0.0

        spearman = self._spearman_rank_correlation(
            list(zip(x, y))
        )

        return CorrelationResult(
            variable_x="x",
            variable_y="y",
            pearson=pearson,
            spearman=spearman,
        )

    def _spearman_rank_correlation(self, pairs: List[Tuple]) -> float:
        """Calculate Spearman rank correlation."""
        n = len(pairs)
        if n < 2:
            return 0.0

        ranked_x = sorted(range(n), key=lambda i: pairs[i][0])
        ranked_y = sorted(range(n), key=lambda i: pairs[i][1])

        d_squared = sum((ranked_x[i] - ranked_y[i]) ** 2 for i in range(n))
        spearman = 1 - (6 * d_squared) / (n * (n**2 - 1))

        return spearman

    def detect_outliers(
        self,
        data: List[float],
        method: str = "iqr",
        threshold: float = 1.5,
    ) -> List[int]:
        """Detect outliers in data.

        Args:
            data: List of numeric values
            method: Detection method ("iqr" or "zscore")
            threshold: Threshold for outlier detection

        Returns:
            List of indices of outliers
        """
        if not data:
            return []

        if method == "iqr":
            return self._detect_outliers_iqr(data, threshold)
        elif method == "zscore":
            return self._detect_outliers_zscore(data, threshold)

        return []

    def _detect_outliers_iqr(
        self,
        data: List[float],
        threshold: float,
    ) -> List[int]:
        """Detect outliers using IQR method."""
        sorted_data = sorted(data)
        q1 = self._percentile(sorted_data, 25)
        q3 = self._percentile(sorted_data, 75)
        iqr = q3 - q1

        lower_bound = q1 - threshold * iqr
        upper_bound = q3 + threshold * iqr

        outliers = [
            i for i, value in enumerate(data)
            if value < lower_bound or value > upper_bound
        ]

        return outliers

    def _detect_outliers_zscore(
        self,
        data: List[float],
        threshold: float,
    ) -> List[int]:
        """Detect outliers using Z-score method."""
        mean_val = sum(data) / len(data)
        std_val = math.sqrt(sum((x - mean_val) ** 2 for x in data) / len(data))

        if std_val == 0:
            return []

        outliers = [
            i for i, value in enumerate(data)
            if abs((value - mean_val) / std_val) > threshold
        ]

        return outliers

    def histogram(
        self,
        data: List[float],
        bins: int = 10,
    ) -> Tuple[List[float], List[int]]:
        """Create histogram of data.

        Args:
            data: List of numeric values
            bins: Number of bins

        Returns:
            Tuple of (bin_edges, frequencies)
        """
        if not data:
            return [], []

        min_val = min(data)
        max_val = max(data)
        bin_width = (max_val - min_val) / bins

        edges = [min_val + i * bin_width for i in range(bins + 1)]
        frequencies = [0] * bins

        for value in data:
            bin_index = min(int((value - min_val) / bin_width), bins - 1)
            frequencies[bin_index] += 1

        return edges, frequencies
