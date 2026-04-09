"""Data Analyzer and Statistics.

This module provides statistical analysis:
- Descriptive statistics
- Distribution analysis
- Correlation computation
- Outlier detection

Example:
    >>> from actions.data_analyzer_action import DataAnalyzer
    >>> analyzer = DataAnalyzer()
    >>> stats = analyzer.analyze(numeric_column)
"""

from __future__ import annotations

import math
import logging
import threading
from typing import Any, Optional
from collections import Counter

logger = logging.getLogger(__name__)


class DataAnalyzer:
    """Statistical data analyzer."""

    def __init__(self) -> None:
        """Initialize the data analyzer."""
        self._lock = threading.Lock()
        self._stats = {"analyses": 0}

    def analyze(self, values: list[float]) -> dict[str, Any]:
        """Perform comprehensive statistical analysis.

        Args:
            values: List of numeric values.

        Returns:
            Dict with statistical measures.
        """
        with self._lock:
            self._stats["analyses"] += 1

        if not values:
            return {}

        sorted_vals = sorted(values)
        n = len(sorted_vals)

        return {
            "count": n,
            "sum": sum(values),
            "mean": sum(values) / n,
            "median": sorted_vals[n // 2],
            "min": sorted_vals[0],
            "max": sorted_vals[-1],
            "range": sorted_vals[-1] - sorted_vals[0],
            "variance": self._variance(values),
            "std_dev": self._std_dev(values),
            "cv": self._coefficient_of_variation(values),
            "skewness": self._skewness(values),
            "kurtosis": self._kurtosis(values),
            "percentiles": self._percentiles(sorted_vals),
            "outliers": self._detect_outliers(values),
        }

    def _variance(self, values: list[float]) -> float:
        """Calculate variance."""
        if len(values) < 2:
            return 0.0
        mean = sum(values) / len(values)
        return sum((x - mean) ** 2 for x in values) / (len(values) - 1)

    def _std_dev(self, values: list[float]) -> float:
        """Calculate standard deviation."""
        return math.sqrt(self._variance(values))

    def _coefficient_of_variation(self, values: list[float]) -> float:
        """Calculate coefficient of variation."""
        if not values:
            return 0.0
        mean = sum(values) / len(values)
        if mean == 0:
            return 0.0
        return self._std_dev(values) / abs(mean)

    def _skewness(self, values: list[float]) -> float:
        """Calculate skewness."""
        if len(values) < 3:
            return 0.0
        mean = sum(values) / len(values)
        std = self._std_dev(values)
        if std == 0:
            return 0.0
        n = len(values)
        return (sum((x - mean) ** 3 for x in values) / n) / (std ** 3)

    def _kurtosis(self, values: list[float]) -> float:
        """Calculate kurtosis."""
        if len(values) < 4:
            return 0.0
        mean = sum(values) / len(values)
        std = self._std_dev(values)
        if std == 0:
            return 0.0
        n = len(values)
        return (sum((x - mean) ** 4 for x in values) / n) / (std ** 4) - 3

    def _percentiles(self, sorted_vals: list[float]) -> dict[str, float]:
        """Calculate percentiles."""
        n = len(sorted_vals)
        return {
            "p10": sorted_vals[int(n * 0.1)],
            "p25": sorted_vals[int(n * 0.25)],
            "p50": sorted_vals[int(n * 0.5)],
            "p75": sorted_vals[int(n * 0.75)],
            "p90": sorted_vals[int(n * 0.9)],
            "p95": sorted_vals[int(n * 0.95)],
            "p99": sorted_vals[int(n * 0.99)] if n >= 100 else sorted_vals[-1],
        }

    def _detect_outliers(self, values: list[float], threshold: float = 1.5) -> list[float]:
        """Detect outliers using IQR method.

        Args:
            values: List of values.
            threshold: IQR multiplier.

        Returns:
            List of outlier values.
        """
        if len(values) < 4:
            return []

        sorted_vals = sorted(values)
        n = len(sorted_vals)
        q1 = sorted_vals[int(n * 0.25)]
        q3 = sorted_vals[int(n * 0.75)]
        iqr = q3 - q1

        lower_bound = q1 - threshold * iqr
        upper_bound = q3 + threshold * iqr

        return [v for v in values if v < lower_bound or v > upper_bound]

    def correlation(
        self,
        x: list[float],
        y: list[float],
    ) -> float:
        """Calculate Pearson correlation coefficient.

        Args:
            x: First variable.
            y: Second variable.

        Returns:
            Correlation coefficient (-1 to 1).
        """
        if len(x) != len(y) or len(x) < 2:
            return 0.0

        n = len(x)
        mean_x = sum(x) / n
        mean_y = sum(y) / n

        numerator = sum((x[i] - mean_x) * (y[i] - mean_y) for i in range(n))
        denom_x = math.sqrt(sum((xi - mean_x) ** 2 for xi in x))
        denom_y = math.sqrt(sum((yi - mean_y) ** 2 for yi in y))

        if denom_x == 0 or denom_y == 0:
            return 0.0

        return numerator / (denom_x * denom_y)

    def frequency_analysis(
        self,
        values: list[Any],
        top_n: int = 10,
    ) -> dict[str, Any]:
        """Analyze value frequencies.

        Args:
            values: List of values.
            top_n: Number of top values to return.

        Returns:
            Dict with frequency analysis.
        """
        counter = Counter(values)
        total = len(values)

        top = counter.most_common(top_n)
        return {
            "total_unique": len(counter),
            "total_count": total,
            "top_values": [{"value": v, "count": c, "percentage": round(c / total * 100, 2)} for v, c in top],
        }

    def moving_average(
        self,
        values: list[float],
        window: int,
    ) -> list[float]:
        """Calculate moving average.

        Args:
            values: List of values.
            window: Window size.

        Returns:
            List of moving averages.
        """
        if window <= 0 or window > len(values):
            return []

        result = []
        for i in range(len(values) - window + 1):
            window_vals = values[i:i + window]
            result.append(sum(window_vals) / window)

        return result

    def z_scores(self, values: list[float]) -> list[float]:
        """Calculate z-scores.

        Args:
            values: List of values.

        Returns:
            List of z-scores.
        """
        if not values:
            return []

        mean = sum(values) / len(values)
        std = self._std_dev(values)

        if std == 0:
            return [0.0] * len(values)

        return [(v - mean) / std for v in values]

    def get_stats(self) -> dict[str, int]:
        """Get analyzer statistics."""
        with self._lock:
            return dict(self._stats)
