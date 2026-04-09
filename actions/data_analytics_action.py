"""Data Analytics Action Module.

Provides data analysis utilities including statistical functions,
aggregation, correlation, and trend analysis.
"""

from __future__ import annotations

import logging
import math
from collections import Counter
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

logger = logging.getLogger(__name__)


@dataclass
class StatSummary:
    """Statistical summary of a dataset."""
    count: int
    sum: float
    mean: float
    median: float
    std_dev: float
    min: float
    max: float
    variance: float


@dataclass
class Distribution:
    """Frequency distribution."""
    values: List[Any]
    frequencies: List[int]
    total: int


class Statistics:
    """Statistical functions."""

    @staticmethod
    def mean(data: List[float]) -> float:
        """Calculate arithmetic mean."""
        if not data:
            return 0.0
        return sum(data) / len(data)

    @staticmethod
    def median(data: List[float]) -> float:
        """Calculate median value."""
        if not data:
            return 0.0
        sorted_data = sorted(data)
        n = len(sorted_data)
        mid = n // 2
        if n % 2 == 0:
            return (sorted_data[mid - 1] + sorted_data[mid]) / 2
        return sorted_data[mid]

    @staticmethod
    def variance(data: List[float]) -> float:
        """Calculate variance."""
        if not data:
            return 0.0
        m = Statistics.mean(data)
        return sum((x - m) ** 2 for x in data) / len(data)

    @staticmethod
    def std_dev(data: List[float]) -> float:
        """Calculate standard deviation."""
        return math.sqrt(Statistics.variance(data))

    @staticmethod
    def percentile(data: List[float], p: float) -> float:
        """Calculate percentile value."""
        if not data:
            return 0.0
        sorted_data = sorted(data)
        k = (len(sorted_data) - 1) * p / 100
        f = math.floor(k)
        c = math.ceil(k)
        if f == c:
            return sorted_data[int(k)]
        return sorted_data[f] * (c - k) + sorted_data[c] * (k - f)

    @staticmethod
    def quartiles(data: List[float]) -> Tuple[float, float, float]:
        """Calculate Q1, Q2 (median), Q3."""
        return (
            Statistics.percentile(data, 25),
            Statistics.percentile(data, 50),
            Statistics.percentile(data, 75)
        )

    @staticmethod
    def iqr(data: List[float]) -> float:
        """Calculate interquartile range."""
        q1, _, q3 = Statistics.quartiles(data)
        return q3 - q1

    @staticmethod
    def summary(data: List[float]) -> StatSummary:
        """Generate complete statistical summary."""
        if not data:
            return StatSummary(0, 0, 0, 0, 0, 0, 0, 0)

        return StatSummary(
            count=len(data),
            sum=sum(data),
            mean=Statistics.mean(data),
            median=Statistics.median(data),
            std_dev=Statistics.std_dev(data),
            min=min(data),
            max=max(data),
            variance=Statistics.variance(data)
        )


class Correlation:
    """Correlation analysis."""

    @staticmethod
    def pearson(x: List[float], y: List[float]) -> float:
        """Calculate Pearson correlation coefficient."""
        if len(x) != len(y) or len(x) < 2:
            return 0.0

        n = len(x)
        mean_x = Statistics.mean(x)
        mean_y = Statistics.mean(y)

        numerator = sum((x[i] - mean_x) * (y[i] - mean_y) for i in range(n))
        denom_x = math.sqrt(sum((xi - mean_x) ** 2 for xi in x))
        denom_y = math.sqrt(sum((yi - mean_y) ** 2 for yi in y))

        if denom_x == 0 or denom_y == 0:
            return 0.0

        return numerator / (denom_x * denom_y)

    @staticmethod
    def spearman(data_x: List[float], data_y: List[float]) -> float:
        """Calculate Spearman rank correlation."""
        if len(data_x) != len(data_y) or len(data_x) < 2:
            return 0.0

        def rank(data: List[float]) -> List[float]:
            sorted_indices = sorted(range(len(data)), key=lambda i: data[i])
            ranks = [0] * len(data)
            for rank_val, idx in enumerate(sorted_indices):
                ranks[idx] = rank_val + 1
            return ranks

        ranks_x = rank(data_x)
        ranks_y = rank(data_y)

        return Correlation.pearson(ranks_x, ranks_y)


class DistributionAnalyzer:
    """Distribution analysis utilities."""

    @staticmethod
    def frequency(data: List[Any]) -> Distribution:
        """Calculate frequency distribution."""
        counter = Counter(data)
        values = list(counter.keys())
        frequencies = list(counter.values())
        return Distribution(
            values=values,
            frequencies=frequencies,
            total=len(data)
        )

    @staticmethod
    def histogram(
        data: List[float],
        bins: int = 10
    ) -> Tuple[List[float], List[int]]:
        """Create histogram bins."""
        if not data:
            return [], []

        min_val = min(data)
        max_val = max(data)
        bin_width = (max_val - min_val) / bins

        bin_edges = [min_val + i * bin_width for i in range(bins + 1)]
        bin_counts = [0] * bins

        for value in data:
            if value == max_val:
                bin_counts[-1] += 1
            else:
                bin_idx = int((value - min_val) / bin_width)
                if 0 <= bin_idx < bins:
                    bin_counts[bin_idx] += 1

        bin_centers = [
            (bin_edges[i] + bin_edges[i + 1]) / 2
            for i in range(bins)
        ]

        return bin_centers, bin_counts

    @staticmethod
    def normalize(data: List[float]) -> List[float]:
        """Normalize data to [0, 1] range."""
        if not data:
            return []
        min_val = min(data)
        max_val = max(data)
        if max_val == min_val:
            return [0.5] * len(data)
        return [(x - min_val) / (max_val - min_val) for x in data]

    @staticmethod
    def standardize(data: List[float]) -> List[float]:
        """Standardize data to z-scores."""
        if not data:
            return []
        mean = Statistics.mean(data)
        std = Statistics.std_dev(data)
        if std == 0:
            return [0.0] * len(data)
        return [(x - mean) / std for x in data]


class TrendAnalyzer:
    """Trend analysis for time series data."""

    @staticmethod
    def linear_trend(data: List[float]) -> Tuple[float, float]:
        """Calculate linear trend (slope, intercept)."""
        if len(data) < 2:
            return 0.0, 0.0

        n = len(data)
        x = list(range(n))
        x_mean = Statistics.mean(x)
        y_mean = Statistics.mean(data)

        numerator = sum((x[i] - x_mean) * (data[i] - y_mean) for i in range(n))
        denominator = sum((x[i] - x_mean) ** 2 for i in range(n))

        if denominator == 0:
            return 0.0, y_mean

        slope = numerator / denominator
        intercept = y_mean - slope * x_mean

        return slope, intercept

    @staticmethod
    def moving_average(
        data: List[float],
        window: int
    ) -> List[float]:
        """Calculate moving average."""
        if window < 1 or window > len(data):
            return data

        result = []
        for i in range(len(data) - window + 1):
            window_data = data[i:i + window]
            result.append(Statistics.mean(window_data))

        return result

    @staticmethod
    def detect_outliers(
        data: List[float],
        threshold: float = 1.5
    ) -> List[int]:
        """Detect outliers using IQR method."""
        if len(data) < 4:
            return []

        q1, median, q3 = Statistics.quartiles(data)
        iqr = q3 - q1
        lower_bound = q1 - threshold * iqr
        upper_bound = q3 + threshold * iqr

        outliers = []
        for i, value in enumerate(data):
            if value < lower_bound or value > upper_bound:
                outliers.append(i)

        return outliers


class Aggregator:
    """Data aggregation utilities."""

    @staticmethod
    def group_by(
        data: List[Dict[str, Any]],
        key: str
    ) -> Dict[Any, List[Dict[str, Any]]]:
        """Group list of dicts by a key field."""
        result: Dict[Any, List[Dict[str, Any]]] = {}
        for item in data:
            if isinstance(item, dict) and key in item:
                group_key = item[key]
                if group_key not in result:
                    result[group_key] = []
                result[group_key].append(item)
        return result

    @staticmethod
    def aggregate(
        data: List[Dict[str, Any]],
        group_key: str,
        agg_field: str,
        agg_func: str = "sum"
    ) -> Dict[Any, float]:
        """Aggregate numeric field by group."""
        grouped = Aggregator.group_by(data, group_key)
        result = {}

        for group_value, items in grouped.items():
            values = [
                item[agg_field]
                for item in items
                if isinstance(item, dict) and agg_field in item
                and isinstance(item[agg_field], (int, float))
            ]

            if agg_func == "sum":
                result[group_value] = sum(values)
            elif agg_func == "mean":
                result[group_value] = Statistics.mean(values)
            elif agg_func == "count":
                result[group_value] = len(values)
            elif agg_func == "min":
                result[group_value] = min(values) if values else 0
            elif agg_func == "max":
                result[group_value] = max(values) if values else 0

        return result


class DataAnalyticsAction:
    """Main action class for data analytics."""

    def __init__(self):
        self._stats = Statistics()

    def analyze(self, data: List[float]) -> Dict[str, Any]:
        """Perform comprehensive analysis on data."""
        summary = Statistics.summary(data)
        q1, q2, q3 = Statistics.quartiles(data)
        outliers = TrendAnalyzer.detect_outliers(data)
        slope, intercept = TrendAnalyzer.linear_trend(data)

        return {
            "summary": {
                "count": summary.count,
                "sum": round(summary.sum, 4),
                "mean": round(summary.mean, 4),
                "median": round(summary.median, 4),
                "std_dev": round(summary.std_dev, 4),
                "min": summary.min,
                "max": summary.max,
                "variance": round(summary.variance, 4)
            },
            "quartiles": {
                "q1": round(q1, 4),
                "q2": round(q2, 4),
                "q3": round(q3, 4),
                "iqr": round(q3 - q1, 4)
            },
            "outliers": outliers,
            "trend": {
                "slope": round(slope, 4),
                "intercept": round(intercept, 4)
            }
        }

    async def execute(
        self,
        context: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Execute the data analytics action.

        Args:
            context: Dictionary containing:
                - operation: Operation to perform
                - data: Data to analyze
                - Other operation-specific fields

        Returns:
            Dictionary with analysis results.
        """
        operation = context.get("operation", "analyze")

        if operation == "analyze":
            data = context.get("data", [])
            if not isinstance(data, list):
                data = [data]
            return self.analyze([float(x) if isinstance(x, (int, float)) else 0 for x in data])

        elif operation == "correlation":
            x = context.get("x", [])
            y = context.get("y", [])
            method = context.get("method", "pearson")

            if method == "spearman":
                r = Correlation.spearman(x, y)
            else:
                r = Correlation.pearson(x, y)

            return {
                "success": True,
                "correlation": round(r, 4),
                "method": method
            }

        elif operation == "histogram":
            data = context.get("data", [])
            bins = context.get("bins", 10)
            centers, counts = DistributionAnalyzer.histogram(data, bins)
            return {
                "success": True,
                "bins": [{"center": c, "count": cnt} for c, cnt in zip(centers, counts)]
            }

        elif operation == "normalize":
            data = context.get("data", [])
            normalized = DistributionAnalyzer.normalize(data)
            return {"success": True, "normalized": normalized}

        elif operation == "moving_average":
            data = context.get("data", [])
            window = context.get("window", 3)
            ma = TrendAnalyzer.moving_average(data, window)
            return {"success": True, "moving_average": ma}

        elif operation == "aggregate":
            data = context.get("data", [])
            group_key = context.get("group_by", "")
            agg_field = context.get("agg_field", "")
            agg_func = context.get("agg_func", "sum")

            result = Aggregator.aggregate(data, group_key, agg_field, agg_func)
            return {"success": True, "aggregated": result}

        else:
            return {"success": False, "error": f"Unknown operation: {operation}"}
