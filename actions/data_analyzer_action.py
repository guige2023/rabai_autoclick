"""
Data Analyzer Action Module.

Performs statistical analysis on datasets: descriptive statistics,
distribution analysis, correlation computation, and outlier detection.
"""
from typing import Any, Optional
from dataclasses import dataclass
from actions.base_action import BaseAction


@dataclass
class AnalysisResult:
    """Statistical analysis result."""
    count: int
    sum: float
    mean: float
    median: float
    std_dev: float
    variance: float
    min: float
    max: float
    quartiles: tuple[float, float, float]
    outliers: list[Any]


class DataAnalyzerAction(BaseAction):
    """Statistical analysis of numeric datasets."""

    def __init__(self) -> None:
        super().__init__("data_analyzer")

    def execute(self, context: dict, params: dict) -> dict:
        """
        Analyze numeric data.

        Args:
            context: Execution context
            params: Parameters:
                - records: List of dict records
                - field: Field name to analyze
                - detect_outliers: Whether to detect outliers (default: True)
                - outlier_method: iqr or zscore (default: iqr)

        Returns:
            dict with AnalysisResult and additional metrics
        """
        records = params.get("records", [])
        field = params.get("field", "")
        detect_outliers = params.get("detect_outliers", True)
        outlier_method = params.get("outlier_method", "iqr")

        if not field:
            return {"error": "Field name is required"}

        values = []
        for r in records:
            if isinstance(r, dict) and field in r:
                v = r[field]
                if isinstance(v, (int, float)):
                    values.append(v)

        if not values:
            return {"error": f"No numeric values found for field '{field}'"}

        sorted_vals = sorted(values)
        n = len(sorted_vals)

        import statistics
        mean = statistics.mean(values)
        std_dev = statistics.stdev(values) if n > 1 else 0.0
        variance = statistics.variance(values) if n > 1 else 0.0
        median = statistics.median(values)

        q1_idx = n // 4
        q3_idx = 3 * n // 4
        q1 = sorted_vals[q1_idx]
        q3 = sorted_vals[q3_idx]
        iqr = q3 - q1

        outliers = []
        if detect_outliers:
            if outlier_method == "iqr":
                lower = q1 - 1.5 * iqr
                upper = q3 + 1.5 * iqr
                outliers = [v for v in values if v < lower or v > upper]
            else:
                z_threshold = 3.0
                for v in values:
                    if n > 1:
                        z = abs((v - mean) / std_dev) if std_dev > 0 else 0
                        if z > z_threshold:
                            outliers.append(v)

        correlations: dict[str, float] = {}
        if len(values) == len(records):
            other_fields = set()
            for r in records[:100]:
                if isinstance(r, dict):
                    other_fields.update(r.keys())
            other_fields.discard(field)
            for other in list(other_fields)[:5]:
                other_vals = []
                for r in records:
                    if isinstance(r, dict) and other in r and isinstance(r[other], (int, float)):
                        other_vals.append(r[other])
                if len(other_vals) == len(values) and len(values) > 2:
                    try:
                        corr = self._pearson_correlation(values, other_vals)
                        correlations[other] = corr
                    except Exception:
                        pass

        result = AnalysisResult(
            count=n,
            sum=sum(values),
            mean=mean,
            median=median,
            std_dev=std_dev,
            variance=variance,
            min=min(values),
            max=max(values),
            quartiles=(q1, median, q3),
            outliers=outliers[:100]
        )

        return {
            "analysis": result,
            "field": field,
            "correlations": correlations,
            "distribution": self._compute_histogram(values, 10)
        }

    def _pearson_correlation(self, x: list[float], y: list[float]) -> float:
        """Compute Pearson correlation coefficient."""
        import math
        n = len(x)
        if n < 2:
            return 0.0
        sum_x = sum(x)
        sum_y = sum(y)
        sum_xy = sum(xi * yi for xi, yi in zip(x, y))
        sum_x2 = sum(xi ** 2 for xi in x)
        sum_y2 = sum(yi ** 2 for yi in y)
        numerator = n * sum_xy - sum_x * sum_y
        denominator = math.sqrt((n * sum_x2 - sum_x ** 2) * (n * sum_y2 - sum_y ** 2))
        return numerator / denominator if denominator != 0 else 0.0

    def _compute_histogram(self, values: list[float], bins: int) -> dict[str, Any]:
        """Compute histogram bins."""
        if not values:
            return {}
        min_v, max_v = min(values), max(values)
        if min_v == max_v:
            return {"bins": [min_v], "counts": [len(values)]}
        bin_width = (max_v - min_v) / bins
        counts = [0] * bins
        for v in values:
            idx = min(int((v - min_v) / bin_width), bins - 1)
            counts[idx] += 1
        bin_edges = [min_v + i * bin_width for i in range(bins + 1)]
        return {"bins": bin_edges, "counts": counts, "bin_width": bin_width}
