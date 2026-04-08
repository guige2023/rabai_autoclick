"""Data statistics action module for RabAI AutoClick.

Provides statistical analysis operations:
- DescriptiveStatsAction: Compute descriptive statistics
- CorrelationAnalysisAction: Compute correlations between variables
- PercentileAnalysisAction: Compute percentiles and quantiles
- DistributionAnalysisAction: Analyze data distributions
"""

import math
from collections import Counter
from typing import Any, Dict, List, Optional

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class DescriptiveStatsAction(BaseAction):
    """Compute descriptive statistics."""
    action_type = "descriptive_stats"
    display_name = "描述性统计"
    description = "计算描述性统计量"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            field = params.get("field", "value")
            compute = params.get("compute", ["mean", "median", "std", "min", "max"])

            if not isinstance(data, list):
                data = [data]

            values = []
            for item in data:
                if isinstance(item, dict):
                    v = item.get(field)
                else:
                    v = item
                if isinstance(v, (int, float)):
                    values.append(v)

            if not values:
                return ActionResult(success=False, message="No numeric values found")

            sorted_vals = sorted(values)
            n = len(sorted_vals)
            stats = {"count": n}

            if "mean" in compute:
                stats["mean"] = round(sum(values) / n, 6)

            if "median" in compute:
                if n % 2 == 0:
                    stats["median"] = (sorted_vals[n // 2 - 1] + sorted_vals[n // 2]) / 2
                else:
                    stats["median"] = sorted_vals[n // 2]

            if "sum" in compute:
                stats["sum"] = round(sum(values), 4)

            if "min" in compute:
                stats["min"] = sorted_vals[0]

            if "max" in compute:
                stats["max"] = sorted_vals[-1]

            if "range" in compute:
                stats["range"] = sorted_vals[-1] - sorted_vals[0]

            if "std" in compute:
                mean = stats.get("mean", sum(values) / n)
                variance = sum((x - mean) ** 2 for x in values) / n
                stats["std"] = round(math.sqrt(variance), 6)

            if "variance" in compute:
                mean = stats.get("mean", sum(values) / n)
                stats["variance"] = round(sum((x - mean) ** 2 for x in values) / n, 6)

            if "cv" in compute:
                mean = stats.get("mean", 0)
                std = stats.get("std", 0)
                stats["cv"] = round(std / mean if mean != 0 else 0, 6)

            if "q1" in compute:
                stats["q1"] = sorted_vals[n // 4]

            if "q3" in compute:
                stats["q3"] = sorted_vals[3 * n // 4]

            if "iqr" in compute:
                q1 = stats.get("q1", sorted_vals[n // 4])
                q3 = stats.get("q3", sorted_vals[3 * n // 4])
                stats["iqr"] = q3 - q1

            if "skewness" in compute:
                mean = stats.get("mean", sum(values) / n)
                std = stats.get("std", 0)
                if std > 0:
                    skewness = sum(((x - mean) / std) ** 3 for x in values) / n
                    stats["skewness"] = round(skewness, 6)

            if "kurtosis" in compute:
                mean = stats.get("mean", sum(values) / n)
                std = stats.get("std", 0)
                if std > 0:
                    kurtosis = sum(((x - mean) / std) ** 4 for x in values) / n - 3
                    stats["kurtosis"] = round(kurtosis, 6)

            return ActionResult(
                success=True,
                message=f"Descriptive stats computed for {n} values",
                data={"stats": stats, "field": field},
            )
        except Exception as e:
            return ActionResult(success=False, message=f"DescriptiveStats error: {e}")


class CorrelationAnalysisAction(BaseAction):
    """Compute correlations between variables."""
    action_type = "correlation_analysis"
    display_name = "相关性分析"
    description: "计算变量间的相关性"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            x_field = params.get("x_field", "x")
            y_field = params.get("y_field", "y")
            method = params.get("method", "pearson")

            if not isinstance(data, list):
                data = [data]

            x_vals = []
            y_vals = []
            for item in data:
                if not isinstance(item, dict):
                    continue
                x = item.get(x_field)
                y = item.get(y_field)
                if isinstance(x, (int, float)) and isinstance(y, (int, float)):
                    x_vals.append(x)
                    y_vals.append(y)

            if len(x_vals) < 2:
                return ActionResult(success=False, message="Need at least 2 data points")

            if method == "pearson":
                r = self._pearson_correlation(x_vals, y_vals)
            elif method == "spearman":
                r = self._spearman_correlation(x_vals, y_vals)
            elif method == "kendall":
                r = self._kendall_correlation(x_vals, y_vals)
            else:
                r = self._pearson_correlation(x_vals, y_vals)

            r_squared = r ** 2

            return ActionResult(
                success=True,
                message=f"Correlation ({method}): r={r:.4f}, r²={r_squared:.4f}",
                data={"correlation": round(r, 6), "r_squared": round(r_squared, 6), "method": method, "n": len(x_vals)},
            )
        except Exception as e:
            return ActionResult(success=False, message=f"CorrelationAnalysis error: {e}")

    def _pearson_correlation(self, x: List, y: List) -> float:
        n = len(x)
        mean_x = sum(x) / n
        mean_y = sum(y) / n
        cov = sum((xi - mean_x) * (yi - mean_y) for xi, yi in zip(x, y)) / n
        std_x = math.sqrt(sum((xi - mean_x) ** 2 for xi in x) / n)
        std_y = math.sqrt(sum((yi - mean_y) ** 2 for yi in y) / n)
        if std_x == 0 or std_y == 0:
            return 0.0
        return cov / (std_x * std_y)

    def _spearman_correlation(self, x: List, y: List) -> float:
        n = len(x)
        rx = self._rank(x)
        ry = self._rank(y)
        return self._pearson_correlation(rx, ry)

    def _kendall_correlation(self, x: List, y: List) -> float:
        n = len(x)
        concordant = 0
        discordant = 0
        for i in range(n):
            for j in range(i + 1, n):
                if (x[i] - x[j]) * (y[i] - y[j]) > 0:
                    concordant += 1
                elif (x[i] - x[j]) * (y[i] - y[j]) < 0:
                    discordant += 1
        return (concordant - discordant) / (n * (n - 1) / 2)

    def _rank(self, values: List) -> List:
        sorted_pairs = sorted(enumerate(values), key=lambda p: p[1])
        ranks = [0] * len(values)
        for rank, (idx, _) in enumerate(sorted_pairs, 1):
            ranks[idx] = rank
        return ranks


class PercentileAnalysisAction(BaseAction):
    """Compute percentiles and quantiles."""
    action_type = "percentile_analysis"
    display_name = "百分位分析"
    description = "计算百分位数和分位数"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            field = params.get("field", "value")
            percentiles = params.get("percentiles", [10, 25, 50, 75, 90])

            if not isinstance(data, list):
                data = [data]

            values = []
            for item in data:
                if isinstance(item, dict):
                    v = item.get(field)
                else:
                    v = item
                if isinstance(v, (int, float)):
                    values.append(v)

            if not values:
                return ActionResult(success=False, message="No numeric values found")

            sorted_vals = sorted(values)
            n = len(sorted_vals)

            results = {}
            for p in percentiles:
                idx = int((p / 100) * (n - 1))
                idx = min(idx, n - 1)
                results[f"p{p}"] = sorted_vals[idx]

            results["min"] = sorted_vals[0]
            results["max"] = sorted_vals[-1]
            results["count"] = n

            deciles = [10, 20, 30, 40, 50, 60, 70, 80, 90]
            decile_results = {}
            for d in deciles:
                idx = int((d / 100) * (n - 1))
                idx = min(idx, n - 1)
                decile_results[f"d{d}"] = sorted_vals[idx]
            results["deciles"] = decile_results

            return ActionResult(
                success=True,
                message=f"Percentile analysis: {len(percentiles)} percentiles computed",
                data={"percentiles": results, "field": field},
            )
        except Exception as e:
            return ActionResult(success=False, message=f"PercentileAnalysis error: {e}")


class DistributionAnalysisAction(BaseAction):
    """Analyze data distributions."""
    action_type = "distribution_analysis"
    display_name = "分布分析"
    description = "分析数据分布"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            field = params.get("field", "value")
            num_bins = params.get("num_bins", 10)
            detect_outliers = params.get("detect_outliers", True)

            if not isinstance(data, list):
                data = [data]

            values = []
            for item in data:
                if isinstance(item, dict):
                    v = item.get(field)
                else:
                    v = item
                if isinstance(v, (int, float)):
                    values.append(v)

            if not values:
                return ActionResult(success=False, message="No numeric values found")

            sorted_vals = sorted(values)
            n = len(sorted_vals)

            min_val = sorted_vals[0]
            max_val = sorted_vals[-1]
            bin_width = (max_val - min_val) / num_bins if num_bins > 0 else 1

            histogram = []
            for i in range(num_bins):
                bin_start = min_val + i * bin_width
                bin_end = bin_start + bin_width
                count = sum(1 for v in values if bin_start <= v < bin_end)
                if i == num_bins - 1:
                    count = sum(1 for v in values if bin_start <= v <= bin_end)
                histogram.append({
                    "bin": i,
                    "range": (round(bin_start, 4), round(bin_end, 4)),
                    "count": count,
                    "percentage": round(count / n * 100, 2),
                })

            outliers = []
            if detect_outliers:
                q1 = sorted_vals[n // 4]
                q3 = sorted_vals[3 * n // 4]
                iqr = q3 - q1
                lower_bound = q1 - 1.5 * iqr
                upper_bound = q3 + 1.5 * iqr
                for v in values:
                    if v < lower_bound or v > upper_bound:
                        outliers.append(v)

            mean = sum(values) / n
            variance = sum((x - mean) ** 2 for x in values) / n
            std = math.sqrt(variance)

            distribution_type = "normal"
            skewness = sum(((x - mean) / std) ** 3 for x in values) / n if std > 0 else 0
            kurtosis = sum(((x - mean) / std) ** 4 for x in values) / n - 3 if std > 0 else 0

            if abs(skewness) > 1:
                distribution_type = "highly_skewed"
            elif abs(skewness) > 0.5:
                distribution_type = "moderately_skewed"
            elif abs(kurtosis) > 1:
                distribution_type = "heavy_tailed"
            elif abs(kurtosis) < -0.5:
                distribution_type = "light_tailed"

            return ActionResult(
                success=True,
                message=f"Distribution analysis: {distribution_type}",
                data={
                    "histogram": histogram,
                    "outliers": outliers[:100],
                    "outlier_count": len(outliers),
                    "distribution_type": distribution_type,
                    "skewness": round(skewness, 4),
                    "kurtosis": round(kurtosis, 4),
                    "n": n,
                },
            )
        except Exception as e:
            return ActionResult(success=False, message=f"DistributionAnalysis error: {e}")
