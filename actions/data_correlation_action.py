"""Data correlation action module for RabAI AutoClick.

Provides data correlation operations:
- DataCorrelationAction: Find correlations between datasets
- DataCorrelationMatrixAction: Generate correlation matrix
- DataCrossCorrelationAction: Cross-correlate time series
- DataCorrelationFilterAction: Filter data by correlation strength
"""

import math
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class DataCorrelationAction(BaseAction):
    """Find correlations between two datasets."""
    action_type = "data_correlation"
    display_name = "数据相关性分析"
    description = "分析两个数据集之间的相关性"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            dataset_a = params.get("dataset_a", [])
            dataset_b = params.get("dataset_b", [])
            method = params.get("method", "pearson")
            min_samples = params.get("min_samples", 3)

            if not dataset_a or not dataset_b:
                return ActionResult(success=False, message="dataset_a and dataset_b are required")

            if len(dataset_a) != len(dataset_b):
                min_len = min(len(dataset_a), len(dataset_b))
                dataset_a = dataset_a[:min_len]
                dataset_b = dataset_b[:min_len]

            if len(dataset_a) < min_samples:
                return ActionResult(success=False, message=f"Need at least {min_samples} samples")

            if method == "pearson":
                corr = self._pearson_correlation(dataset_a, dataset_b)
            elif method == "spearman":
                corr = self._spearman_correlation(dataset_a, dataset_b)
            elif method == "kendall":
                corr = self._kendall_correlation(dataset_a, dataset_b)
            else:
                corr = self._pearson_correlation(dataset_a, dataset_b)

            strength = self._interpret_correlation(corr)

            return ActionResult(
                success=True,
                message=f"Correlation ({method}): {corr:.4f} ({strength})",
                data={"correlation": corr, "method": method, "strength": strength, "sample_count": len(dataset_a)}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Correlation error: {e}")

    def _pearson_correlation(self, x: List[float], y: List[float]) -> float:
        """Calculate Pearson correlation coefficient."""
        n = len(x)
        mean_x = sum(x) / n
        mean_y = sum(y) / n

        numerator = sum((x[i] - mean_x) * (y[i] - mean_y) for i in range(n))
        denom_x = math.sqrt(sum((xi - mean_x) ** 2 for xi in x))
        denom_y = math.sqrt(sum((yi - mean_y) ** 2 for yi in y))

        if denom_x == 0 or denom_y == 0:
            return 0.0

        return numerator / (denom_x * denom_y)

    def _spearman_correlation(self, x: List[float], y: List[float]) -> float:
        """Calculate Spearman rank correlation."""
        ranks_x = self._rank_data(x)
        ranks_y = self._rank_data(y)
        return self._pearson_correlation(ranks_x, ranks_y)

    def _kendall_correlation(self, x: List[float], y: List[float]) -> float:
        """Calculate Kendall tau correlation."""
        n = len(x)
        concordant = 0
        discordant = 0

        for i in range(n):
            for j in range(i + 1, n):
                sign_x = 1 if x[i] > x[j] else (-1 if x[i] < x[j] else 0)
                sign_y = 1 if y[i] > y[j] else (-1 if y[i] < y[j] else 0)
                if sign_x * sign_y > 0:
                    concordant += 1
                elif sign_x * sign_y < 0:
                    discordant += 1

        total = n * (n - 1) / 2
        if total == 0:
            return 0.0
        return (concordant - discordant) / total

    def _rank_data(self, data: List[float]) -> List[float]:
        """Rank data values."""
        sorted_pairs = sorted(enumerate(data), key=lambda p: p[1])
        ranks = [0.0] * len(data)
        i = 0
        while i < len(sorted_pairs):
            j = i
            while j < len(sorted_pairs) - 1 and sorted_pairs[j + 1][1] == sorted_pairs[i][1]:
                j += 1
            avg_rank = (i + j) / 2.0 + 1
            for k in range(i, j + 1):
                ranks[sorted_pairs[k][0]] = avg_rank
            i = j + 1
        return ranks

    def _interpret_correlation(self, corr: float) -> str:
        """Interpret correlation strength."""
        abs_corr = abs(corr)
        if abs_corr >= 0.9:
            return "very strong"
        elif abs_corr >= 0.7:
            return "strong"
        elif abs_corr >= 0.5:
            return "moderate"
        elif abs_corr >= 0.3:
            return "weak"
        return "negligible"


class DataCorrelationMatrixAction(BaseAction):
    """Generate correlation matrix for multiple variables."""
    action_type = "data_correlation_matrix"
    display_name = "数据相关性矩阵"
    description = "生成多变量相关性矩阵"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            datasets = params.get("datasets", {})
            method = params.get("method", "pearson")
            variable_names = params.get("variable_names")

            if not datasets:
                return ActionResult(success=False, message="datasets dict is required")

            var_names = variable_names or [f"var_{i}" for i in range(len(datasets))]
            var_list = list(datasets.keys())

            matrix = []
            for i, var_i in enumerate(var_list):
                row = []
                for j, var_j in enumerate(var_list):
                    if i == j:
                        row.append(1.0)
                    elif j < i:
                        row.append(matrix[j][i])
                    else:
                        corr = self._compute_correlation(datasets[var_i], datasets[var_j], method)
                        row.append(corr)
                matrix.append(row)

            return ActionResult(
                success=True,
                message=f"Generated {len(var_list)}x{len(var_list)} correlation matrix",
                data={"matrix": matrix, "variables": var_names, "method": method}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Correlation matrix error: {e}")

    def _compute_correlation(self, x: List[float], y: List[float], method: str) -> float:
        """Compute correlation between two lists."""
        if len(x) != len(y):
            min_len = min(len(x), len(y))
            x, y = x[:min_len], y[:min_len]
        if len(x) < 2:
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


class DataCrossCorrelationAction(BaseAction):
    """Cross-correlate two time series."""
    action_type = "data_cross_correlation"
    display_name = "数据互相关分析"
    description = "对两个时间序列进行互相关分析"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            series_a = params.get("series_a", [])
            series_b = params.get("series_b", [])
            max_lag = params.get("max_lag", 10)
            normalize = params.get("normalize", True)

            if not series_a or not series_b:
                return ActionResult(success=False, message="series_a and series_b are required")

            results = []
            for lag in range(-max_lag, max_lag + 1):
                if lag < 0:
                    corr = self._lagged_correlation(series_a[-lag:], series_b[:lag], normalize)
                elif lag > 0:
                    corr = self._lagged_correlation(series_a[:len(series_b) - lag], series_b[lag:], normalize)
                else:
                    corr = self._lagged_correlation(series_a, series_b, normalize)
                results.append({"lag": lag, "correlation": corr})

            max_corr = max(results, key=lambda r: abs(r["correlation"]))

            return ActionResult(
                success=True,
                message=f"Cross-correlation complete, max correlation at lag={max_corr['lag']}",
                data={"cross_correlations": results, "max_lag": max_corr["lag"], "max_correlation": max_corr["correlation"]}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Cross-correlation error: {e}")

    def _lagged_correlation(self, a: List[float], b: List[float], normalize: bool) -> float:
        """Correlation with optional normalization."""
        if len(a) != len(b) or len(a) == 0:
            return 0.0

        n = len(a)
        mean_a = sum(a) / n
        mean_b = sum(b) / n

        numerator = sum((a[i] - mean_a) * (b[i] - mean_b) for i in range(n))

        if normalize:
            denom_a = math.sqrt(sum((xi - mean_a) ** 2 for xi in a))
            denom_b = math.sqrt(sum((yi - mean_b) ** 2 for yi in b))
            if denom_a == 0 or denom_b == 0:
                return 0.0
            return numerator / (denom_a * denom_b)

        return numerator / n


class DataCorrelationFilterAction(BaseAction):
    """Filter data based on correlation strength."""
    action_type = "data_correlation_filter"
    display_name = "数据相关性过滤"
    description = "按相关性强度过滤数据"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            dataset = params.get("dataset", {})
            threshold = params.get("threshold", 0.5)
            reference_variable = params.get("reference_variable")
            method = params.get("method", "pearson")

            if not dataset:
                return ActionResult(success=False, message="dataset dict is required")

            if not reference_variable or reference_variable not in dataset:
                return ActionResult(success=False, message="reference_variable is required and must be in dataset")

            ref_data = dataset[reference_variable]
            filtered = {}
            excluded = {}

            for var_name, values in dataset.items():
                if var_name == reference_variable:
                    continue

                if len(values) != len(ref_data):
                    min_len = min(len(values), len(ref_data))
                    values = values[:min_len]
                    ref_trimmed = ref_data[:min_len]
                else:
                    ref_trimmed = ref_data

                corr = self._compute_correlation(ref_trimmed, values, method)
                if abs(corr) >= threshold:
                    filtered[var_name] = {"correlation": corr, "values": values}
                else:
                    excluded[var_name] = {"correlation": corr, "values": values}

            return ActionResult(
                success=True,
                message=f"Filtered: {len(filtered)} variables pass threshold {threshold}",
                data={"filtered": filtered, "excluded": excluded, "threshold": threshold}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Correlation filter error: {e}")

    def _compute_correlation(self, x: List[float], y: List[float], method: str) -> float:
        """Compute correlation."""
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
