"""Data outlier detection action module for RabAI AutoClick.

Provides outlier detection operations:
- DataOutlierDetectionAction: Detect outliers in data
- DataOutlierIQRAction: IQR-based outlier detection
- DataOutlierZScoreAction: Z-score based outlier detection
- DataOutlierIsolationAction: Isolation forest-style outlier detection
"""

import math
from typing import Any, Dict, List, Optional, Tuple
from collections import deque
from datetime import datetime

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class DataOutlierDetectionAction(BaseAction):
    """Detect outliers using multiple methods."""
    action_type = "data_outlier_detection"
    display_name = "数据异常值检测"
    description = "多方法异常值检测"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            method = params.get("method", "zscore")
            threshold = params.get("threshold", 3.0)
            column = params.get("column")

            if not data:
                return ActionResult(success=False, message="data is required")

            if isinstance(data, list) and isinstance(data[0], dict) and column:
                values = [row.get(column) for row in data]
            else:
                values = list(data) if isinstance(data, list) else [data]

            numeric = [v for v in values if v is not None and isinstance(v, (int, float))]
            if not numeric:
                return ActionResult(success=False, message="No numeric values found")

            outliers_indices = []

            if method == "zscore":
                outliers_indices = self._zscore_method(values, threshold)
            elif method == "iqr":
                outliers_indices = self._iqr_method(values, threshold)
            elif method == "modified_zscore":
                outliers_indices = self._modified_zscore(values, threshold)
            elif method == "percentile":
                p_low = params.get("percentile_low", 5)
                p_high = params.get("percentile_high", 95)
                outliers_indices = self._percentile_method(values, p_low, p_high)
            elif method == "mad":
                outliers_indices = self._mad_method(values, threshold)
            else:
                outliers_indices = self._zscore_method(values, threshold)

            outliers = [data[i] if i < len(data) else None for i in outliers_indices]
            inliers = [data[i] if i < len(data) and i not in outliers_indices else None for i in range(len(data))]
            inliers = [x for x in inliers if x is not None]

            return ActionResult(
                success=True,
                message=f"Found {len(outliers)} outliers using {method}",
                data={"outliers": outliers, "inliers": inliers, "outlier_count": len(outliers), "method": method, "threshold": threshold}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Outlier detection error: {e}")

    def _zscore_method(self, values: List[float], threshold: float) -> List[int]:
        """Z-score based detection."""
        n = len(values)
        mean = sum(values) / n
        std = math.sqrt(sum((v - mean) ** 2 for v in values) / n)
        if std == 0:
            return []
        return [i for i, v in enumerate(values) if abs((v - mean) / std) > threshold]

    def _iqr_method(self, values: List[float], factor: float = 1.5) -> List[int]:
        """IQR-based detection."""
        sorted_vals = sorted(values)
        n = len(sorted_vals)
        q1_idx = n // 4
        q3_idx = 3 * n // 4
        q1 = sorted_vals[q1_idx]
        q3 = sorted_vals[q3_idx]
        iqr = q3 - q1
        lower = q1 - factor * iqr
        upper = q3 + factor * iqr
        return [i for i, v in enumerate(values) if v < lower or v > upper]

    def _modified_zscore(self, values: List[float], threshold: float) -> List[int]:
        """Modified Z-score using median."""
        sorted_vals = sorted(values)
        median = sorted_vals[len(sorted_vals) // 2]
        diffs = [abs(v - median) for v in values]
        mad = sorted(diffs)[len(diffs) // 2]
        if mad == 0:
            return []
        modified_zscores = [0.6745 * diff / mad for diff in diffs]
        return [i for i, z in enumerate(modified_zscores) if abs(z) > threshold]

    def _percentile_method(self, values: List[float], p_low: float, p_high: float) -> List[int]:
        """Percentile-based detection."""
        sorted_vals = sorted(values)
        low_val = sorted_vals[int(len(sorted_vals) * p_low / 100)]
        high_val = sorted_vals[int(len(sorted_vals) * p_high / 100)]
        return [i for i, v in enumerate(values) if v < low_val or v > high_val]

    def _mad_method(self, values: List[float], threshold: float) -> List[int]:
        """Median Absolute Deviation method."""
        median = sorted(values)[len(values) // 2]
        deviations = [abs(v - median) for v in values]
        mad = sorted(deviations)[len(deviations) // 2]
        if mad == 0:
            return []
        return [i for i, d in enumerate(deviations) if d / mad > threshold]


class DataOutlierIQRAction(BaseAction):
    """IQR-based outlier detection."""
    action_type = "data_outlier_iqr"
    display_name = "IQR异常值检测"
    description = "四分位距异常值检测"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            factor = params.get("factor", 1.5)
            column = params.get("column")

            if not data:
                return ActionResult(success=False, message="data is required")

            if isinstance(data, list) and isinstance(data[0], dict) and column:
                values = [row.get(column) for row in data]
            else:
                values = list(data) if isinstance(data, list) else [data]

            numeric = [v for v in values if v is not None and isinstance(v, (int, float))]
            if len(numeric) < 4:
                return ActionResult(success=False, message="Need at least 4 values for IQR")

            sorted_vals = sorted(numeric)
            n = len(sorted_vals)
            q1 = sorted_vals[n // 4]
            q3 = sorted_vals[3 * n // 4]
            iqr = q3 - q1
            lower = q1 - factor * iqr
            upper = q3 + factor * iqr

            outliers = [v for v in numeric if v < lower or v > upper]
            outlier_data = []
            for i, v in enumerate(data):
                val = values[i] if i < len(values) else None
                if val is not None and (val < lower or val > upper):
                    outlier_data.append({"index": i, "value": val, "bounds": {"lower": lower, "upper": upper}})

            return ActionResult(
                success=True,
                message=f"Found {len(outliers)} outliers (IQR={iqr:.4f})",
                data={"outliers": outliers, "outlier_data": outlier_data, "q1": q1, "q3": q3, "iqr": iqr, "lower": lower, "upper": upper}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"IQR outlier error: {e}")


class DataOutlierZScoreAction(BaseAction):
    """Z-score based outlier detection."""
    action_type = "data_outlier_zscore"
    display_name = "Z分数异常值检测"
    description = "Z分数异常值检测"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            threshold = params.get("threshold", 3.0)
            column = params.get("column")

            if not data:
                return ActionResult(success=False, message="data is required")

            if isinstance(data, list) and isinstance(data[0], dict) and column:
                values = [row.get(column) for row in data]
            else:
                values = list(data) if isinstance(data, list) else [data]

            numeric = [v for v in values if v is not None and isinstance(v, (int, float))]
            if not numeric:
                return ActionResult(success=False, message="No numeric values")

            n = len(numeric)
            mean = sum(numeric) / n
            variance = sum((v - mean) ** 2 for v in numeric) / n
            std = math.sqrt(variance)

            if std == 0:
                return ActionResult(success=False, message="Standard deviation is zero")

            zscores = [(v - mean) / std for v in numeric]
            outlier_indices = [i for i, z in enumerate(zscores) if abs(z) > threshold]

            return ActionResult(
                success=True,
                message=f"Found {len(outlier_indices)} outliers (threshold={threshold})",
                data={"zscores": zscores, "outlier_indices": outlier_indices, "mean": mean, "std": std, "threshold": threshold}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Z-score error: {e}")


class DataOutlierIsolationAction(BaseAction):
    """Isolation forest-style outlier detection."""
    action_type = "data_outlier_isolation"
    display_name = "隔离森林异常值检测"
    description = "Isolation forest风格异常值检测"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            num_trees = params.get("num_trees", 100)
            sample_size = params.get("sample_size", 256)
            threshold = params.get("threshold", 0.5)
            column = params.get("column")

            if not data:
                return ActionResult(success=False, message="data is required")

            if isinstance(data, list) and isinstance(data[0], dict) and column:
                values = [row.get(column) for row in data]
            else:
                values = list(data) if isinstance(data, list) else [data]

            numeric = [v for v in values if v is not None and isinstance(v, (int, float))]
            if len(numeric) < 2:
                return ActionResult(success=False, message="Need at least 2 values")

            anomaly_scores = []
            for i, val in enumerate(numeric):
                path_lengths = []
                for _ in range(num_trees):
                    path_length = self._isolation_path(val, numeric, sample_size)
                    path_lengths.append(path_length)
                avg_path = sum(path_lengths) / len(path_lengths)
                c = 2 * (math.log(len(numeric) - 1) + 0.5772156649) if len(numeric) > 1 else 1
                score = 2 ** (-avg_path / c)
                anomaly_scores.append(score)

            outlier_indices = [i for i, s in enumerate(anomaly_scores) if s > threshold]
            outliers = [numeric[i] for i in outlier_indices]

            return ActionResult(
                success=True,
                message=f"Found {len(outliers)} outliers (score > {threshold})",
                data={"anomaly_scores": anomaly_scores, "outlier_indices": outlier_indices, "outliers": outliers}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Isolation outlier error: {e}")

    def _isolation_path(self, value: float, data: List[float], sample_size: int) -> int:
        """Simulate isolation path length."""
        import random
        sample = random.sample(data, min(sample_size, len(data)))
        if not sample:
            return 0
        min_val, max_val = min(sample), max(sample)
        if min_val == max_val:
            return 1
        return 1 + self._isolation_path(value, sample, sample_size // 2) if len(sample) > 1 else 1
