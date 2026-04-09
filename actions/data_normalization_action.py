"""Data normalization action module for RabAI AutoClick.

Provides data normalization operations:
- DataNormalizationAction: Normalize numeric data
- DataMinMaxNormalizeAction: Min-max normalization
- DataZScoreNormalizeAction: Z-score normalization
- DataRobustNormalizeAction: Robust scaling normalization
"""

import math
from typing import Any, Dict, List, Optional
from datetime import datetime

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class DataNormalizationAction(BaseAction):
    """Normalize data using various methods."""
    action_type = "data_normalization"
    display_name = "数据归一化"
    description = "多种数据归一化方法"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            method = params.get("method", "minmax")
            column = params.get("column")
            feature_range = params.get("feature_range", (0, 1))

            if not data:
                return ActionResult(success=False, message="data is required")

            if isinstance(data, list) and isinstance(data[0], dict) and column:
                values = [row.get(column) for row in data]
            else:
                values = list(data) if isinstance(data, list) else [data]

            numeric = [v for v in values if v is not None and isinstance(v, (int, float))]
            if not numeric:
                return ActionResult(success=False, message="No numeric values found")

            if method == "minmax":
                result, stats = self._minmax_normalize(values, feature_range)
            elif method == "zscore":
                result, stats = self._zscore_normalize(values)
            elif method == "robust":
                result, stats = self._robust_normalize(values)
            elif method == "log":
                result, stats = self._log_normalize(values)
            elif method == "decimal":
                result, stats = self._decimal_normalize(values)
            else:
                result, stats = self._minmax_normalize(values, feature_range)

            if isinstance(data, list) and isinstance(data[0], dict) and column:
                for i, row in enumerate(data):
                    row[column] = result[i]

            return ActionResult(
                success=True,
                message=f"Normalized with {method}",
                data={"data": result, "stats": stats, "method": method}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Normalization error: {e}")

    def _minmax_normalize(self, values: List[float], feature_range: tuple) -> tuple:
        """Min-max normalization."""
        numeric = [v for v in values if v is not None]
        if not numeric:
            return values, {}
        min_v, max_v = min(numeric), max(numeric)
        range_v = max_v - min_v
        a, b = feature_range
        if range_v == 0:
            result = [a for _ in values]
        else:
            result = [a + (v - min_v) / range_v * (b - a) if v is not None else None for v in values]
        return result, {"min": min_v, "max": max_v, "range": range_v}

    def _zscore_normalize(self, values: List[float]) -> tuple:
        """Z-score normalization."""
        numeric = [v for v in values if v is not None]
        if not numeric:
            return values, {}
        mean = sum(numeric) / len(numeric)
        variance = sum((v - mean) ** 2 for v in numeric) / len(numeric)
        std = math.sqrt(variance)
        result = [(v - mean) / std if v is not None else None for v in values] if std != 0 else values
        return result, {"mean": mean, "std": std}

    def _robust_normalize(self, values: List[float]) -> tuple:
        """Robust scaling normalization."""
        numeric = sorted([v for v in values if v is not None])
        if not numeric:
            return values, {}
        q1 = numeric[len(numeric) // 4]
        q3 = numeric[3 * len(numeric) // 4]
        iqr = q3 - q1
        median = numeric[len(numeric) // 2]
        result = [(v - median) / iqr if v is not None and iqr != 0 else None for v in values]
        return result, {"median": median, "q1": q1, "q3": q3, "iqr": iqr}

    def _log_normalize(self, values: List[float]) -> tuple:
        """Log normalization."""
        numeric = [v for v in values if v is not None and v > 0]
        if not numeric:
            return values, {}
        result = [math.log(v) if v is not None and v > 0 else None for v in values]
        return result, {"log_transformed": True}

    def _decimal_normalize(self, values: List[float]) -> tuple:
        """Decimal scaling normalization."""
        numeric = [v for v in values if v is not None]
        if not numeric:
            return values, {}
        max_abs = max(abs(v) for v in numeric)
        if max_abs == 0:
            return values, {}
        digits = len(str(int(max_abs)))
        divisor = 10 ** digits
        result = [v / divisor if v is not None else None for v in values]
        return result, {"divisor": divisor, "digits": digits}


class DataMinMaxNormalizeAction(BaseAction):
    """Min-max normalization."""
    action_type = "data_minmax_normalize"
    display_name = "Min-Max归一化"
    description = "Min-Max归一化到指定范围"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            column = params.get("column")
            min_val = params.get("min", 0.0)
            max_val = params.get("max", 1.0)

            if not data:
                return ActionResult(success=False, message="data is required")

            if isinstance(data, list) and isinstance(data[0], dict) and column:
                values = [row.get(column) for row in data]
            else:
                values = list(data) if isinstance(data, list) else [data]

            numeric = [v for v in values if v is not None and isinstance(v, (int, float))]
            if not numeric:
                return ActionResult(success=False, message="No numeric values")

            data_min, data_max = min(numeric), max(numeric)
            data_range = data_max - data_min

            if data_range == 0:
                result = [(min_val + max_val) / 2 for _ in values]
            else:
                result = [min_val + (v - data_min) / data_range * (max_val - min_val) if v is not None else None for v in values]

            if isinstance(data, list) and isinstance(data[0], dict) and column:
                for i, row in enumerate(data):
                    row[column] = result[i]

            return ActionResult(
                success=True,
                message=f"Min-max normalized: [{data_min:.2f}, {data_max:.2f}] → [{min_val}, {max_val}]",
                data={"data": result, "original_min": data_min, "original_max": data_max, "target_range": (min_val, max_val)}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"MinMax normalize error: {e}")


class DataZScoreNormalizeAction(BaseAction):
    """Z-score normalization."""
    action_type = "data_zscore_normalize"
    display_name = "Z-Score归一化"
    description = "标准化为均值0标准差1"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
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

            mean = sum(numeric) / len(numeric)
            variance = sum((v - mean) ** 2 for v in numeric) / len(numeric)
            std = math.sqrt(variance)

            if std == 0:
                return ActionResult(success=False, message="Standard deviation is zero")

            result = [(v - mean) / std if v is not None else None for v in values]

            if isinstance(data, list) and isinstance(data[0], dict) and column:
                for i, row in enumerate(data):
                    row[column] = result[i]

            return ActionResult(
                success=True,
                message=f"Z-score normalized: mean={mean:.4f}, std={std:.4f}",
                data={"data": result, "mean": mean, "std": std}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Z-score normalize error: {e}")


class DataRobustNormalizeAction(BaseAction):
    """Robust scaling normalization."""
    action_type = "data_robust_normalize"
    display_name = "稳健归一化"
    description = "基于中位数和IQR的稳健归一化"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            column = params.get("column")

            if not data:
                return ActionResult(success=False, message="data is required")

            if isinstance(data, list) and isinstance(data[0], dict) and column:
                values = [row.get(column) for row in data]
            else:
                values = list(data) if isinstance(data, list) else [data]

            numeric = sorted([v for v in values if v is not None and isinstance(v, (int, float))])
            if len(numeric) < 4:
                return ActionResult(success=False, message="Need at least 4 numeric values")

            q1 = numeric[len(numeric) // 4]
            q3 = numeric[3 * len(numeric) // 4]
            iqr = q3 - q1
            median = numeric[len(numeric) // 2]

            if iqr == 0:
                return ActionResult(success=False, message="IQR is zero")

            result = [(v - median) / iqr if v is not None else None for v in values]

            if isinstance(data, list) and isinstance(data[0], dict) and column:
                for i, row in enumerate(data):
                    row[column] = result[i]

            return ActionResult(
                success=True,
                message=f"Robust normalized: median={median:.4f}, IQR={iqr:.4f}",
                data={"data": result, "median": median, "q1": q1, "q3": q3, "iqr": iqr}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Robust normalize error: {e}")
