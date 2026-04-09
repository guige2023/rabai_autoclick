"""Data imputation action module for RabAI AutoClick.

Provides data imputation operations:
- DataImputationAction: Handle missing values
- DataImputationMeanAction: Mean/median imputation
- DataImputationForwardFillAction: Forward fill missing values
- DataImputationInterpolateAction: Interpolate missing values
"""

import math
from typing import Any, Dict, List, Optional, Callable
from datetime import datetime

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class DataImputationAction(BaseAction):
    """Handle missing values with various strategies."""
    action_type = "data_imputation"
    display_name = "数据缺失值填充"
    description = "多种缺失值填充策略"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            strategy = params.get("strategy", "mean")
            column = params.get("column")
            custom_value = params.get("custom_value")
            default_value = params.get("default_value")

            if not data:
                return ActionResult(success=False, message="data is required")

            if isinstance(data, list) and data and isinstance(data[0], dict) and column:
                return self._impute_column(data, column, strategy, custom_value, default_value)
            elif isinstance(data, list):
                return self._impute_list(data, strategy, custom_value, default_value)
            elif isinstance(data, dict):
                return self._impute_dict(data, strategy, custom_value, default_value)
            else:
                return ActionResult(success=False, message="Unsupported data format")

        except Exception as e:
            return ActionResult(success=False, message=f"Imputation error: {e}")

    def _impute_column(self, data: List[Dict[str, Any]], column: str, strategy: str, custom_value: Any, default_value: Any) -> ActionResult:
        """Impute missing values in a column."""
        values = [row.get(column) for row in data]
        result, stats = self._compute_imputation(values, strategy, custom_value, default_value)

        for i, row in enumerate(data):
            if row.get(column) is None or row.get(column) == "":
                row[column] = result[i]

        return ActionResult(
            success=True,
            message=f"Imputed column '{column}' with strategy '{strategy}'",
            data={"data": data, "stats": stats}
        )

    def _impute_list(self, data: List[Any], strategy: str, custom_value: Any, default_value: Any) -> ActionResult:
        """Impute missing values in a list."""
        result, stats = self._compute_imputation(data, strategy, custom_value, default_value)
        return ActionResult(success=True, message=f"Imputed list with '{strategy}'", data={"data": result, "stats": stats})

    def _impute_dict(self, data: Dict[str, Any], strategy: str, custom_value: Any, default_value: Any) -> ActionResult:
        """Impute missing values in a dict."""
        keys = list(data.keys())
        values = list(data.values())
        result, stats = self._compute_imputation(values, strategy, custom_value, default_value)
        imputed_dict = dict(zip(keys, result))
        return ActionResult(success=True, message=f"Imputed dict with '{strategy}'", data={"data": imputed_dict, "stats": stats})

    def _compute_imputation(self, data: List[Any], strategy: str, custom_value: Any, default_value: Any) -> tuple:
        """Compute imputation values."""
        non_null = [v for v in data if v is not None and v != ""]
        numeric_values = [v for v in non_null if isinstance(v, (int, float)) and not math.isnan(v) if isinstance(v, float)]
        numeric_values = [v for v in non_null if isinstance(v, (int, float))]

        stats = {"missing_count": len(data) - len(non_null), "total_count": len(data)}

        if strategy == "mean" and numeric_values:
            mean_val = sum(numeric_values) / len(numeric_values)
            result = [mean_val if v is None or v == "" else v for v in data]
            stats["mean"] = mean_val
        elif strategy == "median" and numeric_values:
            sorted_vals = sorted(numeric_values)
            mid = len(sorted_vals) // 2
            median_val = sorted_vals[mid] if len(sorted_vals) % 2 else (sorted_vals[mid - 1] + sorted_vals[mid]) / 2
            result = [median_val if v is None or v == "" else v for v in data]
            stats["median"] = median_val
        elif strategy == "mode" and non_null:
            from collections import Counter
            mode_val = Counter(non_null).most_common(1)[0][0]
            result = [mode_val if v is None or v == "" else v for v in data]
            stats["mode"] = mode_val
        elif strategy == "custom" and custom_value is not None:
            result = [custom_value if v is None or v == "" else v for v in data]
            stats["custom_value"] = custom_value
        elif strategy == "forward_fill":
            result = self._forward_fill(data)
        elif strategy == "backward_fill":
            result = self._backward_fill(data)
        elif strategy == "interpolate":
            result = self._interpolate(data)
        elif strategy == "default" and default_value is not None:
            result = [default_value if v is None or v == "" else v for v in data]
            stats["default"] = default_value
        else:
            result = [v if v is not None and v != "" else None for v in data]

        return result, stats

    def _forward_fill(self, data: List[Any]) -> List[Any]:
        """Forward fill."""
        result = []
        last_valid = None
        for v in data:
            if v is not None and v != "":
                last_valid = v
            result.append(last_valid)
        return result

    def _backward_fill(self, data: List[Any]) -> List[Any]:
        """Backward fill."""
        result = [None] * len(data)
        next_valid = None
        for i in range(len(data) - 1, -1, -1):
            if data[i] is not None and data[i] != "":
                next_valid = data[i]
            result[i] = next_valid
        return result

    def _interpolate(self, data: List[Any]) -> List[Any]:
        """Linear interpolation for numeric values."""
        result = list(data)
        indices = [(i, v) for i, v in enumerate(data) if v is not None and v != "" and isinstance(v, (int, float))]

        for j in range(len(indices) - 1):
            i1, v1 = indices[j]
            i2, v2 = indices[j + 1]
            step = (v2 - v1) / (i2 - i1)
            for k in range(i1 + 1, i2):
                result[k] = v1 + step * (k - i1)

        return result


class DataImputationMeanAction(BaseAction):
    """Mean/median imputation for numeric data."""
    action_type = "data_imputation_mean"
    display_name = "数据均值填充"
    description = "均值或中位数填充"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            method = params.get("method", "mean")
            column = params.get("column")

            if not data:
                return ActionResult(success=False, message="data is required")

            if isinstance(data, list) and isinstance(data[0], dict) and column:
                values = [row.get(column) for row in data]
            else:
                values = data

            numeric = [v for v in values if v is not None and isinstance(v, (int, float))]
            if not numeric:
                return ActionResult(success=False, message="No numeric values found")

            if method == "mean":
                fill_value = sum(numeric) / len(numeric)
            elif method == "median":
                sorted_vals = sorted(numeric)
                mid = len(sorted_vals) // 2
                fill_value = sorted_vals[mid] if len(sorted_vals) % 2 else (sorted_vals[mid - 1] + sorted_vals[mid]) / 2
            else:
                return ActionResult(success=False, message=f"Unknown method: {method}")

            result = [fill_value if v is None else v for v in values]

            if isinstance(data, list) and isinstance(data[0], dict) and column:
                for i, row in enumerate(data):
                    row[column] = result[i]

            return ActionResult(success=True, message=f"{method.capitalize()} = {fill_value:.4f}", data={"fill_value": fill_value, "result": result, "method": method})
        except Exception as e:
            return ActionResult(success=False, message=f"Mean imputation error: {e}")


class DataImputationForwardFillAction(BaseAction):
    """Forward fill missing values."""
    action_type = "data_imputation_forward_fill"
    display_name = "数据前向填充"
    description = "前向填充缺失值"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            column = params.get("column")
            limit = params.get("limit", 0)

            if not data:
                return ActionResult(success=False, message="data is required")

            if isinstance(data, list) and isinstance(data[0], dict) and column:
                values = [row.get(column) for row in data]
            else:
                values = list(data)

            result = []
            last_valid = None
            consecutive_count = 0

            for v in values:
                if v is not None and v != "":
                    last_valid = v
                    consecutive_count = 0
                    result.append(v)
                elif limit > 0 and consecutive_count >= limit:
                    result.append(None)
                else:
                    if last_valid is not None:
                        result.append(last_valid)
                        consecutive_count += 1
                    else:
                        result.append(None)

            if isinstance(data, list) and isinstance(data[0], dict) and column:
                for i, row in enumerate(data):
                    row[column] = result[i]

            return ActionResult(success=True, message=f"Forward filled {values.count(None)} values", data={"result": result})
        except Exception as e:
            return ActionResult(success=False, message=f"Forward fill error: {e}")


class DataImputationInterpolateAction(BaseAction):
    """Interpolate missing values."""
    action_type = "data_imputation_interpolate"
    display_name = "数据插值填充"
    description = "插值填充缺失值"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            method = params.get("method", "linear")
            column = params.get("column")

            if not data:
                return ActionResult(success=False, message="data is required")

            if isinstance(data, list) and isinstance(data[0], dict) and column:
                values = [row.get(column) for row in data]
            else:
                values = list(data)

            result = list(values)
            numeric_pairs = [(i, v) for i, v in enumerate(values) if v is not None and isinstance(v, (int, float))]

            if len(numeric_pairs) < 2:
                return ActionResult(success=False, message="Not enough numeric values for interpolation")

            if method == "linear":
                for j in range(len(numeric_pairs) - 1):
                    i1, v1 = numeric_pairs[j]
                    i2, v2 = numeric_pairs[j + 1]
                    step = (v2 - v1) / (i2 - i1) if i2 != i1 else 0
                    for k in range(i1 + 1, i2):
                        result[k] = v1 + step * (k - i1)
            elif method == "cubic":
                if len(numeric_pairs) >= 4:
                    indices = [p[0] for p in numeric_pairs]
                    vals = [p[1] for p in numeric_pairs]
                    for j in range(len(numeric_pairs) - 3):
                        i0, i1, i2, i3 = indices[j:j+4]
                        v0, v1, v2, v3 = vals[j:j+4]
                        for k in range(i1 + 1, i2):
                            t = (k - i1) / (i2 - i1)
                            result[k] = self._cubic_interp(v0, v1, v2, v3, t)
            else:
                return ActionResult(success=False, message=f"Unknown method: {method}")

            if isinstance(data, list) and isinstance(data[0], dict) and column:
                for i, row in enumerate(data):
                    row[column] = result[i]

            missing_filled = sum(1 for i, v in enumerate(values) if (v is None or isinstance(v, str)) and result[i] != values[i])

            return ActionResult(success=True, message=f"Interpolated {missing_filled} values", data={"result": result, "method": method})
        except Exception as e:
            return ActionResult(success=False, message=f"Interpolation error: {e}")

    def _cubic_interp(self, v0: float, v1: float, v2: float, v3: float, t: float) -> float:
        """Cubic interpolation."""
        a0 = v3 - v2 - v0 + v1
        a1 = v0 - v1 - a0
        a2 = v2 - v0
        a3 = v1
        return a0 * t**3 + a1 * t**2 + a2 * t + a3
