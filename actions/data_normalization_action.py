"""Data normalization action module for RabAI AutoClick.

Provides data normalization operations:
- MinMaxNormalizeAction: Min-max normalization
- ZScoreNormalizeAction: Z-score normalization
- RobustNormalizeAction: Robust scaling normalization
- LogTransformAction: Log transformation
"""

import math
from typing import Any, Dict, List, Optional

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class MinMaxNormalizeAction(BaseAction):
    """Min-max normalization."""
    action_type = "minmax_normalize"
    display_name = "Min-Max归一化"
    description = "Min-Max标准化到[0,1]范围"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            field = params.get("field", "value")
            output_field = params.get("output_field", None)
            feature_range = params.get("feature_range", (0, 1))

            if not isinstance(data, list):
                data = [data]

            if not data:
                return ActionResult(success=False, message="data is empty")

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

            min_val = min(values)
            max_val = max(values)
            target_min, target_max = feature_range

            normalized = []
            for item in data:
                if isinstance(item, dict):
                    v = item.get(field)
                    if isinstance(v, (int, float)):
                        if max_val > min_val:
                            norm_val = target_min + (v - min_val) / (max_val - min_val) * (target_max - target_min)
                        else:
                            norm_val = (target_min + target_max) / 2
                        if output_field:
                            item[output_field] = norm_val
                        else:
                            item[field] = norm_val
                    normalized.append(item)
                else:
                    if isinstance(item, (int, float)):
                        if max_val > min_val:
                            norm_val = target_min + (item - min_val) / (max_val - min_val) * (target_max - target_min)
                        else:
                            norm_val = (target_min + target_max) / 2
                        normalized.append(norm_val)
                    else:
                        normalized.append(item)

            return ActionResult(
                success=True,
                message=f"Min-max normalized {len(normalized)} values to range {feature_range}",
                data={
                    "normalized": normalized,
                    "count": len(normalized),
                    "original_range": (min_val, max_val),
                    "target_range": feature_range,
                },
            )
        except Exception as e:
            return ActionResult(success=False, message=f"MinMaxNormalize error: {e}")


class ZScoreNormalizeAction(BaseAction):
    """Z-score normalization."""
    action_type = "zscore_normalize"
    display_name = "Z-Score归一化"
    description = "Z-Score标准化到标准正态分布"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            field = params.get("field", "value")
            output_field = params.get("output_field", None)

            if not isinstance(data, list):
                data = [data]

            if not data:
                return ActionResult(success=False, message="data is empty")

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

            n = len(values)
            mean = sum(values) / n
            variance = sum((x - mean) ** 2 for x in values) / n
            std_dev = math.sqrt(variance)

            normalized = []
            for item in data:
                if isinstance(item, dict):
                    v = item.get(field)
                    if isinstance(v, (int, float)):
                        z_score = (v - mean) / std_dev if std_dev > 0 else 0
                        if output_field:
                            item[output_field] = round(z_score, 6)
                        else:
                            item[field] = round(z_score, 6)
                    normalized.append(item)
                else:
                    if isinstance(item, (int, float)):
                        z_score = (item - mean) / std_dev if std_dev > 0 else 0
                        normalized.append(round(z_score, 6))
                    else:
                        normalized.append(item)

            return ActionResult(
                success=True,
                message=f"Z-score normalized {len(normalized)} values (mean={mean:.4f}, std={std_dev:.4f})",
                data={
                    "normalized": normalized,
                    "count": len(normalized),
                    "mean": mean,
                    "std_dev": std_dev,
                },
            )
        except Exception as e:
            return ActionResult(success=False, message=f"ZScoreNormalize error: {e}")


class RobustNormalizeAction(BaseAction):
    """Robust scaling normalization using median and IQR."""
    action_type = "robust_normalize"
    display_name = "Robust归一化"
    description = "使用中位数和IQR的鲁棒标准化"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            field = params.get("field", "value")
            output_field = params.get("output_field", None)

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

            if len(values) < 3:
                return ActionResult(success=False, message="Need at least 3 values for robust scaling")

            sorted_vals = sorted(values)
            n = len(sorted_vals)
            median = sorted_vals[n // 2]
            q1 = sorted_vals[n // 4]
            q3 = sorted_vals[3 * n // 4]
            iqr = q3 - q1

            normalized = []
            for item in data:
                if isinstance(item, dict):
                    v = item.get(field)
                    if isinstance(v, (int, float)):
                        if iqr > 0:
                            robust_val = (v - median) / iqr
                        else:
                            robust_val = 0
                        if output_field:
                            item[output_field] = round(robust_val, 6)
                        else:
                            item[field] = round(robust_val, 6)
                    normalized.append(item)
                else:
                    if isinstance(item, (int, float)):
                        if iqr > 0:
                            robust_val = (item - median) / iqr
                        else:
                            robust_val = 0
                        normalized.append(round(robust_val, 6))
                    else:
                        normalized.append(item)

            return ActionResult(
                success=True,
                message=f"Robust normalized {len(normalized)} values (median={median}, IQR={iqr})",
                data={
                    "normalized": normalized,
                    "count": len(normalized),
                    "median": median,
                    "q1": q1,
                    "q3": q3,
                    "iqr": iqr,
                },
            )
        except Exception as e:
            return ActionResult(success=False, message=f"RobustNormalize error: {e}")


class LogTransformAction(BaseAction):
    """Log transformation."""
    action_type = "log_transform"
    display_name = "对数变换"
    description = "对数据进行对数变换"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            field = params.get("field", "value")
            output_field = params.get("output_field", None)
            log_base = params.get("base", "natural")
            shift = params.get("shift", 0)

            if not isinstance(data, list):
                data = [data]

            if log_base == "natural":
                transform_fn = math.log
            elif log_base == "10":
                transform_fn = math.log10
            elif log_base == "2":
                transform_fn = math.log2
            else:
                transform_fn = math.log

            transformed = []
            for item in data:
                if isinstance(item, dict):
                    v = item.get(field)
                    if isinstance(v, (int, float)) and v + shift > 0:
                        try:
                            t_val = transform_fn(v + shift)
                            if output_field:
                                item[output_field] = round(t_val, 6)
                            else:
                                item[field] = round(t_val, 6)
                        except ValueError:
                            if output_field:
                                item[output_field] = None
                            else:
                                item[field] = None
                    transformed.append(item)
                else:
                    if isinstance(item, (int, float)) and item + shift > 0:
                        try:
                            transformed.append(round(transform_fn(item + shift), 6))
                        except ValueError:
                            transformed.append(None)
                    else:
                        transformed.append(item)

            return ActionResult(
                success=True,
                message=f"Log{log_base} transformed {len(transformed)} values (shift={shift})",
                data={"transformed": transformed, "count": len(transformed), "base": log_base, "shift": shift},
            )
        except Exception as e:
            return ActionResult(success=False, message=f"LogTransform error: {e}")
