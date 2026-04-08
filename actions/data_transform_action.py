"""Data transform action module for RabAI AutoClick.

Provides data transformation operations:
- TransformNormalizeAction: Normalize numeric data
- TransformStandardizeAction: Standardize data
- TransformEncodeAction: Encode categorical data
- TransformScaleAction: Scale data to range
- TransformLogAction: Log transformation
"""

from typing import Any, Dict, List

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class TransformNormalizeAction(BaseAction):
    """Normalize numeric data to 0-1 range."""
    action_type = "transform_normalize"
    display_name = "归一化"
    description = "归一化数据到0-1范围"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            field = params.get("field", "value")

            if not data:
                return ActionResult(success=False, message="data is required")

            values = [d.get(field, 0) for d in data]
            min_val = min(values)
            max_val = max(values)
            val_range = max_val - min_val

            if val_range == 0:
                normalized = [0.5] * len(data)
            else:
                normalized = [(v - min_val) / val_range for v in values]

            result = []
            for i, d in enumerate(data):
                new_d = d.copy()
                new_d[f"{field}_normalized"] = normalized[i]
                result.append(new_d)

            return ActionResult(
                success=True,
                data={"result": result, "min": min_val, "max": max_val, "count": len(result)},
                message=f"Normalized {len(data)} values: [{min_val}, {max_val}] → [0, 1]",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Transform normalize failed: {e}")


class TransformStandardizeAction(BaseAction):
    """Standardize data (z-score)."""
    action_type = "transform_standardize"
    display_name = "标准化"
    description = "Z-score标准化"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            field = params.get("field", "value")

            if not data:
                return ActionResult(success=False, message="data is required")

            values = [d.get(field, 0) for d in data]
            mean = sum(values) / len(values)
            variance = sum((v - mean) ** 2 for v in values) / len(values)
            std = variance ** 0.5

            if std == 0:
                standardized = [0.0] * len(data)
            else:
                standardized = [(v - mean) / std for v in values]

            result = []
            for i, d in enumerate(data):
                new_d = d.copy()
                new_d[f"{field}_standardized"] = standardized[i]
                result.append(new_d)

            return ActionResult(
                success=True,
                data={"result": result, "mean": mean, "std": std, "count": len(result)},
                message=f"Standardized {len(data)} values: mean={mean:.2f}, std={std:.2f}",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Transform standardize failed: {e}")


class TransformEncodeAction(BaseAction):
    """Encode categorical data."""
    action_type = "transform_encode"
    display_name = "编码"
    description = "类别数据编码"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            field = params.get("field", "category")
            encoding_type = params.get("encoding_type", "label")

            if not data:
                return ActionResult(success=False, message="data is required")

            if encoding_type == "label":
                unique_vals = sorted(set(d.get(field, "unknown") for d in data))
                val_to_idx = {v: i for i, v in enumerate(unique_vals)}
                result = []
                for d in data:
                    new_d = d.copy()
                    new_d[f"{field}_encoded"] = val_to_idx.get(d.get(field), -1)
                    result.append(new_d)
                encoding_map = val_to_idx
            else:
                result = data
                encoding_map = {}

            return ActionResult(
                success=True,
                data={"result": result, "encoding_type": encoding_type, "unique_values": len(encoding_map)},
                message=f"Encoded {field} with {encoding_type}: {len(encoding_map)} unique values",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Transform encode failed: {e}")


class TransformScaleAction(BaseAction):
    """Scale data to specified range."""
    action_type = "transform_scale"
    display_name = "缩放"
    description = "缩放到指定范围"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            field = params.get("field", "value")
            target_min = params.get("target_min", 0)
            target_max = params.get("target_max", 100)

            if not data:
                return ActionResult(success=False, message="data is required")

            values = [d.get(field, 0) for d in data]
            src_min = min(values)
            src_max = max(values)
            src_range = src_max - src_min

            if src_range == 0:
                scaled = [(target_min + target_max) / 2] * len(data)
            else:
                scaled = [target_min + (v - src_min) / src_range * (target_max - target_min) for v in values]

            result = []
            for i, d in enumerate(data):
                new_d = d.copy()
                new_d[f"{field}_scaled"] = scaled[i]
                result.append(new_d)

            return ActionResult(
                success=True,
                data={"result": result, "target_range": [target_min, target_max], "count": len(result)},
                message=f"Scaled {len(data)} values to [{target_min}, {target_max}]",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Transform scale failed: {e}")


class TransformLogAction(BaseAction):
    """Apply log transformation."""
    action_type = "transform_log"
    display_name = "对数变换"
    description = "对数变换"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            import math
            data = params.get("data", [])
            field = params.get("field", "value")
            log_base = params.get("base", "natural")

            if not data:
                return ActionResult(success=False, message="data is required")

            if log_base == "10":
                log_fn = math.log10
            elif log_base == "2":
                log_fn = math.log2
            else:
                log_fn = math.log

            result = []
            for d in data:
                val = d.get(field, 0)
                if val <= 0:
                    transformed = None
                else:
                    try:
                        transformed = log_fn(val)
                    except ValueError:
                        transformed = None
                new_d = d.copy()
                new_d[f"{field}_log"] = transformed
                result.append(new_d)

            return ActionResult(
                success=True,
                data={"result": result, "log_base": log_base, "count": len(result)},
                message=f"Log{log_base} transformed {len(data)} values",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Transform log failed: {e}")
