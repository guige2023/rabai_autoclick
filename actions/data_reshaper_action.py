"""Data reshaper action module for RabAI AutoClick.

Provides data reshaping operations:
- ReshapePivotAction: Pivot data
- ReshapeUnpivotAction: Unpivot data
- ReshapeTransposeAction: Transpose data
- ReshapeFlattenAction: Flatten nested data
- ReshapeNestAction: Nest flat data
"""

import time
from typing import Any, Dict, List, Optional

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class ReshapePivotAction(BaseAction):
    """Pivot data from rows to columns."""
    action_type = "reshape_pivot"
    display_name = "透视转换"
    description = "数据透视转换"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            index = params.get("index", "")
            columns = params.get("columns", "")
            values = params.get("values", "")

            if not data:
                return ActionResult(success=False, message="data is required")

            pivot = {}
            for item in data:
                idx_key = str(item.get(index, ""))
                col_key = str(item.get(columns, ""))
                val = item.get(values, 0)
                if idx_key not in pivot:
                    pivot[idx_key] = {}
                pivot[idx_key][col_key] = val

            return ActionResult(
                success=True,
                data={"pivot_keys": list(pivot.keys()), "pivot_count": len(pivot), "column_count": len(set(d.get(columns) for d in data))},
                message=f"Pivoted {len(data)} rows into {len(pivot)} rows x columns",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Reshape pivot failed: {e}")


class ReshapeUnpivotAction(BaseAction):
    """Unpivot data from columns to rows."""
    action_type = "reshape_unpivot"
    display_name = "逆透视"
    description = "数据逆透视转换"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            id_vars = params.get("id_vars", [])
            value_vars = params.get("value_vars", [])

            if not data:
                return ActionResult(success=False, message="data is required")

            unpivoted = []
            for item in data:
                for var in value_vars:
                    if var in item:
                        unpivoted.append({**{k: item[k] for k in id_vars if k in item}, "variable": var, "value": item[var]})

            return ActionResult(
                success=True,
                data={"original_count": len(data), "unpivoted_count": len(unpivoted), "value_vars": value_vars},
                message=f"Unpivoted to {len(unpivoted)} rows from {len(data)} rows",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Reshape unpivot failed: {e}")


class ReshapeTransposeAction(BaseAction):
    """Transpose data (rows to columns)."""
    action_type = "reshape_transpose"
    display_name = "转置"
    description = "转置数据"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            if not data:
                return ActionResult(success=False, message="data is required")

            if not isinstance(data, list):
                return ActionResult(success=False, message="data must be a list of records")

            transposed = []
            if data:
                keys = list(data[0].keys()) if isinstance(data[0], dict) else []
                for key in keys:
                    col_vals = [row.get(key) if isinstance(row, dict) else row for row in data]
                    transposed.append({key: col_vals})

            return ActionResult(
                success=True,
                data={"original_rows": len(data), "transposed_rows": len(transposed)},
                message=f"Transposed: {len(data)} rows -> {len(transposed)} rows",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Reshape transpose failed: {e}")


class ReshapeFlattenAction(BaseAction):
    """Flatten nested data."""
    action_type = "reshape_flatten"
    display_name = "扁平化"
    description = "扁平化嵌套数据"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", {})
            separator = params.get("separator", ".")
            max_depth = params.get("max_depth", 10)

            if not data:
                return ActionResult(success=False, message="data is required")

            def flatten(obj, prefix="", depth=0):
                result = {}
                if depth > max_depth:
                    return {prefix: str(obj)}
                if isinstance(obj, dict):
                    for k, v in obj.items():
                        new_key = f"{prefix}{separator}{k}" if prefix else k
                        result.update(flatten(v, new_key, depth + 1))
                elif isinstance(obj, list):
                    for i, v in enumerate(obj):
                        new_key = f"{prefix}[{i}]"
                        result.update(flatten(v, new_key, depth + 1))
                else:
                    result[prefix] = obj
                return result

            flattened = flatten(data)

            return ActionResult(
                success=True,
                data={"flattened_keys": list(flattened.keys()), "key_count": len(flattened)},
                message=f"Flattened to {len(flattened)} keys",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Reshape flatten failed: {e}")


class ReshapeNestAction(BaseAction):
    """Nest flat data."""
    action_type = "reshape_nest"
    display_name = "嵌套化"
    description = "嵌套化扁平数据"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            keys = params.get("keys", [])
            separator = params.get("separator", ".")

            if not data or not keys:
                return ActionResult(success=False, message="data and keys are required")

            nested = {}
            for item in data:
                current = nested
                for i, key in enumerate(keys):
                    k = str(item.get(key, ""))
                    if i == len(keys) - 1:
                        current[k] = item
                    else:
                        if k not in current:
                            current[k] = {}
                        current = current[k]

            return ActionResult(
                success=True,
                data={"nested_depth": len(keys), "top_level_keys": list(nested.keys())},
                message=f"Nested data by {len(keys)} keys: {len(nested)} top-level groups",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Reshape nest failed: {e}")
