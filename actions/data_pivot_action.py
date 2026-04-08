"""Data pivot action module for RabAI AutoClick.

Provides data pivoting operations:
- PivotCreateAction: Create pivot table
- PivotRotateAction: Rotate pivot axes
- PivotAggregateAction: Pivot with aggregation
- PivotUnpivotAction: Unpivot data
"""

from typing import Any, Dict, List

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class PivotCreateAction(BaseAction):
    """Create a pivot table."""
    action_type = "pivot_create"
    display_name = "创建透视表"
    description = "创建数据透视表"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            index = params.get("index", [])
            columns = params.get("columns", [])
            values = params.get("values", [])
            aggfunc = params.get("aggfunc", "sum")

            if not data:
                return ActionResult(success=False, message="data is required")

            pivot: Dict = {}
            for item in data:
                idx_val = tuple(item.get(i) for i in index)
                col_val = tuple(item.get(c) for c in columns)
                if idx_val not in pivot:
                    pivot[idx_val] = {}
                if col_val not in pivot[idx_val]:
                    pivot[idx_val][col_val] = []
                for v in values:
                    pivot[idx_val][col_val].append(item.get(v, 0))

            result_rows = []
            for idx, col_data in pivot.items():
                row = dict(zip(index, idx))
                for col, vals in col_data.items():
                    col_key = "_".join(str(c) for c in col)
                    if aggfunc == "sum":
                        row[col_key] = sum(vals)
                    elif aggfunc == "avg":
                        row[col_key] = sum(vals) / len(vals) if vals else 0
                    elif aggfunc == "count":
                        row[col_key] = len(vals)
                    elif aggfunc == "min":
                        row[col_key] = min(vals) if vals else None
                    elif aggfunc == "max":
                        row[col_key] = max(vals) if vals else None
                result_rows.append(row)

            return ActionResult(
                success=True,
                data={"pivot_table": result_rows, "row_count": len(result_rows), "index": index, "columns": columns},
                message=f"Pivot table created: {len(result_rows)} rows",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Pivot create failed: {e}")


class PivotRotateAction(BaseAction):
    """Rotate pivot axes."""
    action_type = "pivot_rotate"
    display_name = "旋转透视"
    description = "旋转透视表轴"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            pivot_data = params.get("pivot_data", [])
            swap_axes = params.get("swap_axes", True)

            if not pivot_data:
                return ActionResult(success=False, message="pivot_data is required")

            rotated = []
            if swap_axes and pivot_data:
                keys = list(pivot_data[0].keys())
                for k in keys:
                    row = {"axis": k}
                    for item in pivot_data:
                        row[f"val_{k}"] = item.get(k)
                    rotated.append(row)

            return ActionResult(
                success=True,
                data={"rotated": rotated, "axis_count": len(rotated)},
                message=f"Rotated pivot: {len(rotated)} axes",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Pivot rotate failed: {e}")


class PivotAggregateAction(BaseAction):
    """Pivot with aggregation."""
    action_type = "pivot_aggregate"
    display_name = "透视聚合"
    description = "透视聚合操作"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            group_by = params.get("group_by", [])
            agg_field = params.get("agg_field", "value")
            agg_funcs = params.get("agg_funcs", ["sum", "avg", "count"])

            if not data:
                return ActionResult(success=False, message="data is required")

            groups: Dict = {}
            for item in data:
                key = tuple(item.get(g) for g in group_by)
                if key not in groups:
                    groups[key] = []
                groups[key].append(item.get(agg_field, 0))

            results = []
            for key, vals in groups.items():
                row = dict(zip(group_by, key))
                for func in agg_funcs:
                    if func == "sum":
                        row[f"{agg_field}_{func}"] = sum(vals)
                    elif func == "avg":
                        row[f"{agg_field}_{func}"] = sum(vals) / len(vals) if vals else 0
                    elif func == "count":
                        row[f"{agg_field}_{func}"] = len(vals)
                    elif func == "min":
                        row[f"{agg_field}_{func}"] = min(vals) if vals else None
                    elif func == "max":
                        row[f"{agg_field}_{func}"] = max(vals) if vals else None
                results.append(row)

            return ActionResult(
                success=True,
                data={"results": results, "group_count": len(results), "agg_funcs": agg_funcs},
                message=f"Pivot aggregate: {len(results)} groups with {[f + '=' + agg_field for f in agg_funcs]}",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Pivot aggregate failed: {e}")


class PivotUnpivotAction(BaseAction):
    """Unpivot data."""
    action_type = "pivot_unpivot"
    display_name = "逆透视"
    description = "逆透视数据"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            id_vars = params.get("id_vars", [])
            value_vars = params.get("value_vars", [])

            if not data:
                return ActionResult(success=False, message="data is required")

            unpivoted = []
            for item in data:
                id_values = {k: item.get(k) for k in id_vars if k in item}
                for v in value_vars:
                    if v in item:
                        new_row = {**id_values, "variable": v, "value": item.get(v)}
                        unpivoted.append(new_row)

            return ActionResult(
                success=True,
                data={"unpivoted": unpivoted, "row_count": len(unpivoted), "variable_count": len(value_vars)},
                message=f"Unpivoted: {len(data)} rows → {len(unpivoted)} rows",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Pivot unpivot failed: {e}")
