"""Data pivot action module for RabAI AutoClick.

Provides pivot table and reshaping operations:
- PivotAction: Create pivot tables
- UnpivotAction: Unpivot/melt data
- TransposeAction: Transpose rows and columns
- WideToLongAction: Reshape wide to long format
"""

from typing import Any, Dict, List, Optional, Set
from collections import defaultdict

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class PivotAction(BaseAction):
    """Create pivot tables."""
    action_type = "pivot"
    display_name = "数据透视"
    description = "创建数据透视表"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            rows = params.get("rows", [])
            columns = params.get("columns", [])
            values = params.get("values", [])
            aggregation = params.get("aggregation", "sum")
            fill_value = params.get("fill_value", 0)

            if not isinstance(data, list):
                data = [data]

            if not rows or not values:
                return ActionResult(success=False, message="rows and values are required")

            pivot: Dict[Tuple, Dict[Tuple, List]] = defaultdict(lambda: defaultdict(list))

            for item in data:
                if not isinstance(item, dict):
                    continue
                row_key = tuple(item.get(r) for r in rows)
                col_key = tuple(item.get(c) for c in columns) if columns else (None,)

                for val_field in values:
                    val = item.get(val_field, 0)
                    if isinstance(val, (int, float)):
                        pivot[row_key][col_key].append(val)

            all_col_keys = set()
            for col_dict in pivot.values():
                all_col_keys.update(col_dict.keys())
            all_col_keys.discard((None,))

            results = []
            for row_key in sorted(pivot.keys()):
                row_data = {rows[i]: row_key[i] for i in range(len(rows))}

                for col_key in all_col_keys:
                    col_label_parts = [str(k) for k in col_key]
                    col_label = "_".join(col_label_parts) if col_key != (None,) else "total"

                    vals = pivot[row_key].get(col_key, [])
                    if aggregation == "sum":
                        agg_val = sum(vals)
                    elif aggregation == "count":
                        agg_val = len(vals)
                    elif aggregation == "avg":
                        agg_val = sum(vals) / len(vals) if vals else 0
                    elif aggregation == "min":
                        agg_val = min(vals) if vals else None
                    elif aggregation == "max":
                        agg_val = max(vals) if vals else None
                    elif aggregation == "first":
                        agg_val = vals[0] if vals else None
                    elif aggregation == "last":
                        agg_val = vals[-1] if vals else None
                    else:
                        agg_val = sum(vals)

                    row_data[f"{val_field}_{col_label}" if len(values) > 1 else col_label] = agg_val

                results.append(row_data)

            return ActionResult(
                success=True,
                message=f"Pivot table: {len(results)} rows x {len(all_col_keys)} columns",
                data={"pivot_table": results, "row_count": len(results), "column_count": len(all_col_keys)},
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Pivot error: {e}")


class UnpivotAction(BaseAction):
    """Unpivot/melt data."""
    action_type = "unpivot"
    display_name = "逆透视"
    description = "将宽格式数据逆透视为长格式"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            id_vars = params.get("id_vars", [])
            value_vars = params.get("value_vars", [])
            var_name = params.get("var_name", "variable")
            value_name = params.get("value_name", "value")

            if not isinstance(data, list):
                data = [data]

            if not value_vars:
                if data and isinstance(data[0], dict):
                    if id_vars:
                        value_vars = [k for k in data[0].keys() if k not in id_vars]
                    else:
                        value_vars = list(data[0].keys())

            unpivoted = []
            for item in data:
                if not isinstance(item, dict):
                    continue
                for var in value_vars:
                    unpivoted.append({
                        **{k: item.get(k) for k in id_vars if k in item},
                        var_name: var,
                        value_name: item.get(var),
                    })

            return ActionResult(
                success=True,
                message=f"Unpivoted {len(data)} rows into {len(unpivoted)} rows",
                data={"unpivoted": unpivoted, "row_count": len(unpivoted), "column_count": len(id_vars) + 2},
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Unpivot error: {e}")


class TransposeAction(BaseAction):
    """Transpose rows and columns."""
    action_type = "transpose"
    display_name = "转置"
    description = "转置数据的行和列"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            index_field = params.get("index_field", None)

            if not isinstance(data, list):
                data = [data]

            if not data:
                return ActionResult(success=False, message="data is empty")

            if not isinstance(data[0], dict):
                data = [{"value": item} for item in data]

            if index_field:
                index_values = [item.get(index_field) for item in data]
                data = [{k: v for k, v in item.items() if k != index_field} for item in data]
            else:
                index_values = [f"row_{i}" for i in range(len(data))]

            all_columns = set()
            for item in data:
                all_columns.update(item.keys())
            all_columns = sorted(all_columns)

            transposed = []
            for col in all_columns:
                row_data = {index_field: col} if index_field else {}
                for i, item in enumerate(data):
                    row_data[index_values[i]] = item.get(col)
                transposed.append(row_data)

            return ActionResult(
                success=True,
                message=f"Transposed {len(data)}x{len(all_columns)} to {len(all_columns)}x{len(data)}",
                data={"transposed": transposed, "rows": len(transposed), "columns": len(data)},
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Transpose error: {e}")


class WideToLongAction(BaseAction):
    """Reshape wide to long format."""
    action_type = "wide_to_long"
    display_name = "宽转长"
    description = "将宽格式数据转换为长格式"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            id_vars = params.get("id_vars", [])
            stubnames = params.get("stubnames", [])
            i = params.get("i", None)
            j = params.get("j", "time")

            if not isinstance(data, list):
                data = [data]

            if not data:
                return ActionResult(success=False, message="data is empty")

            if not stubnames and data and isinstance(data[0], dict):
                all_keys = set(data[0].keys())
                stubnames = [k for k in all_keys if any(k.startswith(s) for s in ["value", "measure", "var"])]

            if not stubnames:
                return ActionResult(success=False, message="stubnames required")

            long_data = []
            for item in data:
                if not isinstance(item, dict):
                    continue

                id_data = {k: item.get(k) for k in id_vars if k in item}

                for stub in stubnames:
                    for key, val in item.items():
                        if key == stub:
                            continue
                        if key.startswith(stub):
                            suffix = key[len(stub) :]
                            long_item = {
                                **id_data,
                                j: suffix,
                                stub: val,
                            }
                            long_data.append(long_item)

            return ActionResult(
                success=True,
                message=f"Reshaped {len(data)} wide rows to {len(long_data)} long rows",
                data={"long_data": long_data, "row_count": len(long_data)},
            )
        except Exception as e:
            return ActionResult(success=False, message=f"WideToLong error: {e}")
