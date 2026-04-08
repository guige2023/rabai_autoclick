"""Data transform action module for RabAI AutoClick.

Provides data transformation operations:
- TransformMapAction: Map/transform field values
- TransformFlattenAction: Flatten nested structures
- TransformPivotAction: Pivot data
- TransformReshapeAction: Reshape data structures
"""

from typing import Any, Callable, Dict, List, Optional, Union


import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class TransformMapAction(BaseAction):
    """Map/transform field values."""
    action_type = "transform_map"
    display_name = "字段映射"
    description = "映射和转换字段值"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            mappings = params.get("mappings", {})
            expression_mappings = params.get("expression_mappings", [])

            if not data:
                return ActionResult(success=False, message="data is required")

            transformed = []
            for record in data:
                if isinstance(record, dict):
                    new_record = dict(record)
                    for old_field, new_value in mappings.items():
                        if old_field in new_record:
                            if callable(new_value):
                                try:
                                    new_record[old_field] = new_value(new_record[old_field])
                                except Exception:
                                    pass
                            else:
                                new_record[old_field] = new_value

                    for em in expression_mappings:
                        field_name = em.get("field", "")
                        expression = em.get("expression", "")
                        try:
                            new_record[field_name] = eval(expression, {"__builtins__": {}}, {"r": new_record})
                        except Exception:
                            new_record[field_name] = None

                    transformed.append(new_record)
                else:
                    transformed.append(record)

            return ActionResult(
                success=True,
                message=f"Transformed {len(transformed)} records",
                data={"transformed": transformed, "count": len(transformed)}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Transform map failed: {str(e)}")


class TransformFlattenAction(BaseAction):
    """Flatten nested data structures."""
    action_type = "transform_flatten"
    display_name = "扁平化"
    description = "将嵌套结构扁平化"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            separator = params.get("separator", "_")
            max_depth = params.get("max_depth", 10)
            prefix = params.get("prefix", "")

            if not data:
                return ActionResult(success=False, message="data is required")

            def flatten_value(value: Any, current_prefix: str = "", depth: int = 0) -> Dict[str, Any]:
                result = {}
                if depth >= max_depth:
                    if current_prefix:
                        result[current_prefix] = value
                    return result

                if isinstance(value, dict):
                    for k, v in value.items():
                        new_key = f"{current_prefix}{separator}{k}" if current_prefix else k
                        result.update(flatten_value(v, new_key, depth + 1))
                elif isinstance(value, (list, tuple)):
                    for i, item in enumerate(value):
                        new_key = f"{current_prefix}{separator}{i}" if current_prefix else str(i)
                        result.update(flatten_value(item, new_key, depth + 1))
                else:
                    if current_prefix:
                        result[current_prefix] = value
                return result

            flattened = []
            for record in data:
                if isinstance(record, dict):
                    flat = flatten_value(record, prefix)
                    flattened.append(flat)
                else:
                    flattened.append(record)

            return ActionResult(
                success=True,
                message=f"Flattened {len(flattened)} records",
                data={"flattened": flattened, "count": len(flattened)}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Transform flatten failed: {str(e)}")


class TransformPivotAction(BaseAction):
    """Pivot data."""
    action_type = "transform_pivot"
    display_name = "数据透视"
    description = "数据透视转换"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            index = params.get("index", [])
            columns = params.get("columns", [])
            values = params.get("values", [])
            aggregation = params.get("aggregation", "first")

            if not data:
                return ActionResult(success=False, message="data is required")
            if not index or not columns or not values:
                return ActionResult(success=False, message="index, columns, and values are required")

            from collections import defaultdict
            pivot = defaultdict(lambda: defaultdict(list))

            for record in data:
                index_key = tuple(record.get(i) for i in index)
                col_key = tuple(record.get(c) for c in columns)
                for v_field in values:
                    val = record.get(v_field)
                    pivot[index_key][col_key].append((v_field, val))

            result = []
            for idx, col_data in pivot.items():
                row = {i: v for i, v in zip(index, idx)}
                for col_key, val_list in col_data.items():
                    for v_field, val in val_list:
                        col_name = "_".join(str(k) for k in col_key) + "_" + v_field
                        if aggregation == "first":
                            row[col_name] = val_list[0][1] if val_list else None
                        elif aggregation == "last":
                            row[col_name] = val_list[-1][1] if val_list else None
                        elif aggregation == "count":
                            row[col_name] = len(val_list)
                        elif aggregation == "sum":
                            try:
                                row[col_name] = sum(float(v[1]) for v in val_list)
                            except (ValueError, TypeError):
                                row[col_name] = 0
                        elif aggregation == "avg":
                            try:
                                row[col_name] = sum(float(v[1]) for v in val_list) / len(val_list)
                            except (ValueError, TypeError):
                                row[col_name] = 0
                        else:
                            row[col_name] = val_list
                result.append(row)

            return ActionResult(
                success=True,
                message=f"Pivoted to {len(result)} rows",
                data={"pivoted": result, "count": len(result)}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Transform pivot failed: {str(e)}")


class TransformReshapeAction(BaseAction):
    """Reshape data structures."""
    action_type = "transform_reshape"
    display_name = "结构重塑"
    description = "重塑数据结构"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            reshape_type = params.get("reshape_type", "unpack")
            key_field = params.get("key_field", "")
            value_field = params.get("value_field", "")
            into_list = params.get("into_list", False)

            if not data:
                return ActionResult(success=False, message="data is required")

            if reshape_type == "unpack":
                if not key_field or not value_field:
                    return ActionResult(success=False, message="key_field and value_field required")
                result = {}
                for record in data:
                    if isinstance(record, dict):
                        key = record.get(key_field)
                        value = record.get(value_field)
                        if key is not None:
                            result[key] = value
                return ActionResult(
                    success=True,
                    message=f"Unpacked into {len(result)} keys",
                    data={"reshaped": result, "count": len(result)}
                )

            elif reshape_type == "pack":
                if not key_field:
                    return ActionResult(success=False, message="key_field required for pack")
                result = []
                if isinstance(data, dict):
                    for k, v in data.items():
                        record = {key_field: k}
                        if isinstance(v, dict):
                            record.update(v)
                        else:
                            record[value_field] = v if value_field else v
                        result.append(record)
                return ActionResult(
                    success=True,
                    message=f"Packed into {len(result)} records",
                    data={"reshaped": result, "count": len(result)}
                )

            elif reshape_type == "transpose":
                if not data:
                    return ActionResult(success=False, message="data is required")
                if not all(isinstance(r, dict) for r in data):
                    return ActionResult(success=False, message="All records must be dicts for transpose")
                keys = set()
                for r in data:
                    keys.update(r.keys())
                keys = sorted(keys)
                result = []
                for k in keys:
                    row = {"_key": k}
                    for r in data:
                        row.update(r)
                    result.append(row)
                return ActionResult(
                    success=True,
                    message=f"Transposed {len(data)} records",
                    data={"reshaped": result, "count": len(result)}
                )

            else:
                return ActionResult(success=False, message=f"Unknown reshape_type: {reshape_type}")

        except Exception as e:
            return ActionResult(success=False, message=f"Transform reshape failed: {str(e)}")
