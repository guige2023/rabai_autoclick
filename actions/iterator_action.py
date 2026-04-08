"""Iterator action module for RabAI AutoClick.

Provides iterator operations for traversing data:
- IteratorAction: Iterate over collections
- IteratorFilterAction: Filter during iteration
- IteratorMapAction: Transform during iteration
- IteratorReduceAction: Reduce collection to single value
"""

from typing import Any, Callable, Dict, Iterator, List, Optional, TypeVar


import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult

T = TypeVar("T")
U = TypeVar("U")


class IteratorAction(BaseAction):
    """Iterate over collections."""
    action_type = "iterator"
    display_name = "迭代器"
    description = "遍历集合"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            start_index = params.get("start_index", 0)
            end_index = params.get("end_index", None)
            step = params.get("step", 1)

            if not isinstance(data, (list, tuple, set, str)):
                return ActionResult(success=False, message="data must be a collection")

            items = list(data)
            if end_index:
                items = items[start_index:end_index]
            else:
                items = items[start_index::step]

            return ActionResult(
                success=True,
                message=f"Iterating over {len(items)} items",
                data={
                    "items": items,
                    "count": len(items),
                    "has_more": end_index is not None and end_index < len(data) if isinstance(data, list) else False
                }
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Iterator failed: {str(e)}")


class IteratorFilterAction(BaseAction):
    """Filter during iteration."""
    action_type = "iterator_filter"
    display_name = "迭代过滤"
    description = "遍历时过滤"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            filter_func_ref = params.get("filter_func_ref", None)
            filter_expr = params.get("filter_expr", None)

            if not data:
                return ActionResult(success=False, message="data is required")

            filtered = []
            for item in data:
                include = False
                if filter_func_ref:
                    try:
                        include = filter_func_ref(item)
                    except Exception:
                        include = False
                elif filter_expr:
                    field = filter_expr.get("field", "")
                    operator = filter_expr.get("operator", "==")
                    value = filter_expr.get("value", None)

                    if isinstance(item, dict):
                        item_val = item.get(field)
                    else:
                        item_val = getattr(item, field, None)

                    if operator == "==":
                        include = item_val == value
                    elif operator == "!=":
                        include = item_val != value
                    elif operator == ">":
                        include = item_val is not None and item_val > value
                    elif operator == "<":
                        include = item_val is not None and item_val < value
                    elif operator == "contains":
                        include = value in str(item_val) if item_val is not None else False
                    elif operator == "is_null":
                        include = item_val is None
                    elif operator == "is_not_null":
                        include = item_val is not None
                else:
                    include = bool(item)

                if include:
                    filtered.append(item)

            return ActionResult(
                success=True,
                message=f"Filtered {len(data)} items to {len(filtered)}",
                data={"filtered": filtered, "original_count": len(data), "filtered_count": len(filtered)}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Iterator filter failed: {str(e)}")


class IteratorMapAction(BaseAction):
    """Transform during iteration."""
    action_type = "iterator_map"
    display_name = "迭代映射"
    description = "遍历时转换"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            map_func_ref = params.get("map_func_ref", None)
            field_mapping = params.get("field_mapping", {})
            expression = params.get("expression", "")

            if not data:
                return ActionResult(success=False, message="data is required")

            mapped = []
            for item in data:
                if map_func_ref:
                    try:
                        mapped.append(map_func_ref(item))
                    except Exception:
                        mapped.append(item)
                elif field_mapping:
                    if isinstance(item, dict):
                        new_item = dict(item)
                        for old_field, new_value in field_mapping.items():
                            if old_field in new_item:
                                if callable(new_value):
                                    new_item[old_field] = new_value(new_item[old_field])
                                else:
                                    new_item[old_field] = new_value
                        mapped.append(new_item)
                    else:
                        mapped.append(item)
                elif expression:
                    try:
                        result = eval(expression, {"__builtins__": {}}, {"item": item, "index": len(mapped)})
                        mapped.append(result)
                    except Exception:
                        mapped.append(item)
                else:
                    mapped.append(item)

            return ActionResult(
                success=True,
                message=f"Mapped {len(data)} items to {len(mapped)}",
                data={"mapped": mapped, "count": len(mapped)}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Iterator map failed: {str(e)}")


class IteratorReduceAction(BaseAction):
    """Reduce collection to single value."""
    action_type = "iterator_reduce"
    display_name = "迭代聚合"
    description = "将集合归约为单个值"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            reduce_func_ref = params.get("reduce_func_ref", None)
            initial_value = params.get("initial_value", None)
            reduce_type = params.get("reduce_type", None)

            if not data:
                return ActionResult(success=False, message="data is required")

            if reduce_func_ref:
                result = initial_value
                for item in data:
                    try:
                        if result is None:
                            result = item
                        else:
                            result = reduce_func_ref(result, item)
                    except Exception:
                        result = item
            elif reduce_type:
                if reduce_type == "sum":
                    try:
                        result = sum(float(item) for item in data if item is not None)
                    except (ValueError, TypeError):
                        result = 0
                elif reduce_type == "product":
                    try:
                        result = 1
                        for item in data:
                            if item is not None:
                                result *= float(item)
                    except (ValueError, TypeError):
                        result = 0
                elif reduce_type == "min":
                    try:
                        result = min(float(item) for item in data if item is not None)
                    except (ValueError, TypeError):
                        result = None
                elif reduce_type == "max":
                    try:
                        result = max(float(item) for item in data if item is not None)
                    except (ValueError, TypeError):
                        result = None
                elif reduce_type == "count":
                    result = len([x for x in data if x is not None])
                elif reduce_type == "avg":
                    try:
                        nums = [float(x) for x in data if x is not None]
                        result = sum(nums) / len(nums) if nums else 0
                    except (ValueError, TypeError):
                        result = 0
                elif reduce_type == "first":
                    result = data[0] if data else None
                elif reduce_type == "last":
                    result = data[-1] if data else None
                elif reduce_type == "concat":
                    result = "".join(str(x) for x in data)
                elif reduce_type == "join":
                    separator = params.get("separator", ",")
                    result = separator.join(str(x) for x in data)
                else:
                    return ActionResult(success=False, message=f"Unknown reduce_type: {reduce_type}")
            else:
                return ActionResult(success=False, message="reduce_func_ref or reduce_type is required")

            return ActionResult(
                success=True,
                message=f"Reduced {len(data)} items to {str(result)[:50]}",
                data={"result": result, "original_count": len(data)}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Iterator reduce failed: {str(e)}")
