"""Sort action module for RabAI AutoClick.

Provides sorting operations:
- SortByFieldAction: Sort by field
- SortMultiFieldAction: Sort by multiple fields
- SortCustomAction: Custom sort order
- SortTopNAction: Get top N sorted
"""

from typing import Any, Callable, Dict, List, Optional


import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class SortByFieldAction(BaseAction):
    """Sort by field."""
    action_type = "sort_by_field"
    display_name = "字段排序"
    description = "按字段排序"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            field = params.get("field", "")
            ascending = params.get("ascending", True)
            nulls_first = params.get("nulls_first", False)
            case_sensitive = params.get("case_sensitive", True)

            if not data:
                return ActionResult(success=False, message="data is required")

            if not field:
                result = sorted(data, key=lambda x: (x is None, x if not isinstance(x, str) else x.lower() if not case_sensitive else x), reverse=not ascending)
                if nulls_first:
                    result = sorted(data, key=lambda x: (x is None, x))
                return ActionResult(success=True, message=f"Sorted {len(result)} items", data={"result": result, "count": len(result)})

            def get_sort_key(item):
                if isinstance(item, dict):
                    val = item.get(field)
                else:
                    val = getattr(item, field, None)

                if val is None:
                    return (0 if nulls_first else 1, "")
                if isinstance(val, (int, float)):
                    return (0 if nulls_first else 1, val)
                if isinstance(val, str):
                    return (0 if nulls_first else 1, val.lower() if not case_sensitive else val)
                return (0 if nulls_first else 1, str(val))

            result = sorted(data, key=get_sort_key, reverse=not ascending)

            return ActionResult(
                success=True,
                message=f"Sorted {len(result)} items by '{field}'",
                data={"result": result, "count": len(result), "field": field}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Sort by field failed: {str(e)}")


class SortMultiFieldAction(BaseAction):
    """Sort by multiple fields."""
    action_type = "sort_multi_field"
    display_name = "多字段排序"
    description = "按多个字段排序"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            sort_fields = params.get("sort_fields", [])

            if not data:
                return ActionResult(success=False, message="data is required")
            if not sort_fields:
                return ActionResult(success=False, message="sort_fields are required")

            def get_sort_key(item):
                keys = []
                for sf in sort_fields:
                    field = sf.get("field", "")
                    ascending = sf.get("ascending", True)
                    nulls_first = sf.get("nulls_first", False)

                    if isinstance(item, dict):
                        val = item.get(field)
                    else:
                        val = getattr(item, field, None)

                    if val is None:
                        keys.append((0 if nulls_first else 1, ""))
                    elif isinstance(val, (int, float)):
                        keys.append((0 if nulls_first else 1, val))
                    else:
                        keys.append((0 if nulls_first else 1, str(val).lower()))
                return keys

            result = sorted(data, key=get_sort_key)

            return ActionResult(
                success=True,
                message=f"Sorted {len(result)} items by {len(sort_fields)} fields",
                data={"result": result, "count": len(result)}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Sort multi field failed: {str(e)}")


class SortCustomAction(BaseAction):
    """Custom sort order."""
    action_type = "sort_custom"
    display_name = "自定义排序"
    description = "自定义排序"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            sort_order = params.get("sort_order", [])
            sort_key = params.get("sort_key", "value")

            if not data:
                return ActionResult(success=False, message="data is required")
            if not sort_order:
                return ActionResult(success=False, message="sort_order is required")

            order_map = {v: i for i, v in enumerate(sort_order)}

            def custom_sort_key(item):
                if isinstance(item, dict):
                    val = item.get(sort_key)
                else:
                    val = item
                return order_map.get(val, len(order_map))

            result = sorted(data, key=custom_sort_key)

            return ActionResult(
                success=True,
                message=f"Custom sorted {len(result)} items",
                data={"result": result, "count": len(result)}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Sort custom failed: {str(e)}")


class SortTopNAction(BaseAction):
    """Get top N sorted."""
    action_type = "sort_top_n"
    display_name = "Top N排序"
    description = "获取排序后的Top N"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            n = params.get("n", 10)
            field = params.get("field", "")
            ascending = params.get("ascending", False)

            if not data:
                return ActionResult(success=False, message="data is required")

            if field:
                result = sorted(
                    data,
                    key=lambda x: (x.get(field) if isinstance(x, dict) else getattr(x, field, 0)) if field else x,
                    reverse=not ascending
                )
            else:
                result = sorted(data, reverse=not ascending)

            result = result[:n]

            return ActionResult(
                success=True,
                message=f"Top {n}: {len(result)} items",
                data={"result": result, "count": len(result), "n": n}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Sort top N failed: {str(e)}")
