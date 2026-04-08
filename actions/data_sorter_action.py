"""Data sorter action module for RabAI AutoClick.

Provides data sorting:
- DataSorterAction: Sort data
- MultiKeySorterAction: Sort by multiple keys
- CustomSorterAction: Custom sort function
"""

from typing import Any, Dict, List, Optional, Callable
from datetime import datetime

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class DataSorterAction(BaseAction):
    """Sort data."""
    action_type = "data_sorter"
    display_name = "数据排序"
    description = "对数据排序"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            sort_by = params.get("sort_by", None)
            order = params.get("order", "asc")

            if not isinstance(data, list):
                return ActionResult(success=False, message="data must be a list")

            if not sort_by:
                sorted_data = sorted(data, reverse=(order == "desc"))
            else:
                sorted_data = sorted(
                    data,
                    key=lambda x: x.get(sort_by, "") if isinstance(x, dict) else getattr(x, sort_by, ""),
                    reverse=(order == "desc")
                )

            return ActionResult(
                success=True,
                data={
                    "sorted": sorted_data,
                    "sort_by": sort_by,
                    "order": order,
                    "count": len(sorted_data)
                },
                message=f"Sorted: {len(data)} items by {sort_by or 'value'} ({order})"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Data sorter error: {str(e)}")


class MultiKeySorterAction(BaseAction):
    """Sort by multiple keys."""
    action_type = "multi_key_sorter"
    display_name = "多键排序"
    description = "按多个键排序"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            sort_keys = params.get("sort_keys", [])
            orders = params.get("orders", ["asc"] * 10)

            if not sort_keys:
                return ActionResult(success=False, message="sort_keys is required")

            def sort_key(item):
                keys = []
                for i, sort_key in enumerate(sort_keys):
                    order = orders[i] if i < len(orders) else "asc"
                    value = item.get(sort_key, "") if isinstance(item, dict) else getattr(item, sort_key, "")
                    if order == "desc":
                        keys.append((order, value))
                    else:
                        keys.append((order, value))
                return keys

            sorted_data = sorted(data, key=sort_key)

            return ActionResult(
                success=True,
                data={
                    "sorted": sorted_data,
                    "sort_keys": sort_keys,
                    "count": len(sorted_data)
                },
                message=f"Multi-key sorted: {len(data)} items by {sort_keys}"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Multi-key sorter error: {str(e)}")


class CustomSorterAction(BaseAction):
    """Custom sort function."""
    action_type = "custom_sorter"
    display_name = "自定义排序"
    description = "自定义排序函数"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            sort_func_name = params.get("sort_func", "length")
            order = params.get("order", "asc")

            def get_sort_key(item):
                if sort_func_name == "length":
                    return len(str(item))
                elif sort_func_name == "str_lower":
                    return str(item).lower()
                elif sort_func_name == "type":
                    return type(item).__name__
                elif sort_func_name == "reverse":
                    return str(item)[::-1]
                else:
                    return str(item)

            sorted_data = sorted(data, key=get_sort_key, reverse=(order == "desc"))

            return ActionResult(
                success=True,
                data={
                    "sorted": sorted_data,
                    "sort_func": sort_func_name,
                    "order": order,
                    "count": len(sorted_data)
                },
                message=f"Custom sorted: {len(data)} items by {sort_func_name}"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Custom sorter error: {str(e)}")
