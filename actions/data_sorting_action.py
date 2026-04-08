"""Data sorting action module for RabAI AutoClick.

Provides data sorting operations:
- SortAction: Sort data by fields
- MultiKeySortAction: Multi-key sorting
- CustomSortAction: Custom sorting with comparator
- TopNAction: Get top/bottom N items
"""

from typing import Any, Callable, Dict, List, Optional

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class SortAction(BaseAction):
    """Sort data by fields."""
    action_type = "sort"
    display_name = "数据排序"
    description = "按字段排序数据"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            sort_by = params.get("sort_by", "value")
            order = params.get("order", "asc")
            numeric = params.get("numeric", True)

            if not isinstance(data, list):
                data = [data]

            if not data:
                return ActionResult(success=True, message="Empty data", data={"sorted": [], "count": 0})

            def get_sort_key(item):
                if isinstance(item, dict):
                    return item.get(sort_by)
                return item

            reverse = order == "desc"

            try:
                if numeric:
                    sorted_data = sorted(data, key=lambda x: float(get_sort_key(x)) if get_sort_key(x) is not None else float("-inf"), reverse=reverse)
                else:
                    sorted_data = sorted(data, key=lambda x: str(get_sort_key(x)) if get_sort_key(x) is not None else "", reverse=reverse)
            except (ValueError, TypeError):
                sorted_data = sorted(data, key=lambda x: str(get_sort_key(x)) if get_sort_key(x) is not None else "", reverse=reverse)

            return ActionResult(
                success=True,
                message=f"Sorted {len(data)} items by {sort_by} ({order})",
                data={"sorted": sorted_data, "count": len(sorted_data), "sort_by": sort_by, "order": order},
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Sort error: {e}")


class MultiKeySortAction(BaseAction):
    """Multi-key sorting."""
    action_type = "multi_key_sort"
    display_name = "多键排序"
    description: "多键组合排序"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            sort_keys = params.get("sort_keys", [])
            orders = params.get("orders", None)

            if not isinstance(data, list):
                data = [data]

            if not sort_keys:
                return ActionResult(success=False, message="sort_keys is required")

            if orders is None:
                orders = ["asc"] * len(sort_keys)

            def make_key(item):
                keys = []
                for i, k in enumerate(sort_keys):
                    val = item.get(k) if isinstance(item, dict) else None
                    order = orders[i] if i < len(orders) else "asc"
                    if val is None:
                        keys.append(("", order == "desc"))
                    elif isinstance(val, (int, float)):
                        keys.append((val, order == "desc"))
                    else:
                        keys.append((str(val), order == "desc"))
                return keys

            sorted_data = sorted(data, key=make_key)

            return ActionResult(
                success=True,
                message=f"Multi-key sorted {len(data)} items by {sort_keys}",
                data={"sorted": sorted_data, "count": len(sorted_data), "sort_keys": sort_keys, "orders": orders},
            )
        except Exception as e:
            return ActionResult(success=False, message=f"MultiKeySort error: {e}")


class CustomSortAction(BaseAction):
    """Custom sorting with comparator."""
    action_type = "custom_sort"
    display_name: "自定义排序"
    description = "使用自定义比较器排序"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            comparator = params.get("comparator", "natural")
            order = params.get("order", "asc")

            if not isinstance(data, list):
                data = [data]

            def natural_compare(a, b):
                if isinstance(a, (int, float)) and isinstance(b, (int, float)):
                    return (a > b) - (a < b)
                return (str(a) > str(b)) - (str(a) < str(b))

            def length_compare(a, b):
                return (len(str(a)) > len(str(b))) - (len(str(a)) < len(str(b)))

            def case_insensitive_compare(a, b):
                return (str(a).lower() > str(b).lower()) - (str(a).lower() < str(b).lower())

            if comparator == "natural":
                cmp_fn = natural_compare
            elif comparator == "length":
                cmp_fn = length_compare
            elif comparator == "case_insensitive":
                cmp_fn = case_insensitive_compare
            else:
                cmp_fn = natural_compare

            reverse = order == "desc"

            def sort_key(item):
                return item

            sorted_data = sorted(data, key=sort_key, reverse=reverse)

            return ActionResult(
                success=True,
                message=f"Custom sorted {len(data)} items ({comparator}, {order})",
                data={"sorted": sorted_data, "count": len(sorted_data), "comparator": comparator, "order": order},
            )
        except Exception as e:
            return ActionResult(success=False, message=f"CustomSort error: {e}")


class TopNAction(BaseAction):
    """Get top/bottom N items."""
    action_type = "top_n"
    display_name = "TopN选取"
    description = "选取最大/最小的N项"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            n = params.get("n", 10)
            sort_by = params.get("sort_by", "value")
            order = params.get("order", "desc")

            if not isinstance(data, list):
                data = [data]

            if n <= 0:
                return ActionResult(success=False, message="n must be positive")

            def get_val(item):
                if isinstance(item, dict):
                    return item.get(sort_by)
                return item

            try:
                sorted_data = sorted(data, key=lambda x: float(get_val(x)) if get_val(x) is not None else float("-inf"), reverse=(order == "desc"))
            except (ValueError, TypeError):
                sorted_data = sorted(data, key=lambda x: str(get_val(x)), reverse=(order == "desc"))

            top_n = sorted_data[:n]

            return ActionResult(
                success=True,
                message=f"Top {n} items by {sort_by} ({order})",
                data={
                    "top_n": top_n,
                    "count": len(top_n),
                    "n": n,
                    "sort_by": sort_by,
                    "order": order,
                },
            )
        except Exception as e:
            return ActionResult(success=False, message=f"TopN error: {e}")
