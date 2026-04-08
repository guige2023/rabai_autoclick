"""Data filter action module for RabAI AutoClick.

Provides data filtering operations:
- FilterApplyAction: Apply filter conditions
- FilterRangeAction: Range-based filtering
- FilterRegexAction: Regex pattern filtering
- FilterDistinctAction: Remove duplicates
- FilterSortAction: Sort and filter combined
"""

from typing import Any, Callable, Dict, List, Optional

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class FilterApplyAction(BaseAction):
    """Apply filter conditions to data."""
    action_type = "filter_apply"
    display_name = "应用过滤器"
    description = "应用过滤条件"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            conditions = params.get("conditions", [])

            if not data:
                return ActionResult(success=False, message="data is required")

            if not conditions:
                return ActionResult(success=True, data={"filtered": data, "count": len(data)}, message="No conditions, returning all")

            filtered = []
            for item in data:
                matches = True
                for cond in conditions:
                    field = cond.get("field", "")
                    operator = cond.get("operator", "eq")
                    value = cond.get("value", None)
                    item_val = item.get(field)

                    if operator == "eq":
                        if item_val != value:
                            matches = False
                    elif operator == "ne":
                        if item_val == value:
                            matches = False
                    elif operator == "gt":
                        if not (item_val is not None and item_val > value):
                            matches = False
                    elif operator == "lt":
                        if not (item_val is not None and item_val < value):
                            matches = False
                    elif operator == "gte":
                        if not (item_val is not None and item_val >= value):
                            matches = False
                    elif operator == "lte":
                        if not (item_val is not None and item_val <= value):
                            matches = False
                    elif operator == "in":
                        if item_val not in (value if isinstance(value, list) else [value]):
                            matches = False
                    elif operator == "contains":
                        if not (isinstance(item_val, str) and value in item_val):
                            matches = False

                if matches:
                    filtered.append(item)

            return ActionResult(
                success=True,
                data={"filtered": filtered, "count": len(filtered), "original_count": len(data)},
                message=f"Filtered {len(data)} → {len(filtered)} items",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Filter apply failed: {e}")


class FilterRangeAction(BaseAction):
    """Filter by range."""
    action_type = "filter_range"
    display_name = "范围过滤"
    description = "按范围过滤"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            field = params.get("field", "value")
            min_val = params.get("min", None)
            max_val = params.get("max", None)

            if not data:
                return ActionResult(success=False, message="data is required")

            filtered = []
            for item in data:
                val = item.get(field)
                if min_val is not None and (val is None or val < min_val):
                    continue
                if max_val is not None and (val is None or val > max_val):
                    continue
                filtered.append(item)

            return ActionResult(
                success=True,
                data={"filtered": filtered, "count": len(filtered), "min": min_val, "max": max_val},
                message=f"Range filter: {len(filtered)}/{len(data)} in range",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Filter range failed: {e}")


class FilterRegexAction(BaseAction):
    """Filter by regex pattern."""
    action_type = "filter_regex"
    display_name = "正则过滤"
    description = "正则表达式过滤"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            import re
            data = params.get("data", [])
            field = params.get("field", "text")
            pattern = params.get("pattern", "")
            flags = params.get("flags", 0)

            if not data:
                return ActionResult(success=False, message="data is required")
            if not pattern:
                return ActionResult(success=False, message="pattern is required")

            try:
                regex = re.compile(pattern, flags)
            except re.error as e:
                return ActionResult(success=False, message=f"Invalid regex: {e}")

            filtered = [item for item in data if isinstance(item.get(field), str) and regex.search(item.get(field, ""))]

            return ActionResult(
                success=True,
                data={"filtered": filtered, "count": len(filtered), "pattern": pattern},
                message=f"Regex filter matched {len(filtered)}/{len(data)}",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Filter regex failed: {e}")


class FilterDistinctAction(BaseAction):
    """Remove duplicate items."""
    action_type = "filter_distinct"
    display_name = "去重过滤"
    description = "去除重复项"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            key_fields = params.get("key_fields", [])

            if not data:
                return ActionResult(success=False, message="data is required")

            seen = set()
            distinct = []
            for item in data:
                if key_fields:
                    key = tuple(item.get(f) for f in key_fields)
                else:
                    key = tuple(sorted(item.items()))
                if key not in seen:
                    seen.add(key)
                    distinct.append(item)

            return ActionResult(
                success=True,
                data={"distinct": distinct, "count": len(distinct), "removed": len(data) - len(distinct)},
                message=f"Distinct: {len(data)} → {len(distinct)} (removed {len(data) - len(distinct)})",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Filter distinct failed: {e}")


class FilterSortAction(BaseAction):
    """Sort and filter combined."""
    action_type = "filter_sort"
    display_name = "排序过滤"
    description = "排序和过滤组合"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            sort_by = params.get("sort_by", "value")
            order = params.get("order", "asc")
            limit = params.get("limit", None)

            if not data:
                return ActionResult(success=False, message="data is required")

            sorted_data = sorted(data, key=lambda x: x.get(sort_by, 0), reverse=(order == "desc"))
            if limit:
                sorted_data = sorted_data[:limit]

            return ActionResult(
                success=True,
                data={"sorted": sorted_data, "count": len(sorted_data), "sort_by": sort_by, "order": order},
                message=f"Sorted by {sort_by} ({order}), {len(sorted_data)} items",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Filter sort failed: {e}")
