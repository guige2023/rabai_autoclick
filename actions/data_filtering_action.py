"""Data filtering action module for RabAI AutoClick.

Provides advanced data filtering operations:
- AdvancedFilterAction: Advanced filtering with multiple conditions
- DateRangeFilterAction: Filter by date ranges
- NumericRangeFilterAction: Filter by numeric ranges
- TextSearchFilterAction: Text search and pattern filtering
"""

import re
from datetime import datetime, timedelta
from typing import Any, Callable, Dict, List, Optional, Union

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class AdvancedFilterAction(BaseAction):
    """Advanced filtering with multiple conditions."""
    action_type = "advanced_filter"
    display_name = "高级过滤"
    description = "多条件高级数据过滤"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            conditions = params.get("conditions", [])
            logic = params.get("logic", "AND")

            if not isinstance(data, list):
                data = [data]

            if not conditions:
                return ActionResult(success=True, message="No conditions, returning all", data={"filtered": data, "count": len(data)})

            filtered = []
            for item in data:
                if not isinstance(item, dict):
                    item = {"value": item}

                results = []
                for cond in conditions:
                    result = self._evaluate_condition(item, cond)
                    results.append(result)

                if logic == "AND":
                    keep = all(results)
                elif logic == "OR":
                    keep = any(results)
                elif logic == "NOT":
                    keep = not any(results)
                else:
                    keep = all(results)

                if keep:
                    filtered.append(item)

            return ActionResult(
                success=True,
                message=f"Filtered {len(data)} items to {len(filtered)}",
                data={"filtered": filtered, "count": len(filtered), "original_count": len(data)},
            )
        except Exception as e:
            return ActionResult(success=False, message=f"AdvancedFilter error: {e}")

    def _evaluate_condition(self, item: Dict, cond: Dict) -> bool:
        field = cond.get("field")
        operator = cond.get("operator", "eq")
        value = cond.get("value")

        item_value = item.get(field) if field else item.get("value")

        if operator == "eq":
            return item_value == value
        elif operator == "ne":
            return item_value != value
        elif operator == "gt":
            return item_value is not None and item_value > value
        elif operator == "ge":
            return item_value is not None and item_value >= value
        elif operator == "lt":
            return item_value is not None and item_value < value
        elif operator == "le":
            return item_value is not None and item_value <= value
        elif operator == "in":
            return item_value in value if isinstance(value, (list, tuple, set)) else False
        elif operator == "not_in":
            return item_value not in value if isinstance(value, (list, tuple, set)) else True
        elif operator == "contains":
            return value in item_value if item_value is not None else False
        elif operator == "not_contains":
            return value not in item_value if item_value is not None else True
        elif operator == "startswith":
            return str(item_value).startswith(str(value)) if item_value is not None else False
        elif operator == "endswith":
            return str(item_value).endswith(str(value)) if item_value is not None else False
        elif operator == "regex":
            return bool(re.search(str(value), str(item_value))) if item_value is not None else False
        elif operator == "is_null":
            return item_value is None
        elif operator == "is_not_null":
            return item_value is not None
        elif operator == "is_empty":
            return item_value in ("", [], {}) or item_value is None
        elif operator == "is_not_empty":
            return item_value not in ("", [], {}) and item_value is not None
        elif operator == "between":
            if isinstance(value, (list, tuple)) and len(value) == 2:
                return value[0] <= item_value <= value[1] if item_value is not None else False
        elif operator == "length_eq":
            return len(item_value) == value if item_value is not None else False
        elif operator == "length_gt":
            return len(item_value) > value if item_value is not None else False
        elif operator == "length_lt":
            return len(item_value) < value if item_value is not None else False

        return False


class DateRangeFilterAction(BaseAction):
    """Filter by date ranges."""
    action_type = "date_range_filter"
    display_name = "日期范围过滤"
    description = "按日期范围过滤数据"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            date_field = params.get("date_field", "date")
            start_date = params.get("start_date")
            end_date = params.get("end_date")
            date_format = params.get("date_format", "%Y-%m-%d")
            timezone = params.get("timezone", "UTC")

            if not isinstance(data, list):
                data = [data]

            if start_date:
                start_dt = self._parse_date(start_date, date_format)
            else:
                start_dt = None

            if end_date:
                end_dt = self._parse_date(end_date, date_format)
                end_dt = end_dt + timedelta(days=1) - timedelta(seconds=1)
            else:
                end_dt = None

            filtered = []
            for item in data:
                if not isinstance(item, dict):
                    continue
                date_value = item.get(date_field)
                item_dt = self._parse_date(date_value, date_format)

                if item_dt is None:
                    continue

                if start_dt and item_dt < start_dt:
                    continue
                if end_dt and item_dt > end_dt:
                    continue

                filtered.append(item)

            return ActionResult(
                success=True,
                message=f"Date filter: {len(data)} -> {len(filtered)} items",
                data={
                    "filtered": filtered,
                    "count": len(filtered),
                    "original_count": len(data),
                    "start_date": str(start_date) if start_date else None,
                    "end_date": str(end_date) if end_date else None,
                },
            )
        except Exception as e:
            return ActionResult(success=False, message=f"DateRangeFilter error: {e}")

    def _parse_date(self, value: Any, fmt: str) -> Optional[datetime]:
        if isinstance(value, datetime):
            return value
        if isinstance(value, (int, float)):
            return datetime.fromtimestamp(value)
        if isinstance(value, str):
            try:
                return datetime.strptime(value, fmt)
            except ValueError:
                for try_fmt in ["%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S"]:
                    try:
                        return datetime.strptime(value, try_fmt)
                    except ValueError:
                        continue
        return None


class NumericRangeFilterAction(BaseAction):
    """Filter by numeric ranges."""
    action_type = "numeric_range_filter"
    display_name = "数值范围过滤"
    description = "按数值范围过滤数据"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            field = params.get("field", "value")
            min_value = params.get("min")
            max_value = params.get("max")
            inclusive = params.get("inclusive", True)

            if not isinstance(data, list):
                data = [data]

            filtered = []
            for item in data:
                if not isinstance(item, dict):
                    continue
                val = item.get(field)

                if not isinstance(val, (int, float)):
                    continue

                if min_value is not None:
                    if inclusive and val < min_value:
                        continue
                    if not inclusive and val <= min_value:
                        continue

                if max_value is not None:
                    if inclusive and val > max_value:
                        continue
                    if not inclusive and val >= max_value:
                        continue

                filtered.append(item)

            return ActionResult(
                success=True,
                message=f"Numeric filter: {len(data)} -> {len(filtered)} items (range: {min_value}-{max_value})",
                data={
                    "filtered": filtered,
                    "count": len(filtered),
                    "original_count": len(data),
                    "min": min_value,
                    "max": max_value,
                },
            )
        except Exception as e:
            return ActionResult(success=False, message=f"NumericRangeFilter error: {e}")


class TextSearchFilterAction(BaseAction):
    """Text search and pattern filtering."""
    action_type = "text_search_filter"
    display_name = "文本搜索过滤"
    description = "文本搜索和模式过滤"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            search_field = params.get("search_field", "text")
            query = params.get("query", "")
            match_mode = params.get("match_mode", "contains")
            case_sensitive = params.get("case_sensitive", False)
            use_regex = params.get("use_regex", False)

            if not isinstance(data, list):
                data = [data]

            filtered = []
            for item in data:
                if not isinstance(item, dict):
                    text = str(item)
                else:
                    text = str(item.get(search_field, ""))

                if not case_sensitive:
                    text = text.lower()
                    query_lower = query.lower()
                else:
                    query_lower = query

                matched = False
                if use_regex:
                    try:
                        matched = bool(re.search(query_lower, text))
                    except re.error:
                        matched = False
                else:
                    if match_mode == "contains":
                        matched = query_lower in text
                    elif match_mode == "startswith":
                        matched = text.startswith(query_lower)
                    elif match_mode == "endswith":
                        matched = text.endswith(query_lower)
                    elif match_mode == "exact":
                        matched = text == query_lower
                    elif match_mode == "word":
                        matched = query_lower in text.split()

                if matched:
                    filtered.append(item)

            return ActionResult(
                success=True,
                message=f"Text search: {len(data)} -> {len(filtered)} items",
                data={
                    "filtered": filtered,
                    "count": len(filtered),
                    "original_count": len(data),
                    "query": query,
                    "match_mode": match_mode,
                },
            )
        except Exception as e:
            return ActionResult(success=False, message=f"TextSearchFilter error: {e}")
