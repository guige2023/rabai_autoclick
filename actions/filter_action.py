"""Data filter action module for RabAI AutoClick.

Provides data filtering operations:
- FilterWhereAction: Filter by where conditions
- FilterSelectAction: Select specific fields
- FilterDistinctAction: Get distinct values
- FilterSortAction: Sort and filter combined
"""

from typing import Any, Callable, Dict, List, Optional, Union


import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class FilterWhereAction(BaseAction):
    """Filter data with where conditions."""
    action_type = "filter_where"
    display_name = "条件过滤"
    description = "按条件过滤数据"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            conditions = params.get("conditions", [])
            logical = params.get("logical", "and")

            if not data:
                return ActionResult(success=False, message="data is required")

            if not conditions:
                return ActionResult(success=True, message="No conditions, returning all", data={"filtered": data})

            def evaluate_condition(record: Dict[str, Any], cond: Dict[str, Any]) -> bool:
                field = cond.get("field", "")
                operator = cond.get("operator", "==")
                value = cond.get("value", None)
                negate = cond.get("negate", False)

                parts = field.split(".")
                current = record
                for part in parts:
                    if isinstance(current, dict) and part in current:
                        current = current[part]
                    else:
                        return False

                result = False
                if operator == "==":
                    result = current == value
                elif operator == "!=":
                    result = current != value
                elif operator == ">":
                    result = current is not None and current > value
                elif operator == "<":
                    result = current is not None and current < value
                elif operator == ">=":
                    result = current is not None and current >= value
                elif operator == "<=":
                    result = current is not None and current <= value
                elif operator == "contains":
                    result = value in str(current) if current is not None else False
                elif operator == "startswith":
                    result = str(current).startswith(str(value)) if current is not None else False
                elif operator == "endswith":
                    result = str(current).endswith(str(value)) if current is not None else False
                elif operator == "in":
                    result = current in value if isinstance(value, (list, tuple, set)) else current == value
                elif operator == "not_in":
                    result = current not in value if isinstance(value, (list, tuple, set)) else current != value
                elif operator == "is_null":
                    result = current is None
                elif operator == "is_not_null":
                    result = current is not None

                return not result if negate else result

            filtered = []
            for record in data:
                if logical == "and":
                    if all(evaluate_condition(record, c) for c in conditions):
                        filtered.append(record)
                else:
                    if any(evaluate_condition(record, c) for c in conditions):
                        filtered.append(record)

            return ActionResult(
                success=True,
                message=f"Filtered {len(data)} records to {len(filtered)}",
                data={"filtered": filtered, "original_count": len(data), "filtered_count": len(filtered)}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Filter where failed: {str(e)}")


class FilterSelectAction(BaseAction):
    """Select specific fields from data."""
    action_type = "filter_select"
    display_name = "字段选择"
    description = "选择数据中的特定字段"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            fields = params.get("fields", [])
            exclude_fields = params.get("exclude_fields", [])
            rename_fields = params.get("rename_fields", {})

            if not data:
                return ActionResult(success=False, message="data is required")

            if not fields and not exclude_fields:
                return ActionResult(success=True, message="No fields specified", data={"selected": data})

            selected = []
            for record in data:
                if isinstance(record, dict):
                    if fields:
                        new_record = {f: record.get(f) for f in fields if f in record}
                    else:
                        new_record = dict(record)
                        for ef in exclude_fields:
                            new_record.pop(ef, None)

                    for old_name, new_name in rename_fields.items():
                        if old_name in new_record:
                            new_record[new_name] = new_record.pop(old_name)

                    selected.append(new_record)
                else:
                    selected.append(record)

            return ActionResult(
                success=True,
                message=f"Selected {len(selected)} records",
                data={"selected": selected, "count": len(selected)}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Filter select failed: {str(e)}")


class FilterDistinctAction(BaseAction):
    """Get distinct values from data."""
    action_type = "filter_distinct"
    display_name = "去重过滤"
    description = "获取数据中的去重值"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            field = params.get("field", None)
            sort_by = params.get("sort_by", None)

            if not data:
                return ActionResult(success=False, message="data is required")

            if field:
                seen = set()
                distinct = []
                for r in data:
                    val = r.get(field) if isinstance(r, dict) else getattr(r, field, None)
                    key = str(val) if val is not None else "null"
                    if key not in seen:
                        seen.add(key)
                        distinct.append({"value": val, "count": 1})
                    else:
                        for d in distinct:
                            if d["value"] == val:
                                d["count"] += 1
                                break
            else:
                seen = set()
                distinct = []
                for r in data:
                    key = str(r) if not isinstance(r, (dict, list)) else str(id(r))
                    if r not in seen:
                        seen.add(r)
                        distinct.append({"value": r, "count": 1})
                    else:
                        for d in distinct:
                            if d["value"] == r:
                                d["count"] += 1
                                break

            if sort_by == "value":
                distinct.sort(key=lambda x: x["value"])
            elif sort_by == "count":
                distinct.sort(key=lambda x: x["count"], reverse=True)

            return ActionResult(
                success=True,
                message=f"Found {len(distinct)} distinct values",
                data={"distinct": distinct, "count": len(distinct)}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Filter distinct failed: {str(e)}")


class FilterSortAction(BaseAction):
    """Sort and filter combined."""
    action_type = "filter_sort"
    display_name = "排序过滤"
    description = "组合排序和过滤"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            sort_by = params.get("sort_by", [])
            ascending = params.get("ascending", True)
            limit = params.get("limit", None)
            offset = params.get("offset", 0)

            if not data:
                return ActionResult(success=False, message="data is required")

            sorted_data = list(data)
            if sort_by:
                if isinstance(sort_by, str):
                    sort_by = [sort_by]
                sorted_data.sort(key=lambda r: tuple(r.get(f) for f in sort_by), reverse=not ascending)
            elif isinstance(data[0], (int, float, str)) if data else False:
                sorted_data.sort(reverse=not ascending)

            if offset > 0:
                sorted_data = sorted_data[offset:]
            if limit:
                sorted_data = sorted_data[:limit]

            return ActionResult(
                success=True,
                message=f"Sorted {len(data)} records, returned {len(sorted_data)}",
                data={
                    "sorted": sorted_data,
                    "count": len(sorted_data),
                    "original_count": len(data)
                }
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Filter sort failed: {str(e)}")
