"""Data filter action module for RabAI AutoClick.

Provides data filtering:
- DataFilterAction: Filter data
- FieldFilterAction: Filter by fields
- ConditionalFilterAction: Filter by conditions
"""

from typing import Any, Dict, List, Optional, Callable
from datetime import datetime

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class DataFilterAction(BaseAction):
    """Filter data."""
    action_type = "data_filter"
    display_name = "数据过滤"
    description = "过滤数据"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            filters = params.get("filters", [])

            if not isinstance(data, list):
                return ActionResult(success=False, message="data must be a list")

            filtered = data
            for f in filters:
                field = f.get("field", "")
                operator = f.get("operator", "==")
                value = f.get("value")

                filtered = [item for item in filtered if self._matches(item, field, operator, value)]

            return ActionResult(
                success=True,
                data={
                    "original_count": len(data),
                    "filtered_count": len(filtered),
                    "removed_count": len(data) - len(filtered),
                    "filtered": filtered,
                    "filters_applied": len(filters)
                },
                message=f"Filtered: {len(data)} -> {len(filtered)} ({len(data) - len(filtered)} removed)"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Data filter error: {str(e)}")

    def _matches(self, item: Any, field: str, op: str, value: Any) -> bool:
        if isinstance(item, dict):
            item_value = item.get(field)
        else:
            item_value = getattr(item, field, None)

        if op == "==":
            return item_value == value
        elif op == "!=":
            return item_value != value
        elif op == ">":
            return item_value > value
        elif op == "<":
            return item_value < value
        elif op == ">=":
            return item_value >= value
        elif op == "<=":
            return item_value <= value
        elif op == "in":
            return item_value in value
        elif op == "not in":
            return item_value not in value
        elif op == "contains":
            return value in str(item_value)
        elif op == "startswith":
            return str(item_value).startswith(str(value))
        elif op == "endswith":
            return str(item_value).endswith(str(value))
        elif op == "is_null":
            return item_value is None
        elif op == "is_not_null":
            return item_value is not None
        return True


class FieldFilterAction(BaseAction):
    """Filter by fields."""
    action_type = "field_filter"
    display_name = "字段过滤"
    description = "按字段过滤"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            include_fields = params.get("include_fields", None)
            exclude_fields = params.get("exclude_fields", [])

            if include_fields:
                filtered = []
                for item in data:
                    if isinstance(item, dict):
                        filtered.append({k: v for k, v in item.items() if k in include_fields})
                    else:
                        filtered.append(item)
            elif exclude_fields:
                filtered = []
                for item in data:
                    if isinstance(item, dict):
                        filtered.append({k: v for k, v in item.items() if k not in exclude_fields})
                    else:
                        filtered.append(item)
            else:
                filtered = data

            return ActionResult(
                success=True,
                data={
                    "filtered": filtered,
                    "original_count": len(data),
                    "filtered_count": len(filtered)
                },
                message=f"Field filter: {len(data)} -> {len(filtered)}"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Field filter error: {str(e)}")


class ConditionalFilterAction(BaseAction):
    """Filter by conditions."""
    action_type = "conditional_filter"
    display_name = "条件过滤"
    description = "按条件过滤"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            condition = params.get("condition", {})
            logic = params.get("logic", "and")

            filtered = []
            for item in data:
                if self._evaluate_condition(item, condition, logic):
                    filtered.append(item)

            return ActionResult(
                success=True,
                data={
                    "filtered": filtered,
                    "original_count": len(data),
                    "filtered_count": len(filtered)
                },
                message=f"Conditional filter: {len(data)} -> {len(filtered)}"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Conditional filter error: {str(e)}")

    def _evaluate_condition(self, item: Any, condition: Dict, logic: str) -> bool:
        conditions = condition.get("conditions", [])
        if not conditions:
            return True

        results = []
        for c in conditions:
            if "field" in c:
                item_value = item.get(c["field"]) if isinstance(item, dict) else None
                expected = c.get("value")
                op = c.get("op", "==")
                results.append(self._compare(item_value, op, expected))
            elif "conditions" in c:
                results.append(self._evaluate_condition(item, c, c.get("logic", "and")))

        if logic == "and":
            return all(results)
        else:
            return any(results)

    def _compare(self, actual: Any, op: str, expected: Any) -> bool:
        if op == "==":
            return actual == expected
        elif op == "!=":
            return actual != expected
        elif op == ">":
            return actual > expected
        elif op == "<":
            return actual < expected
        return False
