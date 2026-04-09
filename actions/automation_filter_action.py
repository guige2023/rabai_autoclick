"""Automation filter action module for RabAI AutoClick.

Provides filtering for automation results:
- AutomationFilterAction: Filter automation results
- AutomationFilterByConditionAction: Filter by condition
- AutomationFilterByRangeAction: Filter by value range
- AutomationFilterByPatternAction: Filter by pattern matching
"""

import re
from typing import Any, Dict, List, Optional, Callable
from datetime import datetime

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class AutomationFilterAction(BaseAction):
    """Filter automation results based on criteria."""
    action_type = "automation_filter"
    display_name = "自动化结果过滤"
    description = "按条件过滤自动化结果"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            filter_type = params.get("filter_type", "value")
            key = params.get("key")
            value = params.get("value")
            operator = params.get("operator", "eq")
            invert = params.get("invert", False)

            if not data:
                return ActionResult(success=False, message="data is required")

            if isinstance(data, list) and isinstance(data[0], dict) and key:
                filtered = [item for item in data if self._evaluate_condition(item.get(key), value, operator)]
            elif isinstance(data, list):
                filtered = [item for item in data if self._evaluate_condition(item, value, operator)]
            elif isinstance(data, dict):
                filtered = {k: v for k, v in data.items() if self._evaluate_condition(v, value, operator)}
            else:
                filtered = data

            if invert:
                if isinstance(filtered, list):
                    filtered = [d for d in data if d not in filtered]
                elif isinstance(filtered, dict):
                    filtered = {k: v for k, v in data.items() if k not in filtered}

            return ActionResult(
                success=True,
                message=f"Filtered: {len(data)} → {len(filtered) if isinstance(filtered, (list, dict)) else filtered}",
                data={"filtered": filtered, "original_count": len(data) if isinstance(data, (list, dict)) else 0, "filtered_count": len(filtered) if isinstance(filtered, (list, dict)) else 0}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Filter error: {e}")

    def _evaluate_condition(self, item: Any, value: Any, operator: str) -> bool:
        """Evaluate a single condition."""
        if operator == "eq":
            return item == value
        elif operator == "ne":
            return item != value
        elif operator == "gt":
            try:
                return float(item) > float(value)
            except (TypeError, ValueError):
                return False
        elif operator == "ge":
            try:
                return float(item) >= float(value)
            except (TypeError, ValueError):
                return False
        elif operator == "lt":
            try:
                return float(item) < float(value)
            except (TypeError, ValueError):
                return False
        elif operator == "le":
            try:
                return float(item) <= float(value)
            except (TypeError, ValueError):
                return False
        elif operator == "in":
            return item in value if isinstance(value, (list, tuple)) else False
        elif operator == "not_in":
            return item not in value if isinstance(value, (list, tuple)) else True
        elif operator == "contains":
            return str(value) in str(item)
        elif operator == "starts_with":
            return str(item).startswith(str(value))
        elif operator == "ends_with":
            return str(item).endswith(str(value))
        elif operator == "is_null":
            return item is None
        elif operator == "is_not_null":
            return item is not None
        return False


class AutomationFilterByConditionAction(BaseAction):
    """Filter using custom conditions."""
    action_type = "automation_filter_by_condition"
    display_name = "自动化条件过滤"
    description = "使用自定义条件过滤"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            conditions = params.get("conditions", [])
            logic = params.get("logic", "and")
            callback = params.get("callback")

            if not data:
                return ActionResult(success=False, message="data is required")

            filtered = []

            for item in data:
                if logic == "and":
                    passes = all(self._check_condition(item, cond) for cond in conditions)
                else:
                    passes = any(self._check_condition(item, cond) for cond in conditions)

                if passes:
                    filtered.append(item)

            return ActionResult(
                success=True,
                message=f"Filtered {len(data)} → {len(filtered)} items ({logic})",
                data={"filtered": filtered, "original_count": len(data), "filtered_count": len(filtered)}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Condition filter error: {e}")

    def _check_condition(self, item: Any, condition: Dict[str, Any]) -> bool:
        """Check a single condition against an item."""
        key = condition.get("key")
        operator = condition.get("operator", "eq")
        value = condition.get("value")
        nested_key = condition.get("nested_key")

        if isinstance(item, dict):
            if nested_key:
                item = self._get_nested(item, nested_key)
            elif key:
                item = item.get(key)

        if operator == "exists":
            return item is not None
        elif operator == "not_exists":
            return item is None
        elif operator == "custom" and callable(condition.get("fn")):
            return condition["fn"](item)
        else:
            return self._compare(item, operator, value)

    def _get_nested(self, data: Dict, key_path: str) -> Any:
        """Get nested value from dict."""
        keys = key_path.split(".")
        value = data
        for k in keys:
            if isinstance(value, dict):
                value = value.get(k)
            else:
                return None
        return value

    def _compare(self, a: Any, op: str, b: Any) -> bool:
        """Compare two values."""
        if op == "==":
            return a == b
        elif op == "!=":
            return a != b
        elif op == ">":
            return a > b
        elif op == ">=":
            return a >= b
        elif op == "<":
            return a < b
        elif op == "<=":
            return a <= b
        elif op == "in":
            return a in b
        elif op == "not in":
            return a not in b
        return False


class AutomationFilterByRangeAction(BaseAction):
    """Filter data by value range."""
    action_type = "automation_filter_by_range"
    display_name = "自动化范围过滤"
    description = "按数值范围过滤"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            key = params.get("key")
            min_value = params.get("min")
            max_value = params.get("max")
            inclusive = params.get("inclusive", True)
            column = params.get("column")

            if not data:
                return ActionResult(success=False, message="data is required")

            filter_key = key or column

            if isinstance(data, list) and isinstance(data[0], dict) and filter_key:
                values = [row.get(filter_key) for row in data]
            elif isinstance(data, list):
                values = data
            else:
                values = [data]

            filtered = []
            for i, item in enumerate(data):
                val = values[i] if i < len(values) else None
                try:
                    num_val = float(val) if val is not None else None
                    in_range = True
                    if min_value is not None and num_val is not None:
                        in_range = in_range and (num_val > min_value if not inclusive else num_val >= min_value)
                    if max_value is not None and num_val is not None:
                        in_range = in_range and (num_val < max_value if not inclusive else num_val <= max_value)
                    if in_range:
                        filtered.append(item)
                except (TypeError, ValueError):
                    if val is None and min_value is None and max_value is None:
                        filtered.append(item)

            return ActionResult(
                success=True,
                message=f"Range filter: {len(data)} → {len(filtered)}",
                data={"filtered": filtered, "original_count": len(data), "filtered_count": len(filtered)}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Range filter error: {e}")


class AutomationFilterByPatternAction(BaseAction):
    """Filter data by pattern matching."""
    action_type = "automation_filter_by_pattern"
    display_name = "自动化模式过滤"
    description = "按模式匹配过滤"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            pattern = params.get("pattern", "")
            pattern_type = params.get("pattern_type", "regex")
            key = params.get("key")
            invert = params.get("invert", False)

            if not data:
                return ActionResult(success=False, message="data is required")
            if not pattern:
                return ActionResult(success=False, message="pattern is required")

            if pattern_type == "regex":
                compiled = re.compile(pattern)
                matches = lambda s: bool(compiled.search(str(s))) if s is not None else False
            elif pattern_type == "glob":
                import fnmatch
                matches = lambda s: bool(fnmatch.fnmatch(str(s), pattern)) if s is not None else False
            else:
                matches = lambda s: pattern in str(s) if s is not None else False

            filtered = []
            for item in data:
                if isinstance(item, dict):
                    val = item.get(key) if key else str(item)
                else:
                    val = item

                if matches(val) != invert:
                    filtered.append(item)

            return ActionResult(
                success=True,
                message=f"Pattern filter: {len(data)} → {len(filtered)}",
                data={"filtered": filtered, "original_count": len(data), "filtered_count": len(filtered)}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Pattern filter error: {e}")
