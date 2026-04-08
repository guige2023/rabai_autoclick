"""Filter action module for RabAI AutoClick.

Provides data filtering utilities:
- DataFilter: Filter data based on conditions
- FilterChain: Chain multiple filters
- FilterBuilder: Build filters from config
"""

from typing import Any, Callable, Dict, List, Optional, Set
from dataclasses import dataclass
import re
import uuid

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class FilterCondition:
    """Base filter condition."""

    def matches(self, item: Any) -> bool:
        """Check if item matches."""
        return True


class EqualsCondition(FilterCondition):
    """Equals condition."""

    def __init__(self, field: str, value: Any):
        self.field = field
        self.value = value

    def matches(self, item: Any) -> bool:
        """Check if equals."""
        if isinstance(item, dict):
            return item.get(self.field) == self.value
        return getattr(item, self.field, None) == self.value


class NotEqualsCondition(FilterCondition):
    """Not equals condition."""

    def __init__(self, field: str, value: Any):
        self.field = field
        self.value = value

    def matches(self, item: Any) -> bool:
        """Check if not equals."""
        if isinstance(item, dict):
            return item.get(self.field) != self.value
        return getattr(item, self.field, None) != self.value


class GreaterThanCondition(FilterCondition):
    """Greater than condition."""

    def __init__(self, field: str, value: Any):
        self.field = field
        self.value = value

    def matches(self, item: Any) -> bool:
        """Check if greater than."""
        if isinstance(item, dict):
            val = item.get(self.field)
        else:
            val = getattr(item, self.field, None)

        try:
            return float(val) > float(self.value)
        except (TypeError, ValueError):
            return False


class LessThanCondition(FilterCondition):
    """Less than condition."""

    def __init__(self, field: str, value: Any):
        self.field = field
        self.value = value

    def matches(self, item: Any) -> bool:
        """Check if less than."""
        if isinstance(item, dict):
            val = item.get(self.field)
        else:
            val = getattr(item, self.field, None)

        try:
            return float(val) < float(self.value)
        except (TypeError, ValueError):
            return False


class ContainsCondition(FilterCondition):
    """Contains condition."""

    def __init__(self, field: str, value: str, case_sensitive: bool = False):
        self.field = field
        self.value = value
        self.case_sensitive = case_sensitive

    def matches(self, item: Any) -> bool:
        """Check if contains."""
        if isinstance(item, dict):
            val = str(item.get(self.field, ""))
        else:
            val = str(getattr(item, self.field, ""))

        check_val = val if self.case_sensitive else val.lower()
        check_match = self.value if self.case_sensitive else self.value.lower()

        return check_match in check_val


class RegexCondition(FilterCondition):
    """Regex match condition."""

    def __init__(self, field: str, pattern: str):
        self.field = field
        self.pattern = re.compile(pattern)

    def matches(self, item: Any) -> bool:
        """Check if regex matches."""
        if isinstance(item, dict):
            val = str(item.get(self.field, ""))
        else:
            val = str(getattr(item, self.field, ""))

        return bool(self.pattern.search(val))


class InCondition(FilterCondition):
    """In list condition."""

    def __init__(self, field: str, values: List[Any]):
        self.field = field
        self.values = set(values)

    def matches(self, item: Any) -> bool:
        """Check if in list."""
        if isinstance(item, dict):
            val = item.get(self.field)
        else:
            val = getattr(item, self.field, None)

        return val in self.values


class AndCondition(FilterCondition):
    """AND condition."""

    def __init__(self, conditions: List[FilterCondition]):
        self.conditions = conditions

    def matches(self, item: Any) -> bool:
        """Check if all match."""
        return all(c.matches(item) for c in self.conditions)


class OrCondition(FilterCondition):
    """OR condition."""

    def __init__(self, conditions: List[FilterCondition]):
        self.conditions = conditions

    def matches(self, item: Any) -> bool:
        """Check if any matches."""
        return any(c.matches(item) for c in self.conditions)


class NotCondition(FilterCondition):
    """NOT condition."""

    def __init__(self, condition: FilterCondition):
        self.condition = condition

    def matches(self, item: Any) -> bool:
        """Check if not matches."""
        return not self.condition.matches(item)


class DataFilter:
    """Data filter."""

    def __init__(self, condition: Optional[FilterCondition] = None):
        self._condition = condition

    def filter(self, items: List[Any]) -> List[Any]:
        """Filter items."""
        if self._condition is None:
            return items.copy()
        return [item for item in items if self._condition.matches(item)]

    def exclude(self, items: List[Any]) -> List[Any]:
        """Exclude matching items."""
        if self._condition is None:
            return []
        return [item for item in items if not self._condition.matches(item)]


class FilterChain:
    """Chain of filters."""

    def __init__(self):
        self._filters: List[DataFilter] = []

    def add(self, filter_obj: DataFilter) -> "FilterChain":
        """Add a filter."""
        self._filters.append(filter_obj)
        return self

    def apply(self, items: List[Any]) -> List[Any]:
        """Apply all filters."""
        result = items
        for f in self._filters:
            result = f.filter(result)
        return result


class FilterBuilder:
    """Build filters from config."""

    @staticmethod
    def build(config: Dict[str, Any]) -> DataFilter:
        """Build filter from config."""
        conditions = []

        for cond_config in config.get("conditions", []):
            cond_type = cond_config.get("type")
            field = cond_config.get("field", "")
            value = cond_config.get("value")

            if cond_type == "equals":
                conditions.append(EqualsCondition(field, value))
            elif cond_type == "not_equals":
                conditions.append(NotEqualsCondition(field, value))
            elif cond_type == "greater_than":
                conditions.append(GreaterThanCondition(field, value))
            elif cond_type == "less_than":
                conditions.append(LessThanCondition(field, value))
            elif cond_type == "contains":
                conditions.append(ContainsCondition(field, value))
            elif cond_type == "regex":
                conditions.append(RegexCondition(field, value))
            elif cond_type == "in":
                conditions.append(InCondition(field, value))

        if not conditions:
            return DataFilter()

        logic = config.get("logic", "and")
        if logic == "and":
            condition = AndCondition(conditions)
        else:
            condition = OrCondition(conditions)

        return DataFilter(condition)


class FilterAction(BaseAction):
    """Filter action."""
    action_type = "filter"
    display_name = "数据过滤器"
    description = "条件数据过滤"

    def __init__(self):
        super().__init__()

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "filter")

            if operation == "filter":
                return self._filter(params)
            elif operation == "exclude":
                return self._exclude(params)
            elif operation == "build":
                return self._build_filter(params)
            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Filter error: {str(e)}")

    def _filter(self, params: Dict[str, Any]) -> ActionResult:
        """Filter items."""
        items = params.get("items", [])
        field = params.get("field")
        operator = params.get("operator", "eq")
        value = params.get("value")

        if not field:
            return ActionResult(success=False, message="field is required")

        if operator == "eq":
            condition = EqualsCondition(field, value)
        elif operator == "ne":
            condition = NotEqualsCondition(field, value)
        elif operator == "gt":
            condition = GreaterThanCondition(field, value)
        elif operator == "lt":
            condition = LessThanCondition(field, value)
        elif operator == "contains":
            condition = ContainsCondition(field, str(value))
        elif operator == "regex":
            condition = RegexCondition(field, str(value))
        elif operator == "in":
            condition = InCondition(field, value)
        else:
            return ActionResult(success=False, message=f"Unknown operator: {operator}")

        filter_obj = DataFilter(condition)
        result = filter_obj.filter(items)

        return ActionResult(success=True, message=f"Filtered: {len(result)}/{len(items)}", data={"items": result, "count": len(result)})

    def _exclude(self, params: Dict[str, Any]) -> ActionResult:
        """Exclude items."""
        items = params.get("items", [])
        field = params.get("field")
        value = params.get("value")

        if not field:
            return ActionResult(success=False, message="field is required")

        condition = EqualsCondition(field, value)
        filter_obj = DataFilter(condition)
        result = filter_obj.exclude(items)

        return ActionResult(success=True, message=f"Excluded: {len(result)}/{len(items)}", data={"items": result, "count": len(result)})

    def _build_filter(self, params: Dict[str, Any]) -> ActionResult:
        """Build filter from config."""
        config = params.get("config", {})

        filter_obj = FilterBuilder.build(config)
        items = params.get("items", [])
        result = filter_obj.filter(items)

        return ActionResult(success=True, message=f"Filtered: {len(result)}/{len(items)}", data={"items": result, "count": len(result)})
