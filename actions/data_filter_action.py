"""
Data Filter Action Module

Advanced data filtering with predicate expressions, 
complex conditions, and transformation support.

MIT License - Copyright (c) 2025 RabAi Research
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set, Union

logger = logging.getLogger(__name__)


class FilterOperator(Enum):
    """Filter operators."""

    EQ = "eq"
    NE = "ne"
    GT = "gt"
    GE = "ge"
    LT = "lt"
    LE = "le"
    IN = "in"
    NOT_IN = "not_in"
    CONTAINS = "contains"
    NOT_CONTAINS = "not_contains"
    STARTS_WITH = "starts_with"
    ENDS_WITH = "ends_with"
    REGEX = "regex"
    IS_NULL = "is_null"
    IS_NOT_NULL = "is_not_null"
    BETWEEN = "between"


@dataclass
class FilterCondition:
    """A single filter condition."""

    field: str
    operator: FilterOperator
    value: Any = None
    case_sensitive: bool = True

    def matches(self, item: Dict[str, Any]) -> bool:
        """Check if item matches this condition."""
        field_value = self._get_nested_field(item, self.field)

        if self.operator == FilterOperator.IS_NULL:
            return field_value is None

        if self.operator == FilterOperator.IS_NOT_NULL:
            return field_value is not None

        if field_value is None:
            return False

        # String operations
        if self.operator in (
            FilterOperator.CONTAINS,
            FilterOperator.NOT_CONTAINS,
            FilterOperator.STARTS_WITH,
            FilterOperator.ENDS_WITH,
        ):
            return self._string_match(field_value)

        # Numeric/string comparisons
        if self.operator == FilterOperator.EQ:
            return self._compare(field_value, lambda a, b: a == b)
        elif self.operator == FilterOperator.NE:
            return self._compare(field_value, lambda a, b: a != b)
        elif self.operator == FilterOperator.GT:
            return self._compare(field_value, lambda a, b: a > b)
        elif self.operator == FilterOperator.GE:
            return self._compare(field_value, lambda a, b: a >= b)
        elif self.operator == FilterOperator.LT:
            return self._compare(field_value, lambda a, b: a < b)
        elif self.operator == FilterOperator.LE:
            return self._compare(field_value, lambda a, b: a <= b)
        elif self.operator == FilterOperator.IN:
            return field_value in self.value
        elif self.operator == FilterOperator.NOT_IN:
            return field_value not in self.value
        elif self.operator == FilterOperator.REGEX:
            return self._regex_match(field_value)
        elif self.operator == FilterOperator.BETWEEN:
            return self._between(field_value)

        return False

    def _get_nested_field(self, item: Dict[str, Any], field_path: str) -> Any:
        """Get nested field value using dot notation."""
        parts = field_path.split(".")
        value = item

        for part in parts:
            if isinstance(value, dict):
                value = value.get(part)
            else:
                return None

        return value

    def _string_match(self, field_value: Any) -> bool:
        """Handle string matching operations."""
        str_value = str(field_value)
        str_pattern = str(self.value)

        if not self.case_sensitive:
            str_value = str_value.lower()
            str_pattern = str_pattern.lower()

        if self.operator == FilterOperator.CONTAINS:
            return str_pattern in str_value
        elif self.operator == FilterOperator.NOT_CONTAINS:
            return str_pattern not in str_value
        elif self.operator == FilterOperator.STARTS_WITH:
            return str_value.startswith(str_pattern)
        elif self.operator == FilterOperator.ENDS_WITH:
            return str_value.endswith(str_pattern)

        return False

    def _compare(self, field_value: Any, op: Callable[[Any, Any], bool]) -> bool:
        """Handle comparison operations."""
        if isinstance(field_value, str) or isinstance(self.value, str):
            if not self.case_sensitive:
                return op(str(field_value).lower(), str(self.value).lower())
            return op(str(field_value), str(self.value))

        try:
            return op(float(field_value), float(self.value))
        except (ValueError, TypeError):
            return op(field_value, self.value)

    def _regex_match(self, field_value: Any) -> bool:
        """Handle regex matching."""
        try:
            pattern = re.compile(self.value, 0 if self.case_sensitive else re.IGNORECASE)
            return bool(pattern.search(str(field_value)))
        except re.error:
            logger.error(f"Invalid regex pattern: {self.value}")
            return False

    def _between(self, field_value: Any) -> bool:
        """Handle between operation."""
        if not isinstance(self.value, (list, tuple)) or len(self.value) != 2:
            return False

        try:
            low = float(self.value[0])
            high = float(self.value[1])
            val = float(field_value)
            return low <= val <= high
        except (ValueError, TypeError):
            return False


class FilterLogic(Enum):
    """Logical operators for combining conditions."""

    AND = "and"
    OR = "or"
    NOT = "not"


@dataclass
class FilterGroup:
    """Group of filter conditions with logical operator."""

    conditions: List[Union[FilterCondition, "FilterGroup"]] = field(default_factory=list)
    logic: FilterLogic = FilterLogic.AND

    def matches(self, item: Dict[str, Any]) -> bool:
        """Check if item matches this filter group."""
        if not self.conditions:
            return True

        if self.logic == FilterLogic.AND:
            return all(self._match_condition(c, item) for c in self.conditions)
        elif self.logic == FilterLogic.OR:
            return any(self._match_condition(c, item) for c in self.conditions)
        elif self.logic == FilterLogic.NOT:
            return not any(self._match_condition(c, item) for c in self.conditions)

        return True

    def _match_condition(
        self,
        condition: Union[FilterCondition, "FilterGroup"],
        item: Dict[str, Any],
    ) -> bool:
        """Match a single condition or group."""
        if isinstance(condition, FilterCondition):
            return condition.matches(item)
        return condition.matches(item)


@dataclass
class FilterResult:
    """Result of a filter operation."""

    passed: List[Dict[str, Any]] = field(default_factory=list)
    rejected: List[Dict[str, Any]] = field(default_factory=list)
    total_count: int = 0
    passed_count: int = 0
    rejected_count: int = 0


class DataFilterAction:
    """
    Main action class for data filtering.

    Features:
    - Multiple filter operators (eq, ne, gt, lt, contains, regex, etc.)
    - Nested field access using dot notation
    - Logical grouping (AND, OR, NOT)
    - Case-insensitive string matching
    - Custom filter functions

    Usage:
        action = DataFilterAction()
        action.where("age", FilterOperator.GT, 18)
        action.where("status", FilterOperator.IN, ["active", "pending"])
        action.where("name", FilterOperator.CONTAINS, "John", case_sensitive=False)
        result = action.apply(data)
    """

    def __init__(self):
        self._conditions: List[FilterCondition] = []
        self._filter_groups: List[FilterGroup] = []
        self._custom_filters: List[Callable[[Dict[str, Any]], bool]] = []
        self._stats = {
            "total_processed": 0,
            "passed": 0,
            "rejected": 0,
        }

    def where(
        self,
        field: str,
        operator: FilterOperator,
        value: Any = None,
        case_sensitive: bool = True,
    ) -> "DataFilterAction":
        """Add a WHERE condition."""
        condition = FilterCondition(
            field=field,
            operator=operator,
            value=value,
            case_sensitive=case_sensitive,
        )
        self._conditions.append(condition)
        return self

    def where_eq(self, field: str, value: Any) -> "DataFilterAction":
        """Add equals condition."""
        return self.where(field, FilterOperator.EQ, value)

    def where_ne(self, field: str, value: Any) -> "DataFilterAction":
        """Add not equals condition."""
        return self.where(field, FilterOperator.NE, value)

    def where_gt(self, field: str, value: Any) -> "DataFilterAction":
        """Add greater than condition."""
        return self.where(field, FilterOperator.GT, value)

    def where_ge(self, field: str, value: Any) -> "DataFilterAction":
        """Add greater than or equals condition."""
        return self.where(field, FilterOperator.GE, value)

    def where_lt(self, field: str, value: Any) -> "DataFilterAction":
        """Add less than condition."""
        return self.where(field, FilterOperator.LT, value)

    def where_le(self, field: str, value: Any) -> "DataFilterAction":
        """Add less than or equals condition."""
        return self.where(field, FilterOperator.LE, value)

    def where_in(self, field: str, values: List[Any]) -> "DataFilterAction":
        """Add IN condition."""
        return self.where(field, FilterOperator.IN, values)

    def where_contains(self, field: str, value: Any, case_sensitive: bool = True) -> "DataFilterAction":
        """Add contains condition."""
        return self.where(field, FilterOperator.CONTAINS, value, case_sensitive)

    def where_starts_with(self, field: str, value: Any, case_sensitive: bool = True) -> "DataFilterAction":
        """Add starts with condition."""
        return self.where(field, FilterOperator.STARTS_WITH, value, case_sensitive)

    def where_regex(self, field: str, pattern: str, case_sensitive: bool = True) -> "DataFilterAction":
        """Add regex condition."""
        return self.where(field, FilterOperator.REGEX, pattern, case_sensitive)

    def where_between(
        self,
        field: str,
        low: Any,
        high: Any,
    ) -> "DataFilterAction":
        """Add between condition."""
        return self.where(field, FilterOperator.BETWEEN, [low, high])

    def where_null(self, field: str) -> "DataFilterAction":
        """Add IS NULL condition."""
        return self.where(field, FilterOperator.IS_NULL)

    def where_not_null(self, field: str) -> "DataFilterAction":
        """Add IS NOT NULL condition."""
        return self.where(field, FilterOperator.IS_NOT_NULL)

    def group(
        self,
        conditions: List[tuple],
        logic: FilterLogic = FilterLogic.AND,
    ) -> "DataFilterAction":
        """Add a group of conditions with logical operator."""
        filter_conditions = []
        for field_name, operator, value in conditions:
            filter_conditions.append(FilterCondition(
                field=field_name,
                operator=operator,
                value=value,
            ))

        self._filter_groups.append(FilterGroup(
            conditions=filter_conditions,
            logic=logic,
        ))
        return self

    def add_custom_filter(
        self,
        filter_fn: Callable[[Dict[str, Any]], bool],
    ) -> "DataFilterAction":
        """Add a custom filter function."""
        self._custom_filters.append(filter_fn)
        return self

    def _matches_conditions(self, item: Dict[str, Any]) -> bool:
        """Check if item matches all conditions."""
        return all(c.matches(item) for c in self._conditions)

    def _matches_groups(self, item: Dict[str, Any]) -> bool:
        """Check if item matches all filter groups."""
        return all(g.matches(item) for g in self._filter_groups)

    def _matches_custom(self, item: Dict[str, Any]) -> bool:
        """Check if item matches all custom filters."""
        return all(f(item) for f in self._custom_filters)

    def apply(self, data: List[Dict[str, Any]]) -> FilterResult:
        """Apply filters to data."""
        result = FilterResult(total_count=len(data))

        for item in data:
            if (
                self._matches_conditions(item)
                and self._matches_groups(item)
                and self._matches_custom(item)
            ):
                result.passed.append(item)
            else:
                result.rejected.append(item)

        result.passed_count = len(result.passed)
        result.rejected_count = len(result.rejected)

        self._stats["total_processed"] += result.total_count
        self._stats["passed"] += result.passed_count
        self._stats["rejected"] += result.rejected_count

        return result

    def filter(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Apply filters and return only passed items."""
        result = self.apply(data)
        return result.passed

    def exclude(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Apply filters and return only rejected items."""
        result = self.apply(data)
        return result.rejected

    def get_stats(self) -> Dict[str, int]:
        """Get filter statistics."""
        return self._stats.copy()


def demo_filter():
    """Demonstrate filter usage."""
    data = [
        {"name": "Alice", "age": 30, "status": "active", "tags": ["admin", "user"]},
        {"name": "Bob", "age": 25, "status": "pending", "tags": ["user"]},
        {"name": "Charlie", "age": 35, "status": "active", "tags": ["manager", "user"]},
        {"name": "Diana", "age": 28, "status": "inactive", "tags": ["user"]},
        {"name": "Eve", "age": 32, "status": "active", "tags": ["admin"]},
    ]

    action = DataFilterAction()
    action.where("status", FilterOperator.EQ, "active")
    action.where("age", FilterOperator.GE, 28)

    result = action.apply(data)
    print(f"Passed: {result.passed_count}, Rejected: {result.rejected_count}")

    for item in result.passed:
        print(f"  - {item['name']}, age={item['age']}")


if __name__ == "__main__":
    demo_filter()
