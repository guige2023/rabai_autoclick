"""
Data Filter Action Module.

Provides declarative data filtering with conditions, expressions,
and support for complex logical operators.

Author: RabAi Team
"""

from __future__ import annotations

import operator
import re
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Union


class FilterOperator(Enum):
    """Filter operators."""
    EQ = "eq"
    NE = "ne"
    GT = "gt"
    GTE = "gte"
    LT = "lt"
    LTE = "lte"
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

    def evaluate(self, record: Dict[str, Any]) -> bool:
        """Evaluate condition against a record."""
        field_value = self._get_field_value(record, self.field)

        op_map = {
            FilterOperator.EQ: operator.eq,
            FilterOperator.NE: operator.ne,
            FilterOperator.GT: operator.gt,
            FilterOperator.GTE: operator.ge,
            FilterOperator.LT: operator.lt,
            FilterOperator.LTE: operator.le,
            FilterOperator.CONTAINS: self._contains,
            FilterOperator.NOT_CONTAINS: lambda a, b: not self._contains(a, b),
            FilterOperator.STARTS_WITH: lambda a, b: str(a).startswith(str(b)),
            FilterOperator.ENDS_WITH: lambda a, b: str(a).endswith(str(b)),
            FilterOperator.REGEX: self._regex_match,
            FilterOperator.IS_NULL: lambda a, b: a is None,
            FilterOperator.IS_NOT_NULL: lambda a, b: a is not None,
        }

        if self.operator == FilterOperator.IN:
            return field_value in self.value
        if self.operator == FilterOperator.NOT_IN:
            return field_value not in self.value
        if self.operator == FilterOperator.BETWEEN:
            return self.value[0] <= field_value <= self.value[1]

        if self.operator in op_map:
            return op_map[self.operator](field_value, self.value)

        return False

    def _get_field_value(self, record: Dict, field_path: str) -> Any:
        """Get field value using dot notation."""
        keys = field_path.split(".")
        value = record
        for key in keys:
            if isinstance(value, dict):
                value = value.get(key)
            else:
                return None
        return value

    def _contains(self, container: Any, item: Any) -> bool:
        """Check if container contains item."""
        if container is None:
            return False
        if isinstance(container, (list, tuple)):
            return item in container
        return str(item) in str(container)

    def _regex_match(self, value: Any, pattern: str) -> bool:
        """Check if value matches regex pattern."""
        if value is None:
            return False
        try:
            return bool(re.search(pattern, str(value)))
        except re.error:
            return False


class LogicalOperator(Enum):
    """Logical operators for combining conditions."""
    AND = "and"
    OR = "or"
    NOT = "not"


@dataclass
class FilterGroup:
    """A group of filter conditions with logical operators."""
    conditions: List[Any] = field(default_factory=list)  # FilterCondition or FilterGroup
    logical_op: LogicalOperator = LogicalOperator.AND

    def evaluate(self, record: Dict[str, Any]) -> bool:
        """Evaluate filter group against a record."""
        if not self.conditions:
            return True

        results = [c.evaluate(record) for c in self.conditions]

        if self.logical_op == LogicalOperator.AND:
            return all(results)
        elif self.logical_op == LogicalOperator.OR:
            return any(results)
        elif self.logical_op == LogicalOperator.NOT:
            return not results[0] if results else True

        return True


class DataFilter:
    """Main data filter class."""

    def __init__(self) -> None:
        self.filters: List[FilterGroup] = []

    def add_condition(
        self,
        field: str,
        operator: FilterOperator,
        value: Any = None,
    ) -> "DataFilter":
        """Add a simple condition."""
        condition = FilterCondition(field, operator, value)
        group = FilterGroup(conditions=[condition])
        self.filters.append(group)
        return self

    def add_group(
        self,
        conditions: List[FilterCondition],
        logical_op: LogicalOperator = LogicalOperator.AND,
    ) -> "DataFilter":
        """Add a group of conditions."""
        group = FilterGroup(conditions=conditions, logical_op=logical_op)
        self.filters.append(group)
        return self

    def where(
        self,
        field: str,
        operator: Union[FilterOperator, str],
        value: Any = None,
    ) -> "DataFilter":
        """Add condition using fluent API."""
        if isinstance(operator, str):
            operator = FilterOperator(operator)
        return self.add_condition(field, operator, value)

    def and_where(
        self,
        field: str,
        operator: Union[FilterOperator, str],
        value: Any = None,
    ) -> "DataFilter":
        """Add AND condition."""
        return self.where(field, operator, value)

    def or_where(
        self,
        field: str,
        operator: Union[FilterOperator, str],
        value: Any = None,
    ) -> "DataFilter":
        """Add OR condition."""
        if isinstance(operator, str):
            operator = FilterOperator(operator)
        condition = FilterCondition(field, operator, value)
        if self.filters:
            last_group = self.filters[-1]
            if len(last_group.conditions) == 1 and last_group.logical_op == LogicalOperator.AND:
                # Merge into existing AND group with OR
                new_group = FilterGroup(
                    conditions=[last_group.conditions[0], condition],
                    logical_op=LogicalOperator.OR,
                )
                self.filters[-1] = new_group
                return self
        group = FilterGroup(conditions=[condition], logical_op=LogicalOperator.OR)
        self.filters.append(group)
        return self

    def evaluate(self, record: Dict[str, Any]) -> bool:
        """Check if record passes all filters."""
        if not self.filters:
            return True
        return all(group.evaluate(record) for group in self.filters)

    def filter(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Filter a list of records."""
        return [record for record in data if self.evaluate(record)]

    async def filter_async(
        self,
        data: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        """Async filter."""
        return self.filter(data)

    @classmethod
    def from_dict(cls, config: Dict[str, Any]) -> "DataFilter":
        """Create filter from dictionary config."""
        filter_obj = cls()

        conditions = config.get("conditions", [])
        for cond in conditions:
            field = cond.get("field")
            op = FilterOperator(cond.get("operator", "eq"))
            value = cond.get("value")
            filter_obj.add_condition(field, op, value)

        return filter_obj


# Convenience functions
def eq(field: str, value: Any) -> FilterCondition:
    return FilterCondition(field, FilterOperator.EQ, value)

def ne(field: str, value: Any) -> FilterCondition:
    return FilterCondition(field, FilterOperator.NE, value)

def gt(field: str, value: Any) -> FilterCondition:
    return FilterCondition(field, FilterOperator.GT, value)

def gte(field: str, value: Any) -> FilterCondition:
    return FilterCondition(field, FilterOperator.GTE, value)

def lt(field: str, value: Any) -> FilterCondition:
    return FilterCondition(field, FilterOperator.LT, value)

def lte(field: str, value: Any) -> FilterCondition:
    return FilterCondition(field, FilterOperator.LTE, value)

def contains(field: str, value: Any) -> FilterCondition:
    return FilterCondition(field, FilterOperator.CONTAINS, value)

def in_list(field: str, values: List[Any]) -> FilterCondition:
    return FilterCondition(field, FilterOperator.IN, values)

def is_null(field: str) -> FilterCondition:
    return FilterCondition(field, FilterOperator.IS_NULL)

def is_not_null(field: str) -> FilterCondition:
    return FilterCondition(field, FilterOperator.IS_NOT_NULL)

def between(field: str, low: Any, high: Any) -> FilterCondition:
    return FilterCondition(field, FilterOperator.BETWEEN, [low, high])
