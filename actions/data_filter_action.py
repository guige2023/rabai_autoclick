"""
Data Filter Action Module.

Filters datasets based on conditions, expressions, and
 custom filter functions with support for complex logic.
"""

from __future__ import annotations

import operator
from typing import Any, Callable, Optional, Union
from dataclasses import dataclass, field
from enum import Enum
import logging

logger = logging.getLogger(__name__)


class FilterOperator(Enum):
    """Filter comparison operators."""
    EQ = "eq"
    NE = "ne"
    GT = "gt"
    GE = "ge"
    LT = "lt"
    LE = "le"
    IN = "in"
    NOT_IN = "not_in"
    CONTAINS = "contains"
    STARTS_WITH = "starts_with"
    ENDS_WITH = "ends_with"
    IS_NULL = "is_null"
    IS_NOT_NULL = "is_not_null"
    REGEX = "regex"


class LogicalOperator(Enum):
    """Logical operators for combining filters."""
    AND = "and"
    OR = "or"
    NOT = "not"


@dataclass
class FilterCondition:
    """A single filter condition."""
    field: str
    operator: FilterOperator
    value: Any = None


@dataclass
class FilterGroup:
    """A group of filters combined with logical operator."""
    conditions: list[Any]
    logical: LogicalOperator = LogicalOperator.AND


@dataclass
class FilterResult:
    """Result of a filter operation."""
    filtered: list[dict[str, Any]]
    total_count: int = 0
    filtered_count: int = 0
    removed_count: int = 0


class DataFilterAction:
    """
    Dataset filtering with complex condition support.

    Filters records based on field values, expressions,
    and custom functions with AND/OR/NOT logic.

    Example:
        filter = DataFilterAction()
        filter.where("age", FilterOperator.GE, 18)
        filter.where("status", FilterOperator.IN, ["active", "pending"])
        result = filter.apply(data)
    """

    def __init__(
        self,
        default_logical: LogicalOperator = LogicalOperator.AND,
    ) -> None:
        self.default_logical = default_logical
        self._filters: list[FilterCondition] = []
        self._groups: list[FilterGroup] = []
        self._custom_funcs: dict[str, Callable] = {}

    def where(
        self,
        field: str,
        operator: FilterOperator,
        value: Any = None,
    ) -> "DataFilterAction":
        """Add a simple where condition."""
        self._filters.append(FilterCondition(field=field, operator=operator, value=value))
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

    def where_in(self, field: str, values: list[Any]) -> "DataFilterAction":
        """Add IN condition."""
        return self.where(field, FilterOperator.IN, values)

    def where_contains(self, field: str, value: str) -> "DataFilterAction":
        """Add contains condition."""
        return self.where(field, FilterOperator.CONTAINS, value)

    def where_null(self, field: str) -> "DataFilterAction":
        """Add IS NULL condition."""
        return self.where(field, FilterOperator.IS_NULL)

    def where_not_null(self, field: str) -> "DataFilterAction":
        """Add IS NOT NULL condition."""
        return self.where(field, FilterOperator.IS_NOT_NULL)

    def add_custom_filter(
        self,
        name: str,
        filter_func: Callable[[dict[str, Any]], bool],
    ) -> "DataFilterAction":
        """Add a custom filter function."""
        self._custom_funcs[name] = filter_func
        return self

    def apply(
        self,
        data: list[dict[str, Any]],
    ) -> FilterResult:
        """Apply all filters to the dataset."""
        total_count = len(data)
        filtered = data

        if self._filters:
            filtered = self._apply_conditions(filtered, self._filters, self.default_logical)

        for group in self._groups:
            group_filtered = self._apply_group(group, data)
            filtered = [r for r in filtered if r in group_filtered]

        for func in self._custom_funcs.values():
            filtered = [r for r in filtered if func(r)]

        return FilterResult(
            filtered=filtered,
            total_count=total_count,
            filtered_count=len(filtered),
            removed_count=total_count - len(filtered),
        )

    def _apply_conditions(
        self,
        data: list[dict[str, Any]],
        conditions: list[FilterCondition],
        logical: LogicalOperator,
    ) -> list[dict[str, Any]]:
        """Apply filter conditions to data."""
        if logical == LogicalOperator.AND:
            result = data
            for cond in conditions:
                result = self._filter_by_condition(result, cond)
            return result

        elif logical == LogicalOperator.OR:
            matched: set[int] = set()
            for cond in conditions:
                cond_matches = self._filter_by_condition(data, cond)
                for record in cond_matches:
                    matched.add(id(record))
            return [r for r in data if id(r) in matched]

        return data

    def _filter_by_condition(
        self,
        data: list[dict[str, Any]],
        condition: FilterCondition,
    ) -> list[dict[str, Any]]:
        """Filter data by a single condition."""
        field = condition.field
        op = condition.operator
        value = condition.value

        ops = {
            FilterOperator.EQ: lambda r, v: r.get(field) == v,
            FilterOperator.NE: lambda r, v: r.get(field) != v,
            FilterOperator.GT: lambda r, v: r.get(field) is not None and r.get(field) > v,
            FilterOperator.GE: lambda r, v: r.get(field) is not None and r.get(field) >= v,
            FilterOperator.LT: lambda r, v: r.get(field) is not None and r.get(field) < v,
            FilterOperator.LE: lambda r, v: r.get(field) is not None and r.get(field) <= v,
            FilterOperator.IN: lambda r, v: r.get(field) in v,
            FilterOperator.NOT_IN: lambda r, v: r.get(field) not in v,
            FilterOperator.CONTAINS: lambda r, v: v in str(r.get(field, "")),
            FilterOperator.STARTS_WITH: lambda r, v: str(r.get(field, "")).startswith(v),
            FilterOperator.ENDS_WITH: lambda r, v: str(r.get(field, "")).endswith(v),
            FilterOperator.IS_NULL: lambda r, v: r.get(field) is None,
            FilterOperator.IS_NOT_NULL: lambda r, v: r.get(field) is not None,
        }

        filter_func = ops.get(op)
        if not filter_func:
            return data

        return [r for r in data if filter_func(r, value)]

    def _apply_group(
        self,
        group: FilterGroup,
        data: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """Apply a filter group to data."""
        return self._apply_conditions(data, group.conditions, group.logical)

    def clear_filters(self) -> None:
        """Clear all filters."""
        self._filters.clear()
        self._groups.clear()
