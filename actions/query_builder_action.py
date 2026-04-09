"""Query Builder Action Module.

Build and execute queries for data processing operations.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Generic, TypeVar

T = TypeVar("T")


class QueryOperator(Enum):
    """Query operators."""
    EQ = "eq"
    NE = "ne"
    GT = "gt"
    GTE = "gte"
    LT = "lt"
    LTE = "lte"
    IN = "in"
    NOT_IN = "not_in"
    CONTAINS = "contains"
    STARTS_WITH = "starts_with"
    ENDS_WITH = "ends_with"
    IS_NULL = "is_null"
    IS_NOT_NULL = "is_not_null"


class SortDirection(Enum):
    """Sort direction."""
    ASC = "asc"
    DESC = "desc"


@dataclass
class QueryCondition:
    """Single query condition."""
    field: str
    operator: QueryOperator
    value: Any = None


@dataclass
class QueryOrderBy:
    """Order by specification."""
    field: str
    direction: SortDirection = SortDirection.ASC


@dataclass
class Query:
    """Query specification."""
    conditions: list[QueryCondition] = field(default_factory=list)
    order_by: list[QueryOrderBy] = field(default_factory=list)
    limit_value: int | None = None
    offset_value: int | None = None
    select_fields: list[str] | None = None
    group_by: list[str] | None = None
    having_conditions: list[QueryCondition] = field(default_factory=list)


class QueryBuilder:
    """Fluent query builder."""

    def __init__(self) -> None:
        self._query = Query()

    def where(self, field: str, operator: QueryOperator, value: Any = None) -> QueryBuilder:
        """Add a WHERE condition."""
        self._query.conditions.append(QueryCondition(field, operator, value))
        return self

    def order_by(self, field: str, direction: SortDirection = SortDirection.ASC) -> QueryBuilder:
        """Add ORDER BY clause."""
        self._query.order_by.append(QueryOrderBy(field, direction))
        return self

    def limit(self, count: int) -> QueryBuilder:
        """Set LIMIT."""
        self._query.limit_value = count
        return self

    def offset(self, count: int) -> QueryBuilder:
        """Set OFFSET."""
        self._query.offset_value = count
        return self

    def select(self, *fields: str) -> QueryBuilder:
        """Set SELECT fields."""
        self._query.select_fields = list(fields)
        return self

    def group_by(self, *fields: str) -> QueryBuilder:
        """Set GROUP BY."""
        self._query.group_by = list(fields)
        return self

    def build(self) -> Query:
        """Build the query."""
        return self._query


class QueryExecutor:
    """Execute queries against data collections."""

    def __init__(self, data: list[dict]) -> None:
        self._data = data

    def execute(self, query: Query) -> list[dict]:
        """Execute query against data."""
        results = list(self._data)
        for condition in query.conditions:
            results = self._apply_condition(results, condition)
        if query.order_by:
            results = self._apply_sort(results, query.order_by)
        if query.select_fields:
            results = self._apply_select(results, query.select_fields)
        if query.limit_value is not None:
            results = results[:query.limit_value]
        if query.offset_value is not None:
            results = results[query.offset_value:]
        return results

    def _apply_condition(self, data: list[dict], condition: QueryCondition) -> list[dict]:
        """Apply a single condition to filter data."""
        op = condition.operator
        field = condition.field
        value = condition.value
        if op == QueryOperator.EQ:
            return [row for row in data if row.get(field) == value]
        elif op == QueryOperator.NE:
            return [row for row in data if row.get(field) != value]
        elif op == QueryOperator.GT:
            return [row for row in data if row.get(field) > value]
        elif op == QueryOperator.GTE:
            return [row for row in data if row.get(field) >= value]
        elif op == QueryOperator.LT:
            return [row for row in data if row.get(field) < value]
        elif op == QueryOperator.LTE:
            return [row for row in data if row.get(field) <= value]
        elif op == QueryOperator.IN:
            return [row for row in data if row.get(field) in value]
        elif op == QueryOperator.NOT_IN:
            return [row for row in data if row.get(field) not in value]
        elif op == QueryOperator.CONTAINS:
            return [row for row in data if value in str(row.get(field, ""))]
        elif op == QueryOperator.STARTS_WITH:
            return [row for row in data if str(row.get(field, "")).startswith(str(value))]
        elif op == QueryOperator.ENDS_WITH:
            return [row for row in data if str(row.get(field, "")).endswith(str(value))]
        elif op == QueryOperator.IS_NULL:
            return [row for row in data if field not in row or row.get(field) is None]
        elif op == QueryOperator.IS_NOT_NULL:
            return [row for row in data if field in row and row.get(field) is not None]
        return data

    def _apply_sort(self, data: list[dict], order_by: list[QueryOrderBy]) -> list[dict]:
        """Apply sorting."""
        def sort_key(row: dict) -> tuple:
            return tuple(row.get(o.field, None) for o in order_by)
        reverse = any(o.direction == SortDirection.DESC for o in order_by)
        return sorted(data, key=sort_key, reverse=reverse)

    def _apply_select(self, data: list[dict], fields: list[str]) -> list[dict]:
        """Apply field selection."""
        return [{f: row.get(f) for f in fields} for row in data]


class QueryResult:
    """Query result with metadata."""
    def __init__(self, data: list[dict], total: int, query: Query) -> None:
        self.data = data
        self.total = total
        self.count = len(data)
        self.query = query
