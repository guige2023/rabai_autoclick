"""Data Filter Engine.

This module provides advanced data filtering:
- Multi-condition filtering
- Field projection
- Custom filter functions
- Filter optimization

Example:
    >>> from actions.data_filter_action import DataFilter
    >>> f = DataFilter()
    >>> result = f.filter(records, conditions=[{"field": "status", "op": "==", "value": "active"}])
"""

from __future__ import annotations

import logging
import threading
import re
from typing import Any, Callable, Optional
from dataclasses import dataclass, field
from enum import Enum

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
    STARTS_WITH = "starts_with"
    ENDS_WITH = "ends_with"
    REGEX = "regex"
    EXISTS = "exists"
    EMPTY = "empty"


@dataclass
class FilterCondition:
    """A single filter condition."""
    field: str
    operator: str
    value: Any = None


@dataclass
class FilterResult:
    """Result of a filter operation."""
    passed: bool
    passed_count: int
    filtered_count: int
    total_count: int


class DataFilter:
    """Advanced data filtering engine."""

    OPERATORS = {
        "==": lambda a, b: a == b,
        "!=": lambda a, b: a != b,
        "eq": lambda a, b: a == b,
        "ne": lambda a, b: a != b,
        ">": lambda a, b: a > b,
        "gt": lambda a, b: a > b,
        ">=": lambda a, b: a >= b,
        "ge": lambda a, b: a >= b,
        "<": lambda a, b: a < b,
        "lt": lambda a, b: a < b,
        "<=": lambda a, b: a <= b,
        "le": lambda a, b: a <= b,
    }

    def __init__(self) -> None:
        """Initialize the data filter."""
        self._lock = threading.Lock()
        self._stats = {"records_filtered": 0, "conditions_applied": 0}

    def filter(
        self,
        records: list[dict[str, Any]],
        conditions: list[FilterCondition],
        mode: str = "AND",
    ) -> list[dict[str, Any]]:
        """Filter records by conditions.

        Args:
            records: List of record dicts.
            conditions: List of FilterConditions.
            mode: "AND" or "OR" combination.

        Returns:
            Filtered records.
        """
        with self._lock:
            self._stats["conditions_applied"] += len(conditions)

        if not conditions:
            with self._lock:
                self._stats["records_filtered"] += len(records)
            return list(records)

        result = []
        for record in records:
            if mode == "AND":
                if all(self._evaluate_condition(record, cond) for cond in conditions):
                    result.append(record)
            else:
                if any(self._evaluate_condition(record, cond) for cond in conditions):
                    result.append(record)

        with self._lock:
            self._stats["records_filtered"] += len(result)

        return result

    def _evaluate_condition(self, record: dict[str, Any], cond: FilterCondition) -> bool:
        """Evaluate a single condition against a record."""
        value = record.get(cond.field)

        op = cond.operator.lower()

        if op == "in":
            return value in cond.value
        elif op == "not_in":
            return value not in cond.value
        elif op == "contains":
            return cond.value in str(value) if value is not None else False
        elif op == "starts_with":
            return str(value).startswith(cond.value) if value is not None else False
        elif op == "ends_with":
            return str(value).endswith(cond.value) if value is not None else False
        elif op == "regex":
            try:
                return bool(re.match(cond.value, str(value)))
            except re.error:
                return False
        elif op == "exists":
            return value is not None
        elif op == "empty":
            return value is None or value == "" or value == []
        elif op in self.OPERATORS:
            try:
                return self.OPERATORS[op](value, cond.value)
            except (TypeError, ValueError):
                return False

        return False

    def filter_and_project(
        self,
        records: list[dict[str, Any]],
        conditions: list[FilterCondition],
        fields: list[str],
    ) -> list[dict[str, Any]]:
        """Filter records and project specified fields.

        Args:
            records: List of record dicts.
            conditions: Filter conditions.
            fields: Fields to include in output.

        Returns:
            Filtered and projected records.
        """
        filtered = self.filter(records, conditions)
        return [{k: r.get(k) for k in fields if k in r} for r in filtered]

    def create_condition(
        self,
        field: str,
        operator: str,
        value: Any = None,
    ) -> FilterCondition:
        """Create a filter condition.

        Args:
            field: Field name.
            operator: Operator name.
            value: Comparison value.

        Returns:
            FilterCondition.
        """
        return FilterCondition(field=field, operator=operator, value=value)

    def exclude_fields(
        self,
        records: list[dict[str, Any]],
        fields: list[str],
    ) -> list[dict[str, Any]]:
        """Exclude fields from records.

        Args:
            records: List of records.
            fields: Fields to exclude.

        Returns:
            Records without excluded fields.
        """
        fields_set = set(fields)
        return [{k: v for k, v in r.items() if k not in fields_set} for r in records]

    def distinct(
        self,
        records: list[dict[str, Any]],
        field: str,
    ) -> list[Any]:
        """Get distinct values for a field.

        Args:
            records: List of records.
            field: Field name.

        Returns:
            List of distinct values.
        """
        seen = set()
        result = []
        for r in records:
            val = r.get(field)
            if val not in seen:
                seen.add(val)
                result.append(val)
        return result

    def sort_and_filter(
        self,
        records: list[dict[str, Any]],
        conditions: list[FilterCondition],
        sort_by: str,
        reverse: bool = False,
        limit: Optional[int] = None,
    ) -> list[dict[str, Any]]:
        """Filter, sort, and limit records.

        Args:
            records: List of records.
            conditions: Filter conditions.
            sort_by: Field to sort by.
            reverse: Sort descending.
            limit: Maximum records.

        Returns:
            Processed records.
        """
        filtered = self.filter(records, conditions)
        sorted_recs = sorted(filtered, key=lambda r: r.get(sort_by, ""), reverse=reverse)
        if limit:
            sorted_recs = sorted_recs[:limit]
        return sorted_recs

    def get_stats(self) -> dict[str, int]:
        """Get filter statistics."""
        with self._lock:
            return dict(self._stats)
