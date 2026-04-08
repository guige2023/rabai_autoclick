"""
Data Filter Engine Action Module.

Advanced data filtering with expressions,
predicates, combinators, and query language support.
"""

from __future__ import annotations

from typing import Any, Callable, Optional, Union
from dataclasses import dataclass
from enum import Enum
import logging
import re

logger = logging.getLogger(__name__)


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
    STARTS_WITH = "starts_with"
    ENDS_WITH = "ends_with"
    REGEX = "regex"
    IS_NULL = "is_null"
    IS_NOT_NULL = "is_not_null"


@dataclass
class FilterCondition:
    """Single filter condition."""
    field: str
    operator: FilterOperator
    value: Any = None


@dataclass
class FilterGroup:
    """Group of conditions with AND/OR logic."""
    conditions: list[Union[FilterCondition, "FilterGroup"]]
    logic: str = "AND"


class DataFilterEngineAction:
    """
    Advanced filtering engine for data structures.

    Supports field expressions, multiple operators,
    condition groups, and nested logic.

    Example:
        engine = DataFilterEngineAction()
        engine.add("age", FilterOperator.GTE, 18)
        engine.add("status", FilterOperator.EQ, "active")
        filtered = engine.apply(data_list)
    """

    def __init__(self) -> None:
        self._conditions: list[FilterCondition] = []
        self._groups: list[FilterGroup] = []
        self._logic: str = "AND"

    def add(
        self,
        field: str,
        operator: FilterOperator,
        value: Any = None,
    ) -> "DataFilterEngineAction":
        """Add a filter condition."""
        self._conditions.append(FilterCondition(
            field=field,
            operator=operator,
            value=value,
        ))
        return self

    def where(
        self,
        field: str,
        operator: FilterOperator,
        value: Any = None,
    ) -> "DataFilterEngineAction":
        """Alias for add()."""
        return self.add(field, operator, value)

    def in_values(
        self,
        field: str,
        values: list[Any],
    ) -> "DataFilterEngineAction":
        """Filter field to be in list of values."""
        return self.add(field, FilterOperator.IN, values)

    def not_null(self, field: str) -> "DataFilterEngineAction":
        """Filter where field is not null."""
        return self.add(field, FilterOperator.IS_NOT_NULL)

    def matches(
        self,
        field: str,
        pattern: str,
    ) -> "DataFilterEngineAction":
        """Filter using regex pattern."""
        return self.add(field, FilterOperator.REGEX, pattern)

    def group(
        self,
        conditions: list[FilterCondition],
        logic: str = "AND",
    ) -> "DataFilterEngineAction":
        """Add a group of conditions."""
        self._groups.append(FilterGroup(
            conditions=conditions,
            logic=logic,
        ))
        return self

    def apply(
        self,
        data: list[dict],
        logic: Optional[str] = None,
    ) -> list[dict]:
        """Apply filters to list of records."""
        logic = logic or self._logic

        results = []
        for record in data:
            if self._record_matches(record, logic):
                results.append(record)

        return results

    def _record_matches(
        self,
        record: dict,
        logic: str,
    ) -> bool:
        """Check if a record matches the filter conditions."""
        if logic == "AND":
            matches = all(self._evaluate_condition(record, c)
                          for c in self._conditions)
        else:
            matches = any(self._evaluate_condition(record, c)
                         for c in self._conditions)

        if not self._groups:
            return matches

        for group in self._groups:
            if group.logic == "AND":
                group_matches = all(self._evaluate_condition(record, c)
                                   for c in group.conditions)
            else:
                group_matches = any(self._evaluate_condition(record, c)
                                   for c in group.conditions)

            if logic == "AND":
                matches = matches and group_matches
            else:
                matches = matches or group_matches

        return matches

    def _evaluate_condition(
        self,
        record: dict,
        condition: FilterCondition,
    ) -> bool:
        """Evaluate a single condition against a record."""
        field_value = self._get_field_value(record, condition.field)

        op = condition.operator

        if op == FilterOperator.IS_NULL:
            return field_value is None
        elif op == FilterOperator.IS_NOT_NULL:
            return field_value is not None
        elif op == FilterOperator.EQ:
            return field_value == condition.value
        elif op == FilterOperator.NE:
            return field_value != condition.value
        elif op == FilterOperator.GT:
            return field_value is not None and field_value > condition.value
        elif op == FilterOperator.GTE:
            return field_value is not None and field_value >= condition.value
        elif op == FilterOperator.LT:
            return field_value is not None and field_value < condition.value
        elif op == FilterOperator.LTE:
            return field_value is not None and field_value <= condition.value
        elif op == FilterOperator.IN:
            return field_value in condition.value
        elif op == FilterOperator.NOT_IN:
            return field_value not in condition.value
        elif op == FilterOperator.CONTAINS:
            return field_value is not None and condition.value in str(field_value)
        elif op == FilterOperator.STARTS_WITH:
            return (field_value is not None and
                    str(field_value).startswith(str(condition.value)))
        elif op == FilterOperator.ENDS_WITH:
            return (field_value is not None and
                    str(field_value).endswith(str(condition.value)))
        elif op == FilterOperator.REGEX:
            try:
                return bool(re.search(condition.value, str(field_value or "")))
            except re.error:
                return False

        return True

    def _get_field_value(self, record: dict, field: str) -> Any:
        """Get field value using dot notation."""
        parts = field.split(".")
        current = record

        for part in parts:
            if isinstance(current, dict):
                current = current.get(part)
            elif isinstance(current, (list, tuple)):
                try:
                    current = current[int(part)]
                except (ValueError, IndexError):
                    return None
            else:
                return None

            if current is None:
                return None

        return current

    def clear(self) -> None:
        """Clear all filter conditions."""
        self._conditions.clear()
        self._groups.clear()
