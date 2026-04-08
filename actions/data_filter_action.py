"""Data Filter Action Module.

Provides advanced data filtering with multi-criteria support,
complex boolean logic, and pattern matching.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Pattern, Union
from datetime import datetime


class FilterOperator(Enum):
    """Filter operator type."""
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
    BETWEEN = "between"
    IS_NULL = "is_null"
    IS_NOT_NULL = "is_not_null"


@dataclass
class FilterCondition:
    """Single filter condition."""
    field: str
    operator: FilterOperator
    value: Any = None
    case_sensitive: bool = True


@dataclass
class FilterGroup:
    """Group of filter conditions with logic."""
    conditions: List[Union["FilterCondition", "FilterGroup"]] = field(default_factory=list)
    logic: str = "AND"
    negate: bool = False


class DataFilterAction:
    """Advanced data filter with complex boolean logic.

    Example:
        filter = DataFilterAction()

        result = filter.filter(data, FilterGroup(
            conditions=[
                FilterCondition("age", FilterOperator.GTE, 18),
                FilterCondition("status", FilterOperator.IN, ["active", "pending"]),
                FilterCondition("name", FilterOperator.CONTAINS, "John", case_sensitive=False),
            ],
            logic="AND"
        ))
    """

    def __init__(self, case_sensitive: bool = True) -> None:
        self.case_sensitive = case_sensitive
        self._compiled_patterns: Dict[str, Pattern] = {}

    def filter(
        self,
        data: List[Dict[str, Any]],
        filter_group: FilterGroup,
    ) -> List[Dict[str, Any]]:
        """Filter data based on filter group.

        Args:
            data: List of records to filter
            filter_group: Filter conditions with logic

        Returns:
            Filtered list of records
        """
        if not data:
            return []

        return [record for record in data if self._evaluate_group(record, filter_group)]

    def filter_one(
        self,
        data: List[Dict[str, Any]],
        filter_group: FilterGroup,
    ) -> Optional[Dict[str, Any]]:
        """Filter data and return first match or None."""
        for record in data:
            if self._evaluate_group(record, filter_group):
                return record
        return None

    def _evaluate_group(
        self,
        record: Dict[str, Any],
        group: FilterGroup,
    ) -> bool:
        """Evaluate filter group against record."""
        if not group.conditions:
            return True

        results = [
            self._evaluate_condition(record, cond)
            if isinstance(cond, FilterCondition)
            else self._evaluate_group(record, cond)
            for cond in group.conditions
        ]

        if group.logic == "AND":
            result = all(results)
        elif group.logic == "OR":
            result = any(results)
        elif group.logic == "XOR":
            result = sum(results) == 1
        else:
            result = all(results)

        return not result if group.negate else result

    def _evaluate_condition(
        self,
        record: Dict[str, Any],
        condition: FilterCondition,
    ) -> bool:
        """Evaluate single filter condition."""
        field_value = record.get(condition.field)
        condition_value = condition.value

        if condition.operator == FilterOperator.EQ:
            return self._compare_eq(field_value, condition_value, condition.case_sensitive)

        elif condition.operator == FilterOperator.NE:
            return not self._compare_eq(field_value, condition_value, condition.case_sensitive)

        elif condition.operator == FilterOperator.GT:
            return self._compare_gt(field_value, condition_value)

        elif condition.operator == FilterOperator.GTE:
            return self._compare_gte(field_value, condition_value)

        elif condition.operator == FilterOperator.LT:
            return self._compare_lt(field_value, condition_value)

        elif condition.operator == FilterOperator.LTE:
            return self._compare_lte(field_value, condition_value)

        elif condition.operator == FilterOperator.IN:
            return field_value in condition_value

        elif condition.operator == FilterOperator.NOT_IN:
            return field_value not in condition_value

        elif condition.operator == FilterOperator.CONTAINS:
            return self._contains(field_value, condition_value, condition.case_sensitive)

        elif condition.operator == FilterOperator.NOT_CONTAINS:
            return not self._contains(field_value, condition_value, condition.case_sensitive)

        elif condition.operator == FilterOperator.STARTS_WITH:
            return self._starts_with(field_value, condition_value, condition.case_sensitive)

        elif condition.operator == FilterOperator.ENDS_WITH:
            return self._ends_with(field_value, condition_value, condition.case_sensitive)

        elif condition.operator == FilterOperator.REGEX:
            return self._regex_match(field_value, condition_value)

        elif condition.operator == FilterOperator.BETWEEN:
            return self._between(field_value, condition_value)

        elif condition.operator == FilterOperator.IS_NULL:
            return field_value is None

        elif condition.operator == FilterOperator.IS_NOT_NULL:
            return field_value is not None

        return False

    def _compare_eq(self, a: Any, b: Any, case_sensitive: bool) -> bool:
        """Compare equality."""
        if isinstance(a, str) and isinstance(b, str):
            if not case_sensitive:
                return a.lower() == b.lower()
        return a == b

    def _compare_gt(self, a: Any, b: Any) -> bool:
        """Compare greater than."""
        try:
            return a > b
        except TypeError:
            return str(a) > str(b)

    def _compare_gte(self, a: Any, b: Any) -> bool:
        """Compare greater than or equal."""
        try:
            return a >= b
        except TypeError:
            return str(a) >= str(b)

    def _compare_lt(self, a: Any, b: Any) -> bool:
        """Compare less than."""
        try:
            return a < b
        except TypeError:
            return str(a) < str(b)

    def _compare_lte(self, a: Any, b: Any) -> bool:
        """Compare less than or equal."""
        try:
            return a <= b
        except TypeError:
            return str(a) <= str(b)

    def _contains(self, field_val: Any, search_val: Any, case_sensitive: bool) -> bool:
        """Check if field contains value."""
        if field_val is None:
            return False
        field_str = str(field_val)
        search_str = str(search_val)
        if not case_sensitive:
            return search_str.lower() in field_str.lower()
        return search_str in field_str

    def _starts_with(self, field_val: Any, prefix: Any, case_sensitive: bool) -> bool:
        """Check if field starts with prefix."""
        if field_val is None:
            return False
        field_str = str(field_val)
        prefix_str = str(prefix)
        if not case_sensitive:
            return field_str.lower().startswith(prefix_str.lower())
        return field_str.startswith(prefix_str)

    def _ends_with(self, field_val: Any, suffix: Any, case_sensitive: bool) -> bool:
        """Check if field ends with suffix."""
        if field_val is None:
            return False
        field_str = str(field_val)
        suffix_str = str(suffix)
        if not case_sensitive:
            return field_str.lower().endswith(suffix_str.lower())
        return field_str.endswith(suffix_str)

    def _regex_match(self, field_val: Any, pattern: Any) -> bool:
        """Check regex match."""
        if field_val is None:
            return False
        try:
            if pattern not in self._compiled_patterns:
                self._compiled_patterns[pattern] = re.compile(str(pattern))
            return bool(self._compiled_patterns[pattern].search(str(field_val)))
        except re.error:
            return False

    def _between(self, field_val: Any, range_vals: Any) -> bool:
        """Check if value is between range."""
        if field_val is None or not range_vals:
            return False
        if len(range_vals) != 2:
            return False
        low, high = range_vals
        return low <= field_val <= high

    def and_(
        self,
        conditions: List[FilterCondition],
    ) -> FilterGroup:
        """Create AND filter group."""
        return FilterGroup(conditions=conditions, logic="AND")

    def or_(
        self,
        conditions: List[FilterCondition],
    ) -> FilterGroup:
        """Create OR filter group."""
        return FilterGroup(conditions=conditions, logic="OR")

    def not_(
        self,
        group: FilterGroup,
    ) -> FilterGroup:
        """Negate filter group."""
        group.negate = True
        return group
