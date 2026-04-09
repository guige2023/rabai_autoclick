"""
Data Filter Action Module.

Filter data with predicates and conditions.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional, Pattern, Union


Predicate = Callable[[Dict[str, Any]], bool]


@dataclass
class FilterCondition:
    """A filter condition."""
    field: str
    operator: str
    value: Any


class DataFilterAction:
    """
    Filter data with various conditions.

    Supports field comparisons, patterns, and custom predicates.
    """

    def __init__(self) -> None:
        self._predicates: List[Predicate] = []

    def add_predicate(
        self,
        predicate: Predicate,
    ) -> "DataFilterAction":
        """
        Add a custom predicate.

        Args:
            predicate: Function returning True to keep item

        Returns:
            Self for chaining
        """
        self._predicates.append(predicate)
        return self

    def filter(
        self,
        data: List[Dict[str, Any]],
        conditions: Optional[List[FilterCondition]] = None,
        predicate: Optional[Predicate] = None,
    ) -> List[Dict[str, Any]]:
        """
        Filter data.

        Args:
            data: Data to filter
            conditions: List of conditions
            predicate: Optional custom predicate

        Returns:
            Filtered data
        """
        result = data

        if conditions:
            result = self._apply_conditions(result, conditions)

        for pred in self._predicates:
            result = [item for item in result if pred(item)]

        if predicate:
            result = [item for item in result if predicate(item)]

        return result

    def _apply_conditions(
        self,
        data: List[Dict[str, Any]],
        conditions: List[FilterCondition],
    ) -> List[Dict[str, Any]]:
        """Apply conditions to data."""
        result = data

        for condition in conditions:
            result = [
                item
                for item in result
                if self._check_condition(item, condition)
            ]

        return result

    def _check_condition(
        self,
        item: Dict[str, Any],
        condition: FilterCondition,
    ) -> bool:
        """Check if item matches condition."""
        field_value = item.get(condition.field)

        ops = {
            "==": lambda a, b: a == b,
            "!=": lambda a, b: a != b,
            ">": lambda a, b: a > b,
            ">=": lambda a, b: a >= b,
            "<": lambda a, b: a < b,
            "<=": lambda a, b: a <= b,
            "in": lambda a, b: a in b if b else False,
            "not in": lambda a, b: a not in b if b else True,
            "contains": lambda a, b: b in a if a else False,
            "startswith": lambda a, b: str(a).startswith(b) if a else False,
            "endswith": lambda a, b: str(a).endswith(b) if a else False,
            "regex": lambda a, b: bool(re.search(b, str(a))) if a else False,
            "is_null": lambda a, b: a is None,
            "is_not_null": lambda a, b: a is not None,
            "exists": lambda a, b: condition.field in item,
        }

        op_func = ops.get(condition.operator)

        if op_func is None:
            return True

        try:
            return op_func(field_value, condition.value)
        except (TypeError, ValueError):
            return False

    def eq(self, field: str, value: Any) -> FilterCondition:
        """Create equals condition."""
        return FilterCondition(field, "==", value)

    def ne(self, field: str, value: Any) -> FilterCondition:
        """Create not equals condition."""
        return FilterCondition(field, "!=", value)

    def gt(self, field: str, value: Any) -> FilterCondition:
        """Create greater than condition."""
        return FilterCondition(field, ">", value)

    def gte(self, field: str, value: Any) -> FilterCondition:
        """Create greater than or equals condition."""
        return FilterCondition(field, ">=", value)

    def lt(self, field: str, value: Any) -> FilterCondition:
        """Create less than condition."""
        return FilterCondition(field, "<", value)

    def lte(self, field: str, value: Any) -> FilterCondition:
        """Create less than or equals condition."""
        return FilterCondition(field, "<=", value)

    def contains(self, field: str, value: Any) -> FilterCondition:
        """Create contains condition."""
        return FilterCondition(field, "contains", value)

    def regex(self, field: str, pattern: str) -> FilterCondition:
        """Create regex match condition."""
        return FilterCondition(field, "regex", pattern)

    def is_null(self, field: str) -> FilterCondition:
        """Create is null condition."""
        return FilterCondition(field, "is_null", None)

    def is_not_null(self, field: str) -> FilterCondition:
        """Create is not null condition."""
        return FilterCondition(field, "is_not_null", None)

    def in_list(self, field: str, values: List[Any]) -> FilterCondition:
        """Create in list condition."""
        return FilterCondition(field, "in", values)

    def filter_by_function(
        self,
        data: List[Dict[str, Any]],
        func: Callable[[Dict[str, Any]], bool],
    ) -> List[Dict[str, Any]]:
        """
        Filter using a function.

        Args:
            data: Data to filter
            func: Filter function

        Returns:
            Filtered data
        """
        return [item for item in data if func(item)]

    def exclude_by_function(
        self,
        data: List[Dict[str, Any]],
        func: Callable[[Dict[str, Any]], bool],
    ) -> List[Dict[str, Any]]:
        """
        Exclude items matching a function.

        Args:
            data: Data to filter
            func: Exclusion function

        Returns:
            Filtered data
        """
        return [item for item in data if not func(item)]

    def filter_unique(
        self,
        data: List[Dict[str, Any]],
        fields: List[str],
    ) -> List[Dict[str, Any]]:
        """
        Filter to unique values by fields.

        Args:
            data: Data to filter
            fields: Fields to check for uniqueness

        Returns:
            Deduplicated data
        """
        seen: set = set()
        result = []

        for item in data:
            key = tuple(item.get(f) for f in fields)

            if key not in seen:
                seen.add(key)
                result.append(item)

        return result

    def partition_by(
        self,
        data: List[Dict[str, Any]],
        field: str,
    ) -> Dict[Any, List[Dict[str, Any]]]:
        """
        Partition data by field value.

        Args:
            data: Data to partition
            field: Field to partition by

        Returns:
            Dict mapping field value to items
        """
        result: Dict[Any, List[Dict[str, Any]]] = {}

        for item in data:
            key = item.get(field)
            if key not in result:
                result[key] = []
            result[key].append(item)

        return result
