"""Data filter action module.

Provides filtering operations for lists, dicts, and data frames.
Supports condition chains, complex predicates, and null handling.
"""

from __future__ import annotations

import logging
from typing import Optional, Dict, Any, List, Callable, TypeVar, Union
from dataclasses import dataclass

logger = logging.getLogger(__name__)

T = TypeVar("T")


class FilterOperator(Enum):
    """Comparison operators for filtering."""
    EQ = "=="
    NE = "!="
    GT = ">"
    GE = ">="
    LT = "<"
    LE = "<="
    IN = "in"
    NOT_IN = "not_in"
    CONTAINS = "contains"
    NOT_CONTAINS = "not_contains"
    STARTS_WITH = "starts_with"
    ENDS_WITH = "ends_with"
    IS_NULL = "is_null"
    IS_NOT_NULL = "is_not_null"


from enum import Enum


@dataclass
class FilterCondition:
    """A single filter condition."""
    field: str
    operator: FilterOperator
    value: Any = None


class DataFilterAction:
    """Data filtering engine.

    Provides flexible filtering for lists of dicts and objects.

    Example:
        data = [{"name": "Alice", "age": 30}, {"name": "Bob", "age": 25}]
        filtered = DataFilterAction().filter(data, "age", FilterOperator.GT, 20)
    """

    def filter(
        self,
        data: List[Dict[str, Any]],
        field: str,
        operator: FilterOperator,
        value: Any = None,
    ) -> List[Dict[str, Any]]:
        """Filter a list of dicts by a single condition.

        Args:
            data: List of dicts to filter.
            field: Field name to check.
            operator: FilterOperator comparison.
            value: Value to compare against.

        Returns:
            Filtered list.
        """
        result = []
        for item in data:
            if self._matches(item, field, operator, value):
                result.append(item)
        return result

    def filter_multiple(
        self,
        data: List[Dict[str, Any]],
        conditions: List[FilterCondition],
        logic: str = "AND",
    ) -> List[Dict[str, Any]]:
        """Filter by multiple conditions.

        Args:
            data: List of dicts to filter.
            conditions: List of FilterCondition objects.
            logic: 'AND' or 'OR' for combining conditions.

        Returns:
            Filtered list.
        """
        result = []
        for item in data:
            matches = [self._matches(item, c.field, c.operator, c.value) for c in conditions]
            if logic == "AND" and all(matches):
                result.append(item)
            elif logic == "OR" and any(matches):
                result.append(item)
        return result

    def filter_by_predicate(
        self,
        data: List[T],
        predicate: Callable[[T], bool],
    ) -> List[T]:
        """Filter using a custom predicate function.

        Args:
            data: List to filter.
            predicate: Function that returns True for items to keep.

        Returns:
            Filtered list.
        """
        return [item for item in data if predicate(item)]

    def exclude_nulls(
        self,
        data: List[Dict[str, Any]],
        fields: Optional[List[str]] = None,
    ) -> List[Dict[str, Any]]:
        """Exclude records where specified fields are null/empty.

        Args:
            data: List of dicts.
            fields: Fields to check (None = all fields).

        Returns:
            Filtered list without null values.
        """
        result = []
        for item in data:
            if fields is None:
                if all(v is not None and v != "" for v in item.values()):
                    result.append(item)
            else:
                if all(item.get(f) is not None and item.get(f) != "" for f in fields):
                    result.append(item)
        return result

    def filter_by_range(
        self,
        data: List[Dict[str, Any]],
        field: str,
        min_val: Optional[Any] = None,
        max_val: Optional[Any] = None,
        inclusive: bool = True,
    ) -> List[Dict[str, Any]]:
        """Filter by numeric or comparable range.

        Args:
            data: List of dicts.
            field: Field to check.
            min_val: Minimum value (None = no minimum).
            max_val: Maximum value (None = no maximum).
            inclusive: Whether bounds are inclusive.

        Returns:
            Filtered list.
        """
        result = []
        for item in data:
            val = item.get(field)
            if val is None:
                continue
            if min_val is not None:
                if inclusive and val < min_val:
                    continue
                if not inclusive and val <= min_val:
                    continue
            if max_val is not None:
                if inclusive and val > max_val:
                    continue
                if not inclusive and val >= max_val:
                    continue
            result.append(item)
        return result

    def filter_unique(
        self,
        data: List[Dict[str, Any]],
        key: str,
        keep: str = "first",
    ) -> List[Dict[str, Any]]:
        """Filter to unique values by a key field.

        Args:
            data: List of dicts.
            key: Field to check for uniqueness.
            keep: 'first' or 'last' occurrence to keep.

        Returns:
            List with unique values.
        """
        seen: Dict[Any, Dict[str, Any]] = {}
        for item in data:
            val = item.get(key)
            if val is None:
                continue
            if keep == "first" and val in seen:
                continue
            if keep == "last":
                seen[val] = item
        if keep == "last":
            return list(seen.values())
        return [item for k, item in seen.items()]

    def _matches(
        self,
        item: Dict[str, Any],
        field: str,
        operator: FilterOperator,
        value: Any,
    ) -> bool:
        """Check if an item matches a condition."""
        field_val = item.get(field)

        if operator == FilterOperator.IS_NULL:
            return field_val is None or field_val == ""
        elif operator == FilterOperator.IS_NOT_NULL:
            return field_val is not None and field_val != ""
        elif operator == FilterOperator.EQ:
            return field_val == value
        elif operator == FilterOperator.NE:
            return field_val != value
        elif operator == FilterOperator.GT:
            return field_val is not None and field_val > value
        elif operator == FilterOperator.GE:
            return field_val is not None and field_val >= value
        elif operator == FilterOperator.LT:
            return field_val is not None and field_val < value
        elif operator == FilterOperator.LE:
            return field_val is not None and field_val <= value
        elif operator == FilterOperator.IN:
            return field_val in value if value else False
        elif operator == FilterOperator.NOT_IN:
            return field_val not in value if value else True
        elif operator == FilterOperator.CONTAINS:
            return value in str(field_val) if field_val else False
        elif operator == FilterOperator.NOT_CONTAINS:
            return value not in str(field_val) if field_val else True
        elif operator == FilterOperator.STARTS_WITH:
            return str(field_val).startswith(str(value)) if field_val else False
        elif operator == FilterOperator.ENDS_WITH:
            return str(field_val).endswith(str(value)) if field_val else False

        return False
