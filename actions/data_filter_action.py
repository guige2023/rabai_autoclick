"""Data filtering and projection utilities.

This module provides data filtering capabilities:
- Field selection and exclusion
- Conditional filtering
- Data projection and transformation
- Set operations on collections

Example:
    >>> from actions.data_filter_action import DataFilter, filter_by, project
    >>> filtered = filter_by(users, age__gte=18)
    >>> projected = project(users, fields=["name", "email"])
"""

from __future__ import annotations

import logging
from typing import Any, Callable, Optional
from dataclasses import dataclass

logger = logging.getLogger(__name__)


class DataFilter:
    """Filter and project collections of data.

    Example:
        >>> filter = DataFilter()
        >>> result = filter.where(users, age__gte=18).sort_by("name").execute()
    """

    def __init__(self) -> None:
        self._data: list[Any] = []
        self._filters: list[Callable[[dict[str, Any]], bool]] = []
        self._sort_key: Optional[str] = None
        self._sort_reverse: bool = False
        self._limit_value: Optional[int] = None
        self._offset_value: int = 0

    def where(self, data: list[Any], **conditions: Any) -> DataFilter:
        """Add filter conditions.

        Args:
            data: Collection to filter.
            **conditions: Field__operator=value conditions.

        Returns:
            Self for chaining.
        """
        self._data = data
        for field, value in conditions.items():
            filter_func = self._parse_condition(field, value)
            if filter_func:
                self._filters.append(filter_func)
        return self

    def sort_by(self, key: str, reverse: bool = False) -> DataFilter:
        """Sort the filtered data.

        Args:
            key: Field to sort by.
            reverse: Sort in descending order.

        Returns:
            Self for chaining.
        """
        self._sort_key = key
        self._sort_reverse = reverse
        return self

    def limit(self, count: int, offset: int = 0) -> DataFilter:
        """Limit result set size.

        Args:
            count: Maximum number of results.
            offset: Number of results to skip.

        Returns:
            Self for chaining.
        """
        self._limit_value = count
        self._offset_value = offset
        return self

    def execute(self) -> list[Any]:
        """Execute the filter chain.

        Returns:
            Filtered and projected results.
        """
        result = self._data
        for filter_func in self._filters:
            result = [item for item in result if self._apply_filter(item, filter_func)]
        if self._sort_key:
            result = sorted(
                result,
                key=lambda x: self._get_value(x, self._sort_key),
                reverse=self._sort_reverse,
            )
        result = result[self._offset_value:]
        if self._limit_value is not None:
            result = result[:self._limit_value]
        return result

    def _parse_condition(
        self,
        field_op: str,
        value: Any,
    ) -> Optional[Callable[[dict[str, Any]], bool]]:
        """Parse a field__operator condition."""
        parts = field_op.rsplit("__", 1)
        field = parts[0]
        if len(parts) == 1:
            op = "eq"
        else:
            op = parts[1]
        ops = {
            "eq": lambda v, o: v == o,
            "ne": lambda v, o: v != o,
            "gt": lambda v, o: v > o,
            "gte": lambda v, o: v >= o,
            "lt": lambda v, o: v < o,
            "lte": lambda v, o: v <= o,
            "in": lambda v, o: v in o,
            "nin": lambda v, o: v not in o,
            "contains": lambda v, o: o in v if v else False,
            "icontains": lambda v, o: o.lower() in v.lower() if v else False,
            "startswith": lambda v, o: v.startswith(o) if v else False,
            "endswith": lambda v, o: v.endswith(o) if v else False,
            "exists": lambda v, o: (v is not None) == o,
        }
        if op not in ops:
            return None
        def make_filter(item_val: Any) -> bool:
            item_val = self._get_value(item_val, field)
            return ops[op](item_val, value)
        return make_filter

    def _apply_filter(
        self,
        item: Any,
        filter_func: Callable[[dict[str, Any]], bool],
    ) -> bool:
        """Apply a filter function to an item."""
        if isinstance(item, dict):
            return filter_func(item)
        return True

    def _get_value(self, item: Any, path: str) -> Any:
        """Get a value from an item by dot-notation path."""
        parts = path.split(".")
        value = item
        for part in parts:
            if isinstance(value, dict):
                value = value.get(part)
            else:
                return None
        return value


def filter_by(data: list[dict[str, Any]], **conditions: Any) -> list[dict[str, Any]]:
    """Filter a collection by conditions.

    Args:
        data: Collection to filter.
        **conditions: Field__operator=value conditions.

    Returns:
        Filtered collection.
    """
    return DataFilter().where(data, **conditions).execute()


def project(
    data: list[dict[str, Any]],
    fields: list[str],
    exclude: Optional[list[str]] = None,
) -> list[dict[str, Any]]:
    """Project fields from a collection.

    Args:
        data: Collection to project.
        fields: Fields to include.
        exclude: Fields to exclude.

    Returns:
        Projected collection.
    """
    if exclude:
        fields = [f for f in fields if f not in exclude]
    return [{k: item.get(k) for k in fields} for item in data]


def group_by(
    data: list[dict[str, Any]],
    key: str,
    agg_func: Optional[Callable[[list[Any]], Any]] = None,
) -> dict[Any, list[Any]]:
    """Group a collection by a field.

    Args:
        data: Collection to group.
        key: Field to group by.
        agg_func: Optional aggregation function.

    Returns:
        Dictionary of grouped items.
    """
    groups: dict[Any, list[Any]] = {}
    for item in data:
        group_key = item.get(key)
        if group_key not in groups:
            groups[group_key] = []
        groups[group_key].append(item)
    if agg_func:
        return {k: agg_func(v) for k, v in groups.items()}
    return groups


def distinct(
    data: list[dict[str, Any]],
    field: Optional[str] = None,
) -> list[Any]:
    """Get distinct values or items.

    Args:
        data: Collection.
        field: Optional field to get distinct values from.

    Returns:
        List of distinct values or items.
    """
    if field:
        seen: set[Any] = set()
        result = []
        for item in data:
            value = item.get(field)
            if value not in seen:
                seen.add(value)
                result.append(value)
        return result
    else:
        seen: set[Any] = set()
        result = []
        for item in data:
            if item not in seen:
                seen.add(item)
                result.append(item)
        return result


def union(*collections: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Union of multiple collections.

    Args:
        *collections: Collections to union.

    Returns:
        Combined collection with duplicates removed.
    """
    seen: set[int] = set()
    result = []
    for coll in collections:
        for item in coll:
            item_id = id(item)
            if item_id not in seen:
                seen.add(item_id)
                result.append(item)
    return result


def intersect(*collections: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Intersection of multiple collections.

    Args:
        *collections: Collections to intersect.

    Returns:
        Items common to all collections.
    """
    if not collections:
        return []
    result = collections[0]
    for coll in collections[1:]:
        result = [item for item in result if item in coll]
    return result
