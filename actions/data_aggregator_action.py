"""Data aggregation and statistical utilities.

This module provides data aggregation functions:
- Grouped aggregations (sum, avg, count, min, max)
- Statistical measures
- Time-based windowing
- Multi-field aggregations

Example:
    >>> from actions.data_aggregator_action import aggregate, group_aggregate
    >>> result = aggregate(sales_data, fields=["revenue"], ops=["sum", "avg"])
"""

from __future__ import annotations

import logging
from typing import Any, Callable, Optional
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


@dataclass
class AggregationResult:
    """Result of an aggregation operation."""
    field: str
    operation: str
    value: Any


class DataAggregator:
    """Aggregate data with various operations.

    Example:
        >>> agg = DataAggregator()
        >>> result = agg.aggregate(sales, "region", revenue="sum")
    """

    OPERATIONS = {
        "sum": lambda x: sum(x),
        "avg": lambda x: sum(x) / len(x) if x else 0,
        "count": lambda x: len(x),
        "min": lambda x: min(x) if x else None,
        "max": lambda x: max(x) if x else None,
        "first": lambda x: x[0] if x else None,
        "last": lambda x: x[-1] if x else None,
        "median": lambda x: sorted(x)[len(x) // 2] if x else None,
    }

    def __init__(self) -> None:
        pass

    def aggregate(
        self,
        data: list[dict[str, Any]],
        group_by: Optional[str] = None,
        **operations: str,
    ) -> Any:
        """Aggregate data.

        Args:
            data: List of dicts to aggregate.
            group_by: Optional field to group by.
            **operations: Field to operation mappings, e.g., revenue="sum".

        Returns:
            Aggregated result.
        """
        if not group_by:
            return self._aggregate_all(data, operations)
        return self._aggregate_grouped(data, group_by, operations)

    def _aggregate_all(
        self,
        data: list[dict[str, Any]],
        operations: dict[str, str],
    ) -> dict[str, Any]:
        """Aggregate entire dataset."""
        result = {}
        for field, op in operations.items():
            values = [item.get(field) for item in data if field in item]
            if op in self.OPERATIONS:
                result[field] = self.OPERATIONS[op](values)
            else:
                result[field] = values
        return result

    def _aggregate_grouped(
        self,
        data: list[dict[str, Any]],
        group_by: str,
        operations: dict[str, str],
    ) -> dict[Any, dict[str, Any]]:
        """Aggregate grouped data."""
        groups: dict[Any, list[dict[str, Any]]] = defaultdict(list)
        for item in data:
            key = item.get(group_by)
            groups[key].append(item)
        result = {}
        for key, items in groups.items():
            group_result = {group_by: key}
            for field, op in operations.items():
                values = [item.get(field) for item in items if field in item]
                if op in self.OPERATIONS:
                    group_result[f"{field}_{op}"] = self.OPERATIONS[op](values)
                else:
                    group_result[field] = values
            result[key] = group_result
        return result

    def window_aggregate(
        self,
        data: list[dict[str, Any]],
        time_field: str,
        window: timedelta,
        operations: dict[str, str],
    ) -> list[dict[str, Any]]:
        """Aggregate with time-based windows.

        Args:
            data: Time-series data.
            time_field: Field containing timestamps.
            window: Time window size.
            operations: Field to operation mappings.

        Returns:
            Windowed aggregation results.
        """
        if not data:
            return []
        sorted_data = sorted(data, key=lambda x: x.get(time_field, ""))
        results = []
        window_start = sorted_data[0][time_field]
        window_data = []
        for item in sorted_data:
            item_time = item[time_field]
            if isinstance(item_time, str):
                item_time = datetime.fromisoformat(item_time)
            if isinstance(window_start, str):
                window_start = datetime.fromisoformat(window_start)
            while item_time - window_start > window:
                if window_data:
                    result = self._aggregate_all(window_data, operations)
                    result[time_field] = window_start
                    results.append(result)
                window_start += window
                window_data = []
            window_data.append(item)
        if window_data:
            result = self._aggregate_all(window_data, operations)
            result[time_field] = window_start
            results.append(result)
        return results


def aggregate(
    data: list[dict[str, Any]],
    field: str,
    operation: str = "sum",
) -> Any:
    """Simple aggregate a single field.

    Args:
        data: Collection to aggregate.
        field: Field to aggregate.
        operation: Operation name.

    Returns:
        Aggregated value.
    """
    values = [item.get(field) for item in data if field in item]
    if operation in DataAggregator.OPERATIONS:
        return DataAggregator.OPERATIONS[operation](values)
    return values


def group_aggregate(
    data: list[dict[str, Any]],
    group_by: str,
    **operations: str,
) -> dict[Any, dict[str, Any]]:
    """Group and aggregate data.

    Args:
        data: Collection to aggregate.
        group_by: Field to group by.
        **operations: Field to operation mappings.

    Returns:
        Dictionary of grouped aggregations.
    """
    return DataAggregator().aggregate(data, group_by=group_by, **operations)


def rolling_aggregate(
    data: list[dict[str, Any]],
    field: str,
    window: int,
    operation: str = "avg",
) -> list[Any]:
    """Calculate rolling aggregation.

    Args:
        data: Collection to aggregate.
        field: Field to aggregate.
        window: Window size.
        operation: Operation name.

    Returns:
        List of rolling aggregated values.
    """
    values = [item.get(field) for item in data if field in item]
    if operation not in DataAggregator.OPERATIONS:
        return values
    op_func = DataAggregator.OPERATIONS[operation]
    result = []
    for i in range(len(values)):
        start = max(0, i - window + 1)
        window_values = values[start:i + 1]
        result.append(op_func(window_values))
    return result
