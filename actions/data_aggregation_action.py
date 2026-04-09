"""Data aggregation action module.

Provides data aggregation functionality for computing
statistics and summaries from collections.
"""

from __future__ import annotations

import logging
from typing import Any, Optional, Callable, TypeVar, Generic
from dataclasses import dataclass, field
from enum import Enum
from collections import defaultdict
import math

logger = logging.getLogger(__name__)

T = TypeVar("T")


class AggregationType(Enum):
    """Aggregation types."""
    SUM = "sum"
    COUNT = "count"
    AVG = "avg"
    MIN = "min"
    MAX = "max"
    FIRST = "first"
    LAST = "last"
    DISTINCT = "distinct"


@dataclass
class AggregationResult:
    """Result of aggregation."""
    key: Any
    value: Any
    aggregation_type: AggregationType


@dataclass
class GroupedAggregation:
    """Grouped aggregation result."""
    group_key: Any
    results: list[AggregationResult]


class Aggregator:
    """Data aggregator."""

    @staticmethod
    def sum(data: list[dict[str, Any]], field: str) -> float:
        """Sum field values.

        Args:
            data: List of dictionaries
            field: Field to sum

        Returns:
            Sum of values
        """
        return sum(item.get(field, 0) for item in data if isinstance(item.get(field), (int, float)))

    @staticmethod
    def count(data: list[dict[str, Any]], field: Optional[str] = None) -> int:
        """Count items or field values.

        Args:
            data: List of items
            field: Optional field to count

        Returns:
            Count
        """
        if field is None:
            return len(data)
        return sum(1 for item in data if field in item and item[field] is not None)

    @staticmethod
    def average(data: list[dict[str, Any]], field: str) -> float:
        """Calculate average.

        Args:
            data: List of dictionaries
            field: Field to average

        Returns:
            Average value
        """
        values = [item.get(field) for item in data if isinstance(item.get(field), (int, float))]
        if not values:
            return 0.0
        return sum(values) / len(values)

    @staticmethod
    def min(data: list[dict[str, Any]], field: str) -> Optional[Any]:
        """Get minimum value.

        Args:
            data: List of dictionaries
            field: Field to min

        Returns:
            Minimum value or None
        """
        values = [item.get(field) for item in data if field in item and item[field] is not None]
        return min(values) if values else None

    @staticmethod
    def max(data: list[dict[str, Any]], field: str) -> Optional[Any]:
        """Get maximum value.

        Args:
            data: List of dictionaries
            field: Field to max

        Returns:
            Maximum value or None
        """
        values = [item.get(field) for item in data if field in item and item[field] is not None]
        return max(values) if values else None

    @staticmethod
    def first(data: list[dict[str, Any]], field: str) -> Optional[Any]:
        """Get first value.

        Args:
            data: List of dictionaries
            field: Field to get

        Returns:
            First value or None
        """
        for item in data:
            if field in item:
                return item[field]
        return None

    @staticmethod
    def last(data: list[dict[str, Any]], field: str) -> Optional[Any]:
        """Get last value.

        Args:
            data: List of dictionaries
            field: Field to get

        Returns:
            Last value or None
        """
        for item in reversed(data):
            if field in item:
                return item[field]
        return None

    @staticmethod
    def distinct(data: list[dict[str, Any]], field: str) -> list[Any]:
        """Get distinct values.

        Args:
            data: List of dictionaries
            field: Field to get distinct values

        Returns:
            List of distinct values
        """
        seen = set()
        result = []
        for item in data:
            value = item.get(field)
            if value is not None and value not in seen:
                seen.add(value)
                result.append(value)
        return result


class GroupedAggregator:
    """Grouped data aggregator."""

    @staticmethod
    def group_and_aggregate(
        data: list[dict[str, Any]],
        group_by: str,
        field: str,
        aggregation_type: AggregationType,
    ) -> list[GroupedAggregation]:
        """Group data and aggregate.

        Args:
            data: List of dictionaries
            group_by: Field to group by
            field: Field to aggregate
            aggregation_type: Type of aggregation

        Returns:
            List of grouped aggregations
        """
        groups: dict[Any, list[dict[str, Any]]] = defaultdict(list)
        for item in data:
            key = item.get(group_by)
            groups[key].append(item)

        results: list[GroupedAggregation] = []
        for key, group_data in groups.items():
            if aggregation_type == AggregationType.SUM:
                value = Aggregator.sum(group_data, field)
            elif aggregation_type == AggregationType.COUNT:
                value = Aggregator.count(group_data, field)
            elif aggregation_type == AggregationType.AVG:
                value = Aggregator.average(group_data, field)
            elif aggregation_type == AggregationType.MIN:
                value = Aggregator.min(group_data, field)
            elif aggregation_type == AggregationType.MAX:
                value = Aggregator.max(group_data, field)
            elif aggregation_type == AggregationType.FIRST:
                value = Aggregator.first(group_data, field)
            elif aggregation_type == AggregationType.LAST:
                value = Aggregator.last(group_data, field)
            elif aggregation_type == AggregationType.DISTINCT:
                value = len(Aggregator.distinct(group_data, field))
            else:
                value = None

            results.append(GroupedAggregation(
                group_key=key,
                results=[AggregationResult(
                    key=field,
                    value=value,
                    aggregation_type=aggregation_type,
                )]
            ))

        return results


class StatisticalAggregator:
    """Statistical aggregation utilities."""

    @staticmethod
    def variance(data: list[float], mean: Optional[float] = None) -> float:
        """Calculate variance.

        Args:
            data: List of numbers
            mean: Pre-calculated mean

        Returns:
            Variance
        """
        if not data:
            return 0.0
        m = mean if mean is not None else sum(data) / len(data)
        return sum((x - m) ** 2 for x in data) / len(data)

    @staticmethod
    def std_dev(data: list[float], mean: Optional[float] = None) -> float:
        """Calculate standard deviation.

        Args:
            data: List of numbers
            mean: Pre-calculated mean

        Returns:
            Standard deviation
        """
        return math.sqrt(StatisticalAggregator.variance(data, mean))

    @staticmethod
    def percentile(data: list[float], p: float) -> float:
        """Calculate percentile.

        Args:
            data: List of numbers
            p: Percentile (0-100)

        Returns:
            Percentile value
        """
        if not data:
            return 0.0
        sorted_data = sorted(data)
        k = (len(sorted_data) - 1) * p / 100
        f = math.floor(k)
        c = math.ceil(k)
        if f == c:
            return sorted_data[int(k)]
        d0 = sorted_data[int(f)] * (c - k)
        d1 = sorted_data[int(c)] * (k - f)
        return d0 + d1

    @staticmethod
    def median(data: list[float]) -> float:
        """Calculate median.

        Args:
            data: List of numbers

        Returns:
            Median value
        """
        return StatisticalAggregator.percentile(data, 50)

    @staticmethod
    def mode(data: list[float]) -> float:
        """Calculate mode (most common value).

        Args:
            data: List of numbers

        Returns:
            Mode value
        """
        if not data:
            return 0.0
        counts = defaultdict(int)
        for value in data:
            counts[value] += 1
        return max(counts.keys(), key=lambda x: counts[x])


def aggregate_sum(data: list[dict[str, Any]], field: str) -> float:
    """Sum field values.

    Args:
        data: List of dictionaries
        field: Field to sum

    Returns:
        Sum of values
    """
    return Aggregator.sum(data, field)


def aggregate_avg(data: list[dict[str, Any]], field: str) -> float:
    """Average field values.

    Args:
        data: List of dictionaries
        field: Field to average

    Returns:
        Average value
    """
    return Aggregator.average(data, field)


def group_aggregate(
    data: list[dict[str, Any]],
    group_by: str,
    field: str,
    aggregation: str = "sum",
) -> list[GroupedAggregation]:
    """Group and aggregate data.

    Args:
        data: List of dictionaries
        group_by: Field to group by
        field: Field to aggregate
        aggregation: Aggregation type

    Returns:
        List of grouped aggregations
    """
    agg_type = AggregationType(aggregation)
    return GroupedAggregator.group_and_aggregate(data, group_by, field, agg_type)
