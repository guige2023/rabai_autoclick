"""Aggregation Engine Action Module.

Perform aggregation operations on data streams and collections.
"""

from __future__ import annotations

import asyncio
from collections import defaultdict
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Generic, TypeVar
import statistics

T = TypeVar("T")


class AggregationType(Enum):
    """Aggregation function types."""
    SUM = "sum"
    COUNT = "count"
    AVG = "avg"
    MIN = "min"
    MAX = "max"
    MEDIAN = "median"
    STDDEV = "stddev"
    FIRST = "first"
    LAST = "last"
    DISTINCT = "distinct"
    PERCENTILE = "percentile"
    RATE = "rate"


@dataclass
class AggregationResult:
    """Result of aggregation."""
    field: str
    aggregation_type: AggregationType
    value: Any
    group_key: dict[str, Any] | None = None


@dataclass
class AggregationConfig:
    """Configuration for aggregation."""
    field: str
    aggregation_type: AggregationType
    output_alias: str | None = None
    percentile_value: float | None = None


class AggregationEngine:
    """Engine for performing aggregations on data."""

    def __init__(self) -> None:
        self._running = False

    def aggregate(
        self,
        data: list[dict],
        group_by: list[str],
        aggregations: list[AggregationConfig]
    ) -> list[dict]:
        """Perform aggregations on data grouped by fields."""
        if not group_by:
            return [self._aggregate_single(data, aggregations)]
        groups: dict[tuple, list[dict]] = defaultdict(list)
        for record in data:
            key = tuple(record.get(f) for f in group_by)
            groups[key].append(record)
        results = []
        for key_tuple, group_data in groups.items():
            group_key = dict(zip(group_by, key_tuple))
            agg_result = self._aggregate_single(group_data, aggregations)
            results.append({**group_key, **agg_result})
        return results

    def _aggregate_single(
        self,
        data: list[dict],
        aggregations: list[AggregationConfig]
    ) -> dict[str, Any]:
        """Perform aggregations without grouping."""
        result = {}
        for config in aggregations:
            values = [r.get(config.field) for r in data if config.get(config.field) is not None]
            value = self._compute_aggregation(values, config.aggregation_type, config.percentile_value)
            alias = config.output_alias or f"{config.aggregation_type.value}_{config.field}"
            result[alias] = value
        return result

    def _compute_aggregation(
        self,
        values: list[Any],
        agg_type: AggregationType,
        percentile: float | None = None
    ) -> Any:
        """Compute a single aggregation."""
        if not values:
            return None
        try:
            if agg_type == AggregationType.SUM:
                return sum(float(v) for v in values)
            elif agg_type == AggregationType.COUNT:
                return len(values)
            elif agg_type == AggregationType.AVG:
                return statistics.mean(float(v) for v in values)
            elif agg_type == AggregationType.MIN:
                return min(float(v) for v in values)
            elif agg_type == AggregationType.MAX:
                return max(float(v) for v in values)
            elif agg_type == AggregationType.MEDIAN:
                return statistics.median(float(v) for v in values)
            elif agg_type == AggregationType.STDDEV:
                return statistics.stdev(float(v) for v in values) if len(values) > 1 else 0
            elif agg_type == AggregationType.FIRST:
                return values[0]
            elif agg_type == AggregationType.LAST:
                return values[-1]
            elif agg_type == AggregationType.DISTINCT:
                return len(set(values))
            elif agg_type == AggregationType.PERCENTILE:
                if percentile is None:
                    percentile = 50
                import numpy as np
                return float(np.percentile(values, percentile))
            elif agg_type == AggregationType.RATE:
                return len(values) / max(sum(float(v) for v in values), 1)
        except Exception:
            return None
        return None


class StreamingAggregator:
    """Streaming aggregator for incremental computation."""

    def __init__(self) -> None:
        self._sums: dict[str, float] = defaultdict(float)
        self._counts: dict[str, int] = defaultdict(int)
        self._mins: dict[str, float] = {}
        self._maxs: dict[str, float] = {}
        self._values: dict[str, list] = defaultdict(list)
        self._lock = asyncio.Lock()

    async def add(self, field: str, value: float) -> None:
        """Add a value to the aggregation."""
        async with self._lock:
            self._sums[field] += value
            self._counts[field] += 1
            self._values[field].append(value)
            if field not in self._mins or value < self._mins[field]:
                self._mins[field] = value
            if field not in self._maxs or value > self._maxs[field]:
                self._maxs[field] = value

    async def get_result(self, field: str, agg_type: AggregationType) -> Any:
        """Get aggregation result for a field."""
        async with self._lock:
            values = self._values.get(field, [])
            if not values:
                return None
            if agg_type == AggregationType.SUM:
                return self._sums[field]
            elif agg_type == AggregationType.COUNT:
                return self._counts[field]
            elif agg_type == AggregationType.AVG:
                return self._sums[field] / self._counts[field]
            elif agg_type == AggregationType.MIN:
                return self._mins[field]
            elif agg_type == AggregationType.MAX:
                return self._maxs[field]
            elif agg_type == AggregationType.MEDIAN:
                return statistics.median(values)
            return None

    async def reset(self, field: str | None = None) -> None:
        """Reset aggregations."""
        async with self._lock:
            if field:
                self._sums.pop(field, None)
                self._counts.pop(field, None)
                self._mins.pop(field, None)
                self._maxs.pop(field, None)
                self._values.pop(field, None)
            else:
                self._sums.clear()
                self._counts.clear()
                self._mins.clear()
                self._maxs.clear()
                self._values.clear()
