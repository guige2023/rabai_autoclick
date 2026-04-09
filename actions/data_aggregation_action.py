"""
Data Aggregation Action Module.

Provides data aggregation capabilities with groupby, windowing,
and custom aggregation functions for analytics workflows.

Author: RabAi Team
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Tuple, TypeVar

T = TypeVar("T")


@dataclass
class AggregationResult:
    """Result of an aggregation operation."""
    key: Any
    value: Any
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class GroupByConfig:
    """Configuration for groupby operations."""
    keys: List[str]
    aggregations: Dict[str, List[str]]  # field -> list of agg functions
    having: Optional[Callable[[Dict], bool]] = None
    order_by: Optional[List[Tuple[str, str]]] = None  # (field, asc|desc)


class AggregationFunction:
    """Built-in aggregation functions."""

    @staticmethod
    def sum_(values: List[float]) -> float:
        return sum(values)

    @staticmethod
    def avg(values: List[float]) -> float:
        return sum(values) / len(values) if values else 0.0

    @staticmethod
    def min_(values: List[float]) -> float:
        return min(values) if values else None

    @staticmethod
    def max_(values: List[float]) -> float:
        return max(values) if values else None

    @staticmethod
    def count(values: List[Any]) -> int:
        return len(values)

    @staticmethod
    def count_distinct(values: List[Any]) -> int:
        return len(set(values))

    @staticmethod
    def first(values: List[Any]) -> Any:
        return values[0] if values else None

    @staticmethod
    def last(values: List[Any]) -> Any:
        return values[-1] if values else None

    @staticmethod
    def std_dev(values: List[float]) -> float:
        if len(values) < 2:
            return 0.0
        import statistics
        return statistics.stdev(values)

    @staticmethod
    def median(values: List[float]) -> float:
        if not values:
            return 0.0
        import statistics
        return statistics.median(values)


class DataAggregator:
    """Main aggregation engine."""

    def __init__(self) -> None:
        self.agg_functions: Dict[str, Callable] = {
            "sum": AggregationFunction.sum_,
            "avg": AggregationFunction.avg,
            "mean": AggregationFunction.avg,
            "min": AggregationFunction.min_,
            "max": AggregationFunction.max_,
            "count": AggregationFunction.count,
            "count_distinct": AggregationFunction.count_distinct,
            "first": AggregationFunction.first,
            "last": AggregationFunction.last,
            "std_dev": AggregationFunction.std_dev,
            "median": AggregationFunction.median,
        }

    def register_function(self, name: str, func: Callable) -> None:
        """Register a custom aggregation function."""
        self.agg_functions[name] = func

    def _get_nested_value(self, obj: Dict, key: str) -> Any:
        """Get value from nested dictionary using dot notation."""
        keys = key.split(".")
        value = obj
        for k in keys:
            if isinstance(value, dict):
                value = value.get(k)
            else:
                return None
        return value

    def _group_data(
        self,
        data: List[Dict],
        keys: List[str],
    ) -> Dict[Tuple, List[Dict]]:
        """Group data by specified keys."""
        groups: Dict[Tuple, List[Dict]] = defaultdict(list)
        for record in data:
            key_values = tuple(self._get_nested_value(record, k) for k in keys)
            groups[key_values].append(record)
        return groups

    def aggregate(
        self,
        data: List[Dict],
        config: GroupByConfig,
    ) -> List[Dict]:
        """Perform aggregation on data."""
        groups = self._group_data(data, config.keys)
        results = []

        for key_tuple, records in groups.items():
            result = {}
            for i, k in enumerate(config.keys):
                result[k] = key_tuple[i]

            for field_name, agg_funcs in config.aggregations.items():
                for agg_name in agg_funcs:
                    if agg_name not in self.agg_functions:
                        continue

                    values = []
                    for record in records:
                        val = self._get_nested_value(record, field_name)
                        if val is not None:
                            values.append(val)

                    agg_func = self.agg_functions[agg_name]
                    result[f"{field_name}_{agg_name}"] = agg_func(values)

            if config.having and not config.having(result):
                continue

            results.append(result)

        if config.order_by:
            for field_name, direction in reversed(config.order_by):
                reverse = direction.lower() == "desc"
                results.sort(key=lambda x: x.get(field_name, 0), reverse=reverse)

        return results

    def rolling_aggregate(
        self,
        data: List[Dict],
        window_size: int,
        field_name: str,
        agg_name: str,
    ) -> List[Dict]:
        """Compute rolling aggregation over a window."""
        if agg_name not in self.agg_functions:
            raise ValueError(f"Unknown aggregation: {agg_name}")

        agg_func = self.agg_functions[agg_name]
        results = []

        for i in range(len(data)):
            window_start = max(0, i - window_size + 1)
            window_values = [
                self._get_nested_value(data[j], field_name)
                for j in range(window_start, i + 1)
            ]
            window_values = [v for v in window_values if v is not None]

            result = data[i].copy()
            result[f"{field_name}_rolling_{agg_name}"] = agg_func(window_values)
            results.append(result)

        return results

    def cumulative_aggregate(
        self,
        data: List[Dict],
        field_name: str,
        agg_name: str,
    ) -> List[Dict]:
        """Compute cumulative aggregation."""
        if agg_name not in self.agg_functions:
            raise ValueError(f"Unknown aggregation: {agg_name}")

        agg_func = self.agg_functions[agg_name]
        results = []
        cumulative_values = []

        for record in data:
            value = self._get_nested_value(record, field_name)
            if value is not None:
                cumulative_values.append(value)

            result = record.copy()
            result[f"{field_name}_cumulative_{agg_name}"] = agg_func(cumulative_values)
            results.append(result)

        return results


class TimeSeriesAggregator(DataAggregator):
    """Specialized aggregator for time series data."""

    def aggregate_by_time_bucket(
        self,
        data: List[Dict],
        timestamp_field: str,
        bucket_seconds: int,
        aggregations: Dict[str, List[str]],
    ) -> List[Dict]:
        """Aggregate by time bucket."""
        import time

        groups: Dict[int, List[Dict]] = defaultdict(list)
        for record in data:
            ts = self._get_nested_value(record, timestamp_field)
            if ts is None:
                continue
            if isinstance(ts, (int, float)):
                bucket = int(ts // bucket_seconds) * bucket_seconds
            else:
                bucket = int(time.mktime(ts.timetuple()) // bucket_seconds) * bucket_seconds
            groups[bucket].append(record)

        results = []
        for bucket_time, records in sorted(groups.items()):
            result = {timestamp_field: bucket_time}
            for field_name, agg_funcs in aggregations.items():
                for agg_name in agg_funcs:
                    values = [
                        self._get_nested_value(r, field_name)
                        for r in records
                        if self._get_nested_value(r, field_name) is not None
                    ]
                    if agg_name in self.agg_functions:
                        result[f"{field_name}_{agg_name}"] = self.agg_functions[agg_name](values)
            results.append(result)

        return results
