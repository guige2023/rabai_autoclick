"""Data aggregator utilities for combining and summarizing data.

Supports group-by aggregation, time-window aggregation, and rollup operations.
"""

from __future__ import annotations

import logging
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Callable, TypeVar

logger = logging.getLogger(__name__)

T = TypeVar("T")
K = TypeVar("K")
V = TypeVar("V")


@dataclass
class AggregationResult:
    """Result of an aggregation operation."""

    groups: dict[Any, Any]
    total: int
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class TimeWindow:
    """Time window configuration."""

    start: datetime
    end: datetime
    size_seconds: float

    def contains(self, dt: datetime) -> bool:
        """Check if datetime falls within window."""
        return self.start <= dt < self.end

    @property
    def duration(self) -> timedelta:
        """Get window duration."""
        return self.end - self.start


class Aggregator:
    """Data aggregation with multiple strategies.

    Args:
        data: List of dicts to aggregate.
    """

    def __init__(self, data: list[dict[str, Any]] | None = None) -> None:
        self._data = data or []

    def group_by(self, key: str | Callable[[dict[str, Any]], Any]) -> dict[Any, list[dict[str, Any]]]:
        """Group data by key.

        Args:
            key: Field name or function to extract group key.

        Returns:
            Dict mapping group keys to lists of items.
        """
        groups: dict[Any, list[dict[str, Any]]] = defaultdict(list)

        for item in self._data:
            if callable(key):
                group_key = key(item)
            else:
                group_key = item.get(key)

            groups[group_key].append(item)

        return dict(groups)

    def aggregate(
        self,
        group_by: str | Callable[[dict[str, Any]], Any],
        aggregations: dict[str, Callable[[list], Any]],
    ) -> list[dict[str, Any]]:
        """Aggregate grouped data with multiple aggregation functions.

        Args:
            group_by: Field name or function to group by.
            aggregations: Dict of {result_field: (source_field, agg_fn)}.

        Returns:
            List of aggregated results.
        """
        groups = self.group_by(group_by)
        results = []

        for group_key, items in groups.items():
            result: dict[str, Any] = {"_group": group_key, "_count": len(items)}

            for agg_name, (src_field, agg_fn) in aggregations.items():
                values = [item.get(src_field) for item in items if src_field in item]
                try:
                    result[agg_name] = agg_fn(values)
                except Exception as e:
                    logger.warning("Aggregation %s failed: %s", agg_name, e)
                    result[agg_name] = None

            results.append(result)

        return results

    def count(self, field: str | None = None) -> int:
        """Count items, optionally filtered by field.

        Args:
            field: Optional field name to check for truthiness.

        Returns:
            Count of items.
        """
        if field is None:
            return len(self._data)
        return sum(1 for item in self._data if item.get(field))

    def sum(self, field: str) -> float:
        """Sum values of a numeric field.

        Args:
            field: Field name to sum.

        Returns:
            Sum of values.
        """
        return sum(float(item.get(field, 0)) for item in self._data if item.get(field) is not None)

    def avg(self, field: str) -> float:
        """Calculate average of a numeric field.

        Args:
            field: Field name to average.

        Returns:
            Average value.
        """
        values = [float(item.get(field, 0)) for item in self._data if item.get(field) is not None]
        return sum(values) / len(values) if values else 0.0

    def min(self, field: str) -> Any:
        """Get minimum value of a field.

        Args:
            field: Field name.

        Returns:
            Minimum value or None.
        """
        values = [item.get(field) for item in self._data if field in item]
        return min(values) if values else None

    def max(self, field: str) -> Any:
        """Get maximum value of a field.

        Args:
            field: Field name.

        Returns:
            Maximum value or None.
        """
        values = [item.get(field) for item in self._data if field in item]
        return max(values) if values else None

    def percentile(self, field: str, percentile: float) -> float | None:
        """Calculate percentile of a numeric field.

        Args:
            field: Field name.
            percentile: Percentile to calculate (0-100).

        Returns:
            Percentile value or None.
        """
        values = sorted([float(item.get(field)) for item in self._data if item.get(field) is not None])
        if not values:
            return None

        idx = (percentile / 100) * (len(values) - 1)
        lower = int(idx)
        upper = min(lower + 1, len(values) - 1)
        weight = idx - lower

        return values[lower] * (1 - weight) + values[upper] * weight

    def distinct(self, field: str) -> list[Any]:
        """Get distinct values of a field.

        Args:
            field: Field name.

        Returns:
            List of distinct values.
        """
        return list({item.get(field) for item in self._data if field in item})

    def histogram(
        self,
        field: str,
        bins: int = 10,
        min_val: float | None = None,
        max_val: float | None = None,
    ) -> list[dict[str, Any]]:
        """Create histogram of numeric field.

        Args:
            field: Field name.
            bins: Number of bins.
            min_val: Minimum value (auto-detected if None).
            max_val: Maximum value (auto-detected if None).

        Returns:
            List of bin descriptions with count and range.
        """
        values = [float(item.get(field)) for item in self._data if item.get(field) is not None]
        if not values:
            return []

        min_val = min_val if min_val is not None else min(values)
        max_val = max_val if max_val is not None else max(values)

        if min_val == max_val:
            return [{"bin_start": min_val, "bin_end": max_val, "count": len(values)}]

        bin_width = (max_val - min_val) / bins
        bin_counts = [0] * bins

        for value in values:
            bin_idx = min(int((value - min_val) / bin_width), bins - 1)
            bin_counts[bin_idx] += 1

        return [
            {"bin_start": min_val + i * bin_width, "bin_end": min_val + (i + 1) * bin_width, "count": count}
            for i, count in enumerate(bin_counts)
        ]


class TimeSeriesAggregator:
    """Aggregate data by time windows.

    Args:
        timestamp_field: Name of timestamp field.
        tz: Timezone for grouping.
    """

    def __init__(self, timestamp_field: str = "timestamp", tz: str = "UTC") -> None:
        self.timestamp_field = timestamp_field
        self.tz = tz

    def time_window_agg(
        self,
        data: list[dict[str, Any]],
        window_seconds: float,
        aggregations: dict[str, Callable[[list], Any]],
        start_time: datetime | None = None,
        end_time: datetime | None = None,
    ) -> list[dict[str, Any]]:
        """Aggregate data into time windows.

        Args:
            data: List of dicts with timestamp field.
            window_seconds: Window size in seconds.
            aggregations: Dict of {result_field: (source_field, agg_fn)}.
            start_time: Window start (auto from data if None).
            end_time: Window end (auto from data if None).

        Returns:
            List of time window aggregations.
        """
        if not data:
            return []

        timestamps = []
        for item in data:
            ts = item.get(self.timestamp_field)
            if isinstance(ts, (int, float)):
                timestamps.append(datetime.fromtimestamp(ts))
            elif isinstance(ts, str):
                try:
                    timestamps.append(datetime.fromisoformat(ts))
                except ValueError:
                    continue
            elif isinstance(ts, datetime):
                timestamps.append(ts)

        if not timestamps:
            return []

        start = start_time or min(timestamps)
        end = end_time or max(timestamps)

        windows: list[dict[str, Any]] = []
        current = start

        while current < end:
            window_end = datetime.fromtimestamp(current.timestamp() + window_seconds)
            window_data = [item for item, ts in zip(data, timestamps) if current <= ts < window_end]

            if window_data:
                result: dict[str, Any] = {"window_start": current, "window_end": window_end, "count": len(window_data)}

                for agg_name, (src_field, agg_fn) in aggregations.items():
                    values = [item.get(src_field) for item in window_data if src_field in item]
                    try:
                        result[agg_name] = agg_fn(values)
                    except Exception:
                        result[agg_name] = None

                windows.append(result)

            current = window_end

        return windows

    def rollup(
        self,
        data: list[dict[str, Any]],
        granularity: str = "hour",
        aggregations: dict[str, Callable[[list], Any]],
    ) -> list[dict[str, Any]]:
        """Rollup data by time granularity.

        Args:
            data: List of dicts with timestamp field.
            granularity: One of 'minute', 'hour', 'day', 'week', 'month'.
            aggregations: Aggregation functions.

        Returns:
            List of rolled up aggregations.
        """
        seconds_map = {"minute": 60, "hour": 3600, "day": 86400, "week": 604800, "month": 2592000}
        window_seconds = seconds_map.get(granularity, 3600)

        return self.time_window_agg(data, window_seconds, aggregations)


class RollingAggregator:
    """Rolling window aggregation for time series data.

    Args:
        window_size: Number of periods in rolling window.
    """

    def __init__(self, window_size: int = 5) -> None:
        self.window_size = window_size

    def rolling_mean(self, values: list[float]) -> list[float | None]:
        """Calculate rolling mean.

        Args:
            values: List of numeric values.

        Returns:
            List with rolling means (None for first elements until window fills).
        """
        result = []
        for i in range(len(values)):
            if i < self.window_size - 1:
                result.append(None)
            else:
                window = values[i - self.window_size + 1 : i + 1]
                result.append(sum(window) / len(window))
        return result

    def rolling_sum(self, values: list[float]) -> list[float | None]:
        """Calculate rolling sum."""
        result = []
        for i in range(len(values)):
            if i < self.window_size - 1:
                result.append(None)
            else:
                window = values[i - self.window_size + 1 : i + 1]
                result.append(sum(window))
        return result

    def rolling_std(self, values: list[float]) -> list[float | None]:
        """Calculate rolling standard deviation."""
        import math

        result = []
        for i in range(len(values)):
            if i < self.window_size - 1:
                result.append(None)
            else:
                window = values[i - self.window_size + 1 : i + 1]
                mean = sum(window) / len(window)
                variance = sum((x - mean) ** 2 for x in window) / len(window)
                result.append(math.sqrt(variance))
        return result


class MultiKeyAggregator:
    """Aggregate by multiple keys (OLAP-style).

    Args:
        data: List of dicts to aggregate.
    """

    def __init__(self, data: list[dict[str, Any]] | None = None) -> None:
        self._data = data or []

    def cube(
        self,
        keys: list[str],
        measures: dict[str, Callable[[list], Any]],
    ) -> list[dict[str, Any]]:
        """Compute OLAP cube (all combinations of keys).

        Args:
            keys: List of key field names.
            measures: Dict of {measure_field: aggregation_fn}.

        Returns:
            List of cube cells.
        """
        from itertools import product

        results = []

        for r in range(len(keys) + 1):
            for key_combo in product(keys, repeat=r):
                groups = self._data
                if key_combo:
                    groups_dict = Aggregator(groups).group_by(list(key_combo))
                else:
                    groups_dict = {"_all": groups}

                for group_key, items in groups_dict.items():
                    result: dict[str, Any] = {"_keys": key_combo}
                    if isinstance(group_key, tuple):
                        for i, k in enumerate(key_combo):
                            result[k] = group_key[i]
                    elif key_combo:
                        result[key_combo[0]] = group_key

                    for measure, agg_fn in measures.items():
                        values = [item.get(measure) for item in items if measure in item]
                        try:
                            result[measure] = agg_fn(values)
                        except Exception:
                            result[measure] = None

                    results.append(result)

        return results

    def rollup(
        self,
        keys: list[str],
        measures: dict[str, Callable[[list], Any]],
    ) -> list[dict[str, Any]]:
        """Compute rollup (hierarchical aggregation).

        Args:
            keys: List of key fields in hierarchical order.
            measures: Dict of {measure_field: aggregation_fn}.

        Returns:
            List of rollup cells.
        """
        results = []

        for num_keys in range(len(keys), -1, -1):
            current_keys = keys[:num_keys]

            if current_keys:
                groups = Aggregator(self._data).group_by(current_keys[0] if len(current_keys) == 1 else lambda x: tuple(x.get(k) for k in current_keys))
            else:
                groups = {"_all": self._data}

            for group_key, items in groups.items():
                result: dict[str, Any] = {}
                if isinstance(group_key, tuple):
                    for i, k in enumerate(current_keys):
                        result[k] = group_key[i]
                elif current_keys:
                    result[current_keys[0]] = group_key

                for measure, agg_fn in measures.items():
                    values = [item.get(measure) for item in items if measure in item]
                    try:
                        result[measure] = agg_fn(values)
                    except Exception:
                        result[measure] = None

                results.append(result)

        return results


def aggregate(data: list[dict[str, Any]], **aggregations: Callable[[list], Any]) -> dict[str, Any]:
    """Quick aggregation helper.

    Args:
        data: List of dicts.
        **aggregations: Named aggregation functions applied to entire dataset.

    Returns:
        Dict of aggregation results.
    """
    result = {}
    for name, fn in aggregations.items():
        result[name] = fn(data)
    return result
