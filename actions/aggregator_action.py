"""aggregator action module for rabai_autoclick.

Provides data aggregation operations: grouping aggregations,
windowed aggregations, multi-level rollups, and
statistical aggregations over collections.
"""

from __future__ import annotations

import math
import statistics
from collections import defaultdict
from dataclasses import dataclass, field
from functools import reduce
from typing import Any, Callable, Dict, Iterable, Iterator, List, Optional, Sequence, Tuple, TypeVar

__all__ = [
    "Aggregator",
    "GroupAggregator",
    "WindowAggregator",
    "RollingAggregator",
    "MultiLevelAggregator",
    "StatsAggregator",
    "PercentileAggregator",
    "HistogramAggregator",
    "aggregate",
    "group_aggregate",
    "rolling_aggregate",
    "windowed_aggregate",
    "merge_aggregates",
    "WeightedAggregator",
]


T = TypeVar("T")
K = TypeVar("K")
V = TypeVar("V")


class Aggregator(Generic[T, V]):
    """Base aggregator class."""

    def __init__(self) -> None:
        self._values: List[T] = []

    def add(self, value: T) -> None:
        """Add a value."""
        self._values.append(value)

    def add_many(self, values: Iterable[T]) -> None:
        """Add multiple values."""
        self._values.extend(values)

    def result(self) -> V:
        """Get aggregated result."""
        raise NotImplementedError

    def reset(self) -> None:
        """Reset state."""
        self._values.clear()

    def count(self) -> int:
        """Number of values added."""
        return len(self._values)


class StatsAggregator(Aggregator[Union[int, float], Dict[str, float]]):
    """Statistical aggregation: mean, std, min, max, etc."""

    def result(self) -> Dict[str, float]:
        """Compute statistics."""
        if not self._values:
            return {
                "count": 0,
                "mean": 0.0,
                "std": 0.0,
                "min": 0.0,
                "max": 0.0,
                "sum": 0.0,
                "median": 0.0,
            }
        return {
            "count": len(self._values),
            "mean": statistics.mean(self._values),
            "std": statistics.stdev(self._values) if len(self._values) > 1 else 0.0,
            "min": min(self._values),
            "max": max(self._values),
            "sum": sum(self._values),
            "median": statistics.median(self._values),
        }


class PercentileAggregator(Aggregator[Union[int, float], Dict[str, float]]):
    """Aggregation with percentiles."""

    def __init__(self, percentiles: Optional[List[float]] = None) -> None:
        super().__init__()
        self.percentiles = percentiles or [0.25, 0.5, 0.75, 0.9, 0.95, 0.99]

    def result(self) -> Dict[str, float]:
        """Compute percentiles."""
        if not self._values:
            return {}
        sorted_vals = sorted(self._values)
        n = len(sorted_vals)
        result = {}
        for p in self.percentiles:
            idx = int(n * p)
            idx = min(idx, n - 1)
            result[f"p{int(p*100)}"] = sorted_vals[idx]
        return result


class HistogramAggregator(Aggregator[Union[int, float], Dict[str, int]]):
    """Binned histogram aggregation."""

    def __init__(
        self,
        bins: int = 10,
        range_min: Optional[float] = None,
        range_max: Optional[float] = None,
    ) -> None:
        super().__init__()
        self.bins = bins
        self.range_min = range_min
        self.range_max = range_max
        self._counts: Dict[int, int] = defaultdict(int)

    def add(self, value: Union[int, float]) -> None:
        """Add value to appropriate bin."""
        super().add(value)
        mn = self.range_min if self.range_min is not None else min(self._values)
        mx = self.range_max if self.range_max is not None else max(self._values)
        if mn == mx:
            idx = 0
        else:
            idx = int((value - mn) / (mx - mn) * self.bins)
            idx = max(0, min(self.bins - 1, idx))
        self._counts[idx] += 1

    def result(self) -> Dict[str, int]:
        """Get histogram counts."""
        return {f"bin_{i}": self._counts[i] for i in range(self.bins)}


class GroupAggregator(Generic[K, T, V]):
    """Aggregator that groups by key and applies aggregation function."""

    def __init__(
        self,
        key_fn: Callable[[T], K],
        agg_fn: Callable[[List[T]], V],
    ) -> None:
        self.key_fn = key_fn
        self.agg_fn = agg_fn
        self._groups: Dict[K, List[T]] = defaultdict(list)

    def add(self, item: T) -> None:
        """Add item to appropriate group."""
        key = self.key_fn(item)
        self._groups[key].append(item)

    def add_many(self, items: Iterable[T]) -> None:
        """Add multiple items."""
        for item in items:
            self.add(item)

    def result(self) -> Dict[K, V]:
        """Get aggregated results per group."""
        return {k: self.agg_fn(v) for k, v in self._groups.items()}

    def reset(self) -> None:
        """Reset all groups."""
        self._groups.clear()


class WindowAggregator(Generic[T, V]):
    """Sliding window aggregator."""

    def __init__(
        self,
        agg_fn: Callable[[List[T]], V],
        window_size: int = 10,
    ) -> None:
        self.agg_fn = agg_fn
        self.window_size = window_size
        self._buffer: List[T] = []
        self._results: List[V] = []

    def add(self, value: T) -> Optional[V]:
        """Add value and compute window aggregate.

        Returns:
            Result if window is full, None otherwise.
        """
        self._buffer.append(value)
        if len(self._buffer) > self.window_size:
            self._buffer.pop(0)
        if len(self._buffer) == self.window_size:
            result = self.agg_fn(list(self._buffer))
            self._results.append(result)
            return result
        return None

    def result(self) -> List[V]:
        """Get all window results."""
        return list(self._results)

    def latest(self) -> Optional[V]:
        """Get most recent result."""
        return self._results[-1] if self._results else None


class RollingAggregator(Generic[T, V]):
    """Rolling aggregator with configurable window."""

    def __init__(
        self,
        agg_fn: Callable[[List[T]], V],
        window_size: int = 10,
        step: int = 1,
    ) -> None:
        self.agg_fn = agg_fn
        self.window_size = window_size
        self.step = step
        self._buffer: List[T] = []
        self._results: List[V] = []

    def add(self, value: T) -> Optional[V]:
        """Add value to rolling window."""
        self._buffer.append(value)
        if len(self._buffer) >= self.window_size:
            result = self.agg_fn(self._buffer[-self.window_size:])
            self._results.append(result)
            if len(self._results) % self.step == 0:
                return result
        return None

    def results(self) -> List[V]:
        return list(self._results)

    def latest(self) -> Optional[V]:
        return self._results[-1] if self._results else None


class MultiLevelAggregator(Generic[T]):
    """Multi-level aggregation for hierarchical rollups."""

    def __init__(
        self,
        level_fn: Callable[[T], str],
        agg_fns: Dict[str, Callable],
    ) -> None:
        self.level_fn = level_fn
        self.agg_fns = agg_fns
        self._data: Dict[str, List] = defaultdict(list)
        self._aggregators: Dict[str, Aggregator] = {}

    def add(self, item: T) -> None:
        """Add item to appropriate level."""
        level = self.level_fn(item)
        self._data[level].append(item)

    def add_many(self, items: Iterable[T]) -> None:
        """Add multiple items."""
        for item in items:
            self.add(item)

    def aggregate_at(self, level: str) -> Any:
        """Aggregate at specific level."""
        if level not in self._data:
            return None
        if level in self._aggregators:
            return self._aggregators[level].result()
        agg_fn = self.agg_fns.get(level)
        if agg_fn is None:
            return self._data[level]
        return agg_fn(self._data[level])

    def aggregate_all(self) -> Dict[str, Any]:
        """Aggregate at all levels."""
        return {level: self.aggregate_at(level) for level in self._data}


class WeightedAggregator(Aggregator[Tuple[T, float], float]):
    """Weighted average aggregator."""

    def result(self) -> float:
        """Compute weighted average."""
        if not self._values:
            return 0.0
        total_weight = sum(w for _, w in self._values)
        if total_weight == 0:
            return 0.0
        weighted_sum = sum(v * w for v, w in self._values)
        return weighted_sum / total_weight


def aggregate(
    items: Iterable[T],
    agg_fn: Callable[[List[T]], V],
) -> V:
    """Aggregate items with a single function.

    Args:
        items: Input items.
        agg_fn: Aggregation function.

    Returns:
        Aggregated result.
    """
    return agg_fn(list(items))


def group_aggregate(
    items: Iterable[T],
    key_fn: Callable[[T], K],
    agg_fn: Callable[[List[T]], V],
) -> Dict[K, V]:
    """Group items by key and aggregate each group.

    Args:
        items: Input items.
        key_fn: Function to extract group key.
        agg_fn: Aggregation function per group.

    Returns:
        Dict mapping key to aggregated result.
    """
    groups: Dict[K, List[T]] = defaultdict(list)
    for item in items:
        groups[key_fn(item)].append(item)
    return {k: agg_fn(v) for k, v in groups.items()}


def rolling_aggregate(
    items: Iterable[T],
    window_size: int,
    agg_fn: Callable[[List[T]], V],
    step: int = 1,
) -> List[V]:
    """Compute rolling aggregates over items.

    Args:
        items: Input items.
        window_size: Size of rolling window.
        agg_fn: Aggregation function.
        step: Compute result every N items.

    Returns:
        List of aggregated values.
    """
    results = []
    buffer = []
    count = 0
    for item in items:
        buffer.append(item)
        if len(buffer) > window_size:
            buffer.pop(0)
        count += 1
        if count >= step and len(buffer) == window_size:
            results.append(agg_fn(list(buffer)))
            count = 0
    return results


def windowed_aggregate(
    items: Iterable[T],
    window_size: int,
    agg_fn: Callable[[List[T]], V],
) -> List[V]:
    """Compute windowed aggregates (non-overlapping windows).

    Args:
        items: Input items.
        window_size: Size of each window.
        agg_fn: Aggregation function.

    Returns:
        List of window aggregates.
    """
    results = []
    buffer = []
    for item in items:
        buffer.append(item)
        if len(buffer) == window_size:
            results.append(agg_fn(list(buffer)))
            buffer = []
    if buffer:
        results.append(agg_fn(buffer))
    return results


def merge_aggregates(
    aggregates: List[Dict[K, V]],
    merge_fn: Callable[[List[V]], V],
) -> Dict[K, V]:
    """Merge multiple aggregation results.

    Args:
        aggregates: List of aggregation dicts.
        merge_fn: Function to merge values for same key.

    Returns:
        Merged aggregation dict.
    """
    if not aggregates:
        return {}
    all_keys = set()
    for agg in aggregates:
        all_keys.update(agg.keys())
    result: Dict[K, List[V]] = {k: [] for k in all_keys}
    for agg in aggregates:
        for k in all_keys:
            if k in agg:
                result[k].append(agg[k])
    return {k: merge_fn(v) for k, v in result.items()}
