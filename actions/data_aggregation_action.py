"""
Data Aggregation Action Module

Streaming and batch data aggregation with support for
windowing, grouping, and custom aggregation functions.

MIT License - Copyright (c) 2025 RabAi Research
"""

from __future__ import annotations

import asyncio
import logging
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set, TypeVar, Generic

logger = logging.getLogger(__name__)

T = TypeVar("T")
R = TypeVar("R")


class WindowType(Enum):
    """Windowing types for stream processing."""

    TUMBLING = "tumbling"
    SLIDING = "sliding"
    SESSION = "session"
    FIXED = "fixed"


class AggregationType(Enum):
    """Built-in aggregation types."""

    SUM = "sum"
    COUNT = "count"
    AVG = "avg"
    MIN = "min"
    MAX = "max"
    FIRST = "first"
    LAST = "last"
    LIST = "list"
    DICT = "dict"
    CUSTOM = "custom"


@dataclass
class TimeWindow:
    """Time-based window for aggregation."""

    window_type: WindowType
    size_seconds: float
    slide_seconds: Optional[float] = None
    start_time: Optional[datetime] = None

    def get_window_end(self, reference_time: datetime) -> datetime:
        """Get window end time."""
        if self.start_time is None:
            return reference_time + timedelta(seconds=self.size_seconds)

        elapsed = (reference_time - self.start_time).total_seconds()
        window_num = int(elapsed / self.slide_seconds) if self.slide_seconds else 0

        if self.window_type == WindowType.TUMBLING:
            return self.start_time + timedelta(
                seconds=(window_num + 1) * self.size_seconds
            )
        elif self.window_type == WindowType.SLIDING:
            return self.start_time + timedelta(
                seconds=window_num * (self.slide_seconds or 1) + self.size_seconds
            )

        return reference_time + timedelta(seconds=self.size_seconds)


@dataclass
class AggregationConfig:
    """Configuration for aggregation operations."""

    window_type: WindowType = WindowType.TUMBLING
    window_size_seconds: float = 60.0
    slide_interval_seconds: float = 60.0
    max_window_size: int = 10000
    emit_on_window_close: bool = True
    late_arrival_tolerance_seconds: float = 0.0


@dataclass
class AggregationResult(Generic[R]):
    """Result of an aggregation operation."""

    window_id: str
    window_start: datetime
    window_end: datetime
    value: R
    item_count: int
    is_final: bool = True


class Aggregator(Generic[T, R]):
    """
    Base aggregator class for custom aggregation logic.
    """

    def __init__(self, aggregation_fn: Callable[[List[T]], R]):
        self.aggregation_fn = aggregation_fn
        self._items: List[T] = []

    def add(self, item: T) -> None:
        """Add an item to the aggregation."""
        self._items.append(item)

    def get_result(self) -> R:
        """Get the aggregation result."""
        return self.aggregation_fn(self._items)

    def reset(self) -> None:
        """Reset the aggregator."""
        self._items = []

    @property
    def count(self) -> int:
        """Get number of items."""
        return len(self._items)


class BuiltInAggregations:
    """Factory for built-in aggregation functions."""

    @staticmethod
    def sum(field_name: Optional[str] = None) -> Callable[[List[Dict]], float]:
        """Sum aggregation."""
        def agg(items: List[Dict]) -> float:
            if field_name:
                return sum(d.get(field_name, 0) for d in items)
            return sum(d for d in items if isinstance(d, (int, float)))
        return agg

    @staticmethod
    def avg(field_name: Optional[str] = None) -> Callable[[List[Dict]], float]:
        """Average aggregation."""
        def agg(items: List[Dict]) -> float:
            if field_name:
                values = [d.get(field_name, 0) for d in items]
            else:
                values = [d for d in items if isinstance(d, (int, float))]
            return sum(values) / len(values) if values else 0.0
        return agg

    @staticmethod
    def count() -> Callable[[List[Any]], int]:
        """Count aggregation."""
        return lambda items: len(items)

    @staticmethod
    def min(field_name: Optional[str] = None) -> Callable[[List[Dict]], Any]:
        """Minimum aggregation."""
        def agg(items: List[Dict]) -> Any:
            if field_name:
                values = [d.get(field_name) for d in items]
            else:
                values = items
            return min(values) if values else None
        return agg

    @staticmethod
    def max(field_name: Optional[str] = None) -> Callable[[List[Dict]], Any]:
        """Maximum aggregation."""
        def agg(items: List[Dict]) -> Any:
            if field_name:
                values = [d.get(field_name) for d in items]
            else:
                values = items
            return max(values) if values else None
        return agg

    @staticmethod
    def list(field_name: Optional[str] = None) -> Callable[[List[Dict]], List]:
        """List aggregation."""
        def agg(items: List[Dict]) -> List:
            if field_name:
                return [d.get(field_name) for d in items]
            return list(items)
        return agg

    @staticmethod
    def dict(
        key_field: str,
        value_field: Optional[str] = None,
    ) -> Callable[[List[Dict]], Dict]:
        """Dictionary aggregation."""
        def agg(items: List[Dict]) -> Dict:
            result = {}
            for d in items:
                key = d.get(key_field)
                if key is not None:
                    if value_field:
                        result[key] = d.get(value_field)
                    else:
                        result[key] = d
            return result
        return agg

    @staticmethod
    def percentile(
        field_name: str,
        percentile: float,
    ) -> Callable[[List[Dict]], float]:
        """Percentile aggregation."""
        def agg(items: List[Dict]) -> float:
            values = sorted(d.get(field_name, 0) for d in items)
            if not values:
                return 0.0
            idx = int(len(values) * percentile / 100)
            return values[min(idx, len(values) - 1)]
        return agg


class WindowedAggregator(Generic[T]):
    """
    Aggregator with time-windowing support.
    """

    def __init__(
        self,
        aggregation_fn: Callable[[List[T]], R],
        config: AggregationConfig,
    ):
        self.aggregation_fn = aggregation_fn
        self.config = config
        self._windows: Dict[str, List[T]] = defaultdict(list)
        self._window_times: Dict[str, datetime] = {}

    def add(self, item: T, timestamp: datetime, window_id: str) -> Optional[AggregationResult[R]]:
        """Add an item and return result if window closes."""
        self._windows[window_id].append(item)

        if len(self._windows[window_id]) >= self.config.max_window_size:
            return self._close_window(window_id)

        return None

    def _close_window(self, window_id: str) -> AggregationResult[R]:
        """Close a window and return results."""
        items = self._windows.get(window_id, [])
        result = AggregationResult(
            window_id=window_id,
            window_start=self._window_times.get(window_id, datetime.now()),
            window_end=datetime.now(),
            value=self.aggregation_fn(items),
            item_count=len(items),
        )

        del self._windows[window_id]
        if window_id in self._window_times:
            del self._window_times[window_id]

        return result

    def close_old_windows(
        self,
        reference_time: datetime,
    ) -> List[AggregationResult[R]]:
        """Close windows older than the reference time."""
        results = []
        cutoff = reference_time - timedelta(seconds=self.config.late_arrival_tolerance_seconds)

        for window_id, window_start in list(self._window_times.items()):
            if window_start < cutoff:
                result = self._close_window(window_id)
                if result:
                    results.append(result)

        return results


class DataAggregationAction(Generic[T]):
    """
    Main action class for data aggregation.

    Features:
    - Built-in aggregations (sum, avg, count, min, max, etc.)
    - Custom aggregation functions
    - Time-windowing (tumbling, sliding, session)
    - Group-by aggregation
    - Streaming aggregation with buffering

    Usage:
        action = DataAggregationAction()
        action.add_aggregation("count", AggregationType.COUNT)
        action.add_group_by("category")
        result = await action.aggregate(data)
    """

    def __init__(self, config: Optional[AggregationConfig] = None):
        self.config = config or AggregationConfig()
        self._aggregations: Dict[str, Callable[[List], Any]] = {}
        self._group_by_fields: List[str] = []
        self._window: Optional[TimeWindow] = None
        self._stats = {
            "items_processed": 0,
            "windows_closed": 0,
            "aggregations_computed": 0,
        }

    def add_aggregation(
        self,
        name: str,
        agg_type: AggregationType,
        field_name: Optional[str] = None,
        **kwargs,
    ) -> "DataAggregationAction":
        """Add an aggregation."""
        if agg_type == AggregationType.SUM:
            self._aggregations[name] = BuiltInAggregations.sum(field_name)
        elif agg_type == AggregationType.AVG:
            self._aggregations[name] = BuiltInAggregations.avg(field_name)
        elif agg_type == AggregationType.COUNT:
            self._aggregations[name] = BuiltInAggregations.count()
        elif agg_type == AggregationType.MIN:
            self._aggregations[name] = BuiltInAggregations.min(field_name)
        elif agg_type == AggregationType.MAX:
            self._aggregations[name] = BuiltInAggregations.max(field_name)
        elif agg_type == AggregationType.LIST:
            self._aggregations[name] = BuiltInAggregations.list(field_name)
        elif agg_type == AggregationType.DICT:
            self._aggregations[name] = BuiltInAggregations.dict(
                kwargs.get("key_field", "id"),
                kwargs.get("value_field"),
            )
        else:
            raise ValueError(f"Unsupported aggregation type: {agg_type}")

        return self

    def add_custom_aggregation(
        self,
        name: str,
        aggregation_fn: Callable[[List[Any]], Any],
    ) -> "DataAggregationAction":
        """Add a custom aggregation function."""
        self._aggregations[name] = aggregation_fn
        return self

    def add_group_by(self, *fields: str) -> "DataAggregationAction":
        """Add group-by fields."""
        self._group_by_fields.extend(fields)
        return self

    def set_window(
        self,
        window_type: WindowType,
        size_seconds: float,
        slide_seconds: Optional[float] = None,
    ) -> "DataAggregationAction":
        """Set time window configuration."""
        self._window = TimeWindow(
            window_type=window_type,
            size_seconds=size_seconds,
            slide_seconds=slide_seconds,
            start_time=datetime.now(),
        )
        return self

    def _get_group_key(self, item: Dict[str, Any]) -> str:
        """Get group key from item."""
        if not self._group_by_fields:
            return "_global"

        parts = []
        for field in self._group_by_fields:
            value = item.get(field, "__none__")
            parts.append(f"{field}={value}")
        return "|".join(parts)

    async def aggregate(
        self,
        data: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """Aggregate data."""
        self._stats["items_processed"] += len(data)

        if self._group_by_fields:
            # Group-by aggregation
            groups: Dict[str, List[Dict]] = defaultdict(list)
            for item in data:
                key = self._get_group_key(item)
                groups[key].append(item)

            results = {}
            for group_key, group_items in groups.items():
                group_result = {}
                for name, agg_fn in self._aggregations.items():
                    group_result[name] = agg_fn(group_items)
                results[group_key] = group_result
                self._stats["aggregations_computed"] += 1

            return {"groups": results, "total_groups": len(groups)}

        else:
            # Simple aggregation
            result = {}
            for name, agg_fn in self._aggregations.items():
                result[name] = agg_fn(data)
                self._stats["aggregations_computed"] += 1

            return result

    async def aggregate_stream(
        self,
        data_stream: List[Dict[str, Any]],
    ) -> List[AggregationResult]:
        """Aggregate data with windowing."""
        results: List[AggregationResult] = []
        window_aggregators: Dict[str, WindowedAggregator] = {}

        for item in data_stream:
            timestamp = datetime.now()
            window_id = "default"

            if self._window:
                window_id = f"window_{int(timestamp.timestamp() / self._window.size_seconds)}"

            if window_id not in window_aggregators:
                window_aggregators[window_id] = WindowedAggregator(
                    self._aggregations.get("value", lambda x: x[0]),
                    self.config,
                )

            agg = window_aggregators[window_id]
            result = agg.add(item, timestamp, window_id)

            if result:
                results.append(result)
                self._stats["windows_closed"] += 1

        # Close remaining windows
        for agg in window_aggregators.values():
            closed = agg.close_old_windows(datetime.now())
            results.extend(closed)

        return results

    def get_stats(self) -> Dict[str, int]:
        """Get aggregation statistics."""
        return self._stats.copy()


def demo_aggregation():
    """Demonstrate aggregation usage."""
    import asyncio

    data = [
        {"category": "A", "value": 10, "count": 1},
        {"category": "A", "value": 20, "count": 1},
        {"category": "B", "value": 15, "count": 1},
        {"category": "B", "value": 25, "count": 1},
        {"category": "A", "value": 30, "count": 1},
    ]

    action = DataAggregationAction()
    action.add_aggregation("total_value", AggregationType.SUM, "value")
    action.add_aggregation("avg_value", AggregationType.AVG, "value")
    action.add_aggregation("min_value", AggregationType.MIN, "value")
    action.add_aggregation("max_value", AggregationType.MAX, "value")
    action.add_aggregation("item_count", AggregationType.COUNT)

    result = asyncio.run(action.aggregate(data))
    print(f"Simple aggregation: {result}")

    # Group by
    action2 = DataAggregationAction()
    action2.add_aggregation("total_value", AggregationType.SUM, "value")
    action2.add_group_by("category")

    result2 = asyncio.run(action2.aggregate(data))
    print(f"Group-by aggregation: {result2}")


if __name__ == "__main__":
    demo_aggregation()
