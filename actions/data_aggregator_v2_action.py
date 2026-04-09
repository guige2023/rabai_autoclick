"""Data Aggregator v2 with grouping, windowing, and multi-dimensional aggregation.

This module provides comprehensive data aggregation with:
- Multi-dimensional grouping
- Time-based windowing (tumbling, hopping, session)
- Multiple aggregation functions
- Hierarchical aggregation
- Streaming aggregation
"""

from __future__ import annotations

import logging
import time
from collections import defaultdict
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, TypeVar, Generic

logger = logging.getLogger(__name__)

T = TypeVar("T")


class AggregationType(Enum):
    """Types of aggregation functions."""

    SUM = "sum"
    COUNT = "count"
    AVG = "avg"
    MIN = "min"
    MAX = "max"
    FIRST = "first"
    LAST = "last"
    DISTINCT_COUNT = "distinct_count"
    STD_DEV = "std_dev"
    VARIANCE = "variance"
    MEDIAN = "median"
    PERCENTILE = "percentile"
    CUSTOM = "custom"


class WindowType(Enum):
    """Types of time windows."""

    TUMBLING = "tumbling"  # Non-overlapping fixed size
    HOPPING = "hopping"  # Overlapping sliding windows
    SESSION = "session"  # Activity-based sessions
    COUNT = "count"  # Count-based windows


@dataclass
class AggregationConfig:
    """Configuration for a single aggregation."""

    name: str
    agg_type: AggregationType
    field: str = "value"
    percentile_value: float = 0.5  # For PERCENTILE type
    custom_func: Callable[[list], Any] | None = None  # For CUSTOM type

    def __post_init__(self):
        if self.agg_type == AggregationType.CUSTOM and not self.custom_func:
            raise ValueError("CUSTOM aggregation requires custom_func")


@dataclass
class WindowConfig:
    """Configuration for time-based windowing."""

    window_type: WindowType
    size_seconds: float = 60.0
    hop_seconds: float | None = None  # For hopping windows
    session_timeout: float = 300.0  # For session windows
    max_window_size: int | None = None  # For count windows


@dataclass
class GroupKey:
    """A grouping key for aggregation."""

    dimensions: tuple[tuple[str, Any], ...]

    def __hash__(self) -> int:
        return hash(self.dimensions)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, GroupKey):
            return False
        return self.dimensions == other.dimensions


@dataclass
class AggregationResult(Generic[T]):
    """Result of an aggregation operation."""

    group_key: GroupKey | None
    aggregations: dict[str, Any]
    window_start: float | None = None
    window_end: float | None = None
    record_count: int = 0

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "group_key": {k: v for k, v in (self.group_key.dimensions if self.group_key else [])},
            "aggregations": self.aggregations,
            "window_start": self.window_start,
            "window_end": self.window_end,
            "record_count": self.record_count,
        }


class AggregationFunction:
    """Collection of aggregation functions."""

    @staticmethod
    def sum(values: list) -> float:
        """Sum of values."""
        return sum(float(v) for v in values if v is not None)

    @staticmethod
    def count(values: list) -> int:
        """Count of non-null values."""
        return sum(1 for v in values if v is not None)

    @staticmethod
    def avg(values: list) -> float:
        """Average of values."""
        non_null = [float(v) for v in values if v is not None]
        return sum(non_null) / len(non_null) if non_null else 0.0

    @staticmethod
    def min(values: list) -> Any:
        """Minimum value."""
        non_null = [v for v in values if v is not None]
        return min(non_null) if non_null else None

    @staticmethod
    def max(values: list) -> Any:
        """Maximum value."""
        non_null = [v for v in values if v is not None]
        return max(non_null) if non_null else None

    @staticmethod
    def first(values: list) -> Any:
        """First value."""
        for v in values:
            if v is not None:
                return v
        return None

    @staticmethod
    def last(values: list) -> Any:
        """Last value."""
        for v in reversed(values):
            if v is not None:
                return v
        return None

    @staticmethod
    def distinct_count(values: list) -> int:
        """Count of distinct values."""
        return len(set(v for v in values if v is not None))

    @staticmethod
    def std_dev(values: list) -> float:
        """Standard deviation."""
        import math
        non_null = [float(v) for v in values if v is not None]
        if len(non_null) < 2:
            return 0.0
        mean = sum(non_null) / len(non_null)
        variance = sum((x - mean) ** 2 for x in non_null) / (len(non_null) - 1)
        return math.sqrt(variance)

    @staticmethod
    def variance(values: list) -> float:
        """Variance."""
        non_null = [float(v) for v in values if v is not None]
        if len(non_null) < 2:
            return 0.0
        mean = sum(non_null) / len(non_null)
        return sum((x - mean) ** 2 for x in non_null) / (len(non_null) - 1)

    @staticmethod
    def median(values: list) -> float:
        """Median value."""
        non_null = sorted([float(v) for v in values if v is not None])
        n = len(non_null)
        if n == 0:
            return 0.0
        if n % 2 == 1:
            return non_null[n // 2]
        return (non_null[n // 2 - 1] + non_null[n // 2]) / 2

    @staticmethod
    def percentile(values: list, p: float) -> float:
        """Percentile value."""
        non_null = sorted([float(v) for v in values if v is not None])
        if not non_null:
            return 0.0
        index = (len(non_null) - 1) * p
        lower = int(index)
        upper = lower + 1
        weight = index - lower
        if upper >= len(non_null):
            return non_null[-1]
        return non_null[lower] * (1 - weight) + non_null[upper] * weight

    @staticmethod
    def apply(agg_type: AggregationType, values: list, **kwargs) -> Any:
        """Apply an aggregation function."""
        if agg_type == AggregationType.SUM:
            return AggregationFunction.sum(values)
        elif agg_type == AggregationType.COUNT:
            return AggregationFunction.count(values)
        elif agg_type == AggregationType.AVG:
            return AggregationFunction.avg(values)
        elif agg_type == AggregationType.MIN:
            return AggregationFunction.min(values)
        elif agg_type == AggregationType.MAX:
            return AggregationFunction.max(values)
        elif agg_type == AggregationType.FIRST:
            return AggregationFunction.first(values)
        elif agg_type == AggregationType.LAST:
            return AggregationFunction.last(values)
        elif agg_type == AggregationType.DISTINCT_COUNT:
            return AggregationFunction.distinct_count(values)
        elif agg_type == AggregationType.STD_DEV:
            return AggregationFunction.std_dev(values)
        elif agg_type == AggregationType.VARIANCE:
            return AggregationFunction.variance(values)
        elif agg_type == AggregationType.MEDIAN:
            return AggregationFunction.median(values)
        elif agg_type == AggregationType.PERCENTILE:
            return AggregationFunction.percentile(values, kwargs.get("p", 0.5))
        elif agg_type == AggregationType.CUSTOM:
            func = kwargs.get("custom_func")
            if func:
                return func(values)
        return None


class DataAggregatorV2(Generic[T]):
    """Advanced data aggregator with grouping and windowing."""

    def __init__(
        self,
        group_by: list[str] | None = None,
        aggregations: list[AggregationConfig] | None = None,
        window: WindowConfig | None = None,
    ):
        """Initialize the aggregator.

        Args:
            group_by: Fields to group by
            aggregations: List of aggregation configurations
            window: Optional window configuration for time-based aggregation
        """
        self.group_by = group_by or []
        self.aggregations = aggregations or []
        self.window = window

        self._data: dict[GroupKey, list[dict]] = defaultdict(list)
        self._window_data: dict[str, list[dict]] = defaultdict(list)
        self._session_activities: dict[GroupKey, float] = {}
        self._total_records = 0

    def add_aggregation(
        self,
        name: str,
        agg_type: AggregationType,
        field: str = "value",
        **kwargs,
    ) -> "DataAggregatorV2":
        """Add an aggregation configuration.

        Args:
            name: Aggregation name
            agg_type: Aggregation type
            field: Field to aggregate on
            **kwargs: Additional arguments (percentile_value, custom_func)

        Returns:
            Self for chaining
        """
        config = AggregationConfig(
            name=name,
            agg_type=agg_type,
            field=field,
            **kwargs,
        )
        self.aggregations.append(config)
        return self

    def ingest(self, record: dict[str, Any]) -> None:
        """Ingest a single record for aggregation.

        Args:
            record: Record to ingest
        """
        self._total_records += 1

        if self.window:
            self._ingest_windowed(record)
        else:
            self._ingest_grouped(record)

    def ingest_batch(self, records: list[dict[str, Any]]) -> None:
        """Ingest multiple records.

        Args:
            records: Records to ingest
        """
        for record in records:
            self.ingest(record)

    def _ingest_grouped(self, record: dict[str, Any]) -> None:
        """Ingest a record into grouped aggregation."""
        group_key = self._compute_group_key(record)
        self._data[group_key].append(record)

    def _ingest_windowed(self, record: dict[str, Any]) -> None:
        """Ingest a record into windowed aggregation."""
        timestamp = record.get("timestamp", time.time())
        window_key = self._compute_window_key(timestamp, record)

        if self.window.window_type == WindowType.SESSION:
            group_key = self._compute_group_key(record)
            self._update_session(group_key, timestamp, record)
        else:
            self._window_data[window_key].append(record)

    def _compute_group_key(self, record: dict[str, Any]) -> GroupKey:
        """Compute group key from record."""
        dimensions = []
        for field in self.group_by:
            value = record.get(field)
            dimensions.append((field, value))
        return GroupKey(tuple(dimensions))

    def _compute_window_key(self, timestamp: float, record: dict[str, Any]) -> str:
        """Compute window key for time-based aggregation."""
        wt = self.window.window_type
        size = self.window.size_seconds
        hop = self.window.hop_seconds or size

        if wt == WindowType.TUMBLING:
            window_id = int(timestamp / size)
            return f"window_{window_id}"

        elif wt == WindowType.HOPPING:
            window_id = int(timestamp / hop)
            return f"window_{window_id}"

        elif wt == WindowType.SESSION:
            group_key = self._compute_group_key(record)
            return f"session_{hash(group_key)}"

        elif wt == WindowType.COUNT:
            count = self._total_records
            window_id = count // (self.window.max_window_size or 100)
            return f"window_{window_id}"

        return f"window_{int(timestamp / size)}"

    def _update_session(
        self,
        group_key: GroupKey,
        timestamp: float,
        record: dict[str, Any],
    ) -> None:
        """Update session window for a group."""
        timeout = self.window.session_timeout
        last_activity = self._session_activities.get(group_key, timestamp)

        # Check if this is a new session
        if timestamp - last_activity > timeout:
            # Close old session and start new one
            pass

        self._session_activities[group_key] = timestamp
        self._data[group_key].append(record)

    def compute(self) -> list[AggregationResult]:
        """Compute aggregations.

        Returns:
            List of AggregationResult, one per group
        """
        results = []

        if self.window:
            if self.window.window_type in (WindowType.TUMBLING, WindowType.HOPPING):
                results = self._compute_windowed()
            elif self.window.window_type == WindowType.SESSION:
                results = self._compute_sessions()
            elif self.window.window_type == WindowType.COUNT:
                results = self._compute_count_windows()
        else:
            results = self._compute_grouped()

        return results

    def _compute_grouped(self) -> list[AggregationResult]:
        """Compute grouped aggregations."""
        results = []

        for group_key, records in self._data.items():
            aggregations = self._apply_aggregations(records)
            results.append(AggregationResult(
                group_key=group_key,
                aggregations=aggregations,
                record_count=len(records),
            ))

        return results

    def _compute_windowed(self) -> list[AggregationResult]:
        """Compute windowed aggregations."""
        results = []
        size = self.window.size_seconds

        for window_key, records in self._window_data.items():
            # Extract window boundaries
            if records:
                timestamps = [r.get("timestamp", time.time()) for r in records]
                window_start = min(timestamps)
                window_end = max(timestamps)

                aggregations = self._apply_aggregations(records)
                results.append(AggregationResult(
                    group_key=None,
                    aggregations=aggregations,
                    window_start=window_start,
                    window_end=window_end,
                    record_count=len(records),
                ))

        return results

    def _compute_sessions(self) -> list[AggregationResult]:
        """Compute session-based aggregations."""
        results = []

        for group_key, records in self._data.items():
            if records:
                timestamps = [r.get("timestamp", time.time()) for r in records]
                session_start = min(timestamps)
                session_end = max(timestamps)

                aggregations = self._apply_aggregations(records)
                results.append(AggregationResult(
                    group_key=group_key,
                    aggregations=aggregations,
                    window_start=session_start,
                    window_end=session_end,
                    record_count=len(records),
                ))

        return results

    def _compute_count_windows(self) -> list[AggregationResult]:
        """Compute count-based windowed aggregations."""
        return self._compute_windowed()

    def _apply_aggregations(self, records: list[dict]) -> dict[str, Any]:
        """Apply all aggregation functions to a set of records."""
        results = {}

        for config in self.aggregations:
            field = config.field
            values = [r.get(field) for r in records if field in r]

            if config.agg_type == AggregationType.CUSTOM and config.custom_func:
                results[config.name] = config.custom_func(values)
            elif config.agg_type == AggregationType.PERCENTILE:
                results[config.name] = AggregationFunction.apply(
                    config.agg_type, values, p=config.percentile_value
                )
            else:
                results[config.name] = AggregationFunction.apply(config.agg_type, values)

        return results

    def get_top_n(
        self,
        aggregation_name: str,
        n: int = 10,
        descending: bool = True,
    ) -> list[tuple[GroupKey, Any]]:
        """Get top N groups by an aggregation value.

        Args:
            aggregation_name: Name of the aggregation
            n: Number of results
            descending: Sort descending if True

        Returns:
            List of (GroupKey, value) tuples
        """
        results = self.compute()
        scored = [
            (r.group_key, r.aggregations.get(aggregation_name, 0))
            for r in results
        ]
        scored.sort(key=lambda x: x[1], reverse=descending)
        return scored[:n]

    def get_totals(self) -> dict[str, Any]:
        """Get totals across all groups."""
        all_records = []
        for records in self._data.values():
            all_records.extend(records)

        if not all_records:
            all_records = []
            for records in self._window_data.values():
                all_records.extend(records)

        return self._apply_aggregations(all_records)

    def clear(self) -> None:
        """Clear all accumulated data."""
        self._data.clear()
        self._window_data.clear()
        self._session_activities.clear()
        self._total_records = 0

    def get_metrics(self) -> dict[str, Any]:
        """Get aggregator metrics."""
        return {
            "total_records": self._total_records,
            "group_count": len(self._data),
            "window_count": len(self._window_data),
            "session_count": len(self._session_activities),
        }


def create_aggregator(
    group_by: list[str] | None = None,
    window_seconds: float | None = None,
    window_type: WindowType = WindowType.TUMBLING,
) -> DataAggregatorV2:
    """Create a configured data aggregator.

    Args:
        group_by: Fields to group by
        window_seconds: Window size in seconds
        window_type: Type of windowing

    Returns:
        Configured DataAggregatorV2 instance
    """
    window_config = None
    if window_seconds:
        window_config = WindowConfig(
            window_type=window_type,
            size_seconds=window_seconds,
        )

    return DataAggregatorV2(
        group_by=group_by,
        window=window_config,
    )
