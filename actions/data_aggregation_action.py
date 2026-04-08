"""Data Aggregation Action Module.

Provides data aggregation, grouping, and statistical analysis
capabilities for processing collections of data.
"""

from typing import Any, Dict, List, Optional, Callable, Generic, TypeVar, Tuple
from dataclasses import dataclass, field
from enum import Enum
from datetime import datetime, timedelta
import math


T = TypeVar("T")
K = TypeVar("K")
V = TypeVar("V")


class AggregationType(Enum):
    """Types of aggregation operations."""
    SUM = "sum"
    AVG = "avg"
    MIN = "min"
    MAX = "max"
    COUNT = "count"
    COUNT_DISTINCT = "count_distinct"
    FIRST = "first"
    LAST = "last"
    MEDIAN = "median"
    STD_DEV = "std_dev"
    PERCENTILE = "percentile"
    HISTOGRAM = "histogram"


@dataclass
class AggregationConfig:
    """Configuration for aggregation operation."""
    group_by: Optional[List[str]] = None
    aggregations: Dict[str, List[AggregationType]] = field(default_factory=dict)
    having: Optional[Callable[[Dict[str, Any]], bool]] = None
    order_by: Optional[List[Tuple[str, str]]] = None
    limit: Optional[int] = None


@dataclass
class AggregationResult:
    """Result of an aggregation operation."""
    groups: List[Dict[str, Any]]
    total_groups: int
    total_records: int
    computation_time_ms: float
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class TimeSeriesPoint:
    """Single point in a time series."""
    timestamp: datetime
    value: float
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class TimeSeriesAggregation:
    """Time series aggregation result."""
    points: List[TimeSeriesPoint]
    interval: str
    start_time: datetime
    end_time: datetime
    stats: Dict[str, float] = field(default_factory=dict)


class DataAggregator:
    """Performs data aggregation operations."""

    def __init__(self):
        self._custom_aggregators: Dict[str, Callable] = {}

    def register_aggregator(self, name: str, func: Callable):
        """Register a custom aggregation function."""
        self._custom_aggregators[name] = func

    def aggregate(
        self,
        data: List[Dict[str, Any]],
        config: AggregationConfig,
    ) -> AggregationResult:
        """Perform aggregation on data."""
        start_time = datetime.now()

        if not data:
            return AggregationResult(
                groups=[],
                total_groups=0,
                total_records=0,
                computation_time_ms=0,
            )

        groups = self._group_data(data, config.group_by or [])

        for group_key, group_data in groups.items():
            group_result = {}
            for field_name, agg_types in config.aggregations.items():
                field_values = [row.get(field_name) for row in group_data]
                for agg_type in agg_types:
                    result = self._apply_aggregation(field_values, agg_type)
                    group_result[f"{field_name}_{agg_type.value}"] = result
            groups[group_key] = group_result

        result_groups = list(groups.values())

        if config.having:
            result_groups = [g for g in result_groups if config.having(g)]

        if config.order_by:
            result_groups = self._sort_groups(result_groups, config.order_by)

        if config.limit:
            result_groups = result_groups[:config.limit]

        computation_time = (datetime.now() - start_time).total_seconds() * 1000

        return AggregationResult(
            groups=result_groups,
            total_groups=len(result_groups),
            total_records=len(data),
            computation_time_ms=computation_time,
            metadata={"group_by": config.group_by},
        )

    def _group_data(
        self,
        data: List[Dict[str, Any]],
        group_by: List[str],
    ) -> Dict[Tuple, List[Dict[str, Any]]]:
        """Group data by specified fields."""
        if not group_by:
            return {(): data}

        groups: Dict[Tuple, List[Dict[str, Any]]] = {}
        for row in data:
            key = tuple(row.get(field) for field in group_by)
            if key not in groups:
                groups[key] = []
            groups[key].append(row)
        return groups

    def _apply_aggregation(
        self,
        values: List[Any],
        agg_type: AggregationType,
    ) -> Any:
        """Apply aggregation function to values."""
        numeric_values = [v for v in values if v is not None and isinstance(v, (int, float))]
        if not numeric_values and agg_type != AggregationType.COUNT:
            return None

        if agg_type == AggregationType.SUM:
            return sum(numeric_values)
        elif agg_type == AggregationType.AVG:
            return sum(numeric_values) / len(numeric_values) if numeric_values else None
        elif agg_type == AggregationType.MIN:
            return min(numeric_values)
        elif agg_type == AggregationType.MAX:
            return max(numeric_values)
        elif agg_type == AggregationType.COUNT:
            return len(values)
        elif agg_type == AggregationType.COUNT_DISTINCT:
            return len(set(values))
        elif agg_type == AggregationType.FIRST:
            return values[0] if values else None
        elif agg_type == AggregationType.LAST:
            return values[-1] if values else None
        elif agg_type == AggregationType.MEDIAN:
            return self._calculate_median(numeric_values)
        elif agg_type == AggregationType.STD_DEV:
            return self._calculate_std_dev(numeric_values)
        elif agg_type == AggregationType.HISTOGRAM:
            return self._calculate_histogram(numeric_values)
        elif agg_type.value in self._custom_aggregators:
            return self._custom_aggregators[agg_type.value](values)

        return None

    def _calculate_median(self, values: List[float]) -> float:
        """Calculate median value."""
        if not values:
            return 0
        sorted_values = sorted(values)
        n = len(sorted_values)
        mid = n // 2
        if n % 2 == 0:
            return (sorted_values[mid - 1] + sorted_values[mid]) / 2
        return sorted_values[mid]

    def _calculate_std_dev(self, values: List[float]) -> float:
        """Calculate standard deviation."""
        if not values:
            return 0
        mean = sum(values) / len(values)
        variance = sum((x - mean) ** 2 for x in values) / len(values)
        return math.sqrt(variance)

    def _calculate_histogram(
        self, values: List[float], bins: int = 10
    ) -> Dict[str, int]:
        """Calculate histogram distribution."""
        if not values:
            return {}
        min_val, max_val = min(values), max(values)
        if min_val == max_val:
            return {f"{min_val}": len(values)}
        bin_width = (max_val - min_val) / bins
        histogram = {}
        for i in range(bins):
            bin_start = min_val + i * bin_width
            bin_end = bin_start + bin_width
            bin_name = f"{bin_start:.2f}-{bin_end:.2f}"
            histogram[bin_name] = 0
        for v in values:
            bin_idx = min(int((v - min_val) / bin_width), bins - 1)
            bin_start = min_val + bin_idx * bin_width
            bin_end = bin_start + bin_width
            histogram[f"{bin_start:.2f}-{bin_end:.2f}"] += 1
        return histogram

    def _sort_groups(
        self,
        groups: List[Dict[str, Any]],
        order_by: List[Tuple[str, str]],
    ) -> List[Dict[str, Any]]:
        """Sort groups by specified fields."""
        def sort_key(group: Dict[str, Any]) -> Tuple:
            result = []
            for field_name, direction in order_by:
                value = group.get(field_name, 0)
                if direction.upper() == "DESC":
                    value = -value if isinstance(value, (int, float)) else value
                result.append(value)
            return tuple(result)
        return sorted(groups, key=sort_key)


class TimeSeriesAggregator:
    """Aggregates time series data."""

    INTERVALS = {
        "1m": timedelta(minutes=1),
        "5m": timedelta(minutes=5),
        "15m": timedelta(minutes=15),
        "30m": timedelta(minutes=30),
        "1h": timedelta(hours=1),
        "6h": timedelta(hours=6),
        "12h": timedelta(hours=12),
        "1d": timedelta(days=1),
        "1w": timedelta(weeks=1),
    }

    def __init__(self):
        self._resample_cache: Dict[str, List[TimeSeriesPoint]] = {}

    def aggregate_time_series(
        self,
        data: List[TimeSeriesPoint],
        interval: str,
        aggregation: AggregationType = AggregationType.AVG,
    ) -> TimeSeriesAggregation:
        """Aggregate time series data by interval."""
        if not data:
            return TimeSeriesAggregation(
                points=[],
                interval=interval,
                start_time=datetime.now(),
                end_time=datetime.now(),
            )

        start_time = min(p.timestamp for p in data)
        end_time = max(p.timestamp for p in data)
        delta = self.INTERVALS.get(interval, timedelta(hours=1))

        buckets: Dict[datetime, List[float]] = {}
        for point in data:
            bucket_time = self._truncate_to_interval(point.timestamp, delta)
            if bucket_time not in buckets:
                buckets[bucket_time] = []
            buckets[bucket_time].append(point.value)

        aggregated_points = []
        for bucket_time in sorted(buckets.keys()):
            values = buckets[bucket_time]
            agg_value = self._aggregate_values(values, aggregation)
            aggregated_points.append(TimeSeriesPoint(
                timestamp=bucket_time,
                value=agg_value,
            ))

        all_values = [p.value for p in data]
        stats = self._calculate_stats(all_values)

        return TimeSeriesAggregation(
            points=aggregated_points,
            interval=interval,
            start_time=start_time,
            end_time=end_time,
            stats=stats,
        )

    def _truncate_to_interval(self, dt: datetime, delta: timedelta) -> datetime:
        """Truncate datetime to interval boundary."""
        epoch = datetime(1970, 1, 1)
        total_seconds = int((dt - epoch).total_seconds() / delta.total_seconds()) * delta.total_seconds()
        return epoch + timedelta(seconds=total_seconds)

    def _aggregate_values(
        self, values: List[float], aggregation: AggregationType
    ) -> float:
        """Aggregate list of values."""
        if not values:
            return 0
        if aggregation == AggregationType.SUM:
            return sum(values)
        elif aggregation == AggregationType.AVG:
            return sum(values) / len(values)
        elif aggregation == AggregationType.MIN:
            return min(values)
        elif aggregation == AggregationType.MAX:
            return max(values)
        elif aggregation == AggregationType.COUNT:
            return len(values)
        elif aggregation == AggregationType.LAST:
            return values[-1]
        return sum(values) / len(values)

    def _calculate_stats(self, values: List[float]) -> Dict[str, float]:
        """Calculate statistical summary."""
        if not values:
            return {}
        return {
            "min": min(values),
            "max": max(values),
            "mean": sum(values) / len(values),
            "count": len(values),
            "sum": sum(values),
        }


class DataAggregationAction:
    """High-level data aggregation action."""

    def __init__(
        self,
        aggregator: Optional[DataAggregator] = None,
        ts_aggregator: Optional[TimeSeriesAggregator] = None,
    ):
        self.aggregator = aggregator or DataAggregator()
        self.ts_aggregator = ts_aggregator or TimeSeriesAggregator()

    def aggregate(
        self,
        data: List[Dict[str, Any]],
        group_by: Optional[List[str]] = None,
        aggregations: Optional[Dict[str, List[str]]] = None,
    ) -> AggregationResult:
        """Simple interface for data aggregation."""
        agg_config = AggregationConfig(
            group_by=group_by,
            aggregations={
                field: [AggregationType(a) for a in aggs]
                for field, aggs in (aggregations or {}).items()
            },
        )
        return self.aggregator.aggregate(data, agg_config)

    def aggregate_time_series(
        self,
        data: List[Dict[str, Any]],
        timestamp_field: str,
        value_field: str,
        interval: str,
        aggregation: str = "avg",
    ) -> TimeSeriesAggregation:
        """Aggregate time series data."""
        points = [
            TimeSeriesPoint(
                timestamp=datetime.fromisoformat(row[timestamp_field]),
                value=float(row[value_field]),
            )
            for row in data
            if timestamp_field in row and value_field in row
        ]
        agg_type = AggregationType(aggregation)
        return self.ts_aggregator.aggregate_time_series(points, interval, agg_type)


# Module exports
__all__ = [
    "DataAggregationAction",
    "DataAggregator",
    "TimeSeriesAggregator",
    "AggregationConfig",
    "AggregationResult",
    "AggregationType",
    "TimeSeriesAggregation",
    "TimeSeriesPoint",
]
