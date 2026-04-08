"""Data Aggregator Action Module.

Provides multi-dimensional data aggregation with grouping,
pivot tables, time-windowing, and rolling statistics.
"""
from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple, TypeVar
from collections import defaultdict
import logging

T = TypeVar("T")

logger = logging.getLogger(__name__)


class AggregationType(Enum):
    """Aggregation type."""
    SUM = "sum"
    COUNT = "count"
    AVG = "avg"
    MIN = "min"
    MAX = "max"
    FIRST = "first"
    LAST = "last"
    MEDIAN = "median"
    STDDEV = "stddev"
    DISTINCT = "distinct"
    LIST = "list"


@dataclass
class AggregationConfig:
    """Aggregation configuration."""
    field: str
    agg_type: AggregationType
    alias: Optional[str] = None


@dataclass
class GroupConfig:
    """Grouping configuration."""
    group_by: List[str]
    aggregations: List[AggregationConfig]
    having: Optional[Callable[[Dict], bool]] = None
    order_by: Optional[List[Tuple[str, bool]]] = None


@dataclass
class PivotConfig:
    """Pivot table configuration."""
    index: List[str]
    columns: str
    values: str
    agg_func: AggregationType = AggregationType.SUM
    fill_value: Any = 0


@dataclass
class TimeWindowConfig:
    """Time window configuration."""
    field: str
    window_size: timedelta
    window_type: str = "tumbling"
    slide_interval: Optional[timedelta] = None


class DataAggregatorAction:
    """Data aggregator with grouping and pivoting.

    Example:
        aggregator = DataAggregatorAction()

        result = aggregator.aggregate(
            data=[
                {"region": "US", "product": "A", "sales": 100},
                {"region": "EU", "product": "A", "sales": 200},
                {"region": "US", "product": "B", "sales": 150},
            ],
            group_by=["region"],
            aggregations=[
                AggregationConfig("sales", AggregationType.SUM, "total_sales"),
                AggregationConfig("sales", AggregationType.AVG, "avg_sales"),
            ]
        )
    """

    def __init(self) -> None:
        self._precomputed: Dict[str, Any] = {}

    def aggregate(
        self,
        data: List[Dict[str, Any]],
        group_by: List[str],
        aggregations: List[AggregationConfig],
        having: Optional[Callable[[Dict], bool]] = None,
        order_by: Optional[List[Tuple[str, bool]]] = None,
    ) -> List[Dict[str, Any]]:
        """Aggregate data with grouping.

        Args:
            data: List of data records
            group_by: Fields to group by
            aggregations: List of aggregation configurations
            having: Optional filter applied after aggregation
            order_by: Optional sort specification [(field, ascending)]

        Returns:
            List of aggregated results
        """
        if not data or not group_by:
            return []

        grouped: Dict[Tuple, List[Dict]] = defaultdict(list)

        for record in data:
            key = tuple(record.get(f) for f in group_by)
            grouped[key].append(record)

        results: List[Dict[str, Any]] = []

        for key, group_data in grouped.items():
            result = dict(zip(group_by, key))

            for agg in aggregations:
                values = [r.get(agg.field) for r in group_data if agg.get(agg.field) is not None]
                result[agg.alias or f"{agg.field}_{agg.agg_type.value}"] = self._compute_aggregation(
                    values, agg.agg_type
                )

            if having is None or having(result):
                results.append(result)

        if order_by:
            results = self._sort_results(results, order_by)

        return results

    def _compute_aggregation(
        self,
        values: List[Any],
        agg_type: AggregationType,
    ) -> Any:
        """Compute single aggregation."""
        if not values:
            return None

        if agg_type == AggregationType.SUM:
            return sum(values)
        elif agg_type == AggregationType.COUNT:
            return len(values)
        elif agg_type == AggregationType.AVG:
            return sum(values) / len(values)
        elif agg_type == AggregationType.MIN:
            return min(values)
        elif agg_type == AggregationType.MAX:
            return max(values)
        elif agg_type == AggregationType.FIRST:
            return values[0]
        elif agg_type == AggregationType.LAST:
            return values[-1]
        elif agg_type == AggregationType.MEDIAN:
            return self._median(values)
        elif agg_type == AggregationType.STDDEV:
            return self._stddev(values)
        elif agg_type == AggregationType.DISTINCT:
            return len(set(values))
        elif agg_type == AggregationType.LIST:
            return list(values)

        return values

    def _median(self, values: List[float]) -> float:
        """Calculate median."""
        sorted_vals = sorted(values)
        n = len(sorted_vals)
        mid = n // 2
        if n % 2 == 0:
            return (sorted_vals[mid - 1] + sorted_vals[mid]) / 2
        return sorted_vals[mid]

    def _stddev(self, values: List[float]) -> float:
        """Calculate standard deviation."""
        if len(values) < 2:
            return 0.0
        mean = sum(values) / len(values)
        variance = sum((x - mean) ** 2 for x in values) / len(values)
        return variance ** 0.5

    def _sort_results(
        self,
        results: List[Dict[str, Any]],
        order_by: List[Tuple[str, bool]],
    ) -> List[Dict[str, Any]]:
        """Sort results by order_by specification."""
        def sort_key(item: Dict) -> Tuple:
            return tuple(item.get(field, None) for field, _ in order_by)

        reverse_flags = [not asc for _, asc in order_by]
        return sorted(results, key=sort_key, reverse=any(reverse_flags))

    def pivot(
        self,
        data: List[Dict[str, Any]],
        index: List[str],
        columns: str,
        values: str,
        agg_func: AggregationType = AggregationType.SUM,
        fill_value: Any = 0,
    ) -> List[Dict[str, Any]]:
        """Create pivot table from data.

        Args:
            data: List of data records
            index: Fields to use as index
            columns: Field to use as columns
            values: Field to aggregate
            agg_func: Aggregation function
            fill_value: Value to fill missing entries

        Returns:
            Pivot table results
        """
        if not data:
            return []

        grouped: Dict[Tuple, List[Any]] = defaultdict(list)

        for record in data:
            key = tuple(record.get(f) for f in index)
            grouped[key].append(record.get(values))

        pivot_map: Dict[Tuple, Dict[str, Any]] = {}

        for key, values_list in grouped.items():
            result = dict(zip(index, key))
            pivot_map[key] = result

        all_columns = sorted(set(record.get(columns) for record in data if record.get(columns)))

        for record in data:
            key = tuple(record.get(f) for f in index)
            col_val = record.get(columns)
            val = record.get(values)

            if col_val:
                pivot_map[key][col_val] = val

        results = list(pivot_map.values())

        for result in results:
            for col in all_columns:
                if col not in result:
                    result[col] = fill_value

        return results

    def time_window_aggregate(
        self,
        data: List[Dict[str, Any]],
        time_field: str,
        window_size: timedelta,
        aggregations: List[AggregationConfig],
        slide_interval: Optional[timedelta] = None,
    ) -> List[Dict[str, Any]]:
        """Aggregate data over time windows.

        Args:
            data: List of data records with timestamp field
            time_field: Name of timestamp field
            window_size: Size of each window
            aggregations: Aggregations to compute
            slide_interval: Optional sliding window interval

        Returns:
            List of windowed aggregations
        """
        if not data:
            return []

        sorted_data = sorted(
            data,
            key=lambda x: x.get(time_field, datetime.min)
        )

        timestamps = [r.get(time_field) for r in sorted_data if r.get(time_field)]
        if not timestamps:
            return []

        min_time = min(timestamps)
        max_time = max(timestamps)

        windows: List[Dict[str, Any]] = []
        current = min_time

        while current <= max_time:
            window_end = current + window_size

            window_data = [
                r for r in sorted_data
                if current <= r.get(time_field, datetime.min) < window_end
            ]

            if window_data:
                result: Dict[str, Any] = {
                    "window_start": current,
                    "window_end": window_end,
                }

                for agg in aggregations:
                    values = [r.get(agg.field) for r in window_data if r.get(agg.field) is not None]
                    result[agg.alias or agg.field] = self._compute_aggregation(values, agg.agg_type)

                windows.append(result)

            current = window_end if slide_interval is None else current + slide_interval

        return windows

    def compute_rolling_stats(
        self,
        data: List[Dict[str, Any]],
        value_field: str,
        window_size: int,
        fields: Optional[List[str]] = None,
    ) -> List[Dict[str, Any]]:
        """Compute rolling statistics.

        Args:
            data: List of data records
            value_field: Field to compute stats on
            window_size: Size of rolling window
            fields: Additional fields to include

        Returns:
            Records with rolling stats added
        """
        results: List[Dict[str, Any]] = []
        values: List[float] = []

        for i, record in enumerate(data):
            value = record.get(value_field, 0)
            values.append(value if value is not None else 0)

            if len(values) > window_size:
                values.pop(0)

            result = dict(record)
            if fields:
                for f in fields:
                    result[f"rolling_{f}_window{window_size}"] = record.get(f)

            if len(values) >= 2:
                result[f"rolling_avg_{window_size}"] = sum(values) / len(values)
                result[f"rolling_stddev_{window_size}"] = self._stddev(values)
                result[f"rolling_min_{window_size}"] = min(values)
                result[f"rolling_max_{window_size}"] = max(values)
            else:
                result[f"rolling_avg_{window_size}"] = values[0] if values else None

            results.append(result)

        return results
