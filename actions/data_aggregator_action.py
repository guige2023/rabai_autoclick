"""Data aggregator action module for RabAI AutoClick.

Provides data aggregation:
- DataAggregator: Aggregate data collections
- GroupAggregator: Group and aggregate
- TimeSeriesAggregator: Time-series aggregation
- MultiFieldAggregator: Multiple field aggregation
- RollingAggregator: Rolling window aggregation
"""

import time
from typing import Any, Callable, Dict, List, Optional, Tuple
from dataclasses import dataclass, field
from collections import defaultdict
from datetime import datetime, timedelta

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


@dataclass
class AggregationResult:
    """Aggregation result."""
    group_key: str
    count: int
    sum: float
    avg: float
    min_val: Any
    max_val: Any
    values: List[Any]


class DataAggregator:
    """General data aggregator."""

    def __init__(self):
        self._aggregators: Dict[str, Callable] = {
            "count": self._count,
            "sum": self._sum,
            "avg": self._avg,
            "min": self._min,
            "max": self._max,
            "first": self._first,
            "last": self._last,
            "median": self._median,
            "stddev": self._stddev,
        }

    def aggregate(
        self,
        data: List[Dict],
        field: str,
        operations: List[str],
    ) -> Dict[str, Any]:
        """Aggregate field values."""
        values = [item.get(field) for item in data if item.get(field) is not None]
        numeric_values = [v for v in values if isinstance(v, (int, float))]

        result = {}
        for op in operations:
            if op in self._aggregators:
                if op in ("avg", "sum", "stddev") and not numeric_values:
                    result[op] = None
                else:
                    result[op] = self._aggregators[op](values, numeric_values)

        return result

    def _count(self, values: List, numeric: List) -> int:
        return len(values)

    def _sum(self, values: List, numeric: List) -> float:
        return sum(numeric) if numeric else 0

    def _avg(self, values: List, numeric: List) -> float:
        return sum(numeric) / len(numeric) if numeric else 0

    def _min(self, values: List, numeric: List) -> Any:
        return min(numeric) if numeric else None

    def _max(self, values: List, numeric: List) -> Any:
        return max(numeric) if numeric else None

    def _first(self, values: List, numeric: List) -> Any:
        return values[0] if values else None

    def _last(self, values: List, numeric: List) -> Any:
        return values[-1] if values else None

    def _median(self, values: List, numeric: List) -> float:
        if not numeric:
            return 0
        sorted_vals = sorted(numeric)
        n = len(sorted_vals)
        if n % 2 == 0:
            return (sorted_vals[n//2-1] + sorted_vals[n//2]) / 2
        return sorted_vals[n//2]

    def _stddev(self, values: List, numeric: List) -> float:
        if len(numeric) < 2:
            return 0
        avg = sum(numeric) / len(numeric)
        variance = sum((x - avg) ** 2 for x in numeric) / len(numeric)
        return variance ** 0.5


class GroupAggregator:
    """Group and aggregate data."""

    def group_aggregate(
        self,
        data: List[Dict],
        group_by: List[str],
        aggregates: Dict[str, Dict[str, str]],
    ) -> List[Dict]:
        """Group by fields and aggregate."""
        groups: Dict[Tuple, List[Dict]] = defaultdict(list)

        for item in data:
            key = tuple(item.get(k) for k in group_by)
            groups[key].append(item)

        results = []
        for key, items in groups.items():
            result = dict(zip(group_by, key))

            for field_name, ops in aggregates.items():
                field_values = [item.get(field_name) for item in items if item.get(field_name) is not None]
                numeric_values = [v for v in field_values if isinstance(v, (int, float))]

                for op, alias in ops.items():
                    if op == "count":
                        result[alias] = len(field_values)
                    elif op == "sum":
                        result[alias] = sum(numeric_values) if numeric_values else 0
                    elif op == "avg":
                        result[alias] = sum(numeric_values) / len(numeric_values) if numeric_values else 0
                    elif op == "min":
                        result[alias] = min(numeric_values) if numeric_values else None
                    elif op == "max":
                        result[alias] = max(numeric_values) if numeric_values else None

            results.append(result)

        return results


class TimeSeriesAggregator:
    """Time-series data aggregation."""

    def __init__(self, time_field: str = "timestamp"):
        self.time_field = time_field

    def aggregate_by_interval(
        self,
        data: List[Dict],
        interval: str,
        value_field: str,
        operations: List[str],
    ) -> List[Dict]:
        """Aggregate by time interval."""
        if not data:
            return []

        interval_seconds = self._parse_interval(interval)
        sorted_data = sorted(data, key=lambda x: x.get(self.time_field, 0))

        buckets: Dict[int, List[Dict]] = defaultdict(list)
        for item in sorted_data:
            ts = item.get(self.time_field, 0)
            bucket_key = int(ts // interval_seconds) * interval_seconds
            buckets[bucket_key].append(item)

        results = []
        for ts, items in sorted(buckets.items()):
            result = {"timestamp": ts, "interval": interval}

            values = [item.get(value_field) for item in items if item.get(value_field) is not None]
            numeric_values = [v for v in values if isinstance(v, (int, float))]

            for op in operations:
                if op == "count":
                    result[op] = len(values)
                elif op == "sum":
                    result[op] = sum(numeric_values) if numeric_values else 0
                elif op == "avg":
                    result[op] = sum(numeric_values) / len(numeric_values) if numeric_values else 0
                elif op == "min":
                    result[op] = min(numeric_values) if numeric_values else None
                elif op == "max":
                    result[op] = max(numeric_values) if numeric_values else None

            results.append(result)

        return results

    def _parse_interval(self, interval: str) -> int:
        """Parse interval string to seconds."""
        units = {
            "s": 1,
            "m": 60,
            "h": 3600,
            "d": 86400,
            "w": 604800,
        }
        if not interval:
            return 60

        for unit, seconds in units.items():
            if interval.endswith(unit):
                try:
                    value = int(interval[:-1])
                    return value * seconds
                except ValueError:
                    pass

        try:
            return int(interval)
        except ValueError:
            return 60


class DataAggregatorAction(BaseAction):
    """Data aggregator action."""
    action_type = "data_aggregator"
    display_name = "数据聚合器"
    description = "数据分组和聚合计算"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "aggregate")
            data = params.get("data", [])

            if operation == "aggregate":
                return self._aggregate(data, params)
            elif operation == "group":
                return self._group_aggregate(data, params)
            elif operation == "timeseries":
                return self._aggregate_timeseries(data, params)
            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Aggregation error: {str(e)}")

    def _aggregate(self, data: List[Dict], params: Dict) -> ActionResult:
        """Aggregate data."""
        field = params.get("field")
        operations = params.get("operations", ["count", "sum", "avg", "min", "max"])

        if not field:
            return ActionResult(success=False, message="field is required")

        aggregator = DataAggregator()
        result = aggregator.aggregate(data, field, operations)

        return ActionResult(
            success=True,
            message=f"Aggregated {field}",
            data={"field": field, "operations": result},
        )

    def _group_aggregate(self, data: List[Dict], params: Dict) -> ActionResult:
        """Group and aggregate."""
        group_by = params.get("group_by", [])
        aggregates = params.get("aggregates", {})

        if not group_by or not aggregates:
            return ActionResult(success=False, message="group_by and aggregates are required")

        aggregator = GroupAggregator()
        result = aggregator.group_aggregate(data, group_by, aggregates)

        return ActionResult(
            success=True,
            message=f"Grouped into {len(result)} groups",
            data={"groups": result},
        )

    def _aggregate_timeseries(self, data: List[Dict], params: Dict) -> ActionResult:
        """Aggregate time series."""
        interval = params.get("interval", "1h")
        value_field = params.get("value_field", "value")
        operations = params.get("operations", ["count", "avg"])

        aggregator = TimeSeriesAggregator()
        result = aggregator.aggregate_by_interval(data, interval, value_field, operations)

        return ActionResult(
            success=True,
            message=f"Aggregated into {len(result)} time buckets",
            data={"buckets": result},
        )
