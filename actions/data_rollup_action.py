"""
Data Rollup Action Module.

Rolls up data into aggregated summaries at different time granularities:
minute, hour, day, week, month. Supports custom aggregation functions.
"""
from typing import Any, Optional, Callable
from dataclasses import dataclass
from actions.base_action import BaseAction


@dataclass
class RollupConfig:
    """Configuration for rollup operation."""
    timestamp_field: str
    value_fields: list[str]
    granularity: str  # minute, hour, day, week, month
    agg_func: str = "sum"


@dataclass
class RollupResult:
    """Result of rollup operation."""
    records: list[dict[str, Any]]
    original_count: int
    rolled_up_count: int
    granularity: str


class DataRollupAction(BaseAction):
    """Aggregate data into time-based summaries."""

    def __init__(self) -> None:
        super().__init__("data_rollup")

    def execute(self, context: dict, params: dict) -> RollupResult:
        """
        Roll up data records.

        Args:
            context: Execution context
            params: Parameters:
                - records: List of timestamped records
                - timestamp_field: Name of timestamp field
                - value_fields: List of numeric fields to aggregate
                - granularity: minute, hour, day, week, month
                - agg_func: sum, count, mean, min, max

        Returns:
            RollupResult with aggregated records
        """
        records = params.get("records", [])
        timestamp_field = params.get("timestamp_field", "timestamp")
        value_fields = params.get("value_fields", [])
        granularity = params.get("granularity", "day")
        agg_func = params.get("agg_func", "sum")

        if not records:
            return RollupResult([], 0, 0, granularity)

        from collections import defaultdict
        groups: dict[tuple, dict[str, list]] = defaultdict(lambda: defaultdict(list))

        for record in records:
            if timestamp_field not in record:
                continue
            ts = record[timestamp_field]
            bucket = self._get_time_bucket(ts, granularity)
            for field in value_fields:
                if field in record:
                    groups[bucket][field].append(record[field])

        agg_funcs = {
            "sum": sum,
            "count": len,
            "mean": lambda x: sum(x) / len(x) if x else None,
            "min": min,
            "max": max
        }
        agg = agg_funcs.get(agg_func, sum)

        rolled_records = []
        for bucket, field_values in sorted(groups.items()):
            rec: dict[str, Any] = {"_bucket": bucket}
            for field, values in field_values.items():
                rec[f"{field}_{agg_func}"] = agg(values)
                rec[f"{field}_count"] = len(values)
            rolled_records.append(rec)

        return RollupResult(
            records=rolled_records,
            original_count=len(records),
            rolled_up_count=len(rolled_records),
            granularity=granularity
        )

    def _get_time_bucket(self, timestamp: Any, granularity: str) -> str:
        """Convert timestamp to time bucket string."""
        import time

        if isinstance(timestamp, (int, float)):
            ts = timestamp
        elif isinstance(timestamp, str):
            try:
                ts = time.mktime(time.strptime(timestamp, "%Y-%m-%d %H:%M:%S"))
            except Exception:
                ts = time.time()
        else:
            ts = time.time()

        dt = time.localtime(ts)

        if granularity == "minute":
            return time.strftime("%Y-%m-%d %H:%M", dt)
        elif granularity == "hour":
            return time.strftime("%Y-%m-%d %H:00", dt)
        elif granularity == "day":
            return time.strftime("%Y-%m-%d", dt)
        elif granularity == "week":
            return time.strftime("%Y-W%U", dt)
        elif granularity == "month":
            return time.strftime("%Y-%m", dt)
        return time.strftime("%Y-%m-%d", dt)
