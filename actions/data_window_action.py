"""
Data Window Action Module.

Computes sliding and tumbling window aggregations over data streams:
moving averages, cumulative sums, windowed counts, and sessionization.
"""
from typing import Any, Optional
from dataclasses import dataclass
from actions.base_action import BaseAction


@dataclass
class WindowResult:
    """Result of window aggregation."""
    records: list[dict[str, Any]]
    windows_computed: int
    window_size: int


class DataWindowAction(BaseAction):
    """Compute windowed aggregations over data."""

    def __init__(self) -> None:
        super().__init__("data_window")

    def execute(self, context: dict, params: dict) -> dict:
        """
        Compute windowed aggregations.

        Args:
            context: Execution context
            params: Parameters:
                - records: List of timestamped records
                - field: Field to aggregate
                - timestamp_field: Field containing timestamps
                - window_type: sliding or tumbling
                - window_size: Window size in seconds or records
                - window_unit: seconds, minutes, hours, records
                - agg_func: sum, mean, count, min, max
                - slide_by: Slide interval for sliding windows

        Returns:
            WindowResult with windowed records
        """
        import time

        records = params.get("records", [])
        field = params.get("field", "")
        timestamp_field = params.get("timestamp_field", "timestamp")
        window_type = params.get("window_type", "tumbling")
        window_size = params.get("window_size", 60)
        window_unit = params.get("window_unit", "seconds")
        agg_func = params.get("agg_func", "sum")
        slide_by = params.get("slide_by", window_size)

        if not records:
            return WindowResult([], 0, window_size)

        if window_unit == "records":
            return self._window_by_records(records, field, window_size, agg_func)

        sorted_records = sorted(records, key=lambda r: r.get(timestamp_field, 0) if isinstance(r, dict) else 0)

        if window_type == "tumbling":
            return self._tumbling_window(sorted_records, field, timestamp_field, window_size, window_unit, agg_func)
        else:
            return self._sliding_window(sorted_records, field, timestamp_field, window_size, window_unit, slide_by, agg_func)

    def _window_by_records(self, records: list[dict], field: str, window_size: int, agg_func: str) -> WindowResult:
        """Window by number of records."""
        agg_funcs = {"sum": sum, "mean": lambda x: sum(x) / len(x) if x else 0, "count": len, "min": min, "max": max}
        agg = agg_funcs.get(agg_func, sum)

        results = []
        for i in range(0, len(records), window_size):
            window = records[i:i + window_size]
            values = [r.get(field, 0) for r in window if isinstance(r.get(field), (int, float))]
            if values:
                window_record = {
                    f"{field}_{agg_func}": agg(values),
                    f"{field}_count": len(values),
                    "window_start": i,
                    "window_end": i + len(window)
                }
                results.append(window_record)
        return WindowResult(results, len(results), window_size)

    def _tumbling_window(self, records: list[dict], field: str, timestamp_field: str, window_size: int, window_unit: str, agg_func: str) -> WindowResult:
        """Compute tumbling windows."""
        agg_funcs = {"sum": sum, "mean": lambda x: sum(x) / len(x) if x else 0, "count": len, "min": min, "max": max}
        agg = agg_funcs.get(agg_func, sum)

        multiplier = {"seconds": 1, "minutes": 60, "hours": 3600}.get(window_unit, 1)
        window_sec = window_size * multiplier

        results = []
        if not records:
            return WindowResult([], 0, window_size)

        start_time = records[0].get(timestamp_field, 0) if isinstance(records[0], dict) else 0
        window_start = start_time
        window_data = []

        for r in records:
            if not isinstance(r, dict):
                continue
            ts = r.get(timestamp_field, 0)
            while ts >= window_start + window_sec:
                if window_data:
                    values = [d.get(field, 0) for d in window_data if isinstance(d.get(field), (int, float))]
                    if values:
                        results.append({
                            f"{field}_{agg_func}": agg(values),
                            "window_start": window_start,
                            "window_end": window_start + window_sec,
                            "record_count": len(window_data)
                        })
                window_start += window_sec
                window_data = []
            window_data.append(r)

        if window_data:
            values = [d.get(field, 0) for d in window_data if isinstance(d.get(field), (int, float))]
            if values:
                results.append({
                    f"{field}_{agg_func}": agg(values),
                    "window_start": window_start,
                    "window_end": window_start + window_sec,
                    "record_count": len(window_data)
                })

        return WindowResult(results, len(results), window_size)

    def _sliding_window(self, records: list[dict], field: str, timestamp_field: str, window_size: int, window_unit: str, slide_by: int, agg_func: str) -> WindowResult:
        """Compute sliding windows."""
        agg_funcs = {"sum": sum, "mean": lambda x: sum(x) / len(x) if x else 0, "count": len, "min": min, "max": max}
        agg = agg_funcs.get(agg_func, sum)

        multiplier = {"seconds": 1, "minutes": 60, "hours": 3600}.get(window_unit, 1)
        window_sec = window_size * multiplier
        slide_sec = slide_by * multiplier

        results = []
        if not records:
            return WindowResult([], 0, window_size)

        start_time = records[0].get(timestamp_field, 0) if isinstance(records[0], dict) else 0
        window_start = start_time

        while window_start < (records[-1].get(timestamp_field, 0) if isinstance(records[-1], dict) else 0):
            window_end = window_start + window_sec
            window_records = [r for r in records if isinstance(r, dict) and window_start <= r.get(timestamp_field, 0) < window_end]
            values = [r.get(field, 0) for r in window_records if isinstance(r.get(field), (int, float))]
            if values:
                results.append({
                    f"{field}_{agg_func}": agg(values),
                    "window_start": window_start,
                    "window_end": window_end,
                    "record_count": len(window_records)
                })
            window_start += slide_sec

        return WindowResult(results, len(results), window_size)
