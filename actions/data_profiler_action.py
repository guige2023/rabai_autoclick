"""
Data Profiler Action Module.

Profiles datasets: computes statistics, detects types, finds patterns,
identifies missing values, and generates data quality reports.
"""
from typing import Any, Optional
from dataclasses import dataclass, field
from actions.base_action import BaseAction


@dataclass
class DataProfile:
    """Data profile report."""
    row_count: int
    column_count: int
    columns: dict[str, dict[str, Any]]
    missing_values: dict[str, int]
    duplicate_rows: int
    memory_bytes: int


class DataProfilerAction(BaseAction):
    """Profile a dataset and generate statistics."""

    def __init__(self) -> None:
        super().__init__("data_profiler")

    def execute(self, context: dict, params: dict) -> DataProfile:
        """
        Profile data records.

        Args:
            context: Execution context
            params: Parameters:
                - records: List of dict records to profile
                - sample_size: Max records to profile (default: all)

        Returns:
            DataProfile with statistics for each column
        """
        import sys

        records = params.get("records", [])
        sample_size = params.get("sample_size", len(records))

        if not records:
            return DataProfile(
                row_count=0,
                column_count=0,
                columns={},
                missing_values={},
                duplicate_rows=0,
                memory_bytes=0
            )

        sample = records[:sample_size]
        columns: dict[str, dict[str, Any]] = {}
        missing_values: dict[str, int] = {}
        seen_rows = set()
        duplicate_rows = 0

        all_keys: set[str] = set()
        for r in sample:
            if isinstance(r, dict):
                all_keys.update(r.keys())

        for key in all_keys:
            values = []
            for r in sample:
                if isinstance(r, dict):
                    values.append(r.get(key))
                else:
                    values.append(None)

            non_null = [v for v in values if v is not None]
            missing_count = len(values) - len(non_null)
            missing_values[key] = missing_count

            col_type = self._infer_type(non_null)
            stats: dict[str, Any] = {"type": col_type, "null_count": missing_count}

            if col_type == "numeric" and non_null:
                numeric_vals = [v for v in non_null if isinstance(v, (int, float))]
                if numeric_vals:
                    stats["min"] = min(numeric_vals)
                    stats["max"] = max(numeric_vals)
                    stats["mean"] = sum(numeric_vals) / len(numeric_vals)
                    stats["sum"] = sum(numeric_vals)
            elif col_type == "string" and non_null:
                str_vals = [str(v) for v in non_null]
                stats["min_length"] = min(len(s) for s in str_vals)
                stats["max_length"] = max(len(s) for s in str_vals)
                stats["unique_count"] = len(set(str_vals))

            columns[key] = stats

        row_bytes = sys.getsizeof(str(sample))
        return DataProfile(
            row_count=len(records),
            column_count=len(columns),
            columns=columns,
            missing_values=missing_values,
            duplicate_rows=duplicate_rows,
            memory_bytes=row_bytes
        )

    def _infer_type(self, values: list) -> str:
        """Infer the type of a list of non-null values."""
        if not values:
            return "unknown"
        sample = values[:100]
        numeric_count = sum(1 for v in sample if isinstance(v, (int, float)))
        if numeric_count / len(sample) > 0.8:
            return "numeric"
        string_count = sum(1 for v in sample if isinstance(v, str))
        if string_count / len(sample) > 0.8:
            return "string"
        bool_count = sum(1 for v in sample if isinstance(v, bool))
        if bool_count / len(sample) > 0.8:
            return "boolean"
        return "mixed"
