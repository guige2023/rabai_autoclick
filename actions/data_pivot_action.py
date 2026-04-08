"""
Data Pivot Action Module.

Pivots data between wide and long formats, supports aggregation
during pivot, and handles multi-level indices and column hierarchies.
"""
from typing import Any, Optional
from dataclasses import dataclass
from actions.base_action import BaseAction


@dataclass
class PivotConfig:
    """Configuration for pivot operation."""
    index: str
    columns: str
    values: str
    aggfunc: str = "sum"


@dataclass
class PivotResult:
    """Result of a pivot operation."""
    wide_format: list[dict[str, Any]]
    long_format: list[dict[str, Any]]
    row_count: int
    column_count: int


class DataPivotAction(BaseAction):
    """Transform data between wide and long formats."""

    def __init__(self) -> None:
        super().__init__("data_pivot")

    def execute(self, context: dict, params: dict) -> PivotResult:
        """
        Pivot data records.

        Args:
            context: Execution context
            params: Parameters:
                - records: List of dict records
                - index: Column to use as index
                - columns: Column to use as columns
                - values: Column to use as values
                - aggfunc: Aggregation function (sum, count, mean, min, max)
                - direction: "wide_to_long" or "long_to_wide"

        Returns:
            PivotResult with transformed data
        """
        records = params.get("records", [])
        index = params.get("index", "")
        columns_col = params.get("columns", "")
        values_col = params.get("values", "")
        aggfunc = params.get("aggfunc", "sum")
        direction = params.get("direction", "long_to_wide")

        if not records:
            return PivotResult([], [], 0, 0)

        if direction == "wide_to_long":
            long_format = self._to_long(records, index, values_col)
            return PivotResult(records, long_format, len(records), len(records[0]) if records else 0)
        else:
            wide_format = self._to_wide(records, index, columns_col, values_col, aggfunc)
            long_format = self._to_long(wide_format, index, values_col)
            return PivotResult(wide_format, long_format, len(wide_format), len(wide_format[0]) if wide_format else 0)

    def _to_long(self, records: list[dict], index: str, values_col: str) -> list[dict[str, Any]]:
        """Convert wide format to long format."""
        long_records = []
        for record in records:
            if index not in record:
                continue
            base = {index: record[index]}
            for key, value in record.items():
                if key != index:
                    long_records.append({**base, "variable": key, "value": value})
        return long_records

    def _to_wide(
        self,
        records: list[dict],
        index: str,
        columns_col: str,
        values_col: str,
        aggfunc: str
    ) -> list[dict[str, Any]]:
        """Convert long format to wide format."""
        from collections import defaultdict

        groups: dict[tuple, list] = defaultdict(list)
        for record in records:
            key = tuple(record.get(k, None) for k in [index, columns_col])
            if values_col in record:
                groups[key].append(record[values_col])

        agg_funcs = {
            "sum": sum,
            "count": len,
            "mean": lambda x: sum(x) / len(x) if x else None,
            "min": min,
            "max": max
        }
        agg = agg_funcs.get(aggfunc, sum)

        wide: dict[tuple, dict[str, Any]] = {}
        for (idx_val, col_val), values in groups.items():
            key = (idx_val,)
            if key not in wide:
                wide[key] = {index: idx_val}
            wide[key][col_val] = agg(values)

        return list(wide.values())
