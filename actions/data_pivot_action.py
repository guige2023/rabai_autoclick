"""Data Pivot and Aggregation Engine.

This module provides pivot table capabilities:
- Row and column pivoting
- Multiple aggregation functions
- Grouped pivoting
- Sparse and dense matrix output

Example:
    >>> from actions.data_pivot_action import DataPivoter
    >>> pivoter = DataPivoter()
    >>> result = pivoter.pivot(records, index="date", columns="region", values="sales", aggfunc="sum")
"""

from __future__ import annotations

import logging
import threading
from typing import Any, Callable, Optional
from collections import defaultdict

logger = logging.getLogger(__name__)


class DataPivoter:
    """Creates pivot tables from tabular data."""

    AGG_FUNCTIONS = {
        "sum": sum,
        "mean": lambda vals: sum(vals) / len(vals) if vals else None,
        "count": len,
        "min": min,
        "max": max,
        "first": lambda vals: vals[0] if vals else None,
        "last": lambda vals: vals[-1] if vals else None,
        "std": None,
        "median": None,
    }

    def __init__(self) -> None:
        """Initialize the data pivoter."""
        self._lock = threading.Lock()
        self._stats = {"pivots_created": 0}

    def pivot(
        self,
        records: list[dict[str, Any]],
        index: str,
        columns: str,
        values: str,
        aggfunc: str = "sum",
        fill_value: Any = None,
    ) -> dict[str, dict[str, Any]]:
        """Create a pivot table.

        Args:
            records: List of record dicts.
            index: Field to use as row index.
            columns: Field to use as columns.
            values: Field to aggregate.
            aggfunc: Aggregation function name.
            fill_value: Value to use for missing cells.

        Returns:
            Dict mapping index values to column values to aggregated values.
        """
        result: dict[str, dict[str, Any]] = defaultdict(dict)
        agg_fn = self.AGG_FUNCTIONS.get(aggfunc)

        if agg_fn is None and aggfunc not in ("std", "median"):
            agg_fn = sum

        self._stats["pivots_created"] += 1

        for record in records:
            index_val = record.get(index)
            column_val = record.get(columns)
            cell_val = record.get(values)

            if index_val is None or column_val is None:
                continue

            str_index = str(index_val)
            str_column = str(column_val)

            if aggfunc == "std":
                if str_index not in result:
                    result[str_index] = {}
                if str_column not in result[str_index]:
                    result[str_index][str_column] = []
                result[str_index][str_column].append(cell_val)
            else:
                if str_column not in result[str_index]:
                    result[str_index][str_column] = []
                result[str_index][str_column].append(cell_val)

        for idx in result:
            for col in result[idx]:
                vals = result[idx][col]
                if aggfunc == "std":
                    import statistics
                    result[idx][col] = round(statistics.stdev(vals), 4) if len(vals) > 1 else 0
                elif aggfunc == "median":
                    import statistics
                    result[idx][col] = statistics.median(vals)
                elif agg_fn:
                    result[idx][col] = agg_fn(vals)

                if result[idx][col] is None and fill_value is not None:
                    result[idx][col] = fill_value

        return dict(result)

    def pivot_multi(
        self,
        records: list[dict[str, Any]],
        rows: list[str],
        columns: str,
        values: str,
        aggfunc: str = "sum",
    ) -> dict[str, Any]:
        """Create a pivot table with multiple row fields.

        Args:
            records: List of record dicts.
            rows: Fields to use as row index (compound).
            columns: Field to use as columns.
            values: Field to aggregate.
            aggfunc: Aggregation function name.

        Returns:
            Nested dict with compound row keys.
        """
        agg_fn = self.AGG_FUNCTIONS.get(aggfunc, sum)
        result: dict[str, Any] = {}

        for record in records:
            row_key_parts = [str(record.get(r, "null")) for r in rows]
            row_key = "|".join(row_key_parts)
            column_val = str(record.get(columns, "null"))
            cell_val = record.get(values)

            if row_key not in result:
                result[row_key] = {}
            if column_val not in result[row_key]:
                result[row_key][column_val] = []
            result[row_key][column_val].append(cell_val)

        for row_key in result:
            for col in result[row_key]:
                vals = result[row_key][col]
                result[row_key][col] = agg_fn(vals)

        return result

    def unstack(
        self,
        pivoted: dict[str, dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """Convert a pivot table back to records.

        Args:
            pivoted: Pivot table result.

        Returns:
            List of records.
        """
        records = []
        for index_val, columns in pivoted.items():
            record = {"index": index_val}
            for col_name, col_val in columns.items():
                record[col_name] = col_val
            records.append(record)
        return records

    def get_stats(self) -> dict[str, int]:
        """Get pivot statistics."""
        with self._lock:
            return dict(self._stats)
