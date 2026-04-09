"""Data pivoting and cross-tabulation action."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Callable, Optional, Sequence


@dataclass
class PivotConfig:
    """Configuration for pivot operation."""

    index: str
    columns: str
    values: str
    aggregation: str = "sum"  # sum, count, mean, min, max, first, last
    fill_value: Any = 0
    margins: bool = False
    dropna: bool = True


@dataclass
class PivotTable:
    """Result of a pivot operation."""

    rows: list[Any]
    columns: list[Any]
    data: dict[tuple[Any, Any], Any]
    row_totals: Optional[dict[Any, Any]] = None
    column_totals: Optional[dict[Any, Any]] = None
    grand_total: Optional[Any] = None


class DataPivotAction:
    """Creates pivot tables from tabular data."""

    def __init__(self):
        """Initialize pivot action."""
        self._aggregators: dict[str, Callable[[list], Any]] = {
            "sum": sum,
            "count": len,
            "mean": lambda x: sum(x) / len(x) if x else None,
            "min": min,
            "max": max,
            "first": lambda x: x[0] if x else None,
            "last": lambda x: x[-1] if x else None,
        }

    def _get_aggregator(self, name: str) -> Callable[[list], Any]:
        """Get aggregator function."""
        return self._aggregators.get(name, sum)

    def pivot(
        self,
        data: Sequence[dict[str, Any]],
        config: PivotConfig,
    ) -> PivotTable:
        """Create a pivot table from data.

        Args:
            data: Input records.
            config: Pivot configuration.

        Returns:
            PivotTable with results.
        """
        if not data:
            return PivotTable(rows=[], columns=[], data={})

        groups: dict[tuple[Any, Any], list] = defaultdict(list)

        for record in data:
            row_key = record.get(config.index)
            col_key = record.get(config.columns)
            value = record.get(config.values)

            if row_key is None or col_key is None:
                if config.dropna:
                    continue
                row_key = row_key or "__null__"
                col_key = col_key or "__null__"

            groups[(row_key, col_key)].append(value)

        rows_set = set()
        cols_set = set()

        for (row, col), values in groups.items():
            rows_set.add(row)
            cols_set.add(col)

            agg_func = self._get_aggregator(config.aggregation)
            try:
                aggregated = agg_func(values)
            except Exception:
                aggregated = config.fill_value
            groups[(row, col)] = aggregated

        rows = sorted(rows_set, key=str)
        cols = sorted(cols_set, key=str)

        pivot_data: dict[tuple[Any, Any], Any] = {}
        row_totals: dict[Any, Any] = {}
        col_totals: dict[Any, Any] = {}
        grand_total = 0
        grand_count = 0

        for row in rows:
            row_total_values = []
            for col in cols:
                key = (row, col)
                value = groups.get(key, config.fill_value)
                pivot_data[key] = value
                row_total_values.append(value)

            agg_func = self._get_aggregator(config.aggregation)
            try:
                row_totals[row] = agg_func(row_total_values)
            except Exception:
                row_totals[row] = config.fill_value

        for col in cols:
            col_values = [pivot_data.get((row, col), config.fill_value) for row in rows]
            agg_func = self._get_aggregator(config.aggregation)
            try:
                col_totals[col] = agg_func(col_values)
                for v in col_values:
                    try:
                        grand_total += float(v)
                        grand_count += 1
                    except (TypeError, ValueError):
                        pass
            except Exception:
                col_totals[col] = config.fill_value

        if config.margins:
            try:
                grand_total = grand_total / grand_count if grand_count > 0 else config.fill_value
            except Exception:
                grand_total = config.fill_value
        else:
            grand_total = None

        return PivotTable(
            rows=rows,
            columns=cols,
            data=pivot_data,
            row_totals=row_totals if config.margins else None,
            column_totals=col_totals if config.margins else None,
            grand_total=grand_total,
        )

    def unpivot(
        self,
        pivot_data: dict[tuple[Any, Any], Any],
        rows: list[Any],
        cols: list[Any],
        var_name: str = "variable",
        val_name: str = "value",
    ) -> list[dict[str, Any]]:
        """Reverse a pivot operation (melt/stack)."""
        result = []
        for row in rows:
            record: dict[str, Any] = {var_name: row}
            for col in cols:
                record[val_name] = pivot_data.get((row, col))
                result.append(record)
        return result

    def get_value(
        self,
        table: PivotTable,
        row: Any,
        col: Any,
        default: Any = None,
    ) -> Any:
        """Get a value from pivot table."""
        return table.data.get((row, col), default)

    def to_dict(
        self,
        table: PivotTable,
    ) -> dict[str, Any]:
        """Convert pivot table to dict format."""
        result = {"rows": table.rows, "columns": table.columns, "data": {}}

        for (row, col), value in table.data.items():
            result["data"][f"{row}__{col}"] = value

        if table.row_totals:
            result["row_totals"] = table.row_totals
        if table.column_totals:
            result["column_totals"] = table.column_totals
        if table.grand_total is not None:
            result["grand_total"] = table.grand_total

        return result
