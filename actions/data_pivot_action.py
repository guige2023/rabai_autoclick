"""Data pivot action for pivot table operations.

Creates pivot tables from datasets with aggregations,
grouping, and multi-dimensional analysis.
"""

import logging
from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Callable, Optional

logger = logging.getLogger(__name__)


class AggregationFunction(Enum := __import__("enum").Enum):
    """Aggregation functions for pivot tables."""
    SUM = "sum"
    COUNT = "count"
    AVG = "avg"
    MIN = "min"
    MAX = "max"
    FIRST = "first"
    LAST = "last"


@dataclass
class PivotConfig:
    """Configuration for pivot table."""
    index: list[str]
    columns: list[str]
    values: list[str]
    aggfunc: AggregationFunction = AggregationFunction.SUM
    fill_value: Any = 0
    margins: bool = False
    dropna: bool = True


@dataclass
class PivotResult:
    """Result of pivot operation."""
    data: list[dict]
    row_count: int
    column_count: int
    aggregations: dict[str, int]
    processing_time_ms: float


class DataPivotAction:
    """Create pivot tables from datasets.

    Example:
        >>> pivot = DataPivotAction()
        >>> result = pivot.pivot(data, index=["region"], columns=["product"], values=["sales"])
    """

    def __init__(self) -> None:
        self._default_agg = AggregationFunction.SUM

    def pivot(
        self,
        data: list[dict],
        index: list[str],
        columns: list[str],
        values: list[str],
        aggfunc: AggregationFunction = AggregationFunction.SUM,
        fill_value: Any = 0,
        margins: bool = False,
    ) -> PivotResult:
        """Create a pivot table from data.

        Args:
            data: Input dataset.
            index: Fields to group by for rows.
            columns: Fields to pivot to columns.
            values: Fields to aggregate.
            aggfunc: Aggregation function.
            fill_value: Value to fill missing cells.
            margins: Add row/column totals.

        Returns:
            Pivot result.
        """
        import time
        start_time = time.time()

        if not data:
            return PivotResult(
                data=[],
                row_count=0,
                column_count=0,
                aggregations={},
                processing_time_ms=0.0,
            )

        grouped: dict = defaultdict(list)
        aggregations: dict[str, int] = {}

        for row in data:
            key = tuple(row.get(f, None) for f in index)
            grouped[key].append(row)

        aggregations["groups"] = len(grouped)

        result: list[dict] = []
        column_values: set = set()

        for key, group in grouped.items():
            row_dict = {index[i]: key[i] for i in range(len(index))}

            col_key_map: dict[str, list] = defaultdict(list)
            for item in group:
                col_key = tuple(item.get(c, None) for c in columns)
                col_key_map[col_key].append(item)

            for col_key, col_items in col_key_map.items():
                for i, col_field in enumerate(columns):
                    row_dict[f"{col_field}_{col_key[i]}"] = col_key[i]

                for val_field in values:
                    col_val_key = f"{val_field}_{col_key[0]}" if columns else val_field
                    column_values.add(col_val_key)
                    row_dict[col_val_key] = self._aggregate(
                        col_items, val_field, aggfunc
                    )

            result.append(row_dict)

        for row in result:
            for col in column_values:
                if col not in row:
                    row[col] = fill_value

        if margins:
            result = self._add_margins(result, index, column_values, values, data, aggfunc)

        return PivotResult(
            data=result,
            row_count=len(result),
            column_count=len(column_values),
            aggregations=aggregations,
            processing_time_ms=(time.time() - start_time) * 1000,
        )

    def _aggregate(
        self,
        items: list[dict],
        field: str,
        func: AggregationFunction,
    ) -> Any:
        """Aggregate values for a field.

        Args:
            items: Items to aggregate.
            field: Field to aggregate.
            func: Aggregation function.

        Returns:
            Aggregated value.
        """
        values = []
        for item in items:
            val = item.get(field)
            if val is not None:
                try:
                    values.append(float(val))
                except (ValueError, TypeError):
                    pass

        if not values:
            return 0

        if func == AggregationFunction.SUM:
            return sum(values)
        elif func == AggregationFunction.COUNT:
            return len(values)
        elif func == AggregationFunction.AVG:
            return sum(values) / len(values)
        elif func == AggregationFunction.MIN:
            return min(values)
        elif func == AggregationFunction.MAX:
            return max(values)
        elif func == AggregationFunction.FIRST:
            return values[0]
        elif func == AggregationFunction.LAST:
            return values[-1]

        return values

    def _add_margins(
        self,
        result: list[dict],
        index: list[str],
        column_values: set[str],
        values: list[str],
        data: list[dict],
        aggfunc: AggregationFunction,
    ) -> list[dict]:
        """Add margin totals.

        Args:
            result: Current result.
            index: Index fields.
            column_values: Column value names.
            values: Value fields.
            data: Original data.
            aggfunc: Aggregation function.

        Returns:
            Result with margins.
        """
        margin_row = {f: "Total" for f in index}

        for col in column_values:
            field_name = col.rsplit("_", 1)[0] if "_" in col else col
            all_values = [item.get(field_name, 0) for item in data]
            try:
                all_values = [float(v) for v in all_values if v is not None]
            except (ValueError, TypeError):
                all_values = []

            if all_values:
                margin_row[col] = self._aggregate(
                    [{field_name: v} for v in all_values],
                    field_name,
                    aggfunc,
                )
            else:
                margin_row[col] = 0

        result.append(margin_row)
        return result

    def unpivot(
        self,
        data: list[dict],
        id_vars: list[str],
        value_vars: list[str],
        var_name: str = "variable",
        value_name: str = "value",
    ) -> list[dict]:
        """Unpivot (melt) a pivot table.

        Args:
            data: Pivot data.
            id_vars: Columns to keep as identifiers.
            value_vars: Columns to unpivot.
            var_name: Name for variable column.
            value_name: Name for value column.

        Returns:
            Unpivoted data.
        """
        result: list[dict] = []

        for row in data:
            base = {var: row[var] for var in id_vars if var in row}

            for val_var in value_vars:
                if val_var in row:
                    new_row = base.copy()
                    new_row[var_name] = val_var
                    new_row[value_name] = row[val_var]
                    result.append(new_row)

        return result

    def multi_index_pivot(
        self,
        data: list[dict],
        rows: list[str],
        cols: list[str],
        values: str,
        aggfunc: AggregationFunction = AggregationFunction.SUM,
    ) -> dict:
        """Create multi-index pivot.

        Args:
            data: Input data.
            rows: Row index fields.
            cols: Column index fields.
            values: Value field.
            aggfunc: Aggregation function.

        Returns:
            Multi-index dictionary.
        """
        result: dict = defaultdict(lambda: defaultdict(list))

        for item in data:
            row_key = tuple(item.get(r) for r in rows)
            col_key = tuple(item.get(c) for c in cols)
            value = item.get(values)

            result[row_key][col_key].append(value)

        final: dict = {}
        for row_key, cols_dict in result.items():
            row_dict = {}
            for col_key, values_list in cols_dict.items():
                row_dict[col_key] = self._aggregate(
                    [{values: v} for v in values_list],
                    values,
                    aggfunc,
                )
            final[row_key] = row_dict

        return dict(final)
