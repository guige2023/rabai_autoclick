"""Data pivot operations for automation workflows.

Provides pivot table functionality for reshaping and aggregating
data in different orientations.
"""

from __future__ import annotations

import threading
import time
import uuid
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

import copy


class AggregationType(Enum):
    """Aggregation functions for pivot tables."""
    SUM = "sum"
    COUNT = "count"
    AVG = "avg"
    MIN = "min"
    MAX = "max"
    FIRST = "first"
    LAST = "last"
    MEDIAN = "median"
    STD_DEV = "std_dev"


@dataclass
class PivotConfig:
    """Configuration for a pivot operation."""
    index: List[str]
    columns: List[str]
    values: List[str]
    aggregation: AggregationType
    fill_value: Any = None
    include_totals: bool = False
    include_subtotals: bool = False


@dataclass
class PivotResult:
    """Result of a pivot operation."""
    result_id: str
    created_at: float
    original_rows: int
    result_rows: int
    result_columns: int
    config: Dict[str, Any]
    data: List[Dict[str, Any]] = field(default_factory=list)
    column_order: List[str] = field(default_factory=list)


class PivotEngine:
    """Core pivot table engine."""

    def __init__(self):
        self._results: List[PivotResult] = []
        self._lock = threading.Lock()

    def _aggregate(
        self,
        values: List[Any],
        aggregation: AggregationType,
    ) -> Any:
        """Aggregate values using specified function."""
        if not values:
            return None

        numeric_vals = []
        for v in values:
            try:
                numeric_vals.append(float(v))
            except (TypeError, ValueError):
                numeric_vals.append(v)

        only_numeric = all(isinstance(v, (int, float)) for v in numeric_vals)

        if aggregation == AggregationType.SUM:
            if only_numeric:
                return sum(numeric_vals)
            return len(values)

        elif aggregation == AggregationType.COUNT:
            return len(values)

        elif aggregation == AggregationType.AVG:
            if only_numeric and values:
                return sum(numeric_vals) / len(numeric_vals)
            return None

        elif aggregation == AggregationType.MIN:
            if only_numeric:
                return min(numeric_vals)
            return min(values, key=str)

        elif aggregation == AggregationType.MAX:
            if only_numeric:
                return max(numeric_vals)
            return max(values, key=str)

        elif aggregation == AggregationType.FIRST:
            return values[0]

        elif aggregation == AggregationType.LAST:
            return values[-1]

        elif aggregation == AggregationType.MEDIAN:
            if only_numeric:
                sorted_vals = sorted(numeric_vals)
                n = len(sorted_vals)
                if n % 2 == 0:
                    return (sorted_vals[n//2-1] + sorted_vals[n//2]) / 2
                return sorted_vals[n//2]
            return None

        elif aggregation == AggregationType.STD_DEV:
            if only_numeric and len(numeric_vals) > 1:
                mean_val = sum(numeric_vals) / len(numeric_vals)
                variance = sum((x - mean_val) ** 2 for x in numeric_vals) / len(numeric_vals)
                return variance ** 0.5
            return None

        return values[0]

    def pivot(
        self,
        data: List[Dict[str, Any]],
        index: List[str],
        columns: List[str],
        values: List[str],
        aggregation: AggregationType = AggregationType.SUM,
        fill_value: Any = None,
        include_totals: bool = False,
    ) -> PivotResult:
        """Perform a pivot operation on data."""
        result_id = str(uuid.uuid4())[:12]
        original_rows = len(data)

        if not data or not index or not values:
            return PivotResult(
                result_id=result_id,
                created_at=time.time(),
                original_rows=original_rows,
                result_rows=0,
                result_columns=0,
                config={"index": index, "columns": columns, "values": values},
            )

        index_key = lambda row: tuple(row.get(k) for k in index)

        grouped_data: Dict[Tuple, Dict[str, List]] = defaultdict(lambda: defaultdict(list))

        for row in data:
            key = index_key(row)
            for val_col in values:
                grouped_data[key][val_col].append(row.get(val_col))

        result_data = []
        column_order = list(index)

        if columns:
            column_values = set()
            for row in data:
                for col in columns:
                    col_val = row.get(col)
                    if col_val is not None:
                        column_values.add(col_val)

            column_values = sorted(column_values, key=str)
            column_order.extend(f"{values[0]}_{cv}" for cv in column_values)

        for key, val_data in grouped_data.items():
            result_row = dict(zip(index, key))

            for val_col in values:
                aggregated = self._aggregate(val_data.get(val_col, []), aggregation)
                if len(values) == 1:
                    result_row[val_col] = aggregated
                else:
                    result_row[f"{val_col}_total"] = aggregated

            if columns and len(columns) == 1:
                col_key = columns[0]
                col_values = set()
                for row in data:
                    if all(row.get(k) == key[i] for i, k in enumerate(index)):
                        col_val = row.get(col_key)
                        if col_val is not None:
                            col_values.add(col_val)

                for cv in sorted(col_values, key=str):
                    matching_values = [
                        row.get(values[0])
                        for row in data
                        if all(row.get(k) == key[i] for i, k in enumerate(index))
                        and row.get(col_key) == cv
                    ]
                    agg_val = self._aggregate(matching_values, aggregation)
                    result_row[f"{values[0]}_{cv}"] = agg_val

            result_data.append(result_row)

        if include_totals and result_data:
            totals_row = {}
            for col in result_data[0].keys():
                if col in index:
                    totals_row[col] = "TOTAL"
                else:
                    all_vals = [row.get(col) for row in result_data if row.get(col) is not None]
                    if all_vals and all(isinstance(v, (int, float)) for v in all_vals):
                        totals_row[col] = sum(all_vals)
                    else:
                        totals_row[col] = len(all_vals)

            result_data.append(totals_row)

        result_columns = len(result_data[0]) if result_data else 0

        pivot_result = PivotResult(
            result_id=result_id,
            created_at=time.time(),
            original_rows=original_rows,
            result_rows=len(result_data),
            result_columns=result_columns,
            config={
                "index": index,
                "columns": columns,
                "values": values,
                "aggregation": aggregation.value,
                "fill_value": fill_value,
                "include_totals": include_totals,
            },
            data=result_data,
            column_order=column_order,
        )

        with self._lock:
            self._results.append(pivot_result)

        return pivot_result

    def unpivot(
        self,
        data: List[Dict[str, Any]],
        id_vars: List[str],
        value_vars: List[str],
        var_name: str = "variable",
        val_name: str = "value",
    ) -> List[Dict[str, Any]]:
        """Reverse a pivot operation (melt)."""
        result = []

        for row in data:
            base = {k: row.get(k) for k in id_vars}

            for var in value_vars:
                new_row = dict(base)
                new_row[var_name] = var
                new_row[val_name] = row.get(var)
                result.append(new_row)

        return result


class AutomationPivotAction:
    """Action providing pivot table operations for automation workflows."""

    def __init__(self, engine: Optional[PivotEngine] = None):
        self._engine = engine or PivotEngine()

    def pivot(
        self,
        data: List[Dict[str, Any]],
        index: List[str],
        columns: List[str],
        values: List[str],
        aggregation: str = "sum",
        fill_value: Any = None,
        include_totals: bool = False,
    ) -> Dict[str, Any]:
        """Perform a pivot operation."""
        try:
            agg_enum = AggregationType(aggregation.lower())
        except ValueError:
            agg_enum = AggregationType.SUM

        result = self._engine.pivot(
            data=data,
            index=index,
            columns=columns,
            values=values,
            aggregation=agg_enum,
            fill_value=fill_value,
            include_totals=include_totals,
        )

        return {
            "result_id": result.result_id,
            "original_rows": result.original_rows,
            "result_rows": result.result_rows,
            "result_columns": result.result_columns,
            "data": result.data,
            "column_order": result.column_order,
        }

    def unpivot(
        self,
        data: List[Dict[str, Any]],
        id_vars: List[str],
        value_vars: List[str],
        var_name: str = "variable",
        val_name: str = "value",
    ) -> Dict[str, Any]:
        """Reverse a pivot operation."""
        result = self._engine.unpivot(
            data=data,
            id_vars=id_vars,
            value_vars=value_vars,
            var_name=var_name,
            val_name=val_name,
        )

        return {
            "original_rows": len(data),
            "result_rows": len(result),
            "data": result,
        }

    def execute(
        self,
        context: Dict[str, Any],
        params: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Execute a pivot operation.

        Required params:
            operation: str - 'pivot' or 'unpivot'
            data: list - Data to process
            index: list - Index columns for pivot
            columns: list - Column headers for pivot
            values: list - Values to aggregate

        Optional params:
            aggregation: str - Aggregation function (sum, count, avg, min, max, first, last)
            fill_value: Any - Value to fill missing cells
            include_totals: bool - Whether to include totals row
        """
        operation = params.get("operation")

        if operation == "pivot":
            data = params.get("data")
            index = params.get("index", [])
            columns = params.get("columns", [])
            values = params.get("values", [])

            if not data:
                raise ValueError("data is required")
            if not index:
                raise ValueError("index is required")
            if not values:
                raise ValueError("values is required")

            return self.pivot(
                data=data,
                index=index,
                columns=columns,
                values=values,
                aggregation=params.get("aggregation", "sum"),
                fill_value=params.get("fill_value"),
                include_totals=params.get("include_totals", False),
            )

        elif operation == "unpivot":
            data = params.get("data")
            id_vars = params.get("id_vars", [])
            value_vars = params.get("value_vars", [])

            if not data:
                raise ValueError("data is required")
            if not id_vars:
                raise ValueError("id_vars is required")
            if not value_vars:
                raise ValueError("value_vars is required")

            return self.unpivot(
                data=data,
                id_vars=id_vars,
                value_vars=value_vars,
                var_name=params.get("var_name", "variable"),
                val_name=params.get("val_name", "value"),
            )

        else:
            raise ValueError(f"Unknown operation: {operation}")

    def get_pivot_history(self) -> List[Dict[str, Any]]:
        """Get history of pivot operations."""
        return [
            {
                "result_id": r.result_id,
                "created_at": datetime.fromtimestamp(r.created_at).isoformat(),
                "original_rows": r.original_rows,
                "result_rows": r.result_rows,
                "config": r.config,
            }
            for r in self._engine._results
        ]
