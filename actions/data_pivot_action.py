"""
Data Pivot Action Module.

Pivot and unpivot data tables.
"""

from __future__ import annotations

from collections import defaultdict
from typing import Any, Dict, List, Optional, Set, Tuple


class DataPivotAction:
    """
    Pivot and unpivot data tables.

    Transform between long and wide data formats.
    """

    def __init__(self) -> None:
        pass

    def pivot(
        self,
        data: List[Dict[str, Any]],
        index: str,
        columns: str,
        values: str,
        aggregation: str = "first",
    ) -> List[Dict[str, Any]]:
        """
        Pivot data from long to wide format.

        Args:
            data: List of records in long format
            index: Column to use as index
            columns: Column containing column names
            values: Column containing values
            aggregation: How to aggregate duplicates

        Returns:
            Pivoted data in wide format
        """
        index_values: Set[Any] = set()
        column_values: Set[Any] = set()

        for record in data:
            if index in record and columns in record and values in record:
                index_values.add(record[index])
                column_values.add(record[columns])

        result: List[Dict[str, Any]] = []

        for idx in index_values:
            row: Dict[str, Any] = {index: idx}

            column_data: Dict[Any, List[Any]] = defaultdict(list)

            for record in data:
                if record.get(index) == idx:
                    col_name = record.get(columns)
                    val = record.get(values)
                    if col_name is not None:
                        column_data[col_name].append(val)

            for col_name in column_values:
                col_vals = column_data.get(col_name, [])

                if not col_vals:
                    row[str(col_name)] = None
                elif aggregation == "first":
                    row[str(col_name)] = col_vals[0]
                elif aggregation == "last":
                    row[str(col_name)] = col_vals[-1]
                elif aggregation == "count":
                    row[str(col_name)] = len(col_vals)
                elif aggregation == "sum":
                    numeric = [v for v in col_vals if isinstance(v, (int, float))]
                    row[str(col_name)] = sum(numeric) if numeric else None
                elif aggregation == "mean":
                    numeric = [v for v in col_vals if isinstance(v, (int, float))]
                    row[str(col_name)] = sum(numeric) / len(numeric) if numeric else None
                else:
                    row[str(col_name)] = col_vals[0]

            result.append(row)

        return result

    def unpivot(
        self,
        data: List[Dict[str, Any]],
        index: str,
        value_columns: List[str],
        variable_name: str = "variable",
        value_name: str = "value",
    ) -> List[Dict[str, Any]]:
        """
        Unpivot data from wide to long format.

        Args:
            data: List of records in wide format
            index: Column to preserve as identifier
            value_columns: Columns to unpivot
            variable_name: Name for variable column
            value_name: Name for value column

        Returns:
            Unpivoted data in long format
        """
        result: List[Dict[str, Any]] = []

        for record in data:
            idx_val = record.get(index)

            for col in value_columns:
                if col in record:
                    result.append({
                        index: idx_val,
                        variable_name: col,
                        value_name: record[col],
                    })

        return result

    def transpose(
        self,
        data: List[Dict[str, Any]],
        columns_as: str = "row",
    ) -> List[Dict[str, Any]]:
        """
        Transpose rows and columns.

        Args:
            data: Data to transpose
            columns_as: Name for column identifier

        Returns:
            Transposed data
        """
        if not data:
            return []

        all_keys = set()
        for record in data:
            all_keys.update(record.keys())

        result: List[Dict[str, Any]] = []

        for key in all_keys:
            row: Dict[str, Any] = {columns_as: key}

            for i, record in enumerate(data):
                if key in record:
                    row[f"col_{i}"] = record[key]
                else:
                    row[f"col_{i}"] = None

            result.append(row)

        return result

    def group_pivot(
        self,
        data: List[Dict[str, Any]],
        row_dim: str,
        col_dim: str,
        value_dim: str,
        agg_func: str = "sum",
    ) -> Dict[Tuple[Any, Any], Any]:
        """
        Create a grouped pivot table.

        Args:
            data: Data to pivot
            row_dim: Row dimension
            col_dim: Column dimension
            value_dim: Value dimension
            agg_func: Aggregation function

        Returns:
            Dict mapping (row, col) to aggregated value
        """
        pivot: Dict[Tuple[Any, Any], List[Any]] = defaultdict(list)

        for record in data:
            row_key = record.get(row_dim)
            col_key = record.get(col_dim)
            value = record.get(value_dim)

            if row_key is not None and col_key is not None:
                pivot[(row_key, col_key)].append(value)

        result: Dict[Tuple[Any, Any], Any] = {}

        for key, values in pivot.items():
            if not values:
                result[key] = None
            elif agg_func == "sum":
                result[key] = sum(v for v in values if isinstance(v, (int, float)))
            elif agg_func == "mean":
                numeric = [v for v in values if isinstance(v, (int, float))]
                result[key] = sum(numeric) / len(numeric) if numeric else None
            elif agg_func == "count":
                result[key] = len(values)
            elif agg_func == "min":
                result[key] = min(values)
            elif agg_func == "max":
                result[key] = max(values)
            else:
                result[key] = values[0]

        return result
