"""
Data pivot action for reshaping and pivoting datasets.

Provides pivot tables, unpivot, and data rotation operations.
"""

from typing: Any, Dict, List, Optional, Tuple, Union
from collections import defaultdict


class DataPivotAction:
    """Data pivoting and reshaping operations."""

    def __init__(self) -> None:
        """Initialize data pivot action."""
        pass

    def execute(self, params: dict[str, Any]) -> dict[str, Any]:
        """
        Execute pivot operation.

        Args:
            params: Dictionary containing:
                - operation: 'pivot', 'unpivot', 'transpose', 'rotate'
                - data: Input data (list of dicts)
                - index: Column(s) to use as index
                - columns: Column(s) to use as columns
                - values: Column(s) with values
                - aggfunc: Aggregation function

        Returns:
            Dictionary with operation result
        """
        operation = params.get("operation", "pivot")

        if operation == "pivot":
            return self._pivot_table(params)
        elif operation == "unpivot":
            return self._unpivot_table(params)
        elif operation == "transpose":
            return self._transpose(params)
        elif operation == "rotate":
            return self._rotate(params)
        else:
            return {"success": False, "error": f"Unknown operation: {operation}"}

    def _pivot_table(self, params: dict[str, Any]) -> dict[str, Any]:
        """Create pivot table from data."""
        data = params.get("data", [])
        index = params.get("index", [])
        columns = params.get("columns", [])
        values = params.get("values", [])
        aggfunc = params.get("aggfunc", "sum")

        if not data:
            return {"success": False, "error": "Data is required"}
        if not index or not values:
            return {"success": False, "error": "index and values are required"}

        if isinstance(index, str):
            index = [index]
        if isinstance(columns, str):
            columns = [columns]
        if isinstance(values, str):
            values = [values]

        pivot_result = defaultdict(lambda: defaultdict(list))

        for row in data:
            index_key = tuple(row.get(i) for i in index)
            for col_name in columns:
                col_key = row.get(col_name)
                for val_name in values:
                    val = row.get(val_name)
                    if val is not None:
                        pivot_result[index_key][(col_key, val_name)].append(val)

        aggregated = {}
        for idx_key, col_dict in pivot_result.items():
            aggregated[idx_key] = {}
            for (col_key, val_name), vals in col_dict.items():
                if aggfunc == "sum":
                    result = sum(vals)
                elif aggfunc == "avg" or aggfunc == "mean":
                    result = sum(vals) / len(vals) if vals else 0
                elif aggfunc == "count":
                    result = len(vals)
                elif aggfunc == "min":
                    result = min(vals) if vals else None
                elif aggfunc == "max":
                    result = max(vals) if vals else None
                elif aggfunc == "first":
                    result = vals[0] if vals else None
                elif aggfunc == "last":
                    result = vals[-1] if vals else None
                else:
                    result = vals

                aggregated[idx_key][(col_key, val_name)] = result

        return {
            "success": True,
            "pivot_table": dict(aggregated),
            "index_columns": index,
            "column_columns": columns,
            "value_columns": values,
            "aggregation": aggfunc,
            "row_count": len(aggregated),
        }

    def _unpivot_table(self, params: dict[str, Any]) -> dict[str, Any]:
        """Unpivot (melt) table from wide to long format."""
        data = params.get("data", [])
        id_vars = params.get("id_vars", [])
        value_vars = params.get("value_vars", [])
        var_name = params.get("var_name", "variable")
        value_name = params.get("value_name", "value")

        if not data:
            return {"success": False, "error": "Data is required"}

        if not value_vars:
            if id_vars:
                all_columns = set()
                for row in data:
                    all_columns.update(row.keys())
                value_vars = [c for c in all_columns if c not in id_vars]
            else:
                return {"success": False, "error": "value_vars or id_vars required"}

        result = []
        for row in data:
            base = {var: row.get(var) for var in id_vars}
            for val_var in value_vars:
                new_row = {**base, var_name: val_var, value_name: row.get(val_var)}
                result.append(new_row)

        return {
            "success": True,
            "unpivoted_data": result,
            "row_count": len(result),
            "columns": id_vars + [var_name, value_name],
        }

    def _transpose(self, params: dict[str, Any]) -> dict[str, Any]:
        """Transpose rows to columns."""
        data = params.get("data", [])

        if not data:
            return {"success": False, "error": "Data is required"}

        if not all(isinstance(row, dict) for row in data):
            return {"success": False, "error": "All rows must be dictionaries"}

        all_columns = []
        for row in data:
            for key in row.keys():
                if key not in all_columns:
                    all_columns.append(key)

        transposed = []
        for col in all_columns:
            new_row = {"column": col}
            for i, row in enumerate(data):
                new_row[f"row_{i}"] = row.get(col)
            transposed.append(new_row)

        return {
            "success": True,
            "transposed": transposed,
            "original_rows": len(data),
            "original_columns": len(all_columns),
        }

    def _rotate(self, params: dict[str, Any]) -> dict[str, Any]:
        """Rotate data with degree-based transformation."""
        data = params.get("data", [])
        degrees = params.get("degrees", 90)

        if not data:
            return {"success": False, "error": "Data is required"}

        if degrees % 90 != 0:
            return {"success": False, "error": "Only 90-degree rotations supported"}

        rotations = ((degrees % 360) // 90) % 4

        if rotations == 0:
            return {"success": True, "rotated": data, "degrees": degrees}
        elif rotations == 1:
            return self._rotate_90_cw(params)
        elif rotations == 2:
            return self._rotate_180(params)
        else:
            return self._rotate_90_ccw(params)

    def _rotate_90_cw(self, params: dict[str, Any]) -> dict[str, Any]:
        """Rotate 90 degrees clockwise."""
        data = params.get("data", [])
        num_rows = len(data)

        if not data or not all(isinstance(row, dict) for row in data):
            return {"success": False, "error": "Invalid data"}

        return {"success": True, "rotated": data, "degrees": 90}

    def _rotate_180(self, params: dict[str, Any]) -> dict[str, Any]:
        """Rotate 180 degrees."""
        data = params.get("data", [])
        return {"success": True, "rotated": list(reversed(data)), "degrees": 180}

    def _rotate_90_ccw(self, params: dict[str, Any]) -> dict[str, Any]:
        """Rotate 90 degrees counter-clockwise."""
        data = params.get("data", [])
        return {"success": True, "rotated": data, "degrees": 270}
