"""
Data Pivot Action - Pivots data between wide and long formats.

This module provides data pivoting capabilities for
reshaping datasets.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class PivotConfig:
    """Configuration for pivot operation."""
    index: str = ""
    columns: str = ""
    values: str = ""
    fill_value: Any = None


class DataPivoter:
    """Pivots data between formats."""
    
    def __init__(self) -> None:
        pass
    
    def pivot_long(
        self,
        data: list[dict[str, Any]],
        id_vars: list[str],
        value_vars: list[str],
        var_name: str = "variable",
        val_name: str = "value",
    ) -> list[dict[str, Any]]:
        """Pivot from wide to long format."""
        result = []
        for record in data:
            base = {k: record[k] for k in id_vars if k in record}
            for var in value_vars:
                if var in record:
                    new_record = {**base, var_name: var, val_name: record[var]}
                    result.append(new_record)
        return result
    
    def pivot_wide(
        self,
        data: list[dict[str, Any]],
        index: str,
        columns: str,
        values: str,
        fill_value: Any = None,
    ) -> list[dict[str, Any]]:
        """Pivot from long to wide format."""
        pivot: dict[Any, dict[str, Any]] = {}
        for record in data:
            idx_val = record.get(index)
            col_val = record.get(columns)
            val = record.get(values)
            if idx_val not in pivot:
                pivot[idx_val] = {index: idx_val}
            pivot[idx_val][col_val] = val
        result = list(pivot.values())
        if fill_value is not None:
            all_cols = set()
            for r in result:
                all_cols.update(r.keys())
            for r in result:
                for col in all_cols:
                    if col not in r:
                        r[col] = fill_value
        return result


class DataPivotAction:
    """Data pivot action for automation workflows."""
    
    def __init__(self) -> None:
        self.pivoter = DataPivoter()
    
    async def pivot_long(
        self,
        data: list[dict[str, Any]],
        id_vars: list[str],
        value_vars: list[str],
    ) -> list[dict[str, Any]]:
        """Pivot from wide to long format."""
        return self.pivoter.pivot_long(data, id_vars, value_vars)
    
    async def pivot_wide(
        self,
        data: list[dict[str, Any]],
        index: str,
        columns: str,
        values: str,
        fill_value: Any = None,
    ) -> list[dict[str, Any]]:
        """Pivot from long to wide format."""
        return self.pivoter.pivot_wide(data, index, columns, values, fill_value)


__all__ = ["PivotConfig", "DataPivoter", "DataPivotAction"]
