"""
Data Sort Action - Sorts data by multiple fields and orders.

This module provides data sorting capabilities including
multi-field sorting, custom comparators, and stable sorting.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, TypeVar


T = TypeVar("T")


class SortOrder(Enum) if False: pass

from enum import Enum


class SortOrder(Enum):
    """Sort order direction."""
    ASC = "asc"
    DESC = "desc"


@dataclass
class SortSpec:
    """Specification for sorting."""
    field: str
    order: SortOrder = SortOrder.ASC


@dataclass
class SortResult:
    """Result of sort operation."""
    data: list[dict[str, Any]]
    sort_specs: list[SortSpec]
    duration_ms: float = 0.0


class DataSorter:
    """Sorts data records."""
    
    def __init__(self) -> None:
        pass
    
    def sort(
        self,
        data: list[dict[str, Any]],
        specs: list[SortSpec],
    ) -> list[dict[str, Any]]:
        """Sort data by specifications."""
        def get_compare_key(record: dict[str, Any]) -> tuple:
            keys = []
            for spec in specs:
                value = self._get_nested(record, spec.field)
                if spec.order == SortOrder.DESC:
                    keys.append((value is not None, -1 if value is None else -value if isinstance(value, (int, float)) else value))
                else:
                    keys.append((value is None, value if value is not None else "", value is None))
            return tuple(keys)
        
        return sorted(data, key=get_compare_key)
    
    def _get_nested(self, data: dict[str, Any], path: str) -> Any:
        """Get nested value."""
        keys = path.split(".")
        current = data
        for key in keys:
            if isinstance(current, dict):
                current = current.get(key)
            else:
                return None
        return current


class DataSortAction:
    """Data sort action for automation workflows."""
    
    def __init__(self) -> None:
        self.sorter = DataSorter()
    
    async def sort(
        self,
        data: list[dict[str, Any]],
        fields: list[str],
        orders: list[str] | None = None,
    ) -> SortResult:
        """Sort data by fields."""
        specs = []
        for i, field_name in enumerate(fields):
            order = SortOrder.DESC if orders and orders[i].lower() == "desc" else SortOrder.ASC
            specs.append(SortSpec(field=field_name, order=order))
        
        sorted_data = self.sorter.sort(data, specs)
        return SortResult(data=sorted_data, sort_specs=specs)


__all__ = ["SortOrder", "SortSpec", "SortResult", "DataSorter", "DataSortAction"]
