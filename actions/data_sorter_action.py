# Copyright (c) 2024. coded by claude
"""Data Sorter Action Module.

Sorts and organizes API response data with support for multi-field sorting,
custom comparators, and stable sorting.
"""
from typing import Optional, Dict, Any, List, Callable, Tuple
from dataclasses import dataclass, field
from enum import Enum
import logging

logger = logging.getLogger(__name__)


class SortOrder(Enum):
    ASC = "asc"
    DESC = "desc"


@dataclass
class SortField:
    field: str
    order: SortOrder = SortOrder.ASC
    numeric: bool = False
    nulls_first: bool = False


@dataclass
class SortConfig:
    fields: List[SortField] = field(default_factory=list)
    stable: bool = True
    case_sensitive: bool = True


class DataSorter:
    def __init__(self, config: Optional[SortConfig] = None):
        self.config = config or SortConfig()

    def sort(self, items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        if not items:
            return []
        if not self.config.fields:
            return list(items)
        reverse = self.config.fields[0].order == SortOrder.DESC
        return sorted(items, key=self._create_sort_key, reverse=reverse)

    def sort_multi(self, items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        if not items:
            return []
        return sorted(items, key=self._create_multi_sort_key)

    def _create_sort_key(self, item: Dict[str, Any]) -> Tuple:
        sort_field = self.config.fields[0]
        value = item.get(sort_field.field)
        return self._normalize_value(value, sort_field)

    def _create_multi_sort_key(self, item: Dict[str, Any]) -> Tuple:
        keys = []
        for sort_field in self.config.fields:
            value = item.get(sort_field.field)
            keys.append(self._normalize_value(value, sort_field))
        return tuple(keys)

    def _normalize_value(self, value: Any, sort_field: SortField) -> Tuple:
        if value is None:
            if sort_field.nulls_first:
                return (0, "")
            return (1, "")
        if sort_field.numeric and isinstance(value, (int, float)):
            return (0, value)
        if isinstance(value, str):
            if sort_field.case_sensitive:
                return (0, value)
            return (0, value.lower())
        return (0, str(value))

    def add_sort_field(self, field_name: str, order: SortOrder = SortOrder.ASC, numeric: bool = False) -> None:
        self.config.fields.append(SortField(field=field_name, order=order, numeric=numeric))

    def sort_by_function(self, items: List[Dict[str, Any]], key_func: Callable[[Dict[str, Any]], Any]) -> List[Dict[str, Any]]:
        return sorted(items, key=key_func)
