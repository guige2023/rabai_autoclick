"""Data Sorter Action Module.

Provides multi-field sorting with ascending/descending,
null handling, and custom comparator support.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Tuple, Union
from enum import Enum


class NullOrder(Enum):
    """Null value ordering."""
    FIRST = "first"
    LAST = "last"


class SortOrder(Enum):
    """Sort order."""
    ASC = "asc"
    DESC = "desc"


@dataclass
class SortConfig:
    """Sort configuration."""
    field: str
    order: SortOrder = SortOrder.ASC
    null_order: NullOrder = NullOrder.LAST
    custom_key: Optional[Callable[[Any], Any]] = None


class DataSorterAction:
    """Multi-field data sorter.

    Example:
        sorter = DataSorterAction()

        result = sorter.sort(
            data=[
                {"name": "Bob", "age": 30},
                {"name": "Alice", "age": 25},
                {"name": "Bob", "age": 35},
            ],
            sort_configs=[
                SortConfig(field="name", order=SortOrder.ASC),
                SortConfig(field="age", order=SortOrder.DESC),
            ]
        )
    """

    def __init__(self) -> None:
        self._custom_comparators: Dict[str, Callable] = {}

    def register_comparator(
        self,
        field: str,
        comparator: Callable[[Any, Any], int],
    ) -> None:
        """Register custom comparator for field."""
        self._custom_comparators[field] = comparator

    def sort(
        self,
        data: List[Dict[str, Any]],
        sort_configs: List[SortConfig],
        stable: bool = True,
    ) -> List[Dict[str, Any]]:
        """Sort data by multiple fields.

        Args:
            data: List of records to sort
            sort_configs: List of sort configurations (applied in order)
            stable: Use stable sort

        Returns:
            Sorted list of records
        """
        if not data or not sort_configs:
            return data

        return self._multi_key_sort(data, sort_configs, stable)

    def _multi_key_sort(
        self,
        data: List[Dict[str, Any]],
        sort_configs: List[SortConfig],
        stable: bool,
    ) -> List[Dict[str, Any]]:
        """Sort by multiple keys in sequence."""
        result = data

        for config in reversed(sort_configs):
            result = self._single_sort(result, config)

        return result

    def _single_sort(
        self,
        data: List[Dict[str, Any]],
        config: SortConfig,
    ) -> List[Dict[str, Any]]:
        """Sort by single configuration."""
        reverse = config.order == SortOrder.DESC

        def sort_key(item: Dict[str, Any]) -> Tuple:
            value = item.get(config.field)
            key_value = self._get_sortable_value(value, config)

            if config.field in self._custom_comparators:
                return (0, key_value)

            is_null = value is None
            if is_null:
                null_priority = 0 if config.null_order == NullOrder.FIRST else 2
                return (null_priority, key_value)

            return (1, key_value)

        return sorted(data, key=sort_key, reverse=reverse)

    def _get_sortable_value(
        self,
        value: Any,
        config: SortConfig,
    ) -> Any:
        """Get sortable value from field value."""
        if config.custom_key:
            return config.custom_key(value)

        if isinstance(value, str):
            return value.lower()

        if isinstance(value, (int, float)):
            return value

        return str(value)

    def sort_by_function(
        self,
        data: List[T],
        key_func: Callable[[T], Any],
        reverse: bool = False,
    ) -> List[T]:
        """Sort data using custom key function.

        Args:
            data: List of items to sort
            key_func: Function to extract sort key
            reverse: Sort in descending order

        Returns:
            Sorted list
        """
        return sorted(data, key=key_func, reverse=reverse)

    def sort_by_field(
        self,
        data: List[Dict[str, Any]],
        field: str,
        order: SortOrder = SortOrder.ASC,
        null_order: NullOrder = NullOrder.LAST,
    ) -> List[Dict[str, Any]]:
        """Sort data by single field.

        Args:
            data: List of records
            field: Field name to sort by
            order: Sort order
            null_order: Where to place null values

        Returns:
            Sorted list
        """
        config = SortConfig(
            field=field,
            order=order,
            null_order=null_order,
        )
        return self._single_sort(data, config)

    def rank(
        self,
        data: List[Dict[str, Any]],
        field: str,
        order: SortOrder = SortOrder.ASC,
    ) -> List[Dict[str, Any]]:
        """Add rank field to data.

        Args:
            data: List of records
            field: Field to rank by
            order: Sort order for ranking

        Returns:
            Records with added 'rank' field
        """
        sorted_data = self.sort_by_field(
            data, field, order, NullOrder.LAST
        )

        for i, record in enumerate(sorted_data, 1):
            record["rank"] = i

        return sorted_data

    def partition(
        self,
        data: List[Dict[str, Any]],
        field: str,
        threshold: Any,
    ) -> Tuple[List[Dict], List[Dict]]:
        """Partition data by threshold value.

        Args:
            data: List of records
            field: Field to partition by
            threshold: Threshold value

        Returns:
            Tuple of (below_threshold, above_threshold)
        """
        below: List[Dict] = []
        above: List[Dict] = []

        for record in data:
            value = record.get(field)
            if value is not None and value < threshold:
                below.append(record)
            else:
                above.append(record)

        return below, above
