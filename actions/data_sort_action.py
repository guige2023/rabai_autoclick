"""
Data Sort Action Module

Multi-field sorting, stable sorting, custom comparators,
and efficient sorting algorithms for large datasets.

MIT License - Copyright (c) 2025 RabAi Research
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

logger = logging.getLogger(__name__)


class SortOrder(Enum):
    """Sort order direction."""

    ASC = "asc"
    DESC = "desc"


class SortNulls(Enum):
    """Null sorting behavior."""

    FIRST = "first"
    LAST = "last"
    NONE_FIRST = "none_first"
    NONE_LAST = "none_last"


@dataclass
class SortField:
    """A single sort field specification."""

    field: str
    order: SortOrder = SortOrder.ASC
    nulls: SortNulls = SortNulls.LAST
    case_sensitive: bool = True
    custom_key: Optional[Callable[[Any], Any]] = None


@dataclass
class SortResult:
    """Result of a sort operation."""

    data: List[Dict[str, Any]]
    duration_ms: float
    items_sorted: int


class DataSortAction:
    """
    Main action class for data sorting.

    Features:
    - Multi-field sorting (primary, secondary, etc.)
    - Stable sorting (preserves order of equal elements)
    - Null handling (first, last, none handling)
    - Case-insensitive string sorting
    - Custom key functions
    - Efficient algorithms for large datasets

    Usage:
        sorter = DataSortAction()
        sorter.add_field("age", SortOrder.ASC)
        sorter.add_field("name", SortOrder.DESC)
        result = sorter.sort(data)
    """

    def __init__(self):
        self._fields: List[SortField] = []
        self._stats = {
            "items_sorted": 0,
            "sorts_performed": 0,
        }

    def add_field(
        self,
        field: str,
        order: SortOrder = SortOrder.ASC,
        nulls: SortNulls = SortNulls.LAST,
        case_sensitive: bool = True,
        custom_key: Optional[Callable[[Any], Any]] = None,
    ) -> "DataSortAction":
        """Add a sort field."""
        self._fields.append(SortField(
            field=field,
            order=order,
            nulls=nulls,
            case_sensitive=case_sensitive,
            custom_key=custom_key,
        ))
        return self

    def clear_fields(self) -> "DataSortAction":
        """Clear all sort fields."""
        self._fields.clear()
        return self

    def _get_nested_field(self, item: Dict[str, Any], field_path: str) -> Any:
        """Get nested field value using dot notation."""
        parts = field_path.split(".")
        value = item

        for part in parts:
            if isinstance(value, dict):
                value = value.get(part)
            else:
                return None

        return value

    def _get_null_position(self, value: Any, nulls: SortNulls) -> Tuple[int, bool]:
        """
        Get sort key for null handling.

        Returns:
            (sort_priority, is_null) tuple for null-aware sorting
        """
        if value is None:
            if nulls == SortNulls.FIRST or nulls == SortNulls.NONE_FIRST:
                return (0, True)
            else:
                return (1, True)
        return (1, False)

    def _create_key(self, item: Dict[str, Any], field_spec: SortField) -> Any:
        """Create sort key for an item."""
        value = self._get_nested_field(item, field_spec.field)

        # Handle custom key function
        if field_spec.custom_key:
            value = field_spec.custom_key(value)

        # Handle nulls position
        null_pos, is_null = self._get_null_position(value, field_spec.nulls)

        # Handle case sensitivity for strings
        if isinstance(value, str) and not field_spec.case_sensitive:
            value = value.lower()

        # Return tuple for multi-criteria sorting
        if field_spec.order == SortOrder.DESC:
            if isinstance(value, (int, float)):
                value = -value if not is_null else value
            elif isinstance(value, str):
                value = value  # For desc, we'll handle in comparator

        return (null_pos, value)

    def _compare_items(
        self,
        item1: Dict[str, Any],
        item2: Dict[str, Any],
    ) -> int:
        """Compare two items using all sort fields."""
        for field_spec in self._fields:
            key1 = self._create_key(item1, field_spec)
            key2 = self._create_key(item2, field_spec)

            # Handle null position first
            if key1[0] != key2[0]:
                return -1 if key1[0] < key2[0] else 1

            # Compare actual values
            val1, val2 = key1[1], key2[1]

            # Handle descending order for non-nulls
            if field_spec.order == SortOrder.DESC:
                val1, val2 = val2, val1

            if val1 < val2:
                return -1
            elif val1 > val2:
                return 1

        return 0

    def sort(
        self,
        data: List[Dict[str, Any]],
        reverse: bool = False,
    ) -> SortResult:
        """
        Sort data using specified sort fields.

        Args:
            data: List of dictionaries to sort
            reverse: If True, reverse the sort order

        Returns:
            SortResult with sorted data
        """
        import time

        start_time = time.time()

        if not self._fields:
            # No sort fields, return as-is
            result = SortResult(
                data=data,
                duration_ms=0.0,
                items_sorted=len(data),
            )
            self._stats["items_sorted"] += len(data)
            self._stats["sorts_performed"] += 1
            return result

        # Create a copy to avoid modifying original
        sorted_data = list(data)

        # Use stable sort with custom comparator
        # Python's sort is stable, so we sort from least to most significant
        for field_spec in reversed(self._fields):
            sorted_data = self._stable_sort(sorted_data, field_spec)

        # Apply global reverse if specified
        if reverse:
            sorted_data.reverse()

        duration_ms = (time.time() - start_time) * 1000

        self._stats["items_sorted"] += len(sorted_data)
        self._stats["sorts_performed"] += 1

        return SortResult(
            data=sorted_data,
            duration_ms=duration_ms,
            items_sorted=len(sorted_data),
        )

    def _stable_sort(
        self,
        data: List[Dict[str, Any]],
        field_spec: SortField,
    ) -> List[Dict[str, Any]]:
        """Perform stable sort on a single field."""
        def key_func(item: Dict[str, Any]) -> Any:
            value = self._get_nested_field(item, field_spec.field)

            # Custom key function
            if field_spec.custom_key:
                value = field_spec.custom_key(value)

            # Null handling
            null_pos, is_null = self._get_null_position(value, field_spec.nulls)

            # Case sensitivity
            if isinstance(value, str) and not field_spec.case_sensitive:
                value = value.lower()

            # For descending, we use negative values for numbers
            # Strings need special handling
            if field_spec.order == SortOrder.DESC:
                if isinstance(value, str):
                    # For strings in desc, we sort in asc with reverse at the end
                    # or use a wrapper
                    return (null_pos, value, field_spec.order)
                elif isinstance(value, (int, float)) and not is_null:
                    return (null_pos, -value)
                else:
                    return (null_pos, value)
            else:
                return (null_pos, value)

        return sorted(data, key=key_func)

    def sort_by(
        self,
        data: List[Dict[str, Any]],
        field: str,
        order: SortOrder = SortOrder.ASC,
    ) -> SortResult:
        """Sort data by a single field (convenience method)."""
        self.clear_fields()
        self.add_field(field, order)
        return self.sort(data)

    def sort_by_multiple(
        self,
        data: List[Dict[str, Any]],
        fields: List[Tuple[str, SortOrder]],
    ) -> SortResult:
        """Sort data by multiple fields (convenience method)."""
        self.clear_fields()
        for field, order in fields:
            self.add_field(field, order)
        return self.sort(data)

    def get_stats(self) -> Dict[str, Any]:
        """Get sorting statistics."""
        return self._stats.copy()


def natural_sort_key(s: str) -> List[Union[str, int]]:
    """
    Generate sort key for natural sorting.

    Splits strings into numeric and non-numeric parts
    so "file10" comes after "file9" not before "file2".
    """
    import re
    parts = re.split(r'(\d+)', s)
    return [int(p) if p.isdigit() else p.lower() for p in parts]


class NaturalSortAction(DataSortAction):
    """
    Data sorter with natural sorting support.

    Natural sorting treats numbers in strings as integers
    for sorting purposes (file1, file2, ..., file10 instead of file1, file10, file2).
    """

    def __init__(self):
        super().__init__()

    def add_field(
        self,
        field: str,
        order: SortOrder = SortOrder.ASC,
        nulls: SortNulls = SortNulls.LAST,
        natural: bool = True,
    ) -> "NaturalSortAction":
        """Add a sort field with natural sorting."""
        custom_key = natural_sort_key if natural else None
        return super().add_field(field, order, nulls, custom_key=custom_key)


def demo_sort():
    """Demonstrate sorting usage."""
    data = [
        {"name": "Alice", "age": 30, "department": "Engineering", "salary": 75000},
        {"name": "Bob", "age": 25, "department": "Sales", "salary": 60000},
        {"name": "Charlie", "age": 35, "department": "Engineering", "salary": 85000},
        {"name": "Diana", "age": 30, "department": "Engineering", "salary": 80000},
        {"name": "eve", "age": 28, "department": "Sales", "salary": 65000},
        {"name": "Frank", "age": None, "department": "HR", "salary": 55000},
    ]

    # Simple sort
    sorter = DataSortAction()
    sorter.add_field("age", SortOrder.ASC)
    result = sorter.sort(data)
    print("Sorted by age (asc):")
    for item in result.data:
        print(f"  {item['name']}: age={item['age']}")

    # Multi-field sort
    print("\nSorted by department (asc), then salary (desc):")
    sorter2 = DataSortAction()
    sorter2.add_field("department", SortOrder.ASC)
    sorter2.add_field("salary", SortOrder.DESC)
    result2 = sorter2.sort(data)
    for item in result2.data:
        print(f"  {item['department']}: {item['name']} salary={item['salary']}")


if __name__ == "__main__":
    demo_sort()
