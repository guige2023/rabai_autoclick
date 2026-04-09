"""
Data Sorter Action Module.

Sort and order data with multiple criteria.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional, Tuple, Union


@dataclass
class SortKey:
    """A sort key configuration."""
    field: str
    reverse: bool = False
    nulls_last: bool = True


class DataSorterAction:
    """
    Sort data with multiple keys and custom comparators.

    Supports multi-level sorting, null handling, and custom functions.
    """

    def __init__(self) -> None:
        self._custom_comparators: Dict[str, Callable[[Any, Any], int]] = {}

    def add_comparator(
        self,
        field: str,
        comparator: Callable[[Any, Any], int],
    ) -> None:
        """
        Add a custom comparator for a field.

        Args:
            field: Field name
            comparator: Function returning -1, 0, or 1
        """
        self._custom_comparators[field] = comparator

    def sort(
        self,
        data: List[Dict[str, Any]],
        keys: Union[str, List[str], List[SortKey]],
        reverse: bool = False,
    ) -> List[Dict[str, Any]]:
        """
        Sort data.

        Args:
            data: Data to sort
            keys: Sort keys (field name or SortKey objects)
            reverse: Reverse sort order

        Returns:
            Sorted data
        """
        if not data:
            return data

        sort_keys = self._normalize_keys(keys)

        return sorted(
            data,
            key=lambda x: self._make_sort_key(x, sort_keys),
            reverse=reverse,
        )

    def _normalize_keys(
        self,
        keys: Union[str, List[str], List[SortKey]],
    ) -> List[SortKey]:
        """Normalize keys to SortKey list."""
        if isinstance(keys, str):
            return [SortKey(field=keys)]
        elif isinstance(keys, list) and keys and isinstance(keys[0], str):
            return [SortKey(field=k) for k in keys]
        elif isinstance(keys, list):
            return keys
        return []

    def _make_sort_key(
        self,
        record: Dict[str, Any],
        sort_keys: List[SortKey],
    ) -> Tuple[Any, ...]:
        """Create sort key tuple for a record."""
        key_parts = []

        for sort_key in sort_keys:
            value = record.get(sort_key.field)

            if sort_key.nulls_last:
                if value is None:
                    key_parts.append((1, ""))
                else:
                    key_parts.append((0, value))
            else:
                key_parts.append((0, value))

        return tuple(key_parts)

    def sort_by_function(
        self,
        data: List[Dict[str, Any]],
        key_func: Callable[[Dict[str, Any]], Any],
        reverse: bool = False,
    ) -> List[Dict[str, Any]]:
        """
        Sort by a function.

        Args:
            data: Data to sort
            key_func: Function to extract sort key
            reverse: Reverse order

        Returns:
            Sorted data
        """
        return sorted(data, key=key_func, reverse=reverse)

    def sort_stable(
        self,
        data: List[Dict[str, Any]],
        keys: Union[str, List[str], List[SortKey]],
    ) -> List[Dict[str, Any]]:
        """
        Stable sort preserving original order for equal keys.

        Args:
            data: Data to sort
            keys: Sort keys

        Returns:
            Stable sorted data
        """
        sort_keys = self._normalize_keys(keys)

        for key in reversed(sort_keys):
            data = sorted(
                data,
                key=lambda x: self._make_sort_key(x, [key]),
            )

        return data

    def rank(
        self,
        data: List[Dict[str, Any]],
        field: str,
        key: Optional[str] = None,
        ascending: bool = True,
    ) -> List[Dict[str, Any]]:
        """
        Add rank field to data.

        Args:
            data: Data to rank
            field: Field name for rank
            key: Field to rank by (defaults to field)
            ascending: Sort ascending

        Returns:
            Data with rank field added
        """
        key = key or field
        sorted_data = self.sort(data, [SortKey(field=key, reverse=not ascending)])

        result = []
        for rank, record in enumerate(sorted_data, 1):
            new_record = record.copy()
            new_record[field] = rank
            result.append(new_record)

        return result

    def partition(
        self,
        data: List[Dict[str, Any]],
        field: str,
        bins: int,
        labels: Optional[List[str]] = None,
    ) -> List[Dict[str, Any]]:
        """
        Partition data into bins.

        Args:
            data: Data to partition
            field: Field to partition by
            bins: Number of bins
            labels: Optional bin labels

        Returns:
            Data with partition field added
        """
        values = [record.get(field) for record in data if record.get(field) is not None]

        if not values:
            return data

        min_val = min(values)
        max_val = max(values)

        if min_val == max_val:
            bin_width = 1
        else:
            bin_width = (max_val - min_val) / bins

        result = []

        for record in data:
            new_record = record.copy()
            value = record.get(field)

            if value is None:
                new_record["_partition"] = None
            else:
                bin_idx = min(int((value - min_val) / bin_width), bins - 1)

                if labels and bin_idx < len(labels):
                    new_record["_partition"] = labels[bin_idx]
                else:
                    new_record["_partition"] = bin_idx

            result.append(new_record)

        return result

    def sort_multi_criteria(
        self,
        data: List[Dict[str, Any]],
        criteria: List[Tuple[str, bool]],
    ) -> List[Dict[str, Any]]:
        """
        Sort by multiple criteria.

        Args:
            data: Data to sort
            criteria: List of (field, ascending) tuples

        Returns:
            Sorted data
        """
        sort_keys = [
            SortKey(field=field, reverse=not ascending)
            for field, ascending in criteria
        ]

        return self.sort(data, sort_keys)
