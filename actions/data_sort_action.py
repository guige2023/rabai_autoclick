"""Data sort action module.

Provides multi-key sorting, custom comparators, and natural sorting
for lists of dicts and records.
"""

from __future__ import annotations

import re
import logging
from typing import Optional, Dict, Any, List, Callable, Union
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class SortKey:
    """A sort key with direction."""
    field: str
    reverse: bool = False
    type: str = "auto"


class DataSortAction:
    """Data sorting engine.

    Supports multi-key sorting, natural sort (1, 2, 10 not 1, 10, 2),
    case-insensitive sort, and custom comparators.

    Example:
        data = [{"name": "alice", "age": 30}, {"name": "Bob", "age": 25}]
        sorted_data = DataSortAction().sort(data, ["name", "-age"])
    """

    def sort(
        self,
        data: List[Dict[str, Any]],
        keys: Union[str, List[str], List[SortKey]],
        reverse: bool = False,
    ) -> List[Dict[str, Any]]:
        """Sort a list of dicts by one or more keys.

        Args:
            data: List of dicts to sort.
            keys: Sort key(s). Can be:
                - String like "name,-age" (- = descending)
                - List like ["name", "-age"]
                - List of SortKey objects.
            reverse: Global reverse flag.

        Returns:
            Sorted list.
        """
        sort_keys = self._parse_keys(keys)

        def sort_tuple(item: Dict[str, Any]) -> tuple:
            values = []
            for key in sort_keys:
                val = item.get(key.field, "")
                if key.type == "num":
                    try:
                        val = float(val) if val is not None else 0
                    except (ValueError, TypeError):
                        val = 0
                elif key.type == "str":
                    val = str(val).lower() if val is not None else ""
                else:
                    if val is None:
                        val = ""
                    elif isinstance(val, str):
                        val = val.lower()
                values.append((key.reverse ^ reverse, val))
            return tuple(values)

        return sorted(data, key=sort_tuple)

    def sort_natural(
        self,
        data: List[Dict[str, Any]],
        field: str,
        reverse: bool = False,
    ) -> List[Dict[str, Any]]:
        """Sort by field using natural ordering (1, 2, 10 not 1, 10, 2).

        Args:
            data: List of dicts to sort.
            field: Field to sort by.
            reverse: Reverse sort order.

        Returns:
            Naturally sorted list.
        """
        def natural_key(s: str) -> tuple:
            return [int(c) if c.isdigit() else c.lower() for c in re.split(r"(\d+)", str(s))]

        def get_key(item: Dict[str, Any]) -> tuple:
            val = item.get(field, "")
            return natural_key(val)

        return sorted(data, key=get_key, reverse=reverse)

    def sort_by_predicate(
        self,
        data: List[T],
        key_func: Callable[[T], Any],
        reverse: bool = False,
    ) -> List[T]:
        """Sort using a custom key function.

        Args:
            data: List to sort.
            key_func: Function to extract sort key.
            reverse: Reverse sort order.

        Returns:
            Sorted list.
        """
        return sorted(data, key=key_func, reverse=reverse)

    def rank(
        self,
        data: List[Dict[str, Any]],
        key: str,
        descending: bool = True,
    ) -> List[Dict[str, Any]]:
        """Add a rank field to each record.

        Args:
            data: List of dicts.
            key: Field to rank by.
            descending: True = highest gets rank 1.

        Returns:
            List with added 'rank' field.
        """
        sorted_data = self.sort(data, [key], reverse=descending)
        for i, item in enumerate(sorted_data, 1):
            item["rank"] = i
        return sorted_data

    def percentile_bins(
        self,
        data: List[Dict[str, Any]],
        field: str,
        bins: int = 5,
    ) -> List[Dict[str, Any]]:
        """Bin data into percentile groups.

        Args:
            data: List of dicts.
            field: Numeric field to bin by.
            bins: Number of percentile bins.

        Returns:
            List with added 'percentile_bin' field.
        """
        sorted_data = sorted(data, key=lambda x: x.get(field, 0))
        n = len(sorted_data)
        if n == 0:
            return data

        bin_size = n / bins
        for i, item in enumerate(sorted_data):
            bin_idx = min(int(i / bin_size), bins - 1)
            item["percentile_bin"] = bin_idx + 1

        return sorted_data

    def _parse_keys(self, keys: Union[str, List[str], List[SortKey]]) -> List[SortKey]:
        """Parse sort keys from various input formats."""
        if isinstance(keys, str):
            keys = [k.strip() for k in keys.split(",")]

        if not keys:
            return []

        result = []
        for key in keys:
            if isinstance(key, SortKey):
                result.append(key)
            elif isinstance(key, str):
                if key.startswith("-"):
                    result.append(SortKey(field=key[1:], reverse=True))
                elif key.startswith("+"):
                    result.append(SortKey(field=key[1:], reverse=False))
                else:
                    result.append(SortKey(field=key, reverse=False))
            else:
                result.append(SortKey(field=str(key), reverse=False))

        return result


def natural_sort_key(s: str) -> tuple:
    """Generate a natural sort key for a string.

    Example: natural_sort_key("file10") -> ("file", 10)
    """
    return [int(c) if c.isdigit() else c.lower() for c in re.split(r"(\d+)", str(s))]
