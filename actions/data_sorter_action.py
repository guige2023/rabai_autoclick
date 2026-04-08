"""
Data Sorter Action Module.

Sorts datasets by multiple fields with ascending/descending
 order support and custom comparator functions.
"""

from __future__ import annotations

from typing import Any, Callable, Optional
from dataclasses import dataclass, field
from enum import Enum
import logging

logger = logging.getLogger(__name__)


class SortDirection(Enum):
    """Sort direction."""
    ASC = "asc"
    DESC = "desc"


@dataclass
class SortKey:
    """A sort key specification."""
    field: str
    direction: SortDirection = SortDirection.ASC
    numeric: bool = False
    custom_key: Optional[Callable[[Any], Any]] = None


@dataclass
class SortResult:
    """Result of a sort operation."""
    success: bool
    data: list[dict[str, Any]]
    sort_keys: list[SortKey]
    comparisons: int = 0


class DataSorterAction:
    """
    Multi-field sorting for datasets.

    Sorts records by one or more fields with support for
    ascending/descending order and custom comparators.

    Example:
        sorter = DataSorterAction()
        sorter.add_key("age", SortDirection.ASC, numeric=True)
        sorter.add_key("name", SortDirection.ASC)
        result = sorter.sort(data)
    """

    def __init__(
        self,
        case_sensitive: bool = True,
        null_first: bool = False,
    ) -> None:
        self.case_sensitive = case_sensitive
        self.null_first = null_first
        self._sort_keys: list[SortKey] = []

    def add_key(
        self,
        field: str,
        direction: SortDirection = SortDirection.ASC,
        numeric: bool = False,
        custom_key: Optional[Callable[[Any], Any]] = None,
    ) -> "DataSorterAction":
        """Add a sort key."""
        key = SortKey(
            field=field,
            direction=direction,
            numeric=numeric,
            custom_key=custom_key,
        )
        self._sort_keys.append(key)
        return self

    def sort(
        self,
        data: list[dict[str, Any]],
    ) -> SortResult:
        """Sort data by configured sort keys."""
        if not data or not self._sort_keys:
            return SortResult(success=True, data=list(data), sort_keys=self._sort_keys)

        sorted_data = sorted(
            data,
            key=lambda x: self._make_composite_key(x),
            reverse=False,
        )

        return SortResult(
            success=True,
            data=sorted_data,
            sort_keys=self._sort_keys,
        )

    def _make_composite_key(
        self,
        record: dict[str, Any],
    ) -> tuple:
        """Create a composite sort key from record."""
        keys: list = []

        for sort_key in self._sort_keys:
            value = record.get(sort_key.field)

            if sort_key.custom_key:
                keys.append(sort_key.custom_key(value))
                continue

            if value is None:
                keys.append((1 if self.null_first else 0, ""))
                continue

            if sort_key.numeric:
                try:
                    keys.append((0, float(value)))
                except (ValueError, TypeError):
                    keys.append((0, value))
                continue

            if not self.case_sensitive and isinstance(value, str):
                keys.append((0, value.lower()))
            else:
                keys.append((0, value))

        return tuple(keys)

    def sort_by_single_field(
        self,
        data: list[dict[str, Any]],
        field: str,
        direction: SortDirection = SortDirection.ASC,
        numeric: bool = False,
    ) -> list[dict[str, Any]]:
        """Sort data by a single field."""
        self._sort_keys = [SortKey(field=field, direction=direction, numeric=numeric)]
        return self.sort(data).data

    def sort_stable(
        self,
        data: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """Sort data using Python's stable sort."""
        result = list(data)

        for key in reversed(self._sort_keys):
            result = sorted(
                result,
                key=lambda x: self._make_single_key(x, key),
                reverse=(key.direction == SortDirection.DESC),
            )

        return result

    def _make_single_key(
        self,
        record: dict[str, Any],
        key: SortKey,
    ) -> Any:
        """Create a single-field sort key."""
        value = record.get(key.field)

        if value is None:
            return ("",) if self.null_first else ("zzz",)

        if key.numeric:
            try:
                return float(value)
            except (ValueError, TypeError):
                return value

        if not self.case_sensitive and isinstance(value, str):
            return value.lower()

        return value
