"""Data transformation utilities for common data operations.

Supports filtering, mapping, grouping, sorting, and aggregations.
"""

from __future__ import annotations

import logging
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Callable, Generator, TypeVar

logger = logging.getLogger(__name__)

T = TypeVar("T")
K = TypeVar("K")
V = TypeVar("V")


@dataclass
class TransformResult:
    """Result of a transformation operation."""

    data: list[Any]
    errors: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)


class DataTransformer:
    """Composable data transformation pipeline."""

    def __init__(self, data: list[dict[str, Any]] | None = None) -> None:
        self._data = data or []
        self._errors: list[str] = []
        self._metadata: dict[str, Any] = {}

    def filter(self, predicate: Callable[[dict[str, Any]], bool]) -> "DataTransformer":
        """Filter rows matching predicate."""
        self._data = [row for row in self._data if predicate(row)]
        return self

    def map(self, fn: Callable[[dict[str, Any]], dict[str, Any]]) -> "DataTransformer":
        """Transform each row with function."""
        result = []
        for row in self._data:
            try:
                result.append(fn(row))
            except Exception as e:
                self._errors.append(f"Map error: {e}")
        self._data = result
        return self

    def flat_map(self, fn: Callable[[dict[str, Any]], list[dict[str, Any]]]) -> "DataTransformer":
        """Map and flatten results."""
        result = []
        for row in self._data:
            try:
                result.extend(fn(row))
            except Exception as e:
                self._errors.append(f"Flat map error: {e}")
        self._data = result
        return self

    def select(self, *keys: str, rename: dict[str, str] | None = None) -> "DataTransformer":
        """Select and optionally rename columns."""
        rename = rename or {}
        result = []
        for row in self._data:
            new_row = {}
            for key in keys:
                if key in row:
                    new_key = rename.get(key, key)
                    new_row[new_key] = row[key]
            result.append(new_row)
        self._data = result
        return self

    def drop(self, *keys: str) -> "DataTransformer":
        """Drop specified columns."""
        result = []
        for row in self._data:
            new_row = {k: v for k, v in row.items() if k not in keys}
            result.append(new_row)
        self._data = result
        return self

    def add_column(self, name: str, fn: Callable[[dict[str, Any]], Any]) -> "DataTransformer":
        """Add computed column."""
        for row in self._data:
            try:
                row[name] = fn(row)
            except Exception as e:
                row[name] = None
                self._errors.append(f"Add column error ({name}): {e}")
        return self

    def where(self, **conditions: Any) -> "DataTransformer":
        """Filter by exact column matches."""
        result = []
        for row in self._data:
            match = all(row.get(k) == v for k, v in conditions.items())
            if match:
                result.append(row)
        self._data = result
        return self

    def sort(self, key: str | Callable[[dict[str, Any]], Any], reverse: bool = False) -> "DataTransformer":
        """Sort by key or function."""
        if callable(key):
            self._data = sorted(self._data, key=key, reverse=reverse)
        else:
            self._data = sorted(self._data, key=lambda r: r.get(key), reverse=reverse)
        return self

    def distinct(self, *keys: str) -> "DataTransformer":
        """Remove duplicate rows, optionally by specific keys."""
        if not keys:
            seen = set()
            result = []
            for row in self._data:
                row_tuple = tuple(sorted(row.items()))
                if row_tuple not in seen:
                    seen.add(row_tuple)
                    result.append(row)
            self._data = result
        else:
            seen = set()
            result = []
            for row in self._data:
                key_tuple = tuple(row.get(k) for k in keys)
                if key_tuple not in seen:
                    seen.add(key_tuple)
                    result.append(row)
            self._data = result
        return self

    def limit(self, n: int) -> "DataTransformer":
        """Limit to first n rows."""
        self._data = self._data[:n]
        return self

    def skip(self, n: int) -> "DataTransformer":
        """Skip first n rows."""
        self._data = self._data[n:]
        return self

    def group_by(self, key: str | Callable[[dict[str, Any]], Any]) -> dict[Any, list[dict[str, Any]]]:
        """Group rows by key."""
        groups: dict[Any, list[dict[str, Any]]] = defaultdict(list)
        for row in self._data:
            if callable(key):
                group_key = key(row)
            else:
                group_key = row.get(key)
            groups[group_key].append(row)
        return dict(groups)

    def aggregate(
        self,
        group_key: str | Callable[[dict[str, Any]], Any],
        aggregations: dict[str, Callable[[list[Any]], Any]],
    ) -> list[dict[str, Any]]:
        """Aggregate grouped data.

        Args:
            group_key: Key to group by.
            aggregations: Dict of {result_column: (source_column, aggregation_fn)}.
        """
        groups = self.group_by(group_key)
        results = []
        for g_key, rows in groups.items():
            result = {"_group": g_key}
            for agg_name, (src_col, agg_fn) in aggregations.items():
                values = [row.get(src_col) for row in rows if src_col in row]
                try:
                    result[agg_name] = agg_fn(values)
                except Exception as e:
                    result[agg_name] = None
                    self._errors.append(f"Aggregation error ({agg_name}): {e}")
            results.append(result)
        return results

    def pivot(
        self,
        index: str,
        columns: str,
        values: str,
        agg_func: Callable[[list[Any]], Any] = sum,
    ) -> list[dict[str, Any]]:
        """Pivot data from long to wide format."""
        groups = self.group_by(index)
        all_column_values = set()
        for rows in groups.values():
            for row in rows:
                all_column_values.add(row.get(columns))

        results = []
        for idx_val, rows in groups.items():
            result = {index: idx_val}
            col_groups = defaultdict(list)
            for row in rows:
                col_groups[row.get(columns)].append(row.get(values))
            for col_val, vals in col_groups.items():
                try:
                    result[str(col_val)] = agg_func(vals)
                except Exception:
                    result[str(col_val)] = None
            results.append(result)
        return results

    def unpivot(
        self,
        id_columns: list[str],
        value_columns: list[str],
        var_column: str = "variable",
        val_column: str = "value",
    ) -> "DataTransformer":
        """Unpivot data from wide to long format."""
        result = []
        for row in self._data:
            id_vals = {col: row.get(col) for col in id_columns if col in row}
            for val_col in value_columns:
                if val_col in row:
                    new_row = {**id_vals, var_column: val_col, val_column: row.get(val_col)}
                    result.append(new_row)
        self._data = result
        return self

    def join(
        self,
        other: list[dict[str, Any]],
        left_key: str,
        right_key: str,
        how: str = "inner",
    ) -> "DataTransformer":
        """Join with another dataset."""
        other_index = {row.get(right_key): row for row in other if row.get(right_key) is not None}
        results = []

        for left_row in self._data:
            l_key = left_row.get(left_key)
            right_row = other_index.get(l_key)

            if right_row:
                results.append({**left_row, **right_row})
            elif how == "left":
                results.append({**left_row, **{k: None for k in other[0] if k != right_key}})
            elif how == "right":
                pass
            elif how == "outer":
                results.append({**left_row})

        if how == "right":
            right_rows = [r for r in other if r.get(right_key) not in self._data]
            for r in right_rows:
                new_row = {**r}
                results.append(new_row)

        self._data = results
        return self

    def union(self, other: list[dict[str, Any]]) -> "DataTransformer":
        """Union with another dataset."""
        self._data = self._data + other
        return self

    def intersect(self, other: list[dict[str, Any]], keys: list[str]) -> "DataTransformer":
        """Intersect with another dataset by key columns."""
        other_keys = set(tuple(row.get(k) for k in keys) for row in other)
        self._data = [row for row in self._data if tuple(row.get(k) for k in keys) in other_keys]
        return self

    def to_list(self) -> list[dict[str, Any]]:
        """Return the transformed data."""
        return self._data

    def result(self) -> TransformResult:
        """Return result with metadata and errors."""
        return TransformResult(data=self._data, errors=self._errors, metadata=self._metadata)

    def __len__(self) -> int:
        return len(self._data)


class Aggregator:
    """Common aggregation functions."""

    @staticmethod
    def sum(values: list[Any]) -> float:
        """Sum of values."""
        return sum(v for v in values if v is not None)

    @staticmethod
    def avg(values: list[Any]) -> float:
        """Average of values."""
        filtered = [v for v in values if v is not None]
        return sum(filtered) / len(filtered) if filtered else 0.0

    @staticmethod
    def min(values: list[Any]) -> Any:
        """Minimum value."""
        filtered = [v for v in values if v is not None]
        return min(filtered) if filtered else None

    @staticmethod
    def max(values: list[Any]) -> Any:
        """Maximum value."""
        filtered = [v for v in values if v is not None]
        return max(filtered) if filtered else None

    @staticmethod
    def count(values: list[Any]) -> int:
        """Count of non-null values."""
        return sum(1 for v in values if v is not None)

    @staticmethod
    def count_distinct(values: list[Any]) -> int:
        """Count of distinct values."""
        return len(set(v for v in values if v is not None))

    @staticmethod
    def first(values: list[Any]) -> Any:
        """First value."""
        return next((v for v in values if v is not None), None)

    @staticmethod
    def last(values: list[Any]) -> Any:
        """Last value."""
        for v in reversed(values):
            if v is not None:
                return v
        return None

    @staticmethod
    def concat(values: list[Any], separator: str = ",") -> str:
        """Concatenate values."""
        return separator.join(str(v) for v in values if v is not None)


def transform(data: list[dict[str, Any]]) -> DataTransformer:
    """Start a transformation chain."""
    return DataTransformer(data)


def safe_get(data: dict[str, Any], path: str, default: Any = None) -> Any:
    """Safely get nested dict value using dot notation.

    Args:
        data: Dictionary to traverse.
        path: Dot-separated path (e.g., 'user.address.city').
        default: Default value if path not found.

    Returns:
        Value at path or default.
    """
    keys = path.split(".")
    value = data
    for key in keys:
        if isinstance(value, dict):
            value = value.get(key)
        else:
            return default
        if value is None:
            return default
    return value


def flatten(data: dict[str, Any], separator: str = ".", prefix: str = "") -> dict[str, Any]:
    """Flatten nested dictionary.

    Args:
        data: Nested dictionary.
        separator: Key separator for flattened keys.
        prefix: Prefix for top-level keys.

    Returns:
        Flattened dictionary.
    """
    result = {}
    for key, value in data.items():
        new_key = f"{prefix}{separator}{key}" if prefix else key
        if isinstance(value, dict):
            result.update(flatten(value, separator, new_key))
        elif isinstance(value, list):
            for i, item in enumerate(value):
                if isinstance(item, dict):
                    result.update(flatten(item, separator, f"{new_key}[{i}]"))
                else:
                    result[f"{new_key}[{i}]"] = item
        else:
            result[new_key] = value
    return result
