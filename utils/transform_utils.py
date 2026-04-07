"""Data transformation utilities: mapping, flattening, pivoting, and reshaping."""

from __future__ import annotations

import json
from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Callable, Generator

__all__ = [
    "DataMapper",
    "Flattener",
    "Transformer",
    "pivot",
    "unpivot",
    "group_by",
]


class DataMapper:
    """Map fields from one structure to another using dot notation or path expressions."""

    def __init__(self, mapping: dict[str, str] | None = None) -> None:
        self._mapping = mapping or {}

    def add_mapping(self, source: str, target: str) -> None:
        self._mapping[source] = target

    def map(self, data: dict[str, Any]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for src, tgt in self._mapping.items():
            val = self._get_nested(data, src)
            if val is not None:
                self._set_nested(result, tgt, val)
        return result

    def map_list(self, items: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return [self.map(item) for item in items]

    def _get_nested(self, data: dict[str, Any], path: str) -> Any:
        parts = path.split(".")
        current = data
        for part in parts:
            if isinstance(current, dict):
                current = current.get(part)
            else:
                return None
            if current is None:
                return None
        return current

    def _set_nested(self, data: dict[str, Any], path: str, value: Any) -> None:
        parts = path.split(".")
        current = data
        for part in parts[:-1]:
            if part not in current:
                current[part] = {}
            current = current[part]
        current[parts[-1]] = value


class Flattener:
    """Flatten nested data structures into dot-notation keys."""

    def __init__(self, separator: str = ".") -> None:
        self.separator = separator

    def flatten(self, data: Any, prefix: str = "") -> dict[str, Any]:
        result: dict[str, Any] = {}
        if isinstance(data, dict):
            for key, value in data.items():
                new_key = f"{prefix}{self.separator}{key}" if prefix else key
                if isinstance(value, dict):
                    result.update(self.flatten(value, new_key))
                elif isinstance(value, list):
                    for i, item in enumerate(value):
                        result.update(self.flatten(item, f"{new_key}[{i}]"))
                else:
                    result[new_key] = item if (item := value) is not None else None
        elif isinstance(data, list):
            for i, item in enumerate(data):
                new_key = f"{prefix}[{i}]"
                result.update(self.flatten(item, new_key))
        else:
            if prefix:
                result[prefix] = data
        return result

    def unflatten(self, data: dict[str, Any]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in data.items():
            self._set_path(result, key, value)
        return result

    def _set_path(self, data: dict[str, Any], path: str, value: Any) -> None:
        parts = path.split(self.separator)
        current = data
        for part in parts[:-1]:
            if part not in current:
                current[part] = {}
            current = current[part]
        current[parts[-1]] = value


class Transformer:
    """General-purpose data transformation pipeline."""

    def __init__(self) -> None:
        self._steps: list[Callable[[Any], Any]] = []

    def add_step(self, fn: Callable[[Any], Any]) -> "Transformer":
        self._steps.append(fn)
        return self

    def transform(self, data: Any) -> Any:
        result = data
        for step in self._steps:
            result = step(result)
        return result

    def transform_many(self, items: list[Any]) -> list[Any]:
        return [self.transform(item) for item in items]


def pivot(
    data: list[dict[str, Any]],
    row_key: str,
    col_key: str,
    value_key: str,
    agg_fn: Callable[[list[Any]], Any] = sum,
) -> dict[str, dict[str, Any]]:
    """Pivot a list of dicts into a matrix."""
    matrix: dict[str, dict[str, Any]] = defaultdict(dict)
    for row in data:
        row_val = row.get(row_key)
        col_val = row.get(col_key)
        val = row.get(value_key)
        if row_val is not None and col_val is not None:
            matrix[row_val][col_val] = val
    return dict(matrix)


def unpivot(
    matrix: dict[str, dict[str, Any]],
    row_key_name: str,
    col_key_name: str,
    value_key_name: str,
) -> list[dict[str, Any]]:
    """Unpivot a matrix back into rows."""
    rows: list[dict[str, Any]] = []
    for row_key, cols in matrix.items():
        for col_key, value in cols.items():
            rows.append({
                row_key_name: row_key,
                col_key_name: col_key,
                value_key_name: value,
            })
    return rows


def group_by(
    items: list[dict[str, Any]],
    key: str,
) -> dict[str, list[dict[str, Any]]]:
    """Group items by a key."""
    groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for item in items:
        key_val = item.get(key)
        if key_val is not None:
            groups[str(key_val)].append(item)
    return dict(groups)
