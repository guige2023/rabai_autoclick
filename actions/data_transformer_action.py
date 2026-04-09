"""Data transformer action module.

Provides data transformation operations:
- DataTransformer: Generic data transformations
- FieldMapper: Map and rename fields
- DataConverter: Convert between data formats
- RowTransformer: Row-level transformations
- AggregationTransformer: Aggregation operations
"""

from __future__ import annotations

import json
from typing import Any, Callable, Dict, List, Optional, Tuple, Union
from dataclasses import dataclass, field
from enum import Enum
import logging

logger = logging.getLogger(__name__)


class TransformType(Enum):
    """Type of transformation."""
    MAP = "map"
    FILTER = "filter"
    REDUCE = "reduce"
    SORT = "sort"
    GROUP = "group"
    MERGE = "merge"
    SPLIT = "split"


@dataclass
class TransformConfig:
    """Configuration for a transformation."""
    name: str
    transform_type: TransformType
    params: Dict[str, Any] = field(default_factory=dict)


@dataclass
class TransformResult:
    """Result of a transformation."""
    success: bool
    data: Any
    transformed_count: int = 0
    error: Optional[str] = None


class FieldMapper:
    """Map and transform fields in records."""

    def __init__(
        self,
        mapping: Optional[Dict[str, str]] = None,
        transforms: Optional[Dict[str, Callable]] = None,
    ):
        self.mapping = mapping or {}
        self.transforms = transforms or {}

    def map_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Map a single record."""
        result = {}
        for source_field, target_field in self.mapping.items():
            if source_field in record:
                value = record[source_field]
                if source_field in self.transforms:
                    value = self.transforms[source_field](value)
                result[target_field] = value
        for field_name, value in record.items():
            if field_name not in self.mapping:
                result[field_name] = value
        return result

    def map_records(self, records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Map multiple records."""
        return [self.map_record(r) for r in records]

    def add_mapping(self, source: str, target: str) -> None:
        """Add a field mapping."""
        self.mapping[source] = target

    def add_transform(self, field_name: str, transform_fn: Callable) -> None:
        """Add a field transformation."""
        self.transforms[field_name] = transform_fn


class DataConverter:
    """Convert data between formats."""

    @staticmethod
    def to_json(
        data: Any,
        pretty: bool = False,
        ensure_ascii: bool = False,
    ) -> str:
        """Convert data to JSON string."""
        if pretty:
            return json.dumps(data, indent=2, ensure_ascii=ensure_ascii)
        return json.dumps(data, ensure_ascii=ensure_ascii)

    @staticmethod
    def from_json(json_str: str) -> Any:
        """Parse JSON string."""
        return json.loads(json_str)

    @staticmethod
    def flatten(
        data: Dict[str, Any],
        separator: str = ".",
        parent_key: str = "",
    ) -> Dict[str, Any]:
        """Flatten a nested dictionary."""
        items: List[Tuple[str, Any]] = []
        for key, value in data.items():
            new_key = f"{parent_key}{separator}{key}" if parent_key else key
            if isinstance(value, dict):
                items.extend(DataConverter.flatten(value, separator, new_key).items())
            elif isinstance(value, list):
                for i, item in enumerate(value):
                    if isinstance(item, dict):
                        items.extend(
                            DataConverter.flatten(item, separator, f"{new_key}[{i}]").items()
                        )
                    else:
                        items.append((f"{new_key}[{i}]", item))
            else:
                items.append((new_key, value))
        return dict(items)

    @staticmethod
    def unflatten(data: Dict[str, Any], separator: str = ".") -> Dict[str, Any]:
        """Unflatten a dictionary."""
        result: Dict[str, Any] = {}
        for key, value in data.items():
            parts = key.split(separator)
            current = result
            for part in parts[:-1]:
                if part not in current:
                    current[part] = {}
                current = current[part]
            current[parts[-1]] = value
        return result


class RowTransformer:
    """Row-level data transformations."""

    def __init__(
        self,
        row_filter: Optional[Callable[[Dict[str, Any]], bool]] = None,
        row_mapper: Optional[Callable[[Dict[str, Any]], Dict[str, Any]]] = None,
    ):
        self.row_filter = row_filter
        self.row_mapper = row_mapper

    def transform(
        self,
        rows: List[Dict[str, Any]],
    ) -> TransformResult:
        """Transform rows with filter and mapper."""
        transformed = []
        errors = []

        for row in rows:
            try:
                if self.row_filter and not self.row_filter(row):
                    continue
                if self.row_mapper:
                    row = self.row_mapper(row)
                transformed.append(row)
            except Exception as e:
                errors.append(str(e))

        return TransformResult(
            success=len(errors) == 0,
            data=transformed,
            transformed_count=len(transformed),
            error="; ".join(errors) if errors else None,
        )

    def filter_rows(
        self,
        rows: List[Dict[str, Any]],
        filter_fn: Callable[[Dict[str, Any]], bool],
    ) -> List[Dict[str, Any]]:
        """Filter rows."""
        return [r for r in rows if filter_fn(r)]

    def sort_rows(
        self,
        rows: List[Dict[str, Any]],
        key_fn: Callable[[Dict[str, Any]], Any],
        reverse: bool = False,
    ) -> List[Dict[str, Any]]:
        """Sort rows."""
        return sorted(rows, key=key_fn, reverse=reverse)


class DataTransformer:
    """General-purpose data transformer."""

    def __init__(self):
        self._pipeline: List[Callable] = []

    def add_step(self, fn: Callable[[Any], Any]) -> "DataTransformer":
        """Add a transformation step to the pipeline."""
        self._pipeline.append(fn)
        return self

    def transform(self, data: Any) -> Any:
        """Execute the transformation pipeline."""
        result = data
        for step in self._pipeline:
            result = step(result)
        return result

    def transform_dict(
        self,
        record: Dict[str, Any],
        field_transforms: Dict[str, Callable],
    ) -> Dict[str, Any]:
        """Transform specific fields in a dictionary."""
        result = dict(record)
        for field_name, transform_fn in field_transforms.items():
            if field_name in result:
                result[field_name] = transform_fn(result[field_name])
        return result

    def batch_transform(
        self,
        records: List[Dict[str, Any]],
        field_transforms: Dict[str, Callable],
    ) -> List[Dict[str, Any]]:
        """Transform fields in a batch of records."""
        return [self.transform_dict(r, field_transforms) for r in records]


def transform_data(
    data: Any,
    transforms: List[Callable[[Any], Any]],
) -> Any:
    """Apply a list of transformations to data."""
    result = data
    for t in transforms:
        result = t(result)
    return result


def filter_records(
    records: List[Dict[str, Any]],
    filter_fn: Callable[[Dict[str, Any]], bool],
) -> List[Dict[str, Any]]:
    """Filter records using a predicate."""
    return [r for r in records if filter_fn(r)]
