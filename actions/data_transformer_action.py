"""Data Transformer Action Module.

Provides data transformation with mapping, conversion, reshape,
and schema evolution capabilities.
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Set, TypeVar, Union
from enum import Enum

T = TypeVar("T")


class TransformType(Enum):
    """Data transformation type."""
    MAP = "map"
    FLATMAP = "flatmap"
    RESHAPE = "reshape"
    ENRICH = "enrich"
    PIVOT = "pivot"
    UNPIVOT = "unpivot"


@dataclass
class FieldMapping:
    """Field mapping definition."""
    source_field: str
    target_field: str
    transformer: Optional[Callable[[Any], Any]] = None
    default: Any = None
    required: bool = False


@dataclass
class TransformConfig:
    """Transformation configuration."""
    mappings: List[FieldMapping]
    drop_fields: Optional[List[str]] = None
    rename_fields: Optional[Dict[str, str]] = None
    computed_fields: Optional[Dict[str, Callable]] = None
    filter_nulls: bool = False


class DataTransformerAction:
    """Data transformer with mapping and reshaping.

    Example:
        transformer = DataTransformerAction()

        config = TransformConfig(
            mappings=[
                FieldMapping("id", "record_id"),
                FieldMapping("name", "full_name"),
                FieldMapping("price", "cost", transformer=lambda x: x * 1.1),
            ],
            drop_fields=["internal_id", "temp_field"],
            computed_fields={
                "total": lambda r: r.get("price", 0) * r.get("quantity", 1)
            }
        )

        result = transformer.transform(data, config)
    """

    def __init__(self) -> None:
        self._custom_transformers: Dict[str, Callable] = {}

    def register_transformer(
        self,
        name: str,
        func: Callable[[Any], Any],
    ) -> None:
        """Register a named transformer function."""
        self._custom_transformers[name] = func

    def transform(
        self,
        data: Union[Dict, List[Dict]],
        config: TransformConfig,
    ) -> Union[Dict, List[Dict]]:
        """Transform data according to configuration.

        Args:
            data: Single record or list of records
            config: Transformation configuration

        Returns:
            Transformed data
        """
        if isinstance(data, list):
            return [self._transform_record(record, config) for record in data]
        return self._transform_record(data, config)

    def _transform_record(
        self,
        record: Dict[str, Any],
        config: TransformConfig,
    ) -> Dict[str, Any]:
        """Transform single record."""
        result: Dict[str, Any] = {}

        for mapping in config.mappings:
            value = record.get(mapping.source_field, mapping.default)

            if value is None and mapping.required:
                continue

            if mapping.transformer:
                value = mapping.transformer(value)

            result[mapping.target_field] = value

        if config.drop_fields:
            for field_name in config.drop_fields:
                result.pop(field_name, None)

        if config.rename_fields:
            for old_name, new_name in config.rename_fields.items():
                if old_name in result:
                    result[new_name] = result.pop(old_name)

        if config.computed_fields:
            for field_name, func in config.computed_fields.items():
                result[field_name] = func(record)

        if config.filter_nulls:
            result = {k: v for k, v in result.items() if v is not None}

        return result

    def reshape(
        self,
        data: List[Dict[str, Any]],
        index_field: str,
        value_field: str,
    ) -> Dict[str, Any]:
        """Reshape data from list to dict keyed by index.

        Args:
            data: List of records
            index_field: Field to use as key
            value_field: Field to use as value

        Returns:
            Dict keyed by index_field
        """
        return {
            record[index_field]: record[value_field]
            for record in data
            if index_field in record and value_field in record
        }

    def enrich(
        self,
        primary: List[Dict[str, Any]],
        lookup: Dict[str, Any],
        key_field: str,
        lookup_fields: Optional[List[str]] = None,
    ) -> List[Dict[str, Any]]:
        """Enrich data with lookup values.

        Args:
            primary: Primary data records
            lookup: Lookup dictionary
            key_field: Field to join on
            lookup_fields: Specific fields to add (None = all)

        Returns:
            Enriched records
        """
        results = []

        for record in primary:
            key = record.get(key_field)
            if key is None:
                results.append(record)
                continue

            enriched = dict(record)

            if key in lookup:
                lookup_data = lookup[key]
                if lookup_fields:
                    for field_name in lookup_fields:
                        enriched[field_name] = lookup_data.get(field_name)
                else:
                    for field_name, value in lookup_data.items():
                        enriched[field_name] = value

            results.append(enriched)

        return results

    def pivot_transform(
        self,
        data: List[Dict[str, Any]],
        row_fields: List[str],
        col_field: str,
        val_field: str,
    ) -> List[Dict[str, Any]]:
        """Pivot data from long to wide format.

        Args:
            data: Long format data
            row_fields: Fields to keep as rows
            col_field: Field to use as column names
            val_field: Field to use as values

        Returns:
            Wide format data
        """
        pivoted: Dict[tuple, Dict[str, Any]] = {}

        for record in data:
            row_key = tuple(record.get(f) for f in row_fields)
            col_name = record.get(col_field)
            value = record.get(val_field)

            if row_key not in pivoted:
                pivoted[row_key] = {f: record.get(f) for f in row_fields}

            if col_name is not None:
                pivoted[row_key][col_name] = value

        return list(pivoted.values())

    def unpivot_transform(
        self,
        data: List[Dict[str, Any]],
        id_fields: List[str],
        value_names: List[str],
        value_name_field: str = "name",
        value_field: str = "value",
    ) -> List[Dict[str, Any]]:
        """Unpivot data from wide to long format.

        Args:
            data: Wide format data
            id_fields: Fields to keep as identifiers
            value_names: Names of the value columns
            value_name_field: Name of the field for value names
            value_field: Name of the field for values

        Returns:
            Long format data
        """
        results = []

        for record in data:
            base = {f: record.get(f) for f in id_fields}

            for val_name in value_names:
                if val_name in record:
                    result = dict(base)
                    result[value_name_field] = val_name
                    result[value_field] = record[val_name]
                    results.append(result)

        return results

    def flatten(
        self,
        data: Dict[str, Any],
        separator: str = ".",
        prefix: str = "",
    ) -> Dict[str, Any]:
        """Flatten nested dict to dot-notation keys.

        Args:
            data: Nested dict
            separator: Key separator
            prefix: Key prefix

        Returns:
            Flattened dict
        """
        result: Dict[str, Any] = {}

        for key, value in data.items():
            new_key = f"{prefix}{separator}{key}" if prefix else key

            if isinstance(value, dict):
                result.update(self.flatten(value, separator, new_key))
            elif isinstance(value, list):
                for i, item in enumerate(value):
                    if isinstance(item, dict):
                        result.update(self.flatten(item, separator, f"{new_key}[{i}]"))
                    else:
                        result[f"{new_key}[{i}]"] = item
            else:
                result[new_key] = value

        return result

    def unflatten(
        self,
        data: Dict[str, Any],
        separator: str = ".",
    ) -> Dict[str, Any]:
        """Unflatten dot-notation keys to nested dict.

        Args:
            data: Flattened dict
            separator: Key separator

        Returns:
            Nested dict
        """
        result: Dict[str, Any] = {}

        for flat_key, value in data.items():
            keys = flat_key.split(separator)
            current = result

            for key in keys[:-1]:
                if key not in current:
                    current[key] = {}
                current = current[key]

            current[keys[-1]] = value

        return result
