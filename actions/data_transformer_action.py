"""
Data Transformer Action Module.

Provides comprehensive data transformation utilities including
mapping, filtering, aggregation, and format conversion.

Author: rabai_autoclick team
"""

import logging
from typing import (
    Optional, Dict, Any, List, Callable, Union, TypeVar, Generic
)
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum

logger = logging.getLogger(__name__)

T = TypeVar("T")


class TransformType(Enum):
    """Types of data transformations."""
    MAP = "map"
    FILTER = "filter"
    REDUCE = "reduce"
    FLATTEN = "flatten"
    GROUP = "group"
    SORT = "sort"
    MERGE = "merge"
    SPLIT = "split"


@dataclass
class TransformConfig:
    """Configuration for a data transformation."""
    source_key: str
    target_key: Optional[str] = None
    transform_func: Optional[Callable] = None
    default_value: Any = None
    required: bool = False
    type_hint: Optional[type] = None


@dataclass
class DataMapRule:
    """Rule for mapping data from source to target."""
    source: str
    target: str
    transform: Optional[Callable] = None
    default: Any = None
    required: bool = False


class DataTransformerAction:
    """
    Data Transformation Engine.

    Provides declarative data transformation with validation,
    error handling, and support for complex data structures.

    Example:
        >>> transformer = DataTransformerAction()
        >>> transformer.add_rule(DataMapRule(source="name", target="full_name", transform=str.upper))
        >>> result = transformer.transform({"name": "john"})
    """

    def __init__(self):
        self._rules: List[DataMapRule] = []
        self._transforms: Dict[str, Callable] = {}
        self._validators: Dict[str, Callable] = {}

    def add_rule(self, rule: DataMapRule) -> "DataTransformerAction":
        """
        Add a mapping rule.

        Args:
            rule: DataMapRule defining the transformation

        Returns:
            Self for chaining
        """
        self._rules.append(rule)
        return self

    def register_transform(self, name: str, func: Callable) -> "DataTransformerAction":
        """
        Register a named transformation function.

        Args:
            name: Name for the transform
            func: Transformation function

        Returns:
            Self for chaining
        """
        self._transforms[name] = func
        return self

    def register_validator(self, field: str, validator: Callable) -> "DataTransformerAction":
        """
        Register a field validator.

        Args:
            field: Field name to validate
            validator: Function that returns bool

        Returns:
            Self for chaining
        """
        self._validators[field] = validator
        return self

    def transform(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Transform data according to registered rules.

        Args:
            data: Input data dictionary

        Returns:
            Transformed data dictionary
        """
        result = {}

        for rule in self._rules:
            value = data.get(rule.source, rule.default)

            if value is None and rule.required:
                raise ValueError(f"Required field '{rule.source}' is missing")

            if value is not None and rule.transform:
                try:
                    value = rule.transform(value)
                except Exception as e:
                    raise ValueError(f"Transform failed for '{rule.source}': {e}")

            result[rule.target] = value

        return result

    def transform_batch(
        self, data_list: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        Transform a batch of data records.

        Args:
            data_list: List of input data dictionaries

        Returns:
            List of transformed data dictionaries
        """
        return [self.transform(data) for data in data_list]

    def map_values(
        self,
        data: Dict[str, Any],
        mapping: Dict[str, Callable],
    ) -> Dict[str, Any]:
        """
        Apply multiple transformations to data values.

        Args:
            data: Input data
            mapping: Dict mapping keys to transform functions

        Returns:
            Transformed data
        """
        result = data.copy()
        for key, transform in mapping.items():
            if key in result:
                try:
                    result[key] = transform(result[key])
                except Exception as e:
                    logger.warning(f"Transform failed for key '{key}': {e}")
        return result

    def filter_fields(
        self,
        data: Dict[str, Any],
        fields: List[str],
        exclude: bool = False,
    ) -> Dict[str, Any]:
        """
        Filter data fields.

        Args:
            data: Input data
            fields: List of field names
            exclude: If True, exclude listed fields instead of including

        Returns:
            Filtered data
        """
        if exclude:
            return {k: v for k, v in data.items() if k not in fields}
        return {k: v for k, v in data.items() if k in fields}

    def flatten(
        self,
        data: Any,
        separator: str = ".",
        parent_key: str = "",
    ) -> Dict[str, Any]:
        """
        Flatten nested data structures.

        Args:
            data: Nested data structure
            separator: Key separator for nested fields
            parent_key: Parent key prefix

        Returns:
            Flattened dictionary
        """
        items: List[tuple] = []

        if isinstance(data, dict):
            for key, value in data.items():
                new_key = f"{parent_key}{separator}{key}" if parent_key else key
                items.extend(self.flatten(value, separator, new_key).items())
        elif isinstance(data, (list, tuple)):
            for i, value in enumerate(data):
                new_key = f"{parent_key}[{i}]"
                items.extend(self.flatten(value, separator, new_key).items())
        else:
            items.append((parent_key, data))

        return dict(items)

    def unflatten(
        self,
        data: Dict[str, Any],
        separator: str = ".",
    ) -> Dict[str, Any]:
        """
        Unflatten a dictionary into nested structure.

        Args:
            data: Flattened dictionary
            separator: Key separator

        Returns:
            Nested dictionary
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

    def group_by(
        self,
        data: List[Dict[str, Any]],
        key: str,
    ) -> Dict[Any, List[Dict[str, Any]]]:
        """
        Group data by a specific key.

        Args:
            data: List of data records
            key: Field name to group by

        Returns:
            Dictionary of grouped records
        """
        groups: Dict[Any, List[Dict[str, Any]]] = {}

        for record in data:
            group_key = record.get(key)
            if group_key not in groups:
                groups[group_key] = []
            groups[group_key].append(record)

        return groups

    def aggregate(
        self,
        data: List[Dict[str, Any]],
        group_key: str,
        agg_funcs: Dict[str, Callable],
    ) -> List[Dict[str, Any]]:
        """
        Aggregate data by grouping and applying functions.

        Args:
            data: List of data records
            group_key: Field to group by
            agg_funcs: Dict mapping result fields to aggregation functions

        Returns:
            List of aggregated results
        """
        groups = self.group_by(data, group_key)
        results = []

        for group_value, records in groups.items():
            result = {group_key: group_value}

            for field, func in agg_funcs.items():
                values = [r.get(field) for r in records if field in r]
                try:
                    result[field] = func(values) if values else None
                except Exception as e:
                    logger.warning(f"Aggregation failed for {field}: {e}")
                    result[field] = None

            results.append(result)

        return results

    def merge(
        self,
        left: Dict[str, Any],
        right: Dict[str, Any],
        strategy: str = "override",
    ) -> Dict[str, Any]:
        """
        Merge two dictionaries.

        Args:
            left: Left dictionary
            right: Right dictionary
            strategy: Merge strategy (override, preserve, combine)

        Returns:
            Merged dictionary
        """
        if strategy == "override":
            return {**left, **right}
        elif strategy == "preserve":
            return {**right, **left}
        elif strategy == "combine":
            result = left.copy()
            for key, value in right.items():
                if key in result and isinstance(result[key], dict) and isinstance(value, dict):
                    result[key] = self.merge(result[key], value, strategy)
                else:
                    result[key] = value
            return result
        else:
            raise ValueError(f"Unknown merge strategy: {strategy}")

    def rename_fields(
        self,
        data: Dict[str, Any],
        mapping: Dict[str, str],
    ) -> Dict[str, Any]:
        """
        Rename data fields.

        Args:
            data: Input data
            mapping: Dict mapping old names to new names

        Returns:
            Data with renamed fields
        """
        result = {}
        for key, value in data.items():
            new_key = mapping.get(key, key)
            result[new_key] = value
        return result

    def cast_types(
        self,
        data: Dict[str, Any],
        type_map: Dict[str, type],
    ) -> Dict[str, Any]:
        """
        Cast field values to specified types.

        Args:
            data: Input data
            type_map: Dict mapping field names to target types

        Returns:
            Data with cast types
        """
        result = data.copy()
        for field, target_type in type_map.items():
            if field in result and result[field] is not None:
                try:
                    result[field] = target_type(result[field])
                except (ValueError, TypeError) as e:
                    logger.warning(f"Type cast failed for {field} to {target_type}: {e}")
        return result
