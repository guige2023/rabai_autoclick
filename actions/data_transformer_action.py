"""Data Transformer Action Module.

Provides data transformation, conversion, and
manipulation capabilities for various data formats.
"""

from typing import Any, Dict, List, Optional, Callable, Union, TypeVar
from dataclasses import dataclass, field
from enum import Enum
import json
import re
from datetime import datetime


T = TypeVar("T")


class TransformType(Enum):
    """Types of data transformations."""
    MAP = "map"
    FILTER = "filter"
    REDUCE = "reduce"
    SORT = "sort"
    GROUP = "group"
    FLATTEN = "flatten"
    PIVOT = "pivot"
    MERGE = "merge"
    SPLIT = "split"
    EXTRACT = "extract"


@dataclass
class TransformConfig:
    """Configuration for a transformation."""
    transform_type: TransformType
    source_field: Optional[str] = None
    target_field: Optional[str] = None
    transform_fn: Optional[Callable] = None
    parameters: Dict[str, Any] = field(default_factory=dict)


class ArrayTransformer:
    """Transforms array/list data."""

    def map(
        self,
        items: List[Any],
        fn: Callable[[Any], Any],
    ) -> List[Any]:
        """Apply function to each item."""
        return [fn(item) for item in items]

    def filter(
        self,
        items: List[Any],
        predicate: Callable[[Any], bool],
    ) -> List[Any]:
        """Filter items by predicate."""
        return [item for item in items if predicate(item)]

    def reduce(
        self,
        items: List[Any],
        fn: Callable[[Any, Any], Any],
        initial: Any = None,
    ) -> Any:
        """Reduce items to single value."""
        if not items:
            return initial

        result = initial if initial is not None else items[0]
        start_idx = 0 if initial is None else -1

        for i in range(start_idx, len(items)):
            result = fn(result, items[i])

        return result

    def sort(
        self,
        items: List[Any],
        key: Optional[Callable[[Any], Any]] = None,
        reverse: bool = False,
    ) -> List[Any]:
        """Sort items."""
        return sorted(items, key=key, reverse=reverse)

    def group_by(
        self,
        items: List[Dict[str, Any]],
        key_field: str,
    ) -> Dict[Any, List[Dict[str, Any]]]:
        """Group items by field."""
        groups: Dict[Any, List[Dict[str, Any]]] = {}
        for item in items:
            key = item.get(key_field)
            if key not in groups:
                groups[key] = []
            groups[key].append(item)
        return groups

    def flatten(
        self,
        nested_list: List[List[Any]],
    ) -> List[Any]:
        """Flatten nested list."""
        result = []
        for item in nested_list:
            if isinstance(item, list):
                result.extend(self.flatten([item]))
            else:
                result.append(item)
        return result

    def chunk(
        self,
        items: List[Any],
        chunk_size: int,
    ) -> List[List[Any]]:
        """Split items into chunks."""
        return [
            items[i:i + chunk_size]
            for i in range(0, len(items), chunk_size)
        ]

    def unique(
        self,
        items: List[Any],
        key: Optional[Callable[[Any], Any]] = None,
    ) -> List[Any]:
        """Get unique items."""
        if key is None:
            seen = set()
            result = []
            for item in items:
                if item not in seen:
                    seen.add(item)
                    result.append(item)
            return result
        else:
            seen_keys = set()
            result = []
            for item in items:
                k = key(item)
                if k not in seen_keys:
                    seen_keys.add(k)
                    result.append(item)
            return result


class DictTransformer:
    """Transforms dictionary data."""

    def rename_keys(
        self,
        data: Dict[str, Any],
        rename_map: Dict[str, str],
    ) -> Dict[str, Any]:
        """Rename dictionary keys."""
        result = {}
        for key, value in data.items():
            new_key = rename_map.get(key, key)
            result[new_key] = value
        return result

    def pick(
        self,
        data: Dict[str, Any],
        fields: List[str],
    ) -> Dict[str, Any]:
        """Pick specific fields."""
        return {k: data.get(k) for k in fields if k in data}

    def omit(
        self,
        data: Dict[str, Any],
        fields: List[str],
    ) -> Dict[str, Any]:
        """Omit specific fields."""
        return {k: v for k, v in data.items() if k not in fields}

    def deep_merge(
        self,
        left: Dict[str, Any],
        right: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Deep merge two dictionaries."""
        result = left.copy()

        for key, value in right.items():
            if key in result and isinstance(result[key], dict) and isinstance(value, dict):
                result[key] = self.deep_merge(result[key], value)
            else:
                result[key] = value

        return result

    def flatten_dict(
        self,
        data: Dict[str, Any],
        separator: str = ".",
        parent_key: str = "",
    ) -> Dict[str, Any]:
        """Flatten nested dictionary."""
        result = {}
        for key, value in data.items():
            new_key = f"{parent_key}{separator}{key}" if parent_key else key

            if isinstance(value, dict):
                result.update(self.flatten_dict(value, separator, new_key))
            else:
                result[new_key] = value

        return result

    def unflatten_dict(
        self,
        data: Dict[str, Any],
        separator: str = ".",
    ) -> Dict[str, Any]:
        """Unflatten dictionary to nested structure."""
        result: Dict[str, Any] = {}

        for flat_key, value in data.items():
            parts = flat_key.split(separator)
            current = result

            for part in parts[:-1]:
                if part not in current:
                    current[part] = {}
                current = current[part]

            current[parts[-1]] = value

        return result

    def transform_values(
        self,
        data: Dict[str, Any],
        fn: Callable[[Any], Any],
    ) -> Dict[str, Any]:
        """Transform all values."""
        return {k: fn(v) for k, v in data.items()}

    def transform_keys(
        self,
        data: Dict[str, Any],
        fn: Callable[[str], str],
    ) -> Dict[str, Any]:
        """Transform all keys."""
        return {fn(k): v for k, v in data.items()}


class StringTransformer:
    """Transforms string data."""

    def capitalize(
        self,
        text: str,
        first_only: bool = True,
    ) -> str:
        """Capitalize text."""
        if first_only:
            return text.capitalize()
        return text.title()

    def camel_to_snake(self, text: str) -> str:
        """Convert camelCase to snake_case."""
        result = re.sub(r'([A-Z])', r'_\1', text)
        return result.lower().lstrip('_')

    def snake_to_camel(self, text: str) -> str:
        """Convert snake_case to camelCase."""
        parts = text.split('_')
        return parts[0] + ''.join(p.title() for p in parts[1:])

    def kebab_to_snake(self, text: str) -> str:
        """Convert kebab-case to snake_case."""
        return text.replace('-', '_')

    def truncate(
        self,
        text: str,
        length: int,
        suffix: str = "...",
    ) -> str:
        """Truncate text to length."""
        if len(text) <= length:
            return text
        return text[:length - len(suffix)] + suffix

    def slugify(self, text: str) -> str:
        """Convert text to URL-friendly slug."""
        text = text.lower()
        text = re.sub(r'[^\w\s-]', '', text)
        text = re.sub(r'[-\s]+', '-', text)
        return text.strip('-')


class DataTransformer:
    """Main data transformation orchestrator."""

    def __init__(self):
        self.array_transformer = ArrayTransformer()
        self.dict_transformer = DictTransformer()
        self.string_transformer = StringTransformer()

    def transform(
        self,
        data: Any,
        config: TransformConfig,
    ) -> Any:
        """Apply transformation based on config."""
        transform_type = config.transform_type

        if transform_type == TransformType.MAP:
            return self.array_transformer.map(data, config.transform_fn)

        elif transform_type == TransformType.FILTER:
            return self.array_transformer.filter(data, config.transform_fn)

        elif transform_type == TransformType.REDUCE:
            return self.array_transformer.reduce(
                data,
                config.transform_fn,
                config.parameters.get("initial"),
            )

        elif transform_type == TransformType.SORT:
            return self.array_transformer.sort(
                data,
                key=config.transform_fn,
                reverse=config.parameters.get("reverse", False),
            )

        elif transform_type == TransformType.GROUP:
            return self.array_transformer.group_by(
                data,
                config.parameters["key_field"],
            )

        elif transform_type == TransformType.FLATTEN:
            return self.array_transformer.flatten(data)

        elif transform_type == TransformType.MERGE:
            return self.dict_transformer.deep_merge(
                config.parameters["left"],
                config.parameters["right"],
            )

        return data

    def transform_batch(
        self,
        items: List[Dict[str, Any]],
        field: str,
        fn: Callable[[Any], Any],
    ) -> List[Dict[str, Any]]:
        """Transform a specific field in each item."""
        return [
            {**item, field: fn(item.get(field))}
            for item in items
        ]


class DataConverter:
    """Converts data between formats."""

    def to_json(self, data: Any, indent: Optional[int] = None) -> str:
        """Convert to JSON string."""
        return json.dumps(data, indent=indent, default=str)

    def from_json(self, json_str: str) -> Any:
        """Parse JSON string."""
        return json.loads(json_str)

    def to_csv_row(
        self,
        data: Dict[str, Any],
        fields: List[str],
    ) -> str:
        """Convert dict to CSV row."""
        values = [str(data.get(f, "")) for f in fields]
        return ",".join(f'"{v}"' if ',' in v else v for v in values)

    def from_csv_row(
        self,
        row: str,
        fields: List[str],
    ) -> Dict[str, Any]:
        """Parse CSV row to dict."""
        values = row.split(",")
        return {f: v.strip('"') for f, v in zip(fields, values)}


class DataTransformerAction:
    """High-level data transformer action."""

    def __init__(
        self,
        transformer: Optional[DataTransformer] = None,
        converter: Optional[DataConverter] = None,
    ):
        self.transformer = transformer or DataTransformer()
        self.converter = converter or DataConverter()

    def map(
        self,
        items: List[Any],
        fn: Callable[[Any], Any],
    ) -> List[Any]:
        """Map function over items."""
        return self.transformer.array_transformer.map(items, fn)

    def filter(
        self,
        items: List[Any],
        predicate: Callable[[Any], bool],
    ) -> List[Any]:
        """Filter items."""
        return self.transformer.array_transformer.filter(items, predicate)

    def group_by(
        self,
        items: List[Dict[str, Any]],
        key_field: str,
    ) -> Dict[Any, List[Dict[str, Any]]]:
        """Group items by field."""
        return self.transformer.array_transformer.group_by(items, key_field)

    def sort_by(
        self,
        items: List[Any],
        key: Callable[[Any], Any],
        reverse: bool = False,
    ) -> List[Any]:
        """Sort items by key."""
        return self.transformer.array_transformer.sort(items, key, reverse)

    def pick(
        self,
        data: Dict[str, Any],
        fields: List[str],
    ) -> Dict[str, Any]:
        """Pick fields from dict."""
        return self.transformer.dict_transformer.pick(data, fields)

    def omit(
        self,
        data: Dict[str, Any],
        fields: List[str],
    ) -> Dict[str, Any]:
        """Omit fields from dict."""
        return self.transformer.dict_transformer.omit(data, fields)

    def merge(
        self,
        left: Dict[str, Any],
        right: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Merge two dictionaries."""
        return self.transformer.dict_transformer.deep_merge(left, right)

    def flatten(
        self,
        data: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Flatten nested dict."""
        return self.transformer.dict_transformer.flatten_dict(data)

    def camel_to_snake(self, text: str) -> str:
        """Convert camelCase to snake_case."""
        return self.transformer.string_transformer.camel_to_snake(text)

    def snake_to_camel(self, text: str) -> str:
        """Convert snake_case to camelCase."""
        return self.transformer.string_transformer.snake_to_camel(text)


# Module exports
__all__ = [
    "DataTransformerAction",
    "DataTransformer",
    "DataConverter",
    "ArrayTransformer",
    "DictTransformer",
    "StringTransformer",
    "TransformConfig",
    "TransformType",
]
