"""Data transformation action for processing and converting data formats.

This module provides data transformation capabilities including
JSON/YAML manipulation, CSV processing, and data type conversions.

Example:
    >>> action = DataTransformAction()
    >>> result = action.execute(operation="flatten", data={"a": {"b": 1}})
"""

from __future__ import annotations

import csv
import io
import json
from dataclasses import dataclass, field
from typing import Any, Callable, Optional
from urllib.parse import parse_qs, urlencode, urlparse


@dataclass
class TransformResult:
    """Result from a data transformation."""
    success: bool = True
    data: Any = None
    error: Optional[str] = None
    metadata: dict[str, Any] = field(default_factory=dict)


class DataTransformAction:
    """Data transformation and format conversion action.

    Supports JSON, YAML, CSV, URL encoding, and custom transformations.

    Example:
        >>> action = DataTransformAction()
        >>> result = action.execute(
        ...     operation="csv_to_json",
        ...     data="a,b\\nc,d"
        ... )
    """

    def __init__(self) -> None:
        """Initialize data transformer."""
        self._cache: dict[str, Any] = {}

    def execute(
        self,
        operation: str,
        data: Any,
        format: Optional[str] = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """Execute data transformation.

        Args:
            operation: Transformation operation name.
            data: Input data to transform.
            format: Target or source format.
            **kwargs: Additional operation parameters.

        Returns:
            Transformation result dictionary.

        Raises:
            ValueError: If operation is unknown or data is invalid.
        """
        op = operation.lower()
        result: dict[str, Any] = {"operation": op, "success": True}

        if op == "flatten":
            result["data"] = self._flatten_dict(data)

        elif op == "unflatten":
            result["data"] = self._unflatten_dict(data)

        elif op == "json_to_csv":
            result["data"] = self._json_to_csv(data)
            result["format"] = "csv"

        elif op == "csv_to_json":
            delimiter = kwargs.get("delimiter", ",")
            result["data"] = self._csv_to_json(data, delimiter)
            result["format"] = "json"

        elif op == "normalize":
            result["data"] = self._normalize_data(data)

        elif op == "deduplicate":
            result["data"] = self._deduplicate_list(data)
            result["removed"] = len(data) - len(result["data"]) if isinstance(data, list) else 0

        elif op == "group_by":
            key = kwargs.get("key")
            if not key:
                raise ValueError("key required for 'group_by'")
            result["data"] = self._group_by(data, key)

        elif op == "merge":
            other = kwargs.get("other", [])
            result["data"] = self._merge_data(data, other)

        elif op == "sort":
            key = kwargs.get("key")
            reverse = kwargs.get("reverse", False)
            result["data"] = self._sort_data(data, key, reverse)

        elif op == "filter":
            predicate = kwargs.get("predicate")
            if not predicate:
                raise ValueError("predicate required for 'filter'")
            result["data"] = self._filter_data(data, predicate)

        elif op == "map":
            func = kwargs.get("func")
            if not func:
                raise ValueError("func required for 'map'")
            result["data"] = self._map_data(data, func)

        elif op == "url_encode":
            result["data"] = self._url_encode(data)
            result["format"] = "url"

        elif op == "url_decode":
            result["data"] = self._url_decode(data)

        elif op == "base64_encode":
            result["data"] = self._base64_encode(data)

        elif op == "base64_decode":
            result["data"] = self._base64_decode(data)

        elif op == "parse_json":
            result["data"] = json.loads(data) if isinstance(data, str) else data

        elif op == "to_json":
            indent = kwargs.get("indent", 2)
            result["data"] = json.dumps(data, indent=indent, ensure_ascii=False)

        else:
            raise ValueError(f"Unknown operation: {operation}")

        return result

    def _flatten_dict(self, d: dict, parent_key: str = "", sep: str = ".") -> dict:
        """Flatten nested dictionary.

        Args:
            d: Dictionary to flatten.
            parent_key: Parent key prefix.
            sep: Separator for keys.

        Returns:
            Flattened dictionary.
        """
        items: list[tuple[str, Any]] = []
        for k, v in d.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            if isinstance(v, dict):
                items.extend(self._flatten_dict(v, new_key, sep=sep).items())
            else:
                items.append((new_key, v))
        return dict(items)

    def _unflatten_dict(self, d: dict, sep: str = ".") -> dict:
        """Unflatten dictionary to nested structure.

        Args:
            d: Flattened dictionary.
            sep: Key separator.

        Returns:
            Nested dictionary.
        """
        result: dict = {}
        for key, value in d.items():
            parts = key.split(sep)
            current = result
            for part in parts[:-1]:
                if part not in current:
                    current[part] = {}
                current = current[part]
            current[parts[-1]] = value
        return result

    def _json_to_csv(self, data: Any) -> str:
        """Convert JSON data to CSV format.

        Args:
            data: JSON data (list of dicts).

        Returns:
            CSV string.
        """
        if isinstance(data, str):
            data = json.loads(data)
        if not isinstance(data, list):
            data = [data]

        if not data:
            return ""

        output = io.StringIO()
        fieldnames = list(data[0].keys()) if isinstance(data[0], dict) else []
        writer = csv.DictWriter(output, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)
        return output.getvalue()

    def _csv_to_json(self, data: str, delimiter: str = ",") -> list[dict]:
        """Convert CSV to JSON array.

        Args:
            data: CSV string.
            delimiter: Column delimiter.

        Returns:
            List of dictionaries.
        """
        reader = csv.DictReader(io.StringIO(data), delimiter=delimiter)
        return list(reader)

    def _normalize_data(self, data: Any) -> Any:
        """Normalize data (trim strings, standardize types).

        Args:
            data: Data to normalize.

        Returns:
            Normalized data.
        """
        if isinstance(data, str):
            return data.strip()
        elif isinstance(data, dict):
            return {k: self._normalize_data(v) for k, v in data.items()}
        elif isinstance(data, list):
            return [self._normalize_data(item) for item in data]
        else:
            return data

    def _deduplicate_list(self, data: list, key: Optional[str] = None) -> list:
        """Remove duplicates from list.

        Args:
            data: List to deduplicate.
            key: Optional key function for comparison.

        Returns:
            Deduplicated list.
        """
        if not key:
            seen = set()
            result = []
            for item in data:
                if item not in seen:
                    seen.add(item)
                    result.append(item)
            return result
        else:
            seen = set()
            result = []
            for item in data:
                k = key(item) if callable(key) else item.get(key) if isinstance(item, dict) else None
                if k not in seen:
                    seen.add(k)
                    result.append(item)
            return result

    def _group_by(self, data: list, key: str) -> dict[str, list]:
        """Group list items by key.

        Args:
            data: List to group.
            key: Key to group by.

        Returns:
            Dictionary of grouped items.
        """
        result: dict[str, list] = {}
        for item in data:
            k = item.get(key) if isinstance(item, dict) else getattr(item, key, None)
            k_str = str(k) if k is not None else "None"
            if k_str not in result:
                result[k_str] = []
            result[k_str].append(item)
        return result

    def _merge_data(self, data: Any, other: Any) -> Any:
        """Merge two data structures.

        Args:
            data: First data structure.
            other: Second data structure.

        Returns:
            Merged result.
        """
        if isinstance(data, dict) and isinstance(other, dict):
            result = dict(data)
            result.update(other)
            return result
        elif isinstance(data, list) and isinstance(other, list):
            return data + other
        else:
            return data

    def _sort_data(self, data: list, key: Optional[str] = None, reverse: bool = False) -> list:
        """Sort list data.

        Args:
            data: List to sort.
            key: Optional sort key.
            reverse: Sort in descending order.

        Returns:
            Sorted list.
        """
        if key:
            return sorted(data, key=lambda x: x.get(key) if isinstance(x, dict) else getattr(x, key, None), reverse=reverse)
        return sorted(data, reverse=reverse)

    def _filter_data(self, data: list, predicate: Callable[[Any], bool]) -> list:
        """Filter list with predicate function.

        Args:
            data: List to filter.
            predicate: Filter function.

        Returns:
            Filtered list.
        """
        return [item for item in data if predicate(item)]

    def _map_data(self, data: list, func: Callable[[Any], Any]) -> list:
        """Map list with transformation function.

        Args:
            data: List to transform.
            func: Transform function.

        Returns:
            Transformed list.
        """
        return [func(item) for item in data]

    def _url_encode(self, data: dict) -> str:
        """Encode dictionary to URL query string.

        Args:
            data: Dictionary to encode.

        Returns:
            URL-encoded string.
        """
        return urlencode(data)

    def _url_decode(self, data: str) -> dict:
        """Decode URL query string to dictionary.

        Args:
            data: URL query string.

        Returns:
            Decoded dictionary.
        """
        return dict(parse_qs(urlparse(data).query))

    def _base64_encode(self, data: str) -> str:
        """Base64 encode string.

        Args:
            data: String to encode.

        Returns:
            Base64 encoded string.
        """
        import base64
        return base64.b64encode(data.encode()).decode()

    def _base64_decode(self, data: str) -> str:
        """Base64 decode string.

        Args:
            data: Base64 string to decode.

        Returns:
            Decoded string.
        """
        import base64
        return base64.b64decode(data.encode()).decode()
