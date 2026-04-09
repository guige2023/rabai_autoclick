"""
JSON manipulation automation module.

Provides JSON parsing, transformation, validation, and
query utilities for API data processing.

Author: Aito Auto Agent
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import (
    Any,
    Callable,
    Optional,
)
import re


class JsonPointer:
    """JSON Pointer (RFC 6901) implementation."""

    _REFERENCE_PATTERN = re.compile(r'/(~0|~1|[^/]+)')

    @classmethod
    def get(cls, data: dict, pointer: str, default: Any = None) -> Any:
        """
        Get value at JSON pointer.

        Args:
            data: JSON data
            pointer: JSON pointer string
            default: Default value if not found

        Returns:
            Value at pointer or default
        """
        if not pointer.startswith("/"):
            return default

        parts = cls._REFERENCE_PATTERN.findall(pointer)
        current = data

        for part in parts:
            if current is None:
                return default

            decoded = cls._decode(part)

            if isinstance(current, dict):
                current = current.get(decoded)
            elif isinstance(current, list):
                try:
                    index = int(decoded)
                    current = current[index] if 0 <= index < len(current) else None
                except ValueError:
                    return default
            else:
                return default

        return current if current is not None else default

    @classmethod
    def set(cls, data: dict, pointer: str, value: Any) -> bool:
        """
        Set value at JSON pointer.

        Args:
            data: JSON data
            pointer: JSON pointer string
            value: Value to set

        Returns:
            True if successful
        """
        if not pointer.startswith("/"):
            return False

        parts = cls._REFERENCE_PATTERN.findall(pointer)

        if not parts:
            return False

        current = data

        for i, part in enumerate(parts[:-1]):
            decoded = cls._decode(part)

            if decoded not in current:
                current[decoded] = {}

            current = current[decoded]

        last_decoded = cls._decode(parts[-1])
        current[last_decoded] = value
        return True

    @staticmethod
    def _decode(part: str) -> str:
        """Decode JSON pointer escape sequences."""
        return part.replace("~1", "/").replace("~0", "~")


class JsonPath:
    """
    Simplified JSONPath for querying JSON data.

    Supports: $, .key, [index], ..recursive, [?filter]
    """

    @classmethod
    def query(cls, data: Any, path: str) -> list[Any]:
        """
        Query JSON data using path expression.

        Args:
            data: JSON data
            path: JSONPath expression

        Returns:
            List of matching values
        """
        if not path or path == "$":
            return [data]

        results = []
        current = [data]

        tokens = cls._tokenize(path)

        for token in tokens:
            next_current = []

            for item in current:
                matched = cls._apply_token(item, token)
                next_current.extend(matched)

            current = next_current

        return current

    @classmethod
    def _tokenize(cls, path: str) -> list[str]:
        """Tokenize JSONPath expression."""
        tokens = []
        current = ""

        i = 0
        while i < len(path):
            char = path[i]

            if char == ".":
                if current:
                    tokens.append(current)
                if i + 1 < len(path) and path[i + 1] == ".":
                    tokens.append("..")
                    i += 1
                    current = ""
                else:
                    current = ""
            elif char == "[":
                if current:
                    tokens.append(current)
                    current = ""
                end = path.find("]", i)
                if end != -1:
                    tokens.append(path[i + 1:end])
                    i = end
            elif char == "]":
                pass
            else:
                current += char

            i += 1

        if current:
            tokens.append(current)

        return tokens

    @classmethod
    def _apply_token(cls, item: Any, token: str) -> list[Any]:
        """Apply a single token to an item."""
        if token == "..":
            return cls._recursive_search(item)

        if token.startswith("[?"):
            return cls._filter(item, token[2:-1])

        if token.startswith("[") and token.endswith("]"):
            try:
                index = int(token[1:-1])
                if isinstance(item, list) and 0 <= index < len(item):
                    return [item[index]]
                return []
            except ValueError:
                return []

        if token.startswith("@."):
            token = token[2:]

        if token == "*":
            if isinstance(item, list):
                return item
            elif isinstance(item, dict):
                return list(item.values())
            return []

        if isinstance(item, dict):
            return [item.get(token)]

        if isinstance(item, list):
            return [item[int(token)]] if token.isdigit() else []

        return []

    @classmethod
    def _recursive_search(cls, item: Any) -> list[Any]:
        """Recursively search for all values."""
        results = []

        if isinstance(item, dict):
            results.extend(item.values())
            for value in item.values():
                results.extend(cls._recursive_search(value))
        elif isinstance(item, list):
            for value in item:
                results.append(value)
                results.extend(cls._recursive_search(value))

        return results

    @classmethod
    def _filter(cls, item: Any, condition: str) -> list[Any]:
        """Apply filter condition."""
        if not isinstance(item, list):
            return []

        if "==" in condition:
            key, value = condition.split("==")
            value = value.strip("'\"")
            return [i for i in item if cls._get_nested(i, key.strip()) == value]

        if "!=" in condition:
            key, value = condition.split("!=")
            value = value.strip("'\"")
            return [i for i in item if cls._get_nested(i, key.strip()) != value]

        return item

    @staticmethod
    def _get_nested(data: Any, path: str) -> Any:
        """Get nested value from path."""
        parts = path.split(".")
        current = data

        for part in parts:
            if isinstance(current, dict):
                current = current.get(part)
            else:
                return None

        return current


class JsonAutomator:
    """
    JSON manipulation and automation.

    Example:
        automator = JsonAutomator()

        # Parse and query
        data = automator.parse(json_string)
        value = automator.query(data, "$.users[0].name")

        # Transform
        transformed = automator.transform(data, {"new_field": "$.old_field"})

        # Validate
        is_valid = automator.validate(data, schema)
    """

    def __init__(self):
        self._pointer = JsonPointer()
        self._path = JsonPath()

    def parse(self, json_str: str) -> Any:
        """
        Parse JSON string.

        Args:
            json_str: JSON string

        Returns:
            Parsed JSON data
        """
        return json.loads(json_str)

    def stringify(
        self,
        data: Any,
        indent: Optional[int] = None,
        sort_keys: bool = False
    ) -> str:
        """
        Convert data to JSON string.

        Args:
            data: Data to stringify
            indent: Indentation spaces
            sort_keys: Sort dictionary keys

        Returns:
            JSON string
        """
        return json.dumps(data, indent=indent, sort_keys=sort_keys, default=str)

    def query(self, data: Any, path: str) -> Any:
        """
        Query JSON data using JSONPath.

        Args:
            data: JSON data
            path: JSONPath expression

        Returns:
            Query results
        """
        results = self._path.query(data, path)
        return results[0] if len(results) == 1 else results

    def get(self, data: dict, pointer: str, default: Any = None) -> Any:
        """
        Get value using JSON Pointer.

        Args:
            data: JSON data
            pointer: JSON pointer
            default: Default value

        Returns:
            Value at pointer
        """
        return self._pointer.get(data, pointer, default)

    def set(self, data: dict, pointer: str, value: Any) -> bool:
        """Set value using JSON Pointer."""
        return self._pointer.set(data, pointer, value)

    def transform(
        self,
        data: Any,
        mapping: dict[str, str]
    ) -> dict:
        """
        Transform JSON data using field mappings.

        Args:
            data: Source data
            mapping: Dict of new_field -> json_path

        Returns:
            Transformed data
        """
        result = {}

        for new_field, path in mapping.items():
            value = self.query(data, path)
            self._set_nested(result, new_field, value)

        return result

    def _set_nested(self, data: dict, path: str, value: Any) -> None:
        """Set nested value using dot notation."""
        parts = path.split(".")
        current = data

        for part in parts[:-1]:
            if part not in current:
                current[part] = {}
            current = current[part]

        current[parts[-1]] = value

    def merge(
        self,
        *dicts: dict,
        deep: bool = False
    ) -> dict:
        """
        Merge multiple dictionaries.

        Args:
            *dicts: Dictionaries to merge
            deep: Use deep merge

        Returns:
            Merged dictionary
        """
        if not dicts:
            return {}

        result = {}

        for d in dicts:
            if deep:
                self._deep_merge(result, d)
            else:
                result.update(d)

        return result

    def _deep_merge(self, target: dict, source: dict) -> None:
        """Recursively merge source into target."""
        for key, value in source.items():
            if key in target and isinstance(target[key], dict) and isinstance(value, dict):
                self._deep_merge(target[key], value)
            else:
                target[key] = value

    def flatten(
        self,
        data: dict,
        separator: str = ".",
        prefix: str = ""
    ) -> dict:
        """
        Flatten nested dictionary.

        Args:
            data: Dictionary to flatten
            separator: Key separator
            prefix: Key prefix

        Returns:
            Flattened dictionary
        """
        result = {}

        for key, value in data.items():
            new_key = f"{prefix}{separator}{key}" if prefix else key

            if isinstance(value, dict):
                result.update(self.flatten(value, separator, new_key))
            else:
                result[new_key] = value

        return result

    def unflatten(
        self,
        data: dict,
        separator: str = "."
    ) -> dict:
        """
        Unflatten dictionary to nested structure.

        Args:
            data: Flattened dictionary
            separator: Key separator

        Returns:
            Nested dictionary
        """
        result = {}

        for key, value in data.items():
            self._set_nested(result, key.replace(separator, "."), value)

        return result

    def diff(
        self,
        left: Any,
        right: Any,
        path: str = ""
    ) -> list[dict]:
        """
        Find differences between two JSON structures.

        Args:
            left: Left JSON
            right: Right JSON
            path: Current path

        Returns:
            List of differences
        """
        differences = []

        if type(left) != type(right):
            differences.append({
                "path": path or "$",
                "type": "type_mismatch",
                "left": left,
                "right": right
            })
            return differences

        if isinstance(left, dict):
            all_keys = set(left.keys()) | set(right.keys())

            for key in all_keys:
                new_path = f"{path}.{key}" if path else key

                if key not in left:
                    differences.append({
                        "path": new_path,
                        "type": "added",
                        "right": right[key]
                    })
                elif key not in right:
                    differences.append({
                        "path": new_path,
                        "type": "removed",
                        "left": left[key]
                    })
                else:
                    differences.extend(self.diff(left[key], right[key], new_path))

        elif isinstance(left, list):
            if left != right:
                differences.append({
                    "path": path or "$",
                    "type": "array_diff",
                    "left": left,
                    "right": right
                })

        else:
            if left != right:
                differences.append({
                    "path": path or "$",
                    "type": "value_diff",
                    "left": left,
                    "right": right
                })

        return differences

    def patch(
        self,
        data: dict,
        operations: list[dict]
    ) -> dict:
        """
        Apply JSON Patch (RFC 6902) operations.

        Args:
            data: Source data
            operations: List of patch operations

        Returns:
            Patched data
        """
        import copy
        result = copy.deepcopy(data)

        for op in operations:
            op_type = op.get("op")

            if op_type == "add":
                self._pointer.set(result, op["path"], op["value"])
            elif op_type == "remove":
                path = op["path"]
                parent_path = path.rsplit("/", 1)[0]
                key = path.split("/")[-1]
                parent = self._pointer.get(result, parent_path, {})
                if isinstance(parent, dict) and key in parent:
                    del parent[key]
            elif op_type == "replace":
                self._pointer.set(result, op["path"], op["value"])
            elif op_type == "move":
                value = self._pointer.get(result, op["from"])
                self._pointer.set(result, op["path"], value)
                self._pointer.set(result, op["from"], None)

        return result


def create_json_automator() -> JsonAutomator:
    """Factory to create JsonAutomator."""
    return JsonAutomator()
