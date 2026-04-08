"""
TOML parsing, manipulation, and serialization utilities.

Provides comprehensive support for reading, writing, validating, and
transforming TOML configuration files with deep dictionary support,
schema validation, and merge operations.

Example:
    >>> from utils.toml_utils import TomlHandler
    >>> handler = TomlHandler()
    >>> data = handler.parse_file("config.toml")
    >>> handler.write_file("output.toml", data)
"""

from __future__ import annotations

import tomllib
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

try:
    import tomli_w
except ImportError:
    tomli_w = None  # type: ignore

try:
    from typing import TypedDict
except ImportError:
    from typing_extensions import TypedDict


class TomlSchema(TypedDict, total=False):
    """Schema definition for TOML validation."""
    required: List[str]
    optional: List[str]
    types: Dict[str, str]


class TomlHandler:
    """
    Comprehensive TOML file handler with parsing, validation, and serialization.

    Supports TOML 1.0 specification with additional utilities for
    schema validation, deep merging, and dotted key manipulation.

    Attributes:
        strict: Whether to enforce strict parsing rules.
        encoding: File encoding for read/write operations.
    """

    def __init__(self, strict: bool = False, encoding: str = "utf-8") -> None:
        """
        Initialize the TOML handler.

        Args:
            strict: Enable strict TOML 1.0 compliance checking.
            encoding: Default file encoding.
        """
        self.strict = strict
        self.encoding = encoding

    def parse_string(self, content: str) -> Dict[str, Any]:
        """
        Parse TOML content from a string.

        Args:
            content: TOML-formatted string.

        Returns:
            Parsed dictionary representation.

        Raises:
            ValueError: If TOML content is invalid.
        """
        try:
            return tomllib.loads(content)
        except Exception as e:
            raise ValueError(f"Failed to parse TOML: {e}") from e

    def parse_file(self, path: Union[str, Path]) -> Dict[str, Any]:
        """
        Parse a TOML file.

        Args:
            path: Path to the TOML file.

        Returns:
            Parsed dictionary representation.

        Raises:
            FileNotFoundError: If the file does not exist.
            ValueError: If the TOML content is invalid.
        """
        path = Path(path)
        if not path.exists():
            raise FileNotFoundError(f"TOML file not found: {path}")

        try:
            with open(path, "rb") as f:
                return tomllib.load(f)
        except Exception as e:
            raise ValueError(f"Failed to parse TOML file {path}: {e}") from e

    def write_string(self, data: Dict[str, Any]) -> str:
        """
        Serialize a dictionary to TOML-formatted string.

        Args:
            data: Dictionary to serialize.

        Returns:
            TOML-formatted string.

        Raises:
            ImportError: If tomli-w is not installed.
        """
        if tomli_w is None:
            raise ImportError("tomli-w is required for writing TOML. Install with: pip install tomli-w")

        try:
            return tomli_w.dumps(data)
        except Exception as e:
            raise ValueError(f"Failed to serialize TOML: {e}") from e

    def write_file(self, path: Union[str, Path], data: Dict[str, Any]) -> None:
        """
        Write a dictionary to a TOML file.

        Args:
            path: Destination file path.
            data: Dictionary to serialize.
        """
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)

        content = self.write_string(data)
        with open(path, "w", encoding=self.encoding) as f:
            f.write(content)

    def merge(
        self,
        base: Dict[str, Any],
        override: Dict[str, Any],
        deep: bool = True
    ) -> Dict[str, Any]:
        """
        Merge two TOML dictionaries with optional deep merge.

        Args:
            base: Base configuration dictionary.
            override: Override values (takes precedence).
            deep: Perform deep merge for nested dictionaries.

        Returns:
            Merged dictionary.
        """
        result = base.copy()

        for key, value in override.items():
            if deep and key in result and isinstance(result[key], dict) and isinstance(value, dict):
                result[key] = self.merge(result[key], value, deep=True)
            else:
                result[key] = value

        return result

    def validate_schema(
        self,
        data: Dict[str, Any],
        schema: TomlSchema
    ) -> List[str]:
        """
        Validate TOML data against a schema.

        Args:
            data: Parsed TOML data.
            schema: Schema definition for validation.

        Returns:
            List of validation error messages (empty if valid).
        """
        errors: List[str] = []

        for key in schema.get("required", []):
            if key not in data:
                errors.append(f"Missing required key: {key}")

        for key, expected_type in schema.get("types", {}).items():
            if key in data:
                actual = type(data[key]).__name__
                if expected_type == "array" and not isinstance(data[key], list):
                    errors.append(f"Key '{key}' expected {expected_type}, got {actual}")
                elif expected_type == "table" and not isinstance(data[key], dict):
                    errors.append(f"Key '{key}' expected {expected_type}, got {actual}")
                elif expected_type == "string" and not isinstance(data[key], str):
                    errors.append(f"Key '{key}' expected {expected_type}, got {actual}")
                elif expected_type == "integer" and not isinstance(data[key], int):
                    errors.append(f"Key '{key}' expected {expected_type}, got {actual}")
                elif expected_type == "float" and not isinstance(data[key], (int, float)):
                    errors.append(f"Key '{key}' expected {expected_type}, got {actual}")
                elif expected_type == "boolean" and not isinstance(data[key], bool):
                    errors.append(f"Key '{key}' expected {expected_type}, got {actual}")

        return errors

    def get_dotted(
        self,
        data: Dict[str, Any],
        key: str,
        default: Any = None
    ) -> Any:
        """
        Get a value using dotted key notation.

        Args:
            data: TOML dictionary.
            key: Dotted key path (e.g., "database.host").
            default: Default value if key not found.

        Returns:
            Value at the key path or default.
        """
        parts = key.split(".")
        current = data

        for part in parts:
            if isinstance(current, dict) and part in current:
                current = current[part]
            else:
                return default

        return current

    def set_dotted(
        self,
        data: Dict[str, Any],
        key: str,
        value: Any
    ) -> Dict[str, Any]:
        """
        Set a value using dotted key notation.

        Args:
            data: TOML dictionary.
            key: Dotted key path (e.g., "database.host").
            value: Value to set.

        Returns:
            Modified dictionary.
        """
        parts = key.split(".")
        current = data

        for i, part in enumerate(parts[:-1]):
            if part not in current:
                current[part] = {}
            elif not isinstance(current[part], dict):
                current[part] = {}
            current = current[part]

        current[parts[-1]] = value
        return data

    def flatten(
        self,
        data: Dict[str, Any],
        parent_key: str = "",
        sep: str = "."
    ) -> Dict[str, Any]:
        """
        Flatten a nested TOML dictionary.

        Args:
            data: Nested TOML dictionary.
            parent_key: Parent key prefix.
            sep: Key separator.

        Returns:
            Flattened dictionary.
        """
        items: Dict[str, Any] = {}

        for key, value in data.items():
            new_key = f"{parent_key}{sep}{key}" if parent_key else key

            if isinstance(value, dict):
                items.update(self.flatten(value, new_key, sep))
            elif isinstance(value, list):
                for i, item in enumerate(value):
                    if isinstance(item, dict):
                        items.update(self.flatten(item, f"{new_key}[{i}]", sep))
                    else:
                        items[f"{new_key}[{i}]"] = item
            else:
                items[new_key] = value

        return items

    def unflatten(
        self,
        data: Dict[str, Any],
        sep: str = "."
    ) -> Dict[str, Any]:
        """
        Unflatten a flattened dictionary to nested TOML structure.

        Args:
            data: Flattened dictionary.
            sep: Key separator.

        Returns:
            Nested dictionary.
        """
        result: Dict[str, Any] = {}

        for key, value in data.items():
            self.set_dotted(result, key, value)

        return result

    def diff(
        self,
        left: Dict[str, Any],
        right: Dict[str, Any],
        path: str = ""
    ) -> List[Dict[str, Any]]:
        """
        Compute differences between two TOML structures.

        Args:
            left: Left-side TOML dictionary.
            right: Right-side TOML dictionary.
            path: Current key path for nested comparison.

        Returns:
            List of difference descriptors with keys: 'path', 'type', 'left', 'right'.
        """
        differences: List[Dict[str, Any]] = []
        all_keys = set(left.keys()) | set(right.keys())

        for key in all_keys:
            current_path = f"{path}.{key}" if path else key
            left_val = left.get(key)
            right_val = right.get(key)

            if key not in left:
                differences.append({
                    "path": current_path,
                    "type": "added",
                    "left": None,
                    "right": right_val
                })
            elif key not in right:
                differences.append({
                    "path": current_path,
                    "type": "removed",
                    "left": left_val,
                    "right": None
                })
            elif isinstance(left_val, dict) and isinstance(right_val, dict):
                differences.extend(self.diff(left_val, right_val, current_path))
            elif left_val != right_val:
                differences.append({
                    "path": current_path,
                    "type": "changed",
                    "left": left_val,
                    "right": right_val
                })

        return differences


def parse_toml_file(path: Union[str, Path]) -> Dict[str, Any]:
    """
    Convenience function to parse a TOML file.

    Args:
        path: Path to the TOML file.

    Returns:
        Parsed dictionary.
    """
    return TomlHandler().parse_file(path)


def write_toml_file(path: Union[str, Path], data: Dict[str, Any]) -> None:
    """
    Convenience function to write data to a TOML file.

    Args:
        path: Destination file path.
        data: Dictionary to serialize.
    """
    TomlHandler().write_file(path, data)


def merge_toml_configs(
    *configs: Dict[str, Any],
    deep: bool = True
) -> Dict[str, Any]:
    """
    Merge multiple TOML configurations.

    Args:
        *configs: Variable number of configuration dictionaries.
        deep: Perform deep merge for nested dictionaries.

    Returns:
        Merged configuration dictionary.
    """
    handler = TomlHandler()
    result = configs[0] if configs else {}

    for config in configs[1:]:
        result = handler.merge(result, config, deep=deep)

    return result
