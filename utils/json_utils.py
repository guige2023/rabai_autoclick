"""JSON utilities for RabAI AutoClick.

Provides:
- JSONEncoder: Custom JSON encoder for special types
- JSONDecoder: Custom JSON decoder
- safe_json_loads: Safe JSON parsing
- safe_json_dumps: Safe JSON serialization
- deep_merge: Deep merge dictionaries
"""

import json
import datetime
import pathlib
from dataclasses import is_dataclass, asdict
from pathlib import Path
from typing import Any, Dict, Optional, Union
from enum import Enum


class ExtendedJSONEncoder(json.JSONEncoder):
    """Extended JSON encoder supporting additional types."""

    def default(self, obj: Any) -> Any:
        if is_dataclass(obj):
            return asdict(obj)
        if isinstance(obj, (datetime.datetime, datetime.date, datetime.time)):
            return obj.isoformat()
        if isinstance(obj, datetime.timedelta):
            return obj.total_seconds()
        if isinstance(obj, pathlib.Path):
            return str(obj)
        if isinstance(obj, Enum):
            return obj.value
        if isinstance(obj, set):
            return list(obj)
        if isinstance(obj, bytes):
            return obj.decode('utf-8', errors='replace')
        if hasattr(obj, 'to_dict'):
            return obj.to_dict()
        return super().default(obj)


def safe_json_loads(
    data: str,
    default: Any = None,
    strict: bool = False,
) -> Any:
    """Safely load JSON data.

    Args:
        data: JSON string to parse.
        default: Value to return on error.
        strict: If False, allows trailing commas and comments.

    Returns:
        Parsed JSON data or default on error.
    """
    if strict:
        try:
            return json.loads(data)
        except (json.JSONDecodeError, TypeError):
            return default
    else:
        # Try to parse, stripping comments and trailing commas
        try:
            cleaned = _clean_json(data)
            return json.loads(cleaned)
        except (json.JSONDecodeError, TypeError):
            return default


def safe_json_dumps(
    obj: Any,
    default: Any = None,
    indent: Optional[int] = None,
    **kwargs: Any,
) -> Optional[str]:
    """Safely dump object to JSON string.

    Args:
        obj: Object to serialize.
        default: Function to call for non-serializable objects.
        indent: Indentation level.
        **kwargs: Additional arguments to json.dumps.

    Returns:
        JSON string or None on error.
    """
    try:
        return json.dumps(
            obj,
            cls=ExtendedJSONEncoder,
            indent=indent,
            ensure_ascii=False,
            **kwargs,
        )
    except (TypeError, ValueError):
        if default is not None:
            try:
                return json.dumps(
                    default(obj),
                    cls=ExtendedJSONEncoder,
                    indent=indent,
                    ensure_ascii=False,
                )
            except Exception:
                return None
        return None


def _clean_json(data: str) -> str:
    """Remove comments and trailing commas from JSON-like string.

    Args:
        data: JSON string to clean.

    Returns:
        Cleaned JSON string.
    """
    lines = data.split('\n')
    cleaned_lines = []
    in_string = False
    escape_next = False

    for line in lines:
        if escape_next:
            escape_next = False
            cleaned_lines.append(line)
            continue

        result = []
        i = 0
        while i < len(line):
            char = line[i]

            if escape_next:
                escape_next = False
                result.append(char)
                i += 1
                continue

            if char == '\\':
                escape_next = True
                result.append(char)
                i += 1
                continue

            if char == '"':
                in_string = not in_string
                result.append(char)
                i += 1
                continue

            if not in_string:
                if char == '/' and i + 1 < len(line) and line[i + 1] == '/':
                    break  # Skip rest of line (comment)
                if char == ' ' and i == len(line) - 1:
                    break  # Skip trailing whitespace
                result.append(char)
                i += 1
            else:
                result.append(char)
                i += 1

        cleaned_line = ''.join(result).rstrip()
        # Remove trailing commas
        if cleaned_line.endswith(','):
            cleaned_line = cleaned_line[:-1]
        cleaned_lines.append(cleaned_line)

    return '\n'.join(cleaned_lines)


def deep_merge(
    base: Dict[str, Any],
    override: Dict[str, Any],
    inplace: bool = False,
) -> Dict[str, Any]:
    """Deep merge two dictionaries.

    Args:
        base: Base dictionary.
        override: Dictionary with values to override.
        inplace: If True, modify base in place.

    Returns:
        Merged dictionary.
    """
    if not inplace:
        base = base.copy()

    for key, value in override.items():
        if key in base and isinstance(base[key], dict) and isinstance(value, dict):
            base[key] = deep_merge(base[key], value)
        else:
            base[key] = value

    return base


def load_json_file(
    path: Union[str, Path],
    default: Any = None,
    encoding: str = 'utf-8',
) -> Any:
    """Load JSON from file.

    Args:
        path: Path to JSON file.
        default: Value to return on error.
        encoding: File encoding.

    Returns:
        Loaded JSON data or default.
    """
    try:
        with open(path, 'r', encoding=encoding) as f:
            return json.load(f)
    except Exception:
        return default


def save_json_file(
    path: Union[str, Path],
    data: Any,
    indent: int = 2,
    encoding: str = 'utf-8',
) -> bool:
    """Save data to JSON file.

    Args:
        path: Path to save file.
        data: Data to serialize.
        indent: Indentation level.
        encoding: File encoding.

    Returns:
        True on success, False on error.
    """
    try:
        with open(path, 'w', encoding=encoding) as f:
            json.dump(data, f, cls=ExtendedJSONEncoder, indent=indent, ensure_ascii=False)
        return True
    except Exception:
        return False


def patch_json_file(
    path: Union[str, Path],
    patch: Dict[str, Any],
    create: bool = False,
) -> bool:
    """Patch a JSON file in place.

    Args:
        path: Path to JSON file.
        patch: Dictionary with patches to apply.
        create: If True, create file if it doesn't exist.

    Returns:
        True on success, False on error.
    """
    try:
        if Path(path).exists():
            data = load_json_file(path, {})
        elif create:
            data = {}
        else:
            return False

        data = deep_merge(data, patch)
        return save_json_file(path, data)
    except Exception:
        return False


class JSONFile:
    """Context manager for reading/writing JSON files.

    Usage:
        with JSONFile("config.json") as data:
            data["key"] = "value"
    """

    def __init__(
        self,
        path: Union[str, Path],
        default: Any = None,
        indent: int = 2,
        encoding: str = 'utf-8',
    ) -> None:
        """Initialize JSON file manager.

        Args:
            path: Path to JSON file.
            default: Default data if file doesn't exist.
            indent: Indentation level for saving.
            encoding: File encoding.
        """
        self.path = Path(path)
        self.default = default or {}
        self.indent = indent
        self.encoding = encoding
        self.data: Dict[str, Any] = {}

    def __enter__(self) -> Dict[str, Any]:
        """Load and return data."""
        self.data = load_json_file(self.path, self.default)
        return self.data

    def __exit__(self, *args: Any) -> None:
        """Save data on exit."""
        save_json_file(self.path, self.data, self.indent, self.encoding)

    def load(self) -> Dict[str, Any]:
        """Load data from file."""
        self.data = load_json_file(self.path, self.default)
        return self.data

    def save(self, data: Optional[Dict[str, Any]] = None) -> bool:
        """Save data to file.

        Args:
            data: Optional data to save (uses self.data if not provided).

        Returns:
            True on success.
        """
        if data is not None:
            self.data = data
        return save_json_file(self.path, self.data, self.indent, self.encoding)