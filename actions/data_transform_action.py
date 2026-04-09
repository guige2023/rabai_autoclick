"""
Data Transform Action Module.

Provides data transformation utilities for restructuring, mapping,
and converting data between different formats.

Author: RabAi Team
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Tuple, Union


@dataclass
class TransformRule:
    """A single transformation rule."""
    source_path: str
    target_path: str
    transform_func: Optional[Callable] = None
    default: Any = None


@dataclass
class MappingConfig:
    """Configuration for data mapping."""
    rules: List[TransformRule] = field(default_factory=list)
    ignore_missing: bool = False
    copy_unmatched: bool = True


class DataTransformer:
    """Main data transformation engine."""

    def __init__(self) -> None:
        self.config: Optional[MappingConfig] = None

    def configure(self, config: MappingConfig) -> "DataTransformer":
        """Configure the transformer."""
        self.config = config
        return self

    def _get_value(
        self,
        data: Dict[str, Any],
        path: str,
        default: Any = None,
    ) -> Any:
        """Get value from nested dict using path."""
        keys = path.split(".")
        value = data
        for key in keys:
            if isinstance(value, dict):
                value = value.get(key)
                if value is None:
                    return default
            else:
                return default
        return value if value is not None else default

    def _set_value(
        self,
        data: Dict[str, Any],
        path: str,
        value: Any,
    ) -> None:
        """Set value in nested dict using path."""
        keys = path.split(".")
        current = data
        for key in keys[:-1]:
            if key not in current:
                current[key] = {}
            current = current[key]
        current[keys[-1]] = value

    def apply_rules(
        self,
        data: Dict[str, Any],
        rules: List[TransformRule],
    ) -> Dict[str, Any]:
        """Apply transformation rules to data."""
        result = {}

        for rule in rules:
            value = self._get_value(data, rule.source_path, rule.default)

            if rule.transform_func:
                try:
                    value = rule.transform_func(value)
                except Exception:
                    value = rule.default

            self._set_value(result, rule.target_path, value)

        return result

    def flatten(
        self,
        data: Dict[str, Any],
        separator: str = ".",
        prefix: str = "",
    ) -> Dict[str, Any]:
        """Flatten nested dictionary."""
        result = {}

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
        """Unflatten dictionary to nested structure."""
        result: Dict[str, Any] = {}

        for flat_key, value in data.items():
            keys = []
            current = ""
            for part in flat_key.split(separator):
                if "[" in part:
                    idx = part.index("[")
                    if current:
                        keys.append(current + part[:idx])
                    else:
                        keys.append(part[:idx])
                    current = part[idx:]
                else:
                    current = part

            if current:
                keys.append(current)

            self._set_nested_value(result, keys, value)

        return result

    def _set_nested_value(
        self,
        data: Dict[str, Any],
        keys: List[str],
        value: Any,
    ) -> None:
        """Set value in nested structure from key list."""
        for key in keys[:-1]:
            if key not in data:
                data[key] = {}
            data = data[key]

        data[keys[-1]] = value

    def rename_keys(
        self,
        data: Dict[str, Any],
        key_map: Dict[str, str],
    ) -> Dict[str, Any]:
        """Rename keys in dictionary."""
        result = {}
        for key, value in data.items():
            new_key = key_map.get(key, key)
            if isinstance(value, dict):
                result[new_key] = self.rename_keys(value, key_map)
            else:
                result[new_key] = value
        return result

    def pick(
        self,
        data: Dict[str, Any],
        keys: List[str],
    ) -> Dict[str, Any]:
        """Pick specific keys from dictionary."""
        return {k: data.get(k) for k in keys if k in data}

    def omit(
        self,
        data: Dict[str, Any],
        keys: List[str],
    ) -> Dict[str, Any]:
        """Omit specific keys from dictionary."""
        return {k: v for k, v in data.items() if k not in keys}

    def transform(
        self,
        data: Union[Dict[str, Any], List[Dict[str, Any]]],
        rules: List[TransformRule],
    ) -> Union[Dict[str, Any], List[Dict[str, Any]]]:
        """Transform data using rules."""
        if isinstance(data, list):
            return [self.apply_rules(item, rules) for item in data]
        return self.apply_rules(data, rules)

    def map_values(
        self,
        data: Dict[str, Any],
        value_map: Dict[str, Callable],
    ) -> Dict[str, Any]:
        """Apply functions to values based on key patterns."""
        result = {}
        for key, value in data.items():
            if key in value_map:
                try:
                    result[key] = value_map[key](value)
                except Exception:
                    result[key] = value
            else:
                result[key] = value
        return result


# Helper functions
def uppercase(value: Any) -> str:
    """Convert value to uppercase string."""
    return str(value).upper()

def lowercase(value: Any) -> str:
    """Convert value to lowercase string."""
    return str(value).lower()

def trim(value: Any) -> str:
    """Trim whitespace from string."""
    return str(value).strip()

def to_int(value: Any, default: int = 0) -> int:
    """Convert to integer."""
    try:
        return int(value)
    except (ValueError, TypeError):
        return default

def to_float(value: Any, default: float = 0.0) -> float:
    """Convert to float."""
    try:
        return float(value)
    except (ValueError, TypeError):
        return default

def to_bool(value: Any) -> bool:
    """Convert to boolean."""
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.lower() in ("true", "1", "yes", "on")
    return bool(value)

def default_if_none(value: Any, default: Any) -> Any:
    """Return default if value is None."""
    return default if value is None else value
