"""
TOML parsing and serialization utilities.

Lightweight TOML parser/writer using the standard library.
"""

from __future__ import annotations

try:
    import tomllib as toml_parser
except ImportError:
    import tomli as toml_parser  # type: ignore

from pathlib import Path
from typing import Any


def parse_toml(toml_string: str) -> dict[str, Any]:
    """
    Parse TOML string into dictionary.

    Args:
        toml_string: TOML content

    Returns:
        Parsed configuration dictionary
    """
    return toml_parser.loads(toml_string)


def parse_toml_file(path: str | Path) -> dict[str, Any]:
    """
    Parse TOML file.

    Args:
        path: Path to TOML file

    Returns:
        Parsed configuration dictionary
    """
    with open(path, "rb") as f:
        return toml_parser.load(f)  # type: ignore


def toml_dumps(data: dict[str, Any]) -> str:
    """
    Serialize dictionary to TOML string.

    Note: Standard library tomllib is read-only.
    Use 'toml' package for writing.

    Args:
        data: Configuration dictionary

    Returns:
        TOML string (requires 'toml' package)
    """
    try:
        import toml
        return toml.dumps(data)
    except ImportError:
        raise ImportError("toml package required: pip install toml")


def toml_dump_file(data: dict[str, Any], path: str | Path) -> None:
    """
    Write dictionary to TOML file.

    Args:
        data: Configuration dictionary
        path: Output file path
    """
    import toml
    with open(path, "w") as f:
        toml.dump(data, f)


def merge_toml_configs(*configs: dict[str, Any]) -> dict[str, Any]:
    """
    Deep-merge multiple TOML configurations.

    Args:
        *configs: Configuration dictionaries (later overrides earlier)

    Returns:
        Merged configuration
    """
    result: dict[str, Any] = {}
    for config in configs:
        result = _deep_merge(result, config)
    return result


def _deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    result = dict(base)
    for key, value in override.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = _deep_merge(result[key], value)
        else:
            result[key] = value
    return result


def get_nested(
    config: dict[str, Any],
    key_path: str,
    default: Any = None,
) -> Any:
    """
    Get nested TOML value using dot notation.

    Args:
        config: Configuration dictionary
        key_path: Dot-separated path (e.g. "server.host")
        default: Default if key not found

    Returns:
        Value or default
    """
    keys = key_path.split(".")
    value: Any = config
    for k in keys:
        if isinstance(value, dict) and k in value:
            value = value[k]
        else:
            return default
    return value


def set_nested(
    config: dict[str, Any],
    key_path: str,
    value: Any,
) -> dict[str, Any]:
    """
    Set nested TOML value using dot notation.

    Args:
        config: Configuration dictionary
        key_path: Dot-separated path
        value: Value to set

    Returns:
        Updated configuration (new dict)
    """
    keys = key_path.split(".")
    result = dict(config)
    current = result
    for k in keys[:-1]:
        if k not in current:
            current[k] = {}
        current = current[k]
    current[keys[-1]] = value
    return result


def extract_section(config: dict[str, Any], section: str) -> dict[str, Any]:
    """
    Extract a top-level TOML section.

    Args:
        config: Full configuration
        section: Section name (table name)

    Returns:
        Section dictionary or empty dict
    """
    return dict(config.get(section, {}))


def validate_toml_schema(
    config: dict[str, Any],
    required_keys: list[str],
) -> list[str]:
    """
    Validate that required keys are present.

    Args:
        config: Configuration dictionary
        required_keys: List of required key paths

    Returns:
        List of missing keys
    """
    missing = []
    for key in required_keys:
        if get_nested(config, key) is None:
            missing.append(key)
    return missing
