"""YAML utilities for RabAI AutoClick.

Provides:
- YAML parsing and dumping
- Safe loading
"""

import yaml
from pathlib import Path
from typing import Any, Dict, Optional, Union


def load_yaml(path: Union[str, Path]) -> Optional[Dict[str, Any]]:
    """Load YAML file.

    Args:
        path: Path to YAML file.

    Returns:
        Parsed YAML as dict or None.
    """
    try:
        with open(path, 'r', encoding='utf-8') as f:
            return yaml.safe_load(f)
    except Exception:
        return None


def dump_yaml(data: Any, path: Optional[Union[str, Path]] = None, indent: int = 2) -> Optional[str]:
    """Dump data to YAML string or file.

    Args:
        data: Data to serialize.
        path: Optional file path to write.
        indent: Indentation level.

    Returns:
        YAML string if path not provided, else None.
    """
    try:
        yaml_str = yaml.dump(data, indent=indent, allow_unicode=True)

        if path:
            with open(path, 'w', encoding='utf-8') as f:
                f.write(yaml_str)
            return None

        return yaml_str
    except Exception:
        return None


def safe_load_yaml(path: Union[str, Path]) -> Optional[Dict[str, Any]]:
    """Safely load YAML file (only basic Python objects).

    Args:
        path: Path to YAML file.

    Returns:
        Parsed YAML or None.
    """
    try:
        with open(path, 'r', encoding='utf-8') as f:
            return yaml.safe_load(f)
    except Exception:
        return None


def load_yaml_or_json(path: Union[str, Path]) -> Optional[Dict[str, Any]]:
    """Load YAML or JSON file based on extension.

    Args:
        path: Path to file.

    Returns:
        Parsed data or None.
    """
    path = Path(path)

    if path.suffix in ('.yaml', '.yml'):
        return load_yaml(path)
    elif path.suffix == '.json':
        import json
        try:
            with open(path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception:
            return None

    return None


def merge_yaml(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
    """Deep merge two YAML/dict structures.

    Args:
        base: Base configuration.
        override: Override configuration.

    Returns:
        Merged configuration.
    """
    result = base.copy()

    for key, value in override.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = merge_yaml(result[key], value)
        else:
            result[key] = value

    return result


def validate_yaml_schema(data: Dict[str, Any], schema: Dict[str, type]) -> bool:
    """Validate YAML data against simple schema.

    Args:
        data: Parsed YAML data.
        schema: Dict mapping keys to expected types.

    Returns:
        True if valid.
    """
    for key, expected_type in schema.items():
        if key not in data:
            return False
        if not isinstance(data[key], expected_type):
            return False
    return True