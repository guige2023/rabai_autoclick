"""YAML utilities for RabAI AutoClick.

Provides:
- YAML parsing and serialization
- Safe loading of untrusted YAML
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional


def load_yaml(yaml_string: str) -> Optional[Any]:
    """Parse a YAML string.

    Args:
        yaml_string: YAML content.

    Returns:
        Parsed Python object or None on error.
    """
    try:
        import yaml
        return yaml.safe_load(yaml_string)
    except Exception:
        return None


def dump_yaml(data: Any) -> str:
    """Serialize data to YAML string.

    Args:
        data: Python object to serialize.

    Returns:
        YAML string.
    """
    import yaml
    return yaml.safe_dump(data, default_flow_style=False)


def load_yaml_file(path: str) -> Optional[Any]:
    """Load YAML from a file.

    Args:
        path: Path to YAML file.

    Returns:
        Parsed Python object or None on error.
    """
    try:
        import yaml
        with open(path, "r") as f:
            return yaml.safe_load(f)
    except Exception:
        return None


def dump_yaml_file(path: str, data: Any) -> bool:
    """Write data to a YAML file.

    Args:
        path: Output file path.
        data: Python object to serialize.

    Returns:
        True on success.
    """
    try:
        import yaml
        with open(path, "w") as f:
            yaml.safe_dump(data, f, default_flow_style=False)
        return True
    except Exception:
        return False


__all__ = [
    "load_yaml",
    "dump_yaml",
    "load_yaml_file",
    "dump_yaml_file",
]
