"""YAML parsing and building utilities.

Provides YAML parsing and serialization for
configuration and data exchange in automation workflows.
"""

import json
from typing import Any, Dict, List, Optional, Union


try:
    import yaml
    YAML_AVAILABLE = True
except ImportError:
    YAML_AVAILABLE = False


def parse_yaml(s: str) -> Any:
    """Parse YAML string to Python object.

    Args:
        s: YAML string.

    Returns:
        Parsed Python object.

    Raises:
        ImportError: If PyYAML is not installed.
    """
    if not YAML_AVAILABLE:
        raise ImportError("PyYAML is required. Install with: pip install pyyaml")
    return yaml.safe_load(s)


def to_yaml(obj: Any, indent: int = 2) -> str:
    """Convert Python object to YAML string.

    Args:
        obj: Object to serialize.
        indent: Indentation spaces.

    Returns:
        YAML string.

    Raises:
        ImportError: If PyYAML is not installed.
    """
    if not YAML_AVAILABLE:
        raise ImportError("PyYAML is required. Install with: pip install pyyaml")
    return yaml.dump(obj, indent=indent, sort_keys=False)


def parse_yaml_file(path: str) -> Any:
    """Parse YAML from file.

    Args:
        path: File path.

    Returns:
        Parsed Python object.

    Raises:
        ImportError: If PyYAML is not installed.
        FileNotFoundError: If file doesn't exist.
    """
    if not YAML_AVAILABLE:
        raise ImportError("PyYAML is required. Install with: pip install pyyaml")
    with open(path, "r") as f:
        return yaml.safe_load(f)


def write_yaml_file(obj: Any, path: str) -> None:
    """Write Python object to YAML file.

    Args:
        obj: Object to serialize.
        path: File path.

    Raises:
        ImportError: If PyYAML is not installed.
    """
    if not YAML_AVAILABLE:
        raise ImportError("PyYAML is required. Install with: pip install pyyaml")
    with open(path, "w") as f:
        yaml.dump(obj, f, indent=2, sort_keys=False)


def yaml_to_json(yaml_str: str) -> str:
    """Convert YAML string to JSON.

    Args:
        yaml_str: YAML string.

    Returns:
        JSON string.
    """
    obj = parse_yaml(yaml_str)
    return json.dumps(obj, indent=2)


def json_to_yaml(json_str: str) -> str:
    """Convert JSON string to YAML.

    Args:
        json_str: JSON string.

    Returns:
        YAML string.
    """
    obj = json.loads(json_str)
    return to_yaml(obj)


class YamlConfig:
    """YAML configuration loader with merging.

    Example:
        config = YamlConfig("config.yaml")
        config.load()
        print(config.get("database.host"))
    """

    def __init__(self, path: Optional[str] = None) -> None:
        self._path = path
        self._data: Dict[str, Any] = {}

    def load(self, path: Optional[str] = None) -> None:
        """Load YAML from file.

        Args:
            path: File path override.
        """
        self._data = parse_yaml_file(path or self._path)

    def save(self, path: Optional[str] = None) -> None:
        """Save data to YAML file.

        Args:
            path: File path override.
        """
        write_yaml_file(self._data, path or self._path)

    def get(self, key: str, default: Any = None) -> Any:
        """Get value by dot-notation key.

        Args:
            key: Dot-notation key (e.g., "a.b.c").
            default: Default value if not found.

        Returns:
            Value or default.
        """
        keys = key.split(".")
        value = self._data
        for k in keys:
            if isinstance(value, dict) and k in value:
                value = value[k]
            else:
                return default
        return value

    def set(self, key: str, value: Any) -> None:
        """Set value by dot-notation key.

        Args:
            key: Dot-notation key.
            value: Value to set.
        """
        keys = key.split(".")
        data = self._data
        for k in keys[:-1]:
            if k not in data:
                data[k] = {}
            data = data[k]
        data[keys[-1]] = value

    @property
    def data(self) -> Dict[str, Any]:
        """Get full configuration data."""
        return self._data
