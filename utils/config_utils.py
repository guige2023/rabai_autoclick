"""Config utilities for RabAI AutoClick.

Provides:
- Config file parsing and generation
- Schema validation
- Environment-based configuration
- Config merging and inheritance
"""

import os
from typing import (
    Any,
    Callable,
    Dict,
    List,
    Optional,
    Union,
)


class Config:
    """Configuration container with dot-notation access."""

    def __init__(
        self,
        data: Optional[Dict[str, Any]] = None,
        *,
        _parent: Optional["Config"] = None,
        _key: Optional[str] = None,
    ) -> None:
        self._data = data or {}
        self._parent = _parent
        self._key = _key
        self._frozen = False

    def get(
        self,
        key: str,
        default: Any = None,
    ) -> Any:
        """Get a configuration value using dot notation.

        Args:
            key: Key in dot notation (e.g., "database.host").
            default: Default value if not found.

        Returns:
            Configuration value.
        """
        keys = key.split(".")
        current = self._data

        for k in keys:
            if isinstance(current, dict) and k in current:
                current = current[k]
            else:
                return default

        return current

    def set(
        self,
        key: str,
        value: Any,
    ) -> None:
        """Set a configuration value using dot notation.

        Args:
            key: Key in dot notation.
            value: Value to set.
        """
        if self._frozen:
            raise RuntimeError("Config is frozen")

        keys = key.split(".")
        current = self._data

        for k in keys[:-1]:
            if k not in current:
                current[k] = {}
            current = current[k]

        current[keys[-1]] = value

    def has(self, key: str) -> bool:
        """Check if a key exists.

        Args:
            key: Key in dot notation.

        Returns:
            True if key exists.
        """
        return self.get(key) is not None

    def update(
        self,
        data: Dict[str, Any],
        *,
        merge: bool = True,
    ) -> None:
        """Update configuration with new data.

        Args:
            data: Dictionary of updates.
            merge: If True, deep merge; else replace.
        """
        if self._frozen:
            raise RuntimeError("Config is frozen")

        if merge:
            self._deep_merge(self._data, data)
        else:
            self._data = data.copy()

    def _deep_merge(
        self,
        target: Dict[str, Any],
        source: Dict[str, Any],
    ) -> None:
        """Deep merge source into target."""
        for key, value in source.items():
            if (
                key in target
                and isinstance(target[key], dict)
                and isinstance(value, dict)
            ):
                self._deep_merge(target[key], value)
            else:
                target[key] = value

    def to_dict(self) -> Dict[str, Any]:
        """Get configuration as dictionary.

        Returns:
            Configuration dictionary.
        """
        return self._data.copy()

    def freeze(self) -> None:
        """Freeze configuration to prevent modifications."""
        self._frozen = True
        for value in self._data.values():
            if isinstance(value, Config):
                value.freeze()
            elif isinstance(value, dict):
                for v in value.values():
                    if isinstance(v, Config):
                        v.freeze()

    def unfreeze(self) -> None:
        """Unfreeze configuration."""
        self._frozen = False

    def __getitem__(self, key: str) -> Any:
        """Get item using bracket notation."""
        return self.get(key)

    def __setitem__(self, key: str, value: Any) -> None:
        """Set item using bracket notation."""
        self.set(key, value)

    def __contains__(self, key: str) -> bool:
        """Check if key exists."""
        return self.has(key)

    def __repr__(self) -> str:
        return f"Config({self._data!r})"


def load_env_config(
    prefix: str = "APP_",
    separator: str = "_",
    lowercase: bool = True,
) -> Dict[str, Any]:
    """Load configuration from environment variables.

    Args:
        prefix: Only load vars starting with this prefix.
        separator: Separator for nested keys (e.g., APP_DATABASE_HOST).
        lowercase: Convert keys to lowercase.

    Returns:
        Configuration dictionary.
    """
    config: Dict[str, Any] = {}
    prefix_len = len(prefix)

    for key, value in os.environ.items():
        if not key.startswith(prefix):
            continue

        key = key[prefix_len:]
        if not key:
            continue

        if lowercase:
            key = key.lower()

        parts = key.split(separator)
        current = config

        for part in parts[:-1]:
            if part not in current:
                current[part] = {}
            current = current[part]

        # Try to parse as JSON, else use string
        current[parts[-1]] = _parse_value(value)

    return config


def _parse_value(value: str) -> Any:
    """Parse a string value to appropriate type."""
    # Bool
    if value.lower() in ("true", "yes", "1"):
        return True
    if value.lower() in ("false", "no", "0"):
        return False

    # None
    if value.lower() == "none":
        return None

    # Number
    try:
        if "." in value:
            return float(value)
        return int(value)
    except ValueError:
        pass

    # JSON-like list/dict
    if value.startswith("[") or value.startswith("{"):
        try:
            import json
            return json.loads(value)
        except Exception:
            pass

    return value


def merge_configs(
    *configs: Dict[str, Any],
) -> Dict[str, Any]:
    """Merge multiple configuration dictionaries.

    Args:
        *configs: Configuration dictionaries to merge.

    Returns:
        Merged configuration.
    """
    result: Dict[str, Any] = {}
    for config in configs:
        _deep_merge_dict(result, config)
    return result


def _deep_merge_dict(
    target: Dict[str, Any],
    source: Dict[str, Any],
) -> None:
    """Deep merge source into target dict."""
    for key, value in source.items():
        if (
            key in target
            and isinstance(target[key], dict)
            and isinstance(value, dict)
        ):
            _deep_merge_dict(target[key], value)
        else:
            target[key] = value


class ConfigSchema:
    """Schema validator for configuration."""

    def __init__(
        self,
        schema: Dict[str, Any],
    ) -> None:
        """Initialize schema.

        Args:
            schema: Schema definition.
        """
        self._schema = schema

    def validate(
        self,
        config: Dict[str, Any],
    ) -> tuple[bool, List[str]]:
        """Validate configuration against schema.

        Args:
            config: Configuration to validate.

        Returns:
            Tuple of (is_valid, list_of_errors).
        """
        errors: List[str] = []
        self._validate_node(self._schema, config, "", errors)
        return len(errors) == 0, errors

    def _validate_node(
        self,
        schema: Dict[str, Any],
        config: Dict[str, Any],
        path: str,
        errors: List[str],
    ) -> None:
        """Recursively validate config node."""
        for key, spec in schema.items():
            full_path = f"{path}.{key}" if path else key

            if spec.get("required", False) and key not in config:
                errors.append(f"{full_path}: required key missing")
                continue

            if key not in config:
                continue

            value = config[key]
            expected_type = spec.get("type")

            if expected_type and not self._check_type(value, expected_type):
                errors.append(
                    f"{full_path}: expected {expected_type}, got {type(value).__name__}"
                )

            if "choices" in spec and value not in spec["choices"]:
                errors.append(
                    f"{full_path}: value must be one of {spec['choices']}"
                )

            if "min" in spec and isinstance(value, (int, float)):
                if value < spec["min"]:
                    errors.append(f"{full_path}: value must be >= {spec['min']}")

            if "max" in spec and isinstance(value, (int, float)):
                if value > spec["max"]:
                    errors.append(f"{full_path}: value must be <= {spec['max']}")

    def _check_type(
        self,
        value: Any,
        expected_type: Union[str, type],
    ) -> bool:
        """Check if value matches expected type."""
        if isinstance(expected_type, type):
            return isinstance(value, expected_type)
        if expected_type == "int":
            return isinstance(value, int)
        if expected_type == "float":
            return isinstance(value, (int, float))
        if expected_type == "str":
            return isinstance(value, str)
        if expected_type == "bool":
            return isinstance(value, bool)
        if expected_type == "list":
            return isinstance(value, list)
        if expected_type == "dict":
            return isinstance(value, dict)
        return True


def apply_env_overrides(
    config: Dict[str, Any],
    prefix: str = "APP_",
) -> Dict[str, Any]:
    """Apply environment variable overrides to config.

    Args:
        config: Base configuration.
        prefix: Environment variable prefix.

    Returns:
        Configuration with overrides applied.
    """
    env_config = load_env_config(prefix)
    if env_config:
        merge_configs(config, env_config)
    return config


class LazyConfig:
    """Configuration loaded lazily from a source."""

    def __init__(
        self,
        loader: Callable[[], Dict[str, Any]],
    ) -> None:
        """Initialize lazy config.

        Args:
            loader: Function that loads and returns config dict.
        """
        self._loader = loader
        self._config: Optional[Dict[str, Any]] = None

    def load(self) -> None:
        """Force load the configuration."""
        if self._config is None:
            self._config = self._loader()

    def get(
        self,
        key: str,
        default: Any = None,
    ) -> Any:
        """Get a configuration value."""
        if self._config is None:
            self.load()
        assert self._config is not None
        return Config(self._config).get(key, default)

    def is_loaded(self) -> bool:
        """Check if configuration is loaded."""
        return self._config is not None
