"""config_action module for rabai_autoclick.

Provides configuration management utilities: config file parsing,
environment variable support, schema validation, and config merging.
"""

from __future__ import annotations

import os
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Union

__all__ = [
    "Config",
    "ConfigSource",
    "ConfigManager",
    "ConfigSchema",
    "load_config",
    "load_json_config",
    "load_yaml_config",
    "load_env_config",
    "merge_configs",
    "ConfigError",
    "ValidationError",
]


class ConfigError(Exception):
    """Raised when config operations fail."""
    pass


class ValidationError(Exception):
    """Raised when config validation fails."""
    pass


class ConfigSource(Enum):
    """Configuration source priority."""
    DEFAULT = 0
    FILE = 1
    ENV = 2
    CLI = 3


@dataclass
class Config:
    """Configuration container."""
    data: Dict[str, Any] = field(default_factory=dict)
    source: ConfigSource = ConfigSource.DEFAULT

    def get(self, key: str, default: Any = None) -> Any:
        """Get config value using dot notation.

        Args:
            key: Key to get (e.g., "database.host").
            default: Default value if not found.

        Returns:
            Config value or default.
        """
        keys = key.split(".")
        value = self.data
        for k in keys:
            if isinstance(value, dict) and k in value:
                value = value[k]
            else:
                return default
        return value

    def set(self, key: str, value: Any) -> None:
        """Set config value using dot notation.

        Args:
            key: Key to set (e.g., "database.host").
            value: Value to set.
        """
        keys = key.split(".")
        data = self.data
        for k in keys[:-1]:
            if k not in data:
                data[k] = {}
            data = data[k]
        data[keys[-1]] = value

    def has(self, key: str) -> bool:
        """Check if key exists."""
        return self.get(key) is not None

    def merge(self, other: "Config", overwrite: bool = True) -> "Config":
        """Merge another config into this one.

        Args:
            other: Config to merge.
            overwrite: Overwrite existing values if True.

        Returns:
            Self for chaining.
        """
        self._deep_merge(self.data, other.data, overwrite=overwrite)
        return self

    def _deep_merge(self, target: dict, source: dict, overwrite: bool = True) -> None:
        """Deep merge source into target."""
        for key, value in source.items():
            if key in target and isinstance(target[key], dict) and isinstance(value, dict):
                self._deep_merge(target[key], value, overwrite=overwrite)
            elif overwrite or key not in target:
                target[key] = value

    def to_dict(self) -> Dict[str, Any]:
        """Return config as dictionary."""
        return dict(self.data)

    def __getitem__(self, key: str) -> Any:
        """Dict-style access."""
        result = self.get(key)
        if result is None:
            raise KeyError(key)
        return result

    def __setitem__(self, key: str, value: Any) -> None:
        """Dict-style setting."""
        self.set(key, value)

    def __contains__(self, key: str) -> bool:
        """Check if key exists."""
        return self.has(key)


class ConfigSchema:
    """Schema validator for configuration."""

    def __init__(self, schema: Dict[str, Any]) -> None:
        self._schema = schema

    def validate(self, config: Config) -> List[str]:
        """Validate config against schema.

        Args:
            config: Config to validate.

        Returns:
            List of validation error messages (empty if valid).
        """
        errors: List[str] = []
        for key, spec in self._schema.items():
            value = config.get(key)
            errs = self._validate_value(key, value, spec)
            errors.extend(errs)
        return errors

    def _validate_value(
        self,
        key: str,
        value: Any,
        spec: Dict[str, Any],
    ) -> List[str]:
        """Validate a single value against spec."""
        errors: List[str] = []

        required = spec.get("required", False)
        if required and value is None:
            errors.append(f"Required key missing: {key}")
            return errors

        if value is None:
            return errors

        vtype = spec.get("type")
        if vtype and not isinstance(value, vtype):
            errors.append(f"Type mismatch for {key}: expected {vtype.__name__}, got {type(value).__name__}")
            return errors

        choices = spec.get("choices")
        if choices and value not in choices:
            errors.append(f"Invalid choice for {key}: {value} (must be one of {choices})")

        min_val = spec.get("min")
        max_val = spec.get("max")
        if min_val is not None and value < min_val:
            errors.append(f"Value for {key} too small: {value} < {min_val}")
        if max_val is not None and value > max_val:
            errors.append(f"Value for {key} too large: {value} > {max_val}")

        validator = spec.get("validate")
        if validator and not validator(value):
            errors.append(f"Custom validation failed for {key}")

        return errors


class ConfigManager:
    """Central configuration manager with multiple sources."""

    def __init__(self) -> None:
        self._configs: Dict[ConfigSource, Config] = {}
        self._merged: Optional[Config] = None
        self._lock = __import__("threading").RLock()

    def add_config(self, config: Config, source: ConfigSource) -> None:
        """Add configuration from a source.

        Args:
            config: Config to add.
            source: Source priority.
        """
        with self._lock:
            self._configs[source] = config
            self._merged = None

    def get(self, key: str, default: Any = None) -> Any:
        """Get value from merged config."""
        return self.merged.get(key, default)

    def set(self, key: str, value: Any, source: ConfigSource = ConfigSource.CLI) -> None:
        """Set value in specific source."""
        with self._lock:
            if source not in self._configs:
                self._configs[source] = Config(source=source)
            self._configs[source].set(key, value)
            self._merged = None

    @property
    def merged(self) -> Config:
        """Get merged configuration (highest priority wins)."""
        with self._lock:
            if self._merged is None:
                self._merged = Config()
                for source in sorted(self._configs.keys(), key=lambda s: s.value):
                    self._merged.merge(self._configs[source])
            return self._merged

    def reload(self) -> None:
        """Force reload of merged config."""
        with self._lock:
            self._merged = None


def load_config(
    path: str,
    format: Optional[str] = None,
) -> Config:
    """Load config from file.

    Args:
        path: Path to config file.
        format: File format ("json", "yaml", "ini"). Auto-detected if None.

    Returns:
        Loaded Config.

    Raises:
        ConfigError: If loading fails.
    """
    p = Path(path)
    if not p.exists():
        raise ConfigError(f"Config file not found: {path}")

    fmt = format or p.suffix.lstrip(".")

    if fmt == "json":
        return load_json_config(path)
    elif fmt in ("yaml", "yml"):
        try:
            import yaml
            data = yaml.safe_load(p.read_text())
            return Config(data=data or {}, source=ConfigSource.FILE)
        except ImportError:
            raise ConfigError("PyYAML not installed")
    elif fmt == "ini":
        import configparser
        parser = configparser.ConfigParser()
        parser.read(path)
        data = {s: dict(parser[s]) for s in parser.sections()}
        return Config(data=data, source=ConfigSource.FILE)
    else:
        raise ConfigError(f"Unsupported config format: {format}")


def load_json_config(path: str) -> Config:
    """Load config from JSON file.

    Args:
        path: Path to JSON file.

    Returns:
        Loaded Config.
    """
    import json
    try:
        data = json.loads(Path(path).read_text())
        return Config(data=data or {}, source=ConfigSource.FILE)
    except json.JSONDecodeError as e:
        raise ConfigError(f"Invalid JSON in {path}: {e}")


def load_yaml_config(path: str) -> Config:
    """Load config from YAML file.

    Args:
        path: Path to YAML file.

    Returns:
        Loaded Config.
    """
    try:
        import yaml
        data = yaml.safe_load(Path(path).read_text())
        return Config(data=data or {}, source=ConfigSource.FILE)
    except ImportError:
        raise ConfigError("PyYAML not installed")


def load_env_config(prefix: str = "", separator: str = "_") -> Config:
    """Load config from environment variables.

    Args:
        prefix: Only load vars starting with this prefix.
        separator: Separator for nested keys (e.g., PREFIX_DB_HOST -> {db: {host: ...}}).

    Returns:
        Config from environment.
    """
    data: Dict[str, Any] = defaultdict(dict)
    for key, value in os.environ.items():
        if prefix and not key.startswith(prefix):
            continue
        parts = key[len(prefix):].lstrip(separator).lower().split(separator)
        target = data
        for part in parts[:-1]:
            target = target.setdefault(part, {})
        target[parts[-1]] = _parse_env_value(value)
    return Config(data=dict(data), source=ConfigSource.ENV)


def _parse_env_value(value: str) -> Union[str, int, float, bool]:
    """Parse environment variable value to appropriate type."""
    if value.lower() in ("true", "yes", "1", "on"):
        return True
    if value.lower() in ("false", "no", "0", "off"):
        return False
    try:
        return int(value)
    except ValueError:
        pass
    try:
        return float(value)
    except ValueError:
        pass
    return value


def merge_configs(*configs: Config) -> Config:
    """Merge multiple configs (later ones override earlier).

    Args:
        *configs: Configs to merge.

    Returns:
        Merged Config.
    """
    result = Config()
    for config in configs:
        result.merge(config)
    return result
