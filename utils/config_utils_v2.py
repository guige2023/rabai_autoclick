"""
Configuration Management Utilities v2

Provides hierarchical configuration with validation,
environment overrides, and hot-reload support.
"""

from __future__ import annotations

import copy
import json
import os
import threading
import time
from abc import ABC, abstractmethod
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, TypeVar

T = TypeVar("T")


class ConfigError(Exception):
    """Configuration-related errors."""
    pass


class ValidationError(ConfigError):
    """Validation failed for a configuration value."""
    pass


@dataclass
class ConfigSchema:
    """Schema definition for configuration validation."""
    name: str
    value_type: type | tuple[type, ...] | None = None
    required: bool = False
    default: Any = None
    validator: Callable[[Any], bool] | None = None
    description: str = ""
    choices: list[Any] | None = None
    min_value: float | None = None
    max_value: float | None = None


@dataclass
class ConfigEntry:
    """Single configuration entry."""
    key: str
    value: Any
    source: str = "default"  # "default", "env", "file", "runtime"
    schema: ConfigSchema | None = None
    timestamp: float = field(default_factory=time.time)


class ConfigSource(ABC):
    """Abstract configuration source."""

    @abstractmethod
    def read(self) -> dict[str, Any]:
        """Read configuration from source."""
        pass

    @abstractmethod
    def supports_watch(self) -> bool:
        """Check if source supports file watching."""
        return False


class DictConfigSource(ConfigSource):
    """Configuration from a dictionary."""

    def __init__(self, data: dict[str, Any]):
        self._data = data

    def read(self) -> dict[str, Any]:
        return copy.deepcopy(self._data)

    def supports_watch(self) -> bool:
        return False


class JSONConfigSource(ConfigSource):
    """Configuration from a JSON file."""

    def __init__(self, path: str | Path):
        self._path = Path(path)
        self._last_mtime: float | None = None

    def read(self) -> dict[str, Any]:
        if not self._path.exists():
            return {}

        mtime = self._path.stat().st_mtime
        if self._last_mtime is not None and mtime == self._last_mtime:
            return self._cached_data

        with open(self._path, "r") as f:
            data = json.load(f)

        self._last_mtime = mtime
        self._cached_data = data
        return copy.deepcopy(data)

    def supports_watch(self) -> bool:
        return True

    def get_mtime(self) -> float | None:
        """Get file modification time."""
        if self._path.exists():
            return self._path.stat().st_mtime
        return None


class EnvConfigSource(ConfigSource):
    """Configuration from environment variables."""

    def __init__(self, prefix: str = "APP_", separator: str = "_"):
        self._prefix = prefix
        self._separator = separator

    def read(self) -> dict[str, Any]:
        result = {}
        prefix_upper = self._prefix.upper()

        for key, value in os.environ.items():
            if key.startswith(prefix_upper):
                config_key = key[len(prefix_upper):]
                # Convert KEY_NAME to key.name
                config_key = config_key.lower().replace(self._separator.lower(), ".")

                # Try to parse as JSON for complex types
                try:
                    result[config_key] = json.loads(value)
                except (json.JSONDecodeError, TypeError):
                    result[config_key] = value

        return result

    def supports_watch(self) -> bool:
        return True


class Configuration(ABC):
    """Abstract configuration interface."""

    @abstractmethod
    def get(self, key: str, default: T = None) -> T:
        """Get a configuration value."""
        pass

    @abstractmethod
    def set(self, key: str, value: Any, source: str = "runtime") -> None:
        """Set a configuration value."""
        pass

    @abstractmethod
    def get_all(self) -> dict[str, Any]:
        """Get all configuration values."""
        pass


class HierarchicalConfig(Configuration):
    """
    Hierarchical configuration with multiple sources,
    validation, and optional hot-reload.
    """

    def __init__(
        self,
        sources: list[ConfigSource] | None = None,
        schema: dict[str, ConfigSchema] | None = None,
    ):
        self._sources = sources or []
        self._config: dict[str, ConfigEntry] = {}
        self._schema = schema or {}
        self._lock = threading.RLock()
        self._watch_callbacks: list[Callable[[str, Any], None]] = []
        self._reload()

    def _reload(self) -> None:
        """Reload configuration from all sources."""
        with self._lock:
            self._config.clear()

            # Read from sources in order (later sources override earlier)
            for source in self._sources:
                try:
                    data = source.read()
                    for key, value in data.items():
                        if key not in self._config:
                            self._config[key] = ConfigEntry(
                                key=key,
                                value=value,
                                source=source.__class__.__name__,
                            )
                        else:
                            # Override existing value
                            self._config[key].value = value
                            self._config[key].source = source.__class__.__name__
                except Exception:
                    pass  # Skip sources that fail

            # Apply defaults for missing required keys
            for key, schema in self._schema.items():
                if schema.required and key not in self._config:
                    self._config[key] = ConfigEntry(
                        key=key,
                        value=schema.default,
                        source="default",
                        schema=schema,
                    )

    def get(self, key: str, default: T = None) -> T:
        """Get a configuration value."""
        with self._lock:
            if key not in self._config:
                return default

            entry = self._config[key]

            # Validate if schema exists
            if entry.schema:
                self._validate_entry(entry)

            return entry.value

    def set(self, key: str, value: Any, source: str = "runtime") -> None:
        """Set a configuration value."""
        with self._lock:
            schema = self._schema.get(key)
            entry = ConfigEntry(
                key=key,
                value=value,
                source=source,
                schema=schema,
            )

            # Validate
            if schema:
                self._validate_entry(entry)

            old_value = self._config.get(key)
            self._config[key] = entry

            # Notify callbacks
            if old_value is None or old_value.value != value:
                self._notify_change(key, value)

    def get_all(self) -> dict[str, Any]:
        """Get all configuration values."""
        with self._lock:
            return {k: v.value for k, v in self._config.items()}

    def get_with_metadata(self, key: str) -> ConfigEntry | None:
        """Get configuration entry with metadata."""
        with self._lock:
            return copy.deepcopy(self._config.get(key))

    def _validate_entry(self, entry: ConfigEntry) -> None:
        """Validate a configuration entry against its schema."""
        if not entry.schema:
            return

        schema = entry.schema

        # Type check
        if schema.value_type is not None:
            if not isinstance(entry.value, schema.value_type):
                raise ValidationError(
                    f"Config '{entry.key}' must be of type "
                    f"{schema.value_type}, got {type(entry.value).__name__}"
                )

        # Choices check
        if schema.choices and entry.value not in schema.choices:
            raise ValidationError(
                f"Config '{entry.key}' must be one of {schema.choices}, "
                f"got {entry.value}"
            )

        # Range check for numbers
        if isinstance(entry.value, (int, float)):
            if schema.min_value is not None and entry.value < schema.min_value:
                raise ValidationError(
                    f"Config '{entry.key}' must be >= {schema.min_value}, "
                    f"got {entry.value}"
                )
            if schema.max_value is not None and entry.value > schema.max_value:
                raise ValidationError(
                    f"Config '{entry.key}' must be <= {schema.max_value}, "
                    f"got {entry.value}"
                )

        # Custom validator
        if schema.validator and not schema.validator(entry.value):
            raise ValidationError(
                f"Config '{entry.key}' failed custom validation"
            )

    def on_change(self, callback: Callable[[str, Any], None]) -> None:
        """Register a callback for configuration changes."""
        self._watch_callbacks.append(callback)

    def _notify_change(self, key: str, value: Any) -> None:
        """Notify callbacks of a configuration change."""
        for callback in self._watch_callbacks:
            try:
                callback(key, value)
            except Exception:
                pass

    def add_source(self, source: ConfigSource) -> None:
        """Add a configuration source."""
        with self._lock:
            self._sources.append(source)
            self._reload()

    def check_reload(self) -> bool:
        """
        Check if any watched sources have changed and reload if needed.

        Returns:
            True if configuration was reloaded.
        """
        reloaded = False

        for source in self._sources:
            if source.supports_watch():
                if isinstance(source, JSONConfigSource):
                    old_mtime = getattr(source, "_last_mtime", None)
                    new_mtime = source.get_mtime()
                    if new_mtime is not None and old_mtime != new_mtime:
                        self._reload()
                        reloaded = True

        return reloaded

    def validate_all(self) -> list[str]:
        """
        Validate all configuration entries.

        Returns:
            List of validation error messages.
        """
        errors = []

        for key, entry in self._config.items():
            try:
                if entry.schema:
                    self._validate_entry(entry)
            except ValidationError as e:
                errors.append(str(e))

        # Check for missing required keys
        for key, schema in self._schema.items():
            if schema.required and key not in self._config:
                errors.append(f"Required config '{key}' is missing")

        return errors


class ConfigBuilder:
    """Builder for creating configured Configuration instances."""

    def __init__(self):
        self._sources: list[ConfigSource] = []
        self._schema: dict[str, ConfigSchema] = {}

    def with_source(self, source: ConfigSource) -> ConfigBuilder:
        """Add a configuration source."""
        self._sources.append(source)
        return self

    def with_json_file(self, path: str | Path) -> ConfigBuilder:
        """Add a JSON file as configuration source."""
        self._sources.append(JSONConfigSource(path))
        return self

    def with_env_vars(self, prefix: str = "APP_") -> ConfigBuilder:
        """Add environment variables as configuration source."""
        self._sources.append(EnvConfigSource(prefix))
        return self

    def with_dict(self, data: dict[str, Any]) -> ConfigBuilder:
        """Add a dictionary as configuration source."""
        self._sources.append(DictConfigSource(data))
        return self

    def with_schema(self, schema: dict[str, ConfigSchema]) -> ConfigBuilder:
        """Add a validation schema."""
        self._schema.update(schema)
        return self

    def with_required(
        self,
        key: str,
        value_type: type | tuple[type, ...],
        default: Any = None,
    ) -> ConfigBuilder:
        """Add a required configuration key."""
        self._schema[key] = ConfigSchema(
            name=key,
            value_type=value_type,
            required=True,
            default=default,
        )
        return self

    def build(self) -> HierarchicalConfig:
        """Build the configuration."""
        return HierarchicalConfig(sources=self._sources, schema=self._schema)
