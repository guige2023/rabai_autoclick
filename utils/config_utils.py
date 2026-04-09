"""Configuration management utilities.

Provides config loading, validation, and environment
handling for application settings.
"""

import os
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Union


@dataclass
class ConfigSchema:
    """Schema definition for configuration validation."""
    name: str
    type: type
    default: Any = None
    required: bool = False
    validator: Optional[Callable[[Any], bool]] = None
    description: str = ""


class Config:
    """Configuration container with validation.

    Example:
        config = Config()
        config.set("debug", True)
        config.set("port", 8080)
        if config.get("debug"):
            print("Debug mode")
    """

    def __init__(self, data: Optional[Dict[str, Any]] = None) -> None:
        self._data: Dict[str, Any] = data or {}
        self._schemas: Dict[str, ConfigSchema] = {}

    def set_schema(self, schema: ConfigSchema) -> None:
        """Register configuration schema."""
        self._schemas[schema.name] = schema

    def set(self, key: str, value: Any, validate: bool = True) -> None:
        """Set configuration value."""
        if validate and key in self._schemas:
            self._validate(key, value)

        self._data[key] = value

    def get(self, key: str, default: Any = None) -> Any:
        """Get configuration value."""
        return self._data.get(key, default)

    def _validate(self, key: str, value: Any) -> None:
        """Validate value against schema."""
        schema = self._schemas.get(key)
        if not schema:
            return

        if schema.validator and not schema.validator(value):
            raise ValueError(f"Validation failed for {key}: {value}")

        if schema.type and not isinstance(value, schema.type):
            if not (schema.type == int and isinstance(value, float) and value.is_integer()):
                raise TypeError(f"{key} must be {schema.type.__name__}, got {type(value).__name__}")

    def has(self, key: str) -> bool:
        """Check if key exists."""
        return key in self._data

    def remove(self, key: str) -> bool:
        """Remove key from configuration."""
        if key in self._data:
            del self._data[key]
            return True
        return False

    def to_dict(self) -> Dict[str, Any]:
        """Export configuration as dict."""
        return dict(self._data)

    def update(self, data: Dict[str, Any]) -> None:
        """Update multiple values."""
        for key, value in data.items():
            self.set(key, value)


class EnvConfig(Config):
    """Configuration loaded from environment variables.

    Example:
        config = EnvConfig(prefix="APP_")
        config.load_env(["PORT", "DEBUG", "DATABASE_URL"])
        port = config.get("PORT", default=8080)
    """

    def __init__(self, prefix: str = "", separator: str = "_") -> None:
        super().__init__()
        self.prefix = prefix
        self.separator = separator

    def load_env(
        self,
        keys: List[str],
        types: Optional[Dict[str, type]] = None,
    ) -> None:
        """Load values from environment variables.

        Args:
            keys: List of environment variable names.
            types: Optional mapping of key to type.
        """
        types = types or {}
        for key in keys:
            env_var = self.prefix + key
            value = os.environ.get(env_var)

            if value is None:
                continue

            value_type = types.get(key, str)
            try:
                if value_type == bool:
                    self.set(key, value.lower() in ("true", "1", "yes"))
                elif value_type == int:
                    self.set(key, int(value))
                elif value_type == float:
                    self.set(key, float(value))
                else:
                    self.set(key, value)
            except (ValueError, TypeError):
                pass

    def require_env(self, keys: List[str]) -> None:
        """Load required environment variables.

        Raises:
            EnvironmentError: If any required variable is missing.
        """
        missing = []
        for key in keys:
            env_var = self.prefix + key
            if os.environ.get(env_var) is None:
                missing.append(env_var)

        if missing:
            raise EnvironmentError(f"Missing required environment variables: {', '.join(missing)}")


def load_json_config(path: Union[str, Path]) -> Dict[str, Any]:
    """Load configuration from JSON file.

    Example:
        config = load_json_config("config.json")
    """
    path = Path(path)
    if not path.exists():
        return {}

    with open(path, "r") as f:
        return json.load(f)


def save_json_config(config: Dict[str, Any], path: Union[str, Path]) -> None:
    """Save configuration to JSON file.

    Example:
        save_json_config({"debug": True}, "config.json")
    """
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)

    with open(path, "w") as f:
        json.dump(config, f, indent=2)


class ConfigSource:
    """Base class for configuration sources."""

    def load(self) -> Dict[str, Any]:
        """Load configuration."""
        raise NotImplementedError

    def save(self, config: Dict[str, Any]) -> None:
        """Save configuration."""
        raise NotImplementedError


class JSONConfigSource(ConfigSource):
    """JSON file configuration source."""

    def __init__(self, path: Union[str, Path]) -> None:
        self.path = Path(path)

    def load(self) -> Dict[str, Any]:
        return load_json_config(self.path)

    def save(self, config: Dict[str, Any]) -> None:
        save_json_config(config, self.path)


class ConfigManager:
    """Multi-source configuration manager.

    Example:
        manager = ConfigManager()
        manager.add_source(JSONConfigSource("defaults.json"))
        manager.add_source(EnvConfig(prefix="APP_"))
        config = manager.load()
    """

    def __init__(self) -> None:
        self._sources: List[ConfigSource] = []

    def add_source(self, source: ConfigSource, priority: int = 0) -> None:
        """Add configuration source with priority.

        Higher priority sources override lower ones.
        """
        self._sources.append((priority, source))
        self._sources.sort(key=lambda x: x[0], reverse=True)

    def load(self) -> Dict[str, Any]:
        """Load merged configuration from all sources."""
        config: Dict[str, Any] = {}

        for _, source in self._sources:
            source_config = source.load()
            config.update(source_config)

        return config

    def save(self, config: Dict[str, Any]) -> None:
        """Save configuration to all writable sources."""
        for _, source in self._sources:
            try:
                source.save(config)
            except (NotImplementedError, OSError):
                pass


@dataclass
class FeatureToggle:
    """Feature toggle configuration."""
    name: str
    enabled: bool = False
    description: str = ""
    rollout_percentage: int = 0


class FeatureToggles:
    """Manage feature toggles.

    Example:
        toggles = FeatureToggles()
        toggles.add("new_ui", enabled=True)
        if toggles.is_enabled("new_ui"):
            show_new_ui()
    """

    def __init__(self) -> None:
        self._toggles: Dict[str, FeatureToggle] = {}

    def add(
        self,
        name: str,
        enabled: bool = False,
        description: str = "",
        rollout_percentage: int = 0,
    ) -> None:
        """Add a feature toggle."""
        self._toggles[name] = FeatureToggle(
            name=name,
            enabled=enabled,
            description=description,
            rollout_percentage=rollout_percentage,
        )

    def enable(self, name: str) -> None:
        """Enable a feature."""
        if name in self._toggles:
            self._toggles[name].enabled = True

    def disable(self, name: str) -> None:
        """Disable a feature."""
        if name in self._toggles:
            self._toggles[name].enabled = False

    def is_enabled(self, name: str) -> bool:
        """Check if feature is enabled."""
        if name not in self._toggles:
            return False
        return self._toggles[name].enabled

    def remove(self, name: str) -> bool:
        """Remove a feature toggle."""
        if name in self._toggles:
            del self._toggles[name]
            return True
        return False

    def list_all(self) -> List[FeatureToggle]:
        """List all feature toggles."""
        return list(self._toggles.values())
