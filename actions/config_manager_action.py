"""Config manager action for handling application configuration.

Provides configuration loading, validation, hot-reloading,
and environment-specific overrides.
"""

import json
import logging
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

logger = logging.getLogger(__name__)


@dataclass
class ConfigSchema:
    name: str
    type: str
    required: bool = False
    default: Any = None
    description: str = ""


@dataclass
class ConfigSource:
    source_type: str
    path: str
    priority: int = 0


class ConfigManagerAction:
    """Manage application configuration with validation and hot-reload.

    Args:
        config_dir: Directory to search for config files.
        env_prefix: Environment variable prefix for overrides.
    """

    def __init__(
        self,
        config_dir: Optional[str] = None,
        env_prefix: str = "APPCFG",
    ) -> None:
        self._config_dir = Path(config_dir) if config_dir else Path.cwd() / "config"
        self._env_prefix = env_prefix
        self._config: dict[str, Any] = {}
        self._schemas: dict[str, ConfigSchema] = {}
        self._sources: list[ConfigSource] = []
        self._watchers: list[callable] = []
        self._loaded = False

    def load(self, config_file: str = "config.json") -> bool:
        """Load configuration from file.

        Args:
            config_file: Configuration file name.

        Returns:
            True if configuration was loaded successfully.
        """
        config_path = self._config_dir / config_file
        try:
            if config_path.exists():
                with open(config_path) as f:
                    self._config = json.load(f)
                self._sources.append(ConfigSource("file", str(config_path), priority=1))
                logger.info(f"Loaded configuration from {config_path}")
                self._loaded = True
                self._apply_env_overrides()
                return True
            else:
                logger.warning(f"Config file not found: {config_path}")
                return False
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON in config file: {e}")
            return False
        except Exception as e:
            logger.error(f"Error loading config: {e}")
            return False

    def _apply_env_overrides(self) -> None:
        """Apply environment variable overrides."""
        for key, value in os.environ.items():
            if key.startswith(f"{self._env_prefix}_"):
                config_key = key[len(self._env_prefix) + 1:].lower()
                self._config[config_key] = self._try_parse(value)
                logger.debug(f"Applied env override: {config_key}")

    def _try_parse(self, value: str) -> Any:
        """Try to parse a string value to appropriate type.

        Args:
            value: String value to parse.

        Returns:
            Parsed value or original string.
        """
        if value.lower() == "true":
            return True
        if value.lower() == "false":
            return False
        if value.lower() == "null":
            return None
        try:
            return int(value)
        except ValueError:
            try:
                return float(value)
            except ValueError:
                return value

    def get(self, key: str, default: Any = None) -> Any:
        """Get a configuration value.

        Args:
            key: Configuration key (supports dot notation).
            default: Default value if key not found.

        Returns:
            Configuration value or default.
        """
        keys = key.split(".")
        value = self._config
        for k in keys:
            if isinstance(value, dict):
                value = value.get(k)
                if value is None:
                    return default
            else:
                return default
        return value if value is not None else default

    def set(self, key: str, value: Any) -> None:
        """Set a configuration value.

        Args:
            key: Configuration key (supports dot notation).
            value: Value to set.
        """
        keys = key.split(".")
        config = self._config
        for k in keys[:-1]:
            if k not in config:
                config[k] = {}
            config = config[k]
        config[keys[-1]] = value
        self._notify_watchers(key, value)

    def register_schema(self, schema: ConfigSchema) -> None:
        """Register a configuration schema for validation.

        Args:
            schema: Configuration schema.
        """
        self._schemas[schema.name] = schema

    def validate(self) -> list[str]:
        """Validate configuration against schemas.

        Returns:
            List of validation error messages.
        """
        errors = []
        for name, schema in self._schemas.items():
            value = self.get(name)
            if schema.required and value is None:
                errors.append(f"Required config '{name}' is missing")
                continue
            if value is not None:
                expected_type = schema.type
                if not self._check_type(value, expected_type):
                    errors.append(
                        f"Config '{name}' has wrong type: "
                        f"expected {expected_type}, got {type(value).__name__}"
                    )
        return errors

    def _check_type(self, value: Any, expected_type: str) -> bool:
        """Check if value matches expected type.

        Args:
            value: Value to check.
            expected_type: Expected type name.

        Returns:
            True if type matches.
        """
        type_map = {
            "string": str,
            "int": int,
            "float": (int, float),
            "bool": bool,
            "list": list,
            "dict": dict,
        }
        expected = type_map.get(expected_type)
        if expected:
            return isinstance(value, expected)
        return True

    def register_watcher(self, watcher: callable) -> None:
        """Register a watcher for configuration changes.

        Args:
            watcher: Callback function(key, value).
        """
        self._watchers.append(watcher)

    def _notify_watchers(self, key: str, value: Any) -> None:
        """Notify all watchers of a configuration change.

        Args:
            key: Changed configuration key.
            value: New value.
        """
        for watcher in self._watchers:
            try:
                watcher(key, value)
            except Exception as e:
                logger.error(f"Config watcher error: {e}")

    def reload(self) -> bool:
        """Reload configuration from file.

        Returns:
            True if reload was successful.
        """
        self._config = {}
        self._sources.clear()
        return self.load()

    def save(self, config_file: str = "config.json") -> bool:
        """Save current configuration to file.

        Args:
            config_file: Configuration file name.

        Returns:
            True if save was successful.
        """
        config_path = self._config_dir / config_file
        try:
            self._config_dir.mkdir(parents=True, exist_ok=True)
            with open(config_path, "w") as f:
                json.dump(self._config, f, indent=2)
            logger.info(f"Saved configuration to {config_path}")
            return True
        except Exception as e:
            logger.error(f"Error saving config: {e}")
            return False

    def get_all(self) -> dict[str, Any]:
        """Get all configuration values.

        Returns:
            Dictionary of all configuration values.
        """
        return self._config.copy()

    def get_sources(self) -> list[ConfigSource]:
        """Get configuration sources.

        Returns:
            List of configuration sources in priority order.
        """
        return sorted(self._sources, key=lambda s: s.priority, reverse=True)

    def is_loaded(self) -> bool:
        """Check if configuration is loaded.

        Returns:
            True if configuration is loaded.
        """
        return self._loaded
