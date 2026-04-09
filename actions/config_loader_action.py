"""
Configuration Loader Action Module.

Loads and manages configuration for automation workflows,
supporting JSON, YAML, and environment variable sources.
"""

import json
import os
from typing import Any, Callable, Optional


class ConfigLoader:
    """Loads and manages configuration from various sources."""

    def __init__(self):
        """Initialize configuration loader."""
        self._config: dict[str, Any] = {}
        self._env_prefix = ""
        self._env_transform: Optional[Callable[[str], str]] = None

    def load_json(
        self,
        filepath: str,
        merge: bool = False,
    ) -> bool:
        """
        Load configuration from JSON file.

        Args:
            filepath: Path to JSON file.
            merge: If True, merge with existing config.

        Returns:
            True if loaded successfully.
        """
        try:
            with open(filepath, "r") as f:
                data = json.load(f)

            if merge:
                self._deep_merge(self._config, data)
            else:
                self._config = data

            return True
        except (FileNotFoundError, json.JSONDecodeError):
            return False

    def load_yaml(
        self,
        filepath: str,
        merge: bool = False,
    ) -> bool:
        """
        Load configuration from YAML file.

        Args:
            filepath: Path to YAML file.
            merge: If True, merge with existing config.

        Returns:
            True if loaded successfully.
        """
        try:
            import yaml
            with open(filepath, "r") as f:
                data = yaml.safe_load(f)

            if data is None:
                data = {}

            if merge:
                self._deep_merge(self._config, data)
            else:
                self._config = data

            return True
        except ImportError:
            return False
        except (FileNotFoundError, yaml.YAMLError):
            return False

    def load_env(
        self,
        prefix: str = "",
        transform: Optional[Callable[[str], str]] = None,
    ) -> None:
        """
        Load configuration from environment variables.

        Args:
            prefix: Only load vars with this prefix.
            transform: Optional function to transform env var names.
        """
        self._env_prefix = prefix
        self._env_transform = transform

        for key, value in os.environ.items():
            if prefix and not key.startswith(prefix):
                continue

            config_key = key
            if transform:
                config_key = transform(key)
            elif prefix:
                config_key = key[len(prefix):].lower()

            self._config[config_key] = self._parse_value(value)

    def get(
        self,
        key: str,
        default: Any = None,
    ) -> Any:
        """
        Get a configuration value.

        Args:
            key: Dot-separated key path (e.g., 'database.host').
            default: Default value if key not found.

        Returns:
            Configuration value or default.
        """
        parts = key.split(".")
        current = self._config

        for part in parts:
            if not isinstance(current, dict):
                return default
            current = current.get(part)
            if current is None:
                return default

        return current

    def set(
        self,
        key: str,
        value: Any,
    ) -> None:
        """
        Set a configuration value.

        Args:
            key: Dot-separated key path.
            value: Value to set.
        """
        parts = key.split(".")
        current = self._config

        for part in parts[:-1]:
            if part not in current:
                current[part] = {}
            current = current[part]

        current[parts[-1]] = value

    def get_all(self) -> dict[str, Any]:
        """
        Get entire configuration dictionary.

        Returns:
            Full configuration.
        """
        return dict(self._config)

    def _deep_merge(
        self,
        target: dict,
        source: dict,
    ) -> None:
        """Deep merge source into target."""
        for key, value in source.items():
            if key in target and isinstance(target[key], dict) and isinstance(value, dict):
                self._deep_merge(target[key], value)
            else:
                target[key] = value

    @staticmethod
    def _parse_value(value: str) -> Any:
        """Parse string value to appropriate type."""
        if value.lower() == "true":
            return True
        if value.lower() == "false":
            return False
        if value.lower() == "null" or value.lower() == "none":
            return None

        try:
            return int(value)
        except ValueError:
            pass

        try:
            return float(value)
        except ValueError:
            pass

        return value
