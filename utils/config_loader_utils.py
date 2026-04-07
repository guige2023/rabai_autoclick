"""Configuration loader utilities: multi-format config parsing (YAML, JSON, TOML, env vars)."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

__all__ = [
    "Config",
    "ConfigLoader",
    "load_config",
    "load_yaml",
    "load_json",
    "load_env",
]


@dataclass
class Config:
    """Configuration container with dot-notation access."""

    _data: dict[str, Any] = field(default_factory=dict)

    def __getattr__(self, key: str) -> Any:
        return self._data.get(key)

    def __getitem__(self, key: str) -> Any:
        return self._data[key]

    def get(self, key: str, default: Any = None) -> Any:
        return self._data.get(key, default)

    def set(self, key: str, value: Any) -> None:
        self._data[key] = value

    def to_dict(self) -> dict[str, Any]:
        return dict(self._data)


class ConfigLoader:
    """Multi-format configuration file loader."""

    @staticmethod
    def load_yaml(path: str) -> dict[str, Any]:
        """Load a YAML configuration file."""
        try:
            import yaml
        except ImportError:
            return {}

        with open(path) as f:
            return yaml.safe_load(f) or {}

    @staticmethod
    def load_json(path: str) -> dict[str, Any]:
        """Load a JSON configuration file."""
        with open(path) as f:
            return json.load(f)

    @staticmethod
    def load_toml(path: str) -> dict[str, Any]:
        """Load a TOML configuration file."""
        try:
            import tomli
        except ImportError:
            try:
                import toml as tomli
            except ImportError:
                return {}
        with open(path, "rb") as f:
            return tomli.load(f)

    @staticmethod
    def load_env(prefix: str = "", separator: str = "_") -> dict[str, str]:
        """Load configuration from environment variables."""
        result: dict[str, str] = {}
        for key, value in os.environ.items():
            if prefix and not key.startswith(prefix):
                continue
            config_key = key
            if prefix:
                config_key = key[len(prefix):].lstrip(separator)
            config_key = config_key.lower().replace(separator, ".")
            result[config_key] = value
        return result

    @classmethod
    def load(
        cls,
        path: str | None = None,
        env_prefix: str = "",
        **overrides: Any,
    ) -> Config:
        """Load configuration from multiple sources with priority."""
        data: dict[str, Any] = {}

        if path:
            p = Path(path)
            if p.suffix in (".yaml", ".yml"):
                data = cls.load_yaml(path)
            elif p.suffix == ".json":
                data = cls.load_json(path)
            elif p.suffix == ".toml":
                data = cls.load_toml(path)

        env_config = cls.load_env(prefix=env_prefix)
        for key, value in env_config.items():
            cls._set_nested(data, key, value)

        for key, value in overrides.items():
            cls._set_nested(data, key, value)

        return Config(_data=data)

    @staticmethod
    def _set_nested(data: dict[str, Any], key: str, value: Any) -> None:
        """Set a nested dictionary value using dot notation."""
        parts = key.split(".")
        current = data
        for part in parts[:-1]:
            if part not in current:
                current[part] = {}
            current = current[part]
        current[parts[-1]] = value


def load_config(path: str | None = None, **kwargs: Any) -> Config:
    """Convenience function to load configuration."""
    return ConfigLoader.load(path, **kwargs)


def load_yaml(path: str) -> dict[str, Any]:
    """Convenience function to load YAML config."""
    return ConfigLoader.load_yaml(path)


def load_json(path: str) -> dict[str, Any]:
    """Convenience function to load JSON config."""
    return ConfigLoader.load_json(path)


def load_env(prefix: str = "") -> dict[str, str]:
    """Convenience function to load env vars as config."""
    return ConfigLoader.load_env(prefix=prefix)
