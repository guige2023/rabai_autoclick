"""
Configuration loader utilities for multi-format config management.

Provides loading, validation, merging, and hot-reloading for
JSON, YAML, TOML, INI, and environment variable configs.
"""

from __future__ import annotations

import json
import logging
import os
import time
from dataclasses import dataclass, field
from enum import Enum, auto
from pathlib import Path
from typing import Any, Optional

logger = logging.getLogger(__name__)


class ConfigFormat(Enum):
    JSON = auto()
    YAML = auto()
    TOML = auto()
    INI = auto()
    ENV = auto()


@dataclass
class ConfigSource:
    """A single configuration source."""
    path: Optional[str] = None
    content: Optional[str] = None
    format: ConfigFormat = ConfigFormat.JSON
    priority: int = 0
    required: bool = False
    watch: bool = False


@dataclass
class ConfigState:
    """Current state of loaded configuration."""
    data: dict[str, Any]
    sources: list[str] = field(default_factory=list)
    last_modified: float = field(default_factory=time.time)
    version: str = "1.0.0"


class ConfigLoader:
    """Multi-format configuration loader with merging and validation."""

    def __init__(self, base_config: Optional[dict[str, Any]] = None) -> None:
        self._sources: list[ConfigSource] = []
        self._state: Optional[ConfigState] = None
        self._callbacks: list[callable] = []
        self._watched_files: dict[str, float] = {}

    def add_json(self, path: str, priority: int = 0, required: bool = False, watch: bool = False) -> "ConfigLoader":
        """Add a JSON config source."""
        self._sources.append(ConfigSource(path=path, format=ConfigFormat.JSON, priority=priority, required=required, watch=watch))
        return self

    def add_yaml(self, path: str, priority: int = 0, required: bool = False, watch: bool = False) -> "ConfigLoader":
        """Add a YAML config source."""
        self._sources.append(ConfigSource(path=path, format=ConfigFormat.YAML, priority=priority, required=required, watch=watch))
        return self

    def add_toml(self, path: str, priority: int = 0, required: bool = False, watch: bool = False) -> "ConfigLoader":
        """Add a TOML config source."""
        self._sources.append(ConfigSource(path=path, format=ConfigFormat.TOML, priority=priority, required=required, watch=watch))
        return self

    def add_env(self, prefix: str = "", priority: int = 100) -> "ConfigLoader":
        """Add environment variables as config source."""
        self._sources.append(ConfigSource(
            content=json.dumps(dict(os.environ)),
            format=ConfigFormat.ENV,
            priority=priority,
        ))
        return self

    def add_content(self, content: str, format: ConfigFormat, priority: int = 0) -> "ConfigLoader":
        """Add raw config content."""
        self._sources.append(ConfigSource(content=content, format=format, priority=priority))
        return self

    def on_change(self, callback: callable) -> "ConfigLoader":
        """Register a callback for config changes."""
        self._callbacks.append(callback)
        return self

    def load(self) -> dict[str, Any]:
        """Load and merge all config sources."""
        self._sources.sort(key=lambda s: s.priority)
        merged: dict[str, Any] = {}

        for source in self._sources:
            try:
                data = self._load_source(source)
                if data:
                    merged = self._deep_merge(merged, data)
            except FileNotFoundError:
                if source.required:
                    raise
                logger.warning("Optional config file not found: %s", source.path)

        self._state = ConfigState(data=merged, sources=[s.path or "env" for s in self._sources])
        return merged

    def reload(self) -> bool:
        """Reload config if any files have changed."""
        changed = False
        for source in self._sources:
            if source.path and source.watch:
                mtime = os.path.getmtime(source.path)
                if source.path in self._watched_files and self._watched_files[source.path] != mtime:
                    changed = True
                    break
                self._watched_files[source.path] = mtime

        if changed:
            self.load()
            for cb in self._callbacks:
                cb(self._state.data)
            return True
        return False

    def _load_source(self, source: ConfigSource) -> dict[str, Any]:
        """Load config from a source."""
        if source.content:
            content = source.content
        elif source.path:
            with open(source.path) as f:
                content = f.read()
        else:
            return {}

        if source.format == ConfigFormat.JSON:
            return json.loads(content)
        elif source.format == ConfigFormat.YAML:
            return self._parse_yaml(content)
        elif source.format == ConfigFormat.TOML:
            return self._parse_toml(content)
        elif source.format == ConfigFormat.INI:
            return self._parse_ini(content)
        elif source.format == ConfigFormat.ENV:
            return self._parse_env(content)
        return {}

    def _parse_yaml(self, content: str) -> dict[str, Any]:
        try:
            import yaml
            return yaml.safe_load(content) or {}
        except ImportError:
            logger.warning("pyyaml not installed, using json")
            return {}

    def _parse_toml(self, content: str) -> dict[str, Any]:
        try:
            import tomli
            return tomli.loads(content)
        except ImportError:
            try:
                import toml
                return toml.loads(content)
            except ImportError:
                logger.warning("tomli/toml not installed")
                return {}

    def _parse_ini(self, content: str) -> dict[str, Any]:
        import configparser
        parser = configparser.ConfigParser()
        parser.read_string(content)
        return {section: dict(parser[section]) for section in parser.sections()}

    def _parse_env(self, content: str) -> dict[str, Any]:
        import json
        return json.loads(content)

    def _deep_merge(self, base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
        result = dict(base)
        for key, value in override.items():
            if key in result and isinstance(result[key], dict) and isinstance(value, dict):
                result[key] = self._deep_merge(result[key], value)
            else:
                result[key] = value
        return result

    def get(self, key: str, default: Any = None) -> Any:
        """Get a config value by dot-notation key."""
        if not self._state:
            self.load()
        keys = key.split(".")
        value = self._state.data
        for k in keys:
            if isinstance(value, dict):
                value = value.get(k)
            else:
                return default
        return value if value is not None else default

    def validate(self, schema: dict[str, Any]) -> list[str]:
        """Validate config against a schema, return list of errors."""
        errors = []
        if not self._state:
            self.load()
        self._validate_recursive(self._state.data, schema, "", errors)
        return errors

    def _validate_recursive(self, data: Any, schema: dict[str, Any], path: str, errors: list[str]) -> None:
        required = schema.get("required", [])
        properties = schema.get("properties", {})

        for req in required:
            if req not in data:
                errors.append(f"{path}.{req}: required field missing")

        for key, value in data.items():
            if key in properties:
                expected_type = properties[key].get("type")
                if expected_type == "string" and not isinstance(value, str):
                    errors.append(f"{path}.{key}: expected string")
                elif expected_type == "number" and not isinstance(value, (int, float)):
                    errors.append(f"{path}.{key}: expected number")
                elif expected_type == "boolean" and not isinstance(value, bool):
                    errors.append(f"{path}.{key}: expected boolean")
                elif expected_type == "object" and isinstance(properties[key].get("properties"), dict):
                    self._validate_recursive(value, properties[key], f"{path}.{key}", errors)
