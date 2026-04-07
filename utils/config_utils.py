"""Configuration management utilities: env vars, JSON/YAML config, validation."""

from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable

__all__ = [
    "ConfigSource",
    "Config",
    "load_config",
    "ConfigManager",
    "env",
]


@dataclass
class ConfigSource:
    """A configuration source."""
    name: str
    priority: int = 0
    get: Callable[[str], str | None] = field(default=lambda _: None)


class Config:
    """Configuration container with environment variable and file support."""

    def __init__(self) -> None:
        self._values: dict[str, Any] = {}
        self._sources: list[ConfigSource] = []

    def add_source(self, source: ConfigSource) -> None:
        self._sources.append(source)
        self._sources.sort(key=lambda s: s.priority)

    def set(self, key: str, value: Any) -> None:
        self._values[key] = value

    def get(
        self,
        key: str,
        default: Any = None,
        type_fn: Callable[[str], Any] | type | None = None,
    ) -> Any:
        value = None
        for source in self._sources:
            raw = source.get(key)
            if raw is not None:
                value = raw
                break
        if value is None:
            value = os.environ.get(key)

        if value is None:
            return default

        if type_fn is not None:
            if callable(type_fn) and not isinstance(type_fn, type):
                value = type_fn(value)
            elif isinstance(type_fn, type):
                if type_fn == bool:
                    value = value.lower() in ("true", "1", "yes", "on")
                elif type_fn in (int, float, str):
                    value = type_fn(value)
                else:
                    value = type_fn(value)
        return value

    def __getitem__(self, key: str) -> Any:
        v = self.get(key)
        if v is None:
            raise KeyError(key)
        return v

    def __contains__(self, key: str) -> bool:
        return self.get(key) is not None

    def to_dict(self) -> dict[str, Any]:
        return dict(self._values)


def env(key: str, default: Any = None, type_fn: type | None = None) -> Any:
    """Quick access to environment variables with type coercion."""
    return Config().get(key, default, type_fn)


class ConfigManager:
    """Manages multiple config files with hot reload support."""

    def __init__(self) -> None:
        self._configs: dict[str, dict[str, Any]] = {}
        self._file_mtimes: dict[str, float] = {}
        self._parsers: dict[str, Callable[[str], dict[str, Any]]] = {
            ".json": self._parse_json,
            ".yaml": self._parse_yaml,
            ".yml": self._parse_yaml,
            ".env": self._parse_env,
        }

    def load(self, path: str | Path) -> None:
        path = str(path)
        self._configs[path] = self._read_file(path)
        self._file_mtimes[path] = Path(path).stat().st_mtime

    def get(self, path: str | Path, key: str, default: Any = None) -> Any:
        p = str(path)
        if p not in self._configs:
            self.load(p)
        return self._configs.get(p, {}).get(key, default)

    def reload_if_changed(self) -> list[str]:
        changed: list[str] = []
        for path, last_mtime in list(self._file_mtimes.items()):
            try:
                current_mtime = Path(path).stat().st_mtime
                if current_mtime > last_mtime:
                    self._configs[path] = self._read_file(path)
                    self._file_mtimes[path] = current_mtime
                    changed.append(path)
            except FileNotFoundError:
                pass
        return changed

    def _read_file(self, path: str) -> dict[str, Any]:
        p = Path(path)
        suffix = p.suffix.lower()

        parser = self._parsers.get(suffix, self._parse_json)
        return parser(path)

    def _parse_json(self, path: str) -> dict[str, Any]:
        try:
            with open(path) as f:
                return json.load(f)
        except Exception:
            return {}

    def _parse_yaml(self, path: str) -> dict[str, Any]:
        try:
            import yaml
            with open(path) as f:
                return yaml.safe_load(f) or {}
        except Exception:
            return {}

    def _parse_env(self, path: str) -> dict[str, Any]:
        result = {}
        try:
            with open(path) as f:
                for line in f:
                    line = line.strip()
                    if not line or line.startswith("#"):
                        continue
                    if "=" in line:
                        k, v = line.split("=", 1)
                        result[k.strip()] = v.strip().strip("\"'")
        except Exception:
            pass
        return result


def load_config(
    path: str | Path,
    env_prefix: str = "",
) -> dict[str, Any]:
    """Load config from file and override with environment variables."""
    p = Path(path)
    config: dict[str, Any] = {}

    if p.suffix == ".json":
        with open(p) as f:
            config = json.load(f) or {}
    elif p.suffix in (".yaml", ".yml"):
        try:
            import yaml
            with open(p) as f:
                config = yaml.safe_load(f) or {}
        except Exception:
            pass

    if env_prefix:
        for key in list(config.keys()):
            env_key = f"{env_prefix}_{key.upper()}"
            if env_key in os.environ:
                config[key] = os.environ[env_key]

    return config
