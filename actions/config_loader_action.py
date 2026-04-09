"""Config Loader Action Module.

Load and manage configuration from multiple sources with hot reload.
"""

from __future__ import annotations

import asyncio
import json
import os
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, Callable
import yaml


class ConfigFormat(Enum):
    """Supported config formats."""
    JSON = "json"
    YAML = "yaml"
    ENV = "env"
    INI = "ini"


@dataclass
class ConfigSource:
    """Configuration source."""
    path: str
    format: ConfigFormat
    priority: int = 0
    required: bool = False
    reload_interval: float | None = None


@dataclass
class ConfigValue:
    """Configuration value with metadata."""
    value: Any
    source: str
    modified_at: float


class ConfigLoadError(Exception):
    """Configuration load error."""
    pass


class ConfigManager:
    """Configuration manager with multi-source loading."""

    def __init__(self) -> None:
        self._sources: dict[str, ConfigSource] = {}
        self._config: dict[str, ConfigValue] = {}
        self._watchers: list[Callable[[str, Any], None]] = []
        self._lock = asyncio.Lock()
        self._reload_tasks: dict[str, asyncio.Task] = {}

    def add_source(self, source: ConfigSource) -> None:
        """Add a configuration source."""
        self._sources[source.path] = source
        if source.reload_interval:
            task = asyncio.create_task(self._watch_source(source))
            self._reload_tasks[source.path] = task

    async def load(self) -> dict[str, Any]:
        """Load all configuration sources."""
        await self._lock.acquire()
        try:
            all_configs = {}
            sorted_sources = sorted(self._sources.values(), key=lambda s: s.priority)
            for source in sorted_sources:
                if source.format == ConfigFormat.JSON:
                    data = await self._load_json(source.path, source.required)
                elif source.format == ConfigFormat.YAML:
                    data = await self._load_yaml(source.path, source.required)
                elif source.format == ConfigFormat.ENV:
                    data = await self._load_env(source.path)
                else:
                    data = {}
                self._merge_config(all_configs, data, source.path)
            return all_configs
        finally:
            self._lock.release()

    async def get(self, key: str, default: Any = None) -> Any:
        """Get configuration value."""
        await self._lock.acquire()
        try:
            return self._config.get(key, ConfigValue(default, "", 0)).value
        finally:
            self._lock.release()

    async def set(self, key: str, value: Any, source: str = "memory") -> None:
        """Set configuration value in memory."""
        await self._lock.acquire()
        try:
            self._config[key] = ConfigValue(value, source, asyncio.get_event_loop().time())
        finally:
            self._lock.release()

    def on_change(self, callback: Callable[[str, Any], None]) -> None:
        """Register change callback."""
        self._watchers.append(callback)

    async def _load_json(self, path: str, required: bool) -> dict:
        """Load JSON configuration."""
        if not os.path.exists(path):
            if required:
                raise ConfigLoadError(f"Required config file not found: {path}")
            return {}
        with open(path) as f:
            return json.load(f)

    async def _load_yaml(self, path: str, required: bool) -> dict:
        """Load YAML configuration."""
        if not os.path.exists(path):
            if required:
                raise ConfigLoadError(f"Required config file not found: {path}")
            return {}
        with open(path) as f:
            return yaml.safe_load(f) or {}

    async def _load_env(self, prefix: str) -> dict:
        """Load environment variables with prefix."""
        result = {}
        for key, value in os.environ.items():
            if key.startswith(prefix):
                config_key = key[len(prefix):].lower()
                result[config_key] = value
        return result

    def _merge_config(self, target: dict, source: dict, source_name: str) -> None:
        """Merge source config into target."""
        import time
        for key, value in source.items():
            target[key] = value
            self._config[key] = ConfigValue(value, source_name, time.time())

    async def _watch_source(self, source: ConfigSource) -> None:
        """Watch source for changes."""
        if not source.reload_interval:
            return
        mtime = os.path.getmtime(source.path) if os.path.exists(source.path) else 0
        while True:
            await asyncio.sleep(source.reload_interval)
            if os.path.exists(source.path):
                new_mtime = os.path.getmtime(source.path)
                if new_mtime != mtime:
                    mtime = new_mtime
                    await self.load()
                    for watcher in self._watchers:
                        watcher(source.path, None)
