"""Configuration management utilities for automation settings persistence.

Provides typed configuration loading from multiple formats
(JSON, YAML, TOML, INI), environment variable interpolation,
schema validation, and hot-reload support.

Example:
    >>> from utils.config_management_utils import load_config, save_config
    >>> cfg = load_config('/tmp/settings.json')
    >>> cfg.get('actions', []).append({'type': 'click', 'x': 100, 'y': 200})
    >>> save_config(cfg, '/tmp/settings.json')
"""

from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

__all__ = [
    "ConfigManager",
    "load_config",
    "save_config",
    "get_config_value",
    "set_config_value",
    "ConfigSchema",
    "ConfigError",
]


class ConfigError(Exception):
    """Raised when configuration operations fail."""
    pass


@dataclass
class ConfigSchema:
    """Schema definition for configuration validation."""

    defaults: dict
    required: list[str] = field(default_factory=list)
    types: dict[str, type] = field(default_factory=dict)


def load_config(
    path: str | Path,
    format: Optional[str] = None,
    schema: Optional[ConfigSchema] = None,
) -> dict:
    """Load configuration from a file.

    Args:
        path: Path to the config file.
        format: File format ('json', 'yaml', 'toml', 'ini'). Auto-detected if None.
        schema: Optional ConfigSchema for validation.

    Returns:
        Configuration dictionary.

    Raises:
        ConfigError: If loading or validation fails.
    """
    p = Path(path)
    if not p.exists():
        if schema:
            return dict(schema.defaults)
        return {}

    fmt = format or _detect_format(p)
    data: dict = {}

    try:
        if fmt == "json":
            data = json.loads(p.read_text())
        elif fmt == "yaml":
            try:
                import yaml
                data = yaml.safe_load(p.read_text()) or {}
            except ImportError:
                raise ConfigError("PyYAML not installed: pip install pyyaml")
        elif fmt == "toml":
            try:
                import tomli
                data = tomli.loads(p.read_text())
            except ImportError:
                try:
                    import toml
                    data = toml.load(str(p))
                except ImportError:
                    raise ConfigError("toml library not installed: pip install toml")
        elif fmt == "ini":
            import configparser
            parser = configparser.ConfigParser()
            parser.read(str(p))
            data = {s: dict(parser[s]) for s in parser.sections()}
        else:
            raise ConfigError(f"Unknown config format: {fmt}")
    except Exception as e:
        raise ConfigError(f"Failed to load config from {path}: {e}")

    # Apply defaults from schema
    if schema:
        for key, val in schema.defaults.items():
            if key not in data:
                data[key] = val
        # Validate required keys
        for key in schema.required:
            if key not in data:
                raise ConfigError(f"Required config key missing: {key}")
        # Validate types
        for key, expected_type in schema.types.items():
            if key in data and not isinstance(data[key], expected_type):
                raise ConfigError(
                    f"Config key '{key}' has wrong type: "
                    f"expected {expected_type.__name__}, "
                    f"got {type(data[key]).__name__}"
                )

    return data


def save_config(
    data: dict,
    path: str | Path,
    format: Optional[str] = None,
    indent: int = 2,
) -> bool:
    """Save configuration to a file.

    Args:
        data: Configuration dictionary.
        path: Destination path.
        format: File format ('json', 'yaml', 'toml'). Auto-detected if None.
        indent: Indentation level for formatted files.

    Returns:
        True if saved successfully.
    """
    p = Path(path)
    fmt = format or _detect_format(p)

    try:
        p.parent.mkdir(parents=True, exist_ok=True)

        if fmt == "json":
            p.write_text(json.dumps(data, indent=indent))
        elif fmt == "yaml":
            try:
                import yaml
                yaml.safe_dump(data, p, indent=indent, default_flow_style=False)
            except ImportError:
                raise ConfigError("PyYAML not installed")
        elif fmt == "toml":
            try:
                import tomli
                p.write_text(tomli.dumps(data))
            except ImportError:
                try:
                    import toml
                    with open(p, "w") as f:
                        toml.dump(data, f)
                except ImportError:
                    raise ConfigError("toml library not installed")
        else:
            raise ConfigError(f"Unsupported format: {fmt}")

        return True
    except Exception as e:
        raise ConfigError(f"Failed to save config to {path}: {e}")


def _detect_format(p: Path) -> str:
    ext = p.suffix.lower()
    format_map = {
        ".json": "json",
        ".yaml": "yaml",
        ".yml": "yaml",
        ".toml": "toml",
        ".ini": "ini",
        ".cfg": "ini",
    }
    return format_map.get(ext, "json")


def get_config_value(
    config: dict,
    key: str,
    default: Any = None,
    env_interpolate: bool = True,
) -> Any:
    """Get a configuration value with optional environment variable interpolation.

    Supports ${VAR_NAME} and ${VAR_NAME:-default} syntax.

    Args:
        config: Configuration dictionary.
        key: Dot-separated key path (e.g., 'database.host').
        default: Default value if key not found.
        env_interpolate: Whether to interpolate environment variables.

    Returns:
        Configuration value.
    """
    import re

    keys = key.split(".")
    value = config
    for k in keys:
        if isinstance(value, dict) and k in value:
            value = value[k]
        else:
            return default

    if env_interpolate and isinstance(value, str):
        value = _interpolate_env(value)

    return value


def _interpolate_env(text: str) -> str:
    """Interpolate environment variables in text."""
    import re

    def replacer(match):
        var_expr = match.group(1)
        if ":-" in var_expr:
            var_name, default = var_expr.split(":-", 1)
            return os.environ.get(var_name.strip(), default.strip())
        return os.environ.get(var_expr.strip(), match.group(0))

    return re.sub(r"\$\{([^}]+)\}", replacer, text)


def set_config_value(config: dict, key: str, value: Any) -> None:
    """Set a nested configuration value by dot-separated key path.

    Args:
        config: Configuration dictionary (modified in place).
        key: Dot-separated key path.
        value: Value to set.
    """
    keys = key.split(".")
    current = config
    for k in keys[:-1]:
        if k not in current:
            current[k] = {}
        current = current[k]
    current[keys[-1]] = value


class ConfigManager:
    """Hot-reloadable configuration manager with change callbacks.

    Example:
        >>> manager = ConfigManager('/tmp/config.json')
        >>> manager.on_change(lambda cfg: print("Changed!"))
        >>> manager.start_watching()  # background thread
        >>> cfg = manager.get()  # gets current config
    """

    def __init__(self, path: str | Path, schema: Optional[ConfigSchema] = None):
        self.path = Path(path)
        self.schema = schema
        self._config: dict = {}
        self._callbacks: list[callable] = []
        self._last_mtime: float = 0
        self._running = False
        self._load()

    def _load(self) -> None:
        self._config = load_config(self.path, schema=self.schema)
        try:
            self._last_mtime = self.path.stat().st_mtime
        except OSError:
            pass

    def get(self) -> dict:
        """Get the current configuration."""
        return dict(self._config)

    def reload(self) -> None:
        """Reload the configuration from disk."""
        self._load()
        self._notify()

    def on_change(self, callback: callable) -> None:
        """Register a callback for configuration changes."""
        self._callbacks.append(callback)

    def _notify(self) -> None:
        for cb in self._callbacks:
            try:
                cb(self._config)
            except Exception:
                pass

    def start_watching(self, interval: float = 2.0) -> None:
        """Start a background thread watching for file changes.

        Args:
            interval: Check interval in seconds.
        """
        import threading

        self._running = True

        def watch():
            while self._running:
                try:
                    mtime = self.path.stat().st_mtime
                    if mtime != self._last_mtime:
                        self._load()
                        self._notify()
                except Exception:
                    pass
                time.sleep(interval)

        t = threading.Thread(target=watch, daemon=True)
        t.start()

    def stop_watching(self) -> None:
        """Stop the watching thread."""
        self._running = False
