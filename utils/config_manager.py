"""Configuration management for RabAI AutoClick.

Provides:
- ConfigManager: Singleton configuration management
- Environment-based config loading
- Config validation and defaults
"""

import os
import threading
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Union


DEFAULT_CONFIG: Dict[str, Any] = {
    # App settings
    "app.name": "RabAI AutoClick",
    "app.version": "22.0.0",
    "app.debug": False,
    "app.log_level": "INFO",

    # Execution settings
    "execution.default_timeout": 30.0,
    "execution.max_retries": 3,
    "execution.retry_delay": 1.0,
    "execution.screenshot_quality": 85,

    # UI settings
    "ui.theme": "light",
    "ui.language": "zh_CN",
    "ui.window_width": 1024,
    "ui.window_height": 768,
    "ui.always_on_top": False,

    # Hotkey settings
    "hotkey.default_stop": "esc",
    "hotkey.default_pause": "F9",
    "hotkey.default_resume": "F10",

    # Cache settings
    "cache.enabled": True,
    "cache.max_size": 100,
    "cache.ttl": 3600,

    # Performance settings
    "perf.image_cache_size": 10,
    "perf.ocr_cache_enabled": True,
    "perf.auto_optimize": True,
    "perf.gc_interval": 300,

    # Workflow settings
    "workflow.save_history": True,
    "workflow.max_history": 100,
    "workflow.auto_save": True,
    "workflow.auto_save_interval": 60,

    # Recording settings
    "recording.format": "json",
    "recording.include_screenshots": False,
    "recording.frame_rate": 30,

    # Network settings (for future cloud features)
    "network.timeout": 10.0,
    "network.retry": 3,
    "network.proxy": None,

    # Security settings
    "security.allow_remote": False,
    "security.api_key": None,
}


@dataclass
class ConfigSource:
    """Represents a configuration source."""
    name: str
    priority: int
    data: Dict[str, Any]


class ConfigManager:
    """Singleton configuration manager with layered sources.

    Priority order (highest to lowest):
    1. Environment variables (prefix: RABAI_)
    2. User config file (~/.rabai/config.json)
    3. Project config file (./config.json)
    4. Default config (hardcoded)
    """

    _instance: Optional['ConfigManager'] = None
    _lock: threading.Lock = threading.Lock()

    def __new__(cls) -> 'ConfigManager':
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialized = False
        return cls._instance

    def __init__(self) -> None:
        if self._initialized:
            return

        self._initialized = True
        self._config: Dict[str, Any] = {}
        self._sources: List[ConfigSource] = []
        self._lock = threading.RLock()

        # Load default config first
        self._add_source("defaults", 0, DEFAULT_CONFIG.copy())

        # Load project config
        project_config = Path("config.json")
        if project_config.exists():
            self._load_json_file(project_config, "project", 10)

        # Load user config
        user_config = Path.home() / ".rabai" / "config.json"
        if user_config.exists():
            self._load_json_file(user_config, "user", 20)

        # Load environment variables last (highest priority)
        self._load_env_vars("env", 30)

    def _add_source(self, name: str, priority: int, data: Dict[str, Any]) -> None:
        """Add a configuration source."""
        source = ConfigSource(name=name, priority=priority, data=data)
        self._sources.append(source)
        self._sources.sort(key=lambda s: s.priority)

        # Merge into main config, expanding dot-separated keys
        with self._lock:
            expanded_data = self._expand_dot_keys(data)
            self._deep_update(self._config, expanded_data)

    def _expand_dot_keys(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Expand dot-separated keys into nested dicts."""
        result: Dict[str, Any] = {}
        for key, value in data.items():
            if "." in key:
                keys = key.split(".")
                current = result
                for k in keys[:-1]:
                    if k not in current:
                        current[k] = {}
                    current = current[k]
                current[keys[-1]] = value
            else:
                result[key] = value
        return result

    def _load_json_file(self, path: Path, name: str, priority: int) -> None:
        """Load configuration from JSON file."""
        import json
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, dict):
                self._add_source(name, priority, data)
        except Exception:
            pass

    def _load_env_vars(self, name: str, priority: int) -> None:
        """Load configuration from environment variables."""
        env_config: Dict[str, Any] = {}
        prefix = "RABAI_"

        for key, value in os.environ.items():
            if not key.startswith(prefix):
                continue

            config_key = key[len(prefix):].lower().replace("_", ".")
            self._parse_env_value(config_key, value, env_config)

        if env_config:
            self._add_source(name, priority, env_config)

    def _parse_env_value(
        self,
        key: str,
        value: str,
        config: Dict[str, Any],
    ) -> None:
        """Parse environment variable value into nested dict."""
        keys = key.split(".")
        current = config

        for k in keys[:-1]:
            if k not in current:
                current[k] = {}
            current = current[k]

        # Try to parse as appropriate type
        parsed_value: Any = value
        if value.lower() in ("true", "false"):
            parsed_value = value.lower() == "true"
        elif value.lower() == "null":
            parsed_value = None
        else:
            try:
                parsed_value = int(value)
            except ValueError:
                try:
                    parsed_value = float(value)
                except ValueError:
                    pass

        current[keys[-1]] = parsed_value

    def _deep_update(
        self,
        target: Dict[str, Any],
        source: Dict[str, Any],
    ) -> None:
        """Deep update target dict with source dict."""
        for key, value in source.items():
            if isinstance(value, dict) and key in target and isinstance(target[key], dict):
                self._deep_update(target[key], value)
            else:
                target[key] = value

    def get(self, key: str, default: Any = None) -> Any:
        """Get a configuration value.

        Args:
            key: Dot-separated configuration key (e.g., "app.debug").
            default: Default value if key not found.

        Returns:
            Configuration value or default.
        """
        with self._lock:
            keys = key.split(".")
            value = self._config

            for k in keys:
                if isinstance(value, dict) and k in value:
                    value = value[k]
                else:
                    return default

            return value

    def set(self, key: str, value: Any, persist: bool = False) -> None:
        """Set a configuration value.

        Args:
            key: Dot-separated configuration key.
            value: Value to set.
            persist: If True, save to user config file.
        """
        with self._lock:
            keys = key.split(".")
            current = self._config

            for k in keys[:-1]:
                if k not in current:
                    current[k] = {}
                current = current[k]

            current[keys[-1]] = value

        if persist:
            self._save_user_config()

    def _save_user_config(self) -> None:
        """Save current config to user config file."""
        import json

        user_config_path = Path.home() / ".rabai" / "config.json"
        user_config_path.parent.mkdir(parents=True, exist_ok=True)

        with open(user_config_path, "w", encoding="utf-8") as f:
            json.dump(self._config, f, indent=2, ensure_ascii=False)

    def get_section(self, section: str) -> Dict[str, Any]:
        """Get all values in a section.

        Args:
            section: Section name (e.g., "app").

        Returns:
            Dictionary of section values.
        """
        with self._lock:
            value = self._config.get(section, {})
            return value if isinstance(value, dict) else {}

    def get_all_keys(self) -> List[str]:
        """Get all configuration keys.

        Returns:
            List of dot-separated configuration keys.
        """
        with self._lock:
            keys: List[str] = []
            self._collect_keys(self._config, "", keys)
            return keys

    def _collect_keys(
        self,
        config: Dict[str, Any],
        prefix: str,
        keys: List[str],
    ) -> None:
        """Recursively collect all keys."""
        for key, value in config.items():
            full_key = f"{prefix}.{key}" if prefix else key
            keys.append(full_key)
            if isinstance(value, dict):
                self._collect_keys(value, full_key, keys)

    def reset(self) -> None:
        """Reset configuration to defaults."""
        with self._lock:
            self._config = DEFAULT_CONFIG.copy()
            self._sources = [ConfigSource("defaults", 0, DEFAULT_CONFIG.copy())]
            self._load_env_vars("env", 30)

    @property
    def sources(self) -> List[ConfigSource]:
        """Get configuration sources sorted by priority."""
        return self._sources.copy()


# Global singleton instance
config_manager: ConfigManager = ConfigManager()


@dataclass
class AppConfig:
    """Application configuration dataclass for type-safe access."""
    debug: bool = False
    log_level: str = "INFO"
    default_timeout: float = 30.0
    max_retries: int = 3
    theme: str = "light"
    language: str = "zh_CN"

    @classmethod
    def from_manager(cls, manager: Optional[ConfigManager] = None) -> 'AppConfig':
        """Create AppConfig from ConfigManager."""
        if manager is None:
            manager = config_manager

        return cls(
            debug=manager.get("app.debug", False),
            log_level=manager.get("app.log_level", "INFO"),
            default_timeout=manager.get("execution.default_timeout", 30.0),
            max_retries=manager.get("execution.max_retries", 3),
            theme=manager.get("ui.theme", "light"),
            language=manager.get("ui.language", "zh_CN"),
        )