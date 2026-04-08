"""Configuration loading utilities for various formats."""

from typing import Any, Dict, Optional, Callable, Union
import json
import os


class ConfigLoader:
    """Load and merge configuration from various sources."""

    def __init__(self):
        """Initialize config loader."""
        self._config: Dict[str, Any] = {}
        self._sources: list = []

    def load_json(self, path: str, merge: bool = True) -> "ConfigLoader":
        """Load JSON configuration file.
        
        Args:
            path: Path to JSON file.
            merge: If True, merge with existing config.
        
        Returns:
            Self for chaining.
        """
        with open(path, "r") as f:
            data = json.load(f)
        if merge:
            self._config = self._deep_merge(self._config, data)
        else:
            self._config = data
        self._sources.append(f"json:{path}")
        return self

    def load_env(self, prefix: str = "APP_", merge: bool = True) -> "ConfigLoader":
        """Load environment variables as config.
        
        Args:
            prefix: Environment variable prefix.
            merge: If True, merge with existing config.
        
        Returns:
            Self for chaining.
        """
        env_config = {}
        for key, value in os.environ.items():
            if key.startswith(prefix):
                config_key = key[len(prefix):].lower()
                env_config[config_key] = self._parse_env_value(value)
        if merge:
            self._config = self._deep_merge(self._config, env_config)
        else:
            self._config = env_config
        self._sources.append("env")
        return self

    def set(self, key: str, value: Any) -> "ConfigLoader":
        """Set a config value.
        
        Args:
            key: Dot-separated key path.
            value: Value to set.
        
        Returns:
            Self for chaining.
        """
        parts = key.split(".")
        current = self._config
        for part in parts[:-1]:
            if part not in current:
                current[part] = {}
            current = current[part]
        current[parts[-1]] = value
        return self

    def get(self, key: str, default: Any = None) -> Any:
        """Get a config value.
        
        Args:
            key: Dot-separated key path.
            default: Default if not found.
        
        Returns:
            Config value or default.
        """
        parts = key.split(".")
        current = self._config
        for part in parts:
            if isinstance(current, dict) and part in current:
                current = current[part]
            else:
                return default
        return current

    def _parse_env_value(self, value: str) -> Any:
        """Parse environment variable value."""
        if value.lower() == "true":
            return True
        if value.lower() == "false":
            return False
        if value.lower() == "none":
            return None
        try:
            return int(value)
        except ValueError:
            try:
                return float(value)
            except ValueError:
                return value

    def _deep_merge(self, base: Dict, override: Dict) -> Dict:
        """Deep merge two dictionaries."""
        result = dict(base)
        for key, value in override.items():
            if key in result and isinstance(result[key], dict) and isinstance(value, dict):
                result[key] = self._deep_merge(result[key], value)
            else:
                result[key] = value
        return result

    def to_dict(self) -> Dict[str, Any]:
        """Get config as dictionary."""
        return dict(self._config)

    def get_sources(self) -> list:
        """Get list of config sources."""
        return list(self._sources)


def load_config(
    files: Optional[list] = None,
    env_prefix: Optional[str] = None
) -> Dict[str, Any]:
    """Convenience function to load config.
    
    Args:
        files: List of config file paths.
        env_prefix: Environment variable prefix.
    
    Returns:
        Merged configuration dictionary.
    """
    loader = ConfigLoader()
    if files:
        for f in files:
            if f.endswith(".json"):
                loader.load_json(f)
    if env_prefix:
        loader.load_env(env_prefix)
    return loader.to_dict()
