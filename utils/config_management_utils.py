"""
Configuration Management Utilities for UI Automation.

This module provides utilities for managing automation configurations,
settings persistence, and environment handling.

Author: AI Assistant
License: MIT
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field, asdict
from enum import Enum, auto
from pathlib import Path
from typing import Any, Optional, Union
import copy


class ConfigSource(Enum):
    """Configuration sources."""
    DEFAULT = auto()
    FILE = auto()
    ENVIRONMENT = auto()
    RUNTIME = auto()
    COMMAND_LINE = auto()


@dataclass
class ConfigValue:
    """
    A configuration value with metadata.
    
    Attributes:
        value: The configuration value
        source: Where this value came from
        key: Configuration key
        default: Default value if any
    """
    value: Any
    source: ConfigSource
    key: str = ""
    default: Any = None
    description: str = ""
    
    @property
    def is_default(self) -> bool:
        """Check if this is a default value."""
        return self.source == ConfigSource.DEFAULT


class ConfigManager:
    """
    Manages configuration for automation workflows.
    
    Example:
        config = ConfigManager()
        config.load_file("config.json")
        value = config.get("timeout", default=30)
    """
    
    def __init__(self):
        self._config: dict[str, ConfigValue] = {}
        self._defaults: dict[str, Any] = {}
        self._load_order = [
            ConfigSource.DEFAULT,
            ConfigSource.FILE,
            ConfigSource.ENVIRONMENT,
            ConfigSource.COMMAND_LINE,
            ConfigSource.RUNTIME
        ]
    
    def set_default(self, key: str, value: Any, description: str = "") -> None:
        """
        Set a default value for a configuration key.
        
        Args:
            key: Configuration key
            value: Default value
            description: Optional description
        """
        self._defaults[key] = value
        if key not in self._config:
            self._config[key] = ConfigValue(
                value=value,
                source=ConfigSource.DEFAULT,
                key=key,
                default=value,
                description=description
            )
    
    def get(
        self, 
        key: str, 
        default: Optional[Any] = None,
        value_type: Optional[type] = None
    ) -> Any:
        """
        Get a configuration value.
        
        Args:
            key: Configuration key
            default: Default value if not found
            value_type: Optional type to cast to
            
        Returns:
            Configuration value
        """
        config_value = self._config.get(key)
        
        if config_value:
            value = config_value.value
        else:
            value = default if default is not None else self._defaults.get(key)
        
        if value_type is not None and value is not None:
            try:
                value = value_type(value)
            except (ValueError, TypeError):
                pass
        
        return value
    
    def set(
        self,
        key: str,
        value: Any,
        source: ConfigSource = ConfigSource.RUNTIME
    ) -> None:
        """
        Set a configuration value.
        
        Args:
            key: Configuration key
            value: Value to set
            source: Source of the value
        """
        old_value = self._config.get(key)
        
        self._config[key] = ConfigValue(
            value=value,
            source=source,
            key=key,
            default=old_value.default if old_value else self._defaults.get(key),
            description=old_value.description if old_value else ""
        )
    
    def get_int(self, key: str, default: Optional[int] = None) -> Optional[int]:
        """Get a configuration value as integer."""
        return self.get(key, default=default, value_type=int)
    
    def get_float(self, key: str, default: Optional[float] = None) -> Optional[float]:
        """Get a configuration value as float."""
        return self.get(key, default=default, value_type=float)
    
    def get_bool(self, key: str, default: Optional[bool] = None) -> Optional[bool]:
        """Get a configuration value as boolean."""
        value = self.get(key, default=default)
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            return value.lower() in ('true', 'yes', '1', 'on')
        return default
    
    def get_str(self, key: str, default: Optional[str] = None) -> Optional[str]:
        """Get a configuration value as string."""
        return self.get(key, default=default, value_type=str)
    
    def load_file(self, path: Union[str, Path]) -> bool:
        """
        Load configuration from a JSON file.
        
        Args:
            path: Path to JSON configuration file
            
        Returns:
            True if loaded successfully
        """
        try:
            with open(path, 'r') as f:
                data = json.load(f)
            
            for key, value in data.items():
                self.set(key, value, ConfigSource.FILE)
            
            return True
        except Exception:
            return False
    
    def save_file(self, path: Union[str, Path]) -> bool:
        """
        Save current configuration to a JSON file.
        
        Args:
            path: Path to save to
            
        Returns:
            True if saved successfully
        """
        try:
            data = {k: v.value for k, v in self._config.items()}
            
            with open(path, 'w') as f:
                json.dump(data, f, indent=2)
            
            return True
        except Exception:
            return False
    
    def load_env(self, prefix: str = "AUTO_") -> int:
        """
        Load configuration from environment variables.
        
        Args:
            prefix: Environment variable prefix to filter by
            
        Returns:
            Number of variables loaded
        """
        count = 0
        
        for key, value in os.environ.items():
            if key.startswith(prefix):
                config_key = key[len(prefix):].lower().replace('_', '.')
                self.set(config_key, value, ConfigSource.ENVIRONMENT)
                count += 1
        
        return count
    
    def to_dict(self) -> dict[str, Any]:
        """Export configuration as dictionary."""
        return {k: v.value for k, v in self._config.items()}
    
    def get_source(self, key: str) -> Optional[ConfigSource]:
        """Get the source of a configuration value."""
        config_value = self._config.get(key)
        return config_value.source if config_value else None
    
    def get_all_with_source(self, source: ConfigSource) -> dict[str, Any]:
        """Get all configuration values from a specific source."""
        return {
            k: v.value for k, v in self._config.items()
            if v.source == source
        }
    
    def reset_to_defaults(self) -> None:
        """Reset all values to their defaults."""
        for key, default in self._defaults.items():
            self._config[key] = ConfigValue(
                value=default,
                source=ConfigSource.DEFAULT,
                key=key,
                default=default
            )
    
    def clear(self) -> None:
        """Clear all configuration."""
        self._config.clear()
    
    def keys(self) -> list[str]:
        """Get all configuration keys."""
        return list(self._config.keys())
    
    def __contains__(self, key: str) -> bool:
        """Check if a key exists."""
        return key in self._config
    
    def __getitem__(self, key: str) -> Any:
        """Get a configuration value."""
        return self.get(key)
    
    def __setitem__(self, key: str, value: Any) -> None:
        """Set a configuration value."""
        self.set(key, value, ConfigSource.RUNTIME)


class EnvironmentManager:
    """
    Manages multiple environment configurations.
    
    Example:
        env_mgr = EnvironmentManager()
        env_mgr.load_environment("dev")
        config = env_mgr.get_current_config()
    """
    
    def __init__(self, base_dir: Optional[Path] = None):
        self.base_dir = base_dir or Path.cwd()
        self._environments: dict[str, ConfigManager] = {}
        self._current_env: Optional[str] = None
        self._default_env = "default"
    
    def create_environment(self, name: str) -> ConfigManager:
        """
        Create a new environment configuration.
        
        Args:
            name: Environment name
            
        Returns:
            New ConfigManager for this environment
        """
        config = ConfigManager()
        self._environments[name] = config
        return config
    
    def get_environment(self, name: str) -> Optional[ConfigManager]:
        """Get an environment configuration by name."""
        return self._environments.get(name)
    
    def load_environment(
        self,
        name: str,
        config_file: Optional[Union[str, Path]] = None
    ) -> bool:
        """
        Load an environment configuration.
        
        Args:
            name: Environment name
            config_file: Optional config file to load
            
        Returns:
            True if loaded successfully
        """
        config = self._environments.get(name)
        if not config:
            config = self.create_environment(name)
        
        if config_file:
            config.load_file(config_file)
        
        self._current_env = name
        return True
    
    def get_current_environment(self) -> Optional[str]:
        """Get the current environment name."""
        return self._current_env
    
    def get_current_config(self) -> Optional[ConfigManager]:
        """Get the current environment's configuration."""
        if self._current_env:
            return self._environments.get(self._current_env)
        return None
