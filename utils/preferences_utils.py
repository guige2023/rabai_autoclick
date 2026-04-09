"""Preferences and Settings Utilities.

This module provides preferences and settings management utilities for
macOS applications, including UserDefaults wrapper, settings persistence,
and configuration management.

Example:
    >>> from preferences_utils import PreferencesManager, Setting
    >>> prefs = PreferencesManager("com.app.identifier")
    >>> value = prefs.get("setting_key", default="default_value")
"""

from __future__ import annotations

import json
import subprocess
from dataclasses import dataclass, field
from enum import Enum, auto
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Type, TypeVar, Union


T = TypeVar('T')


class PreferenceType(Enum):
    """Preference value types."""
    STRING = auto()
    INT = auto()
    FLOAT = auto()
    BOOL = auto()
    LIST = auto()
    DICT = auto()
    DATA = auto()


@dataclass
class Setting:
    """Represents a preference setting.
    
    Attributes:
        key: Setting key
        value: Current value
        default_value: Default value
        type: Value type
        description: Setting description
    """
    key: str
    value: Any = None
    default_value: Any = None
    type: PreferenceType = PreferenceType.STRING
    description: str = ""
    min_value: Optional[float] = None
    max_value: Optional[float] = None
    choices: Optional[List[Any]] = None
    is_hidden: bool = False


class PreferencesManager:
    """Manages application preferences and settings.
    
    Provides a high-level interface for reading and writing
    preferences using UserDefaults.
    
    Attributes:
        app_id: Application bundle identifier
        defaults: UserDefaults instance
    """
    
    def __init__(
        self,
        app_id: str,
        defaults_path: Optional[str] = None,
    ):
        self.app_id = app_id
        self._defaults_path = defaults_path
        self._settings: Dict[str, Setting] = {}
        self._callbacks: Dict[str, List[Callable]] = {
            'value_changed': [],
        }
        self._defaults = self._load_defaults()
    
    def _load_defaults(self) -> Dict[str, Any]:
        """Load defaults from UserDefaults."""
        try:
            result = subprocess.run(
                ['defaults', 'read', self.app_id],
                capture_output=True,
                text=True,
                timeout=5,
            )
            
            if result.returncode == 0 and result.stdout:
                try:
                    return json.loads(result.stdout)
                except json.JSONDecodeError:
                    pass
        except Exception:
            pass
        
        return {}
    
    def _save_defaults(self) -> None:
        """Save defaults to UserDefaults."""
        try:
            data = json.dumps(self._defaults, indent=2)
            proc = subprocess.Popen(
                ['defaults', 'write', self.app_id, '-dict'],
                stdin=subprocess.PIPE,
            )
            proc.communicate(input=data.encode())
        except Exception:
            pass
    
    def register_setting(
        self,
        key: str,
        default_value: Any,
        setting_type: Optional[PreferenceType] = None,
        description: str = "",
        choices: Optional[List[Any]] = None,
        min_value: Optional[float] = None,
        max_value: Optional[float] = None,
    ) -> Setting:
        """Register a setting with metadata.
        
        Args:
            key: Setting key
            default_value: Default value
            setting_type: Type of value
            description: Setting description
            choices: List of valid choices
            min_value: Minimum value (for numeric)
            max_value: Maximum value (for numeric)
            
        Returns:
            Created Setting object
        """
        if setting_type is None:
            setting_type = self._infer_type(default_value)
        
        setting = Setting(
            key=key,
            value=self._defaults.get(key, default_value),
            default_value=default_value,
            type=setting_type,
            description=description,
            choices=choices,
            min_value=min_value,
            max_value=max_value,
        )
        
        self._settings[key] = setting
        
        if key not in self._defaults:
            self._defaults[key] = default_value
        
        return setting
    
    def _infer_type(self, value: Any) -> PreferenceType:
        """Infer preference type from value."""
        if isinstance(value, bool):
            return PreferenceType.BOOL
        elif isinstance(value, int):
            return PreferenceType.INT
        elif isinstance(value, float):
            return PreferenceType.FLOAT
        elif isinstance(value, str):
            return PreferenceType.STRING
        elif isinstance(value, list):
            return PreferenceType.LIST
        elif isinstance(value, dict):
            return PreferenceType.DICT
        return PreferenceType.DATA
    
    def get(
        self,
        key: str,
        default: Optional[Any] = None,
        value_type: Optional[Type[T]] = None,
    ) -> Any:
        """Get a preference value.
        
        Args:
            key: Setting key
            default: Default value if not found
            value_type: Expected type for type coercion
            
        Returns:
            Preference value or default
        """
        setting = self._settings.get(key)
        
        if setting:
            value = setting.value if setting.value is not None else default
        else:
            value = self._defaults.get(key, default)
        
        if value_type and value is not None:
            try:
                if value_type == bool and isinstance(value, str):
                    value = value.lower() in ('true', '1', 'yes')
                else:
                    value = value_type(value)
            except (ValueError, TypeError):
                pass
        
        return value
    
    def set(self, key: str, value: Any) -> bool:
        """Set a preference value.
        
        Args:
            key: Setting key
            value: Value to set
            
        Returns:
            True if set successfully
        """
        setting = self._settings.get(key)
        
        if setting:
            if setting.choices and value not in setting.choices:
                return False
            
            if setting.min_value is not None and isinstance(value, (int, float)):
                if value < setting.min_value:
                    return False
            
            if setting.max_value is not None and isinstance(value, (int, float)):
                if value > setting.max_value:
                    return False
            
            setting.value = value
        
        self._defaults[key] = value
        self._save_defaults()
        self._notify_value_changed(key, value)
        
        return True
    
    def _notify_value_changed(self, key: str, value: Any) -> None:
        """Notify callbacks of value change."""
        for callback in self._callbacks.get('value_changed', []):
            try:
                callback(key, value)
            except Exception:
                pass
    
    def get_setting(self, key: str) -> Optional[Setting]:
        """Get setting metadata."""
        return self._settings.get(key)
    
    def get_all_settings(self) -> Dict[str, Setting]:
        """Get all registered settings."""
        return self._settings.copy()
    
    def reset_to_defaults(self, key: Optional[str] = None) -> None:
        """Reset settings to default values.
        
        Args:
            key: Specific key to reset, or None for all
        """
        if key:
            if key in self._settings:
                setting = self._settings[key]
                setting.value = setting.default_value
                self._defaults[key] = setting.default_value
        else:
            for setting in self._settings.values():
                setting.value = setting.default_value
                self._defaults[setting.key] = setting.default_value
        
        self._save_defaults()
    
    def on_value_changed(self, key: str, callback: Callable[[Any], None]) -> None:
        """Register callback for value change of specific key."""
        if 'value_changed' not in self._callbacks:
            self._callbacks['value_changed'] = []
        
        self._callbacks['value_changed'].append(
            lambda k, v: callback(v) if k == key else None
        )
    
    def import_from_file(self, path: str) -> bool:
        """Import preferences from a JSON file."""
        try:
            with open(path, 'r') as f:
                data = json.load(f)
            
            for key, value in data.items():
                self._defaults[key] = value
                if key in self._settings:
                    self._settings[key].value = value
            
            self._save_defaults()
            return True
        except Exception:
            return False
    
    def export_to_file(self, path: str) -> bool:
        """Export preferences to a JSON file."""
        try:
            with open(path, 'w') as f:
                json.dump(self._defaults, f, indent=2)
            return True
        except Exception:
            return False


class SettingsPanel:
    """Settings panel with grouped preferences."""
    
    def __init__(self, title: str = "Settings"):
        self.title = title
        self._groups: Dict[str, List[str]] = {}
        self._order: List[str] = []
    
    def add_group(self, group_name: str, setting_keys: List[str]) -> None:
        """Add a settings group."""
        self._groups[group_name] = setting_keys
        self._order.append(group_name)
    
    def get_group(self, group_name: str) -> List[str]:
        """Get setting keys in a group."""
        return self._groups.get(group_name, [])
    
    def get_groups(self) -> List[str]:
        """Get all group names in order."""
        return self._order
    
    def remove_group(self, group_name: str) -> bool:
        """Remove a settings group."""
        if group_name in self._groups:
            del self._groups[group_name]
            self._order.remove(group_name)
            return True
        return False


class PreferencesObserver:
    """Observe preference changes."""
    
    def __init__(self, manager: PreferencesManager):
        self.manager = manager
        self._callbacks: List[Callable[[str, Any], None]] = []
    
    def add_callback(self, callback: Callable[[str, Any], None]) -> None:
        """Add preference change callback."""
        self._callbacks.append(callback)
    
    def remove_callback(self, callback: Callable[[str, Any], None]) -> None:
        """Remove preference change callback."""
        if callback in self._callbacks:
            self._callbacks.remove(callback)
    
    def notify(self, key: str, value: Any) -> None:
        """Notify all callbacks of preference change."""
        for callback in self._callbacks:
            try:
                callback(key, value)
            except Exception:
                pass
