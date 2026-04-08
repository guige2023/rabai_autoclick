"""Input profile utilities for managing device-specific input settings.

This module provides utilities for managing input profiles that store
calibration and behavior settings for different input devices.
"""

from __future__ import annotations

import json
import platform
from dataclasses import dataclass, asdict
from typing import Optional


IS_MACOS = platform.system() == "Darwin"
IS_LINUX = platform.system() == "Linux"
IS_WINDOWS = platform.system() == "Windows"


@dataclass
class MouseProfile:
    """Profile settings for mouse input."""
    name: str
    speed_multiplier: float = 1.0
    double_click_delay: float = 0.5
    scroll_speed: float = 1.0
    accel_threshold: float = 0.0
    smoothing: bool = True
    
    # Calibration offsets
    calibration_x: int = 0
    calibration_y: int = 0
    
    def to_dict(self) -> dict:
        return asdict(self)
    
    @classmethod
    def from_dict(cls, data: dict) -> "MouseProfile":
        return cls(**data)


@dataclass
class KeyboardProfile:
    """Profile settings for keyboard input."""
    name: str
    key_repeat_delay: float = 0.5
    key_repeat_rate: float = 0.1
    modifier_sticky: bool = False
    hotkey_prefix: Optional[str] = None
    
    def to_dict(self) -> dict:
        return asdict(self)
    
    @classmethod
    def from_dict(cls, data: dict) -> "KeyboardProfile":
        return cls(**data)


class InputProfileManager:
    """Manages input profiles for different devices and use cases."""
    
    def __init__(self, config_dir: Optional[str] = None):
        """Initialize the profile manager.
        
        Args:
            config_dir: Directory to store profile configs.
        """
        self.config_dir = config_dir or self._get_default_config_dir()
        self._profiles: dict[str, dict] = {}
        self._active_mouse_profile: Optional[MouseProfile] = None
        self._active_keyboard_profile: Optional[KeyboardProfile] = None
    
    def _get_default_config_dir(self) -> str:
        """Get the default config directory."""
        import os
        if IS_MACOS:
            base = os.path.expanduser("~/Library/Application Support")
        elif IS_WINDOWS:
            base = os.environ.get("APPDATA", "")
        else:
            base = os.path.expanduser("~/.config")
        return os.path.join(base, "rabai_autoclick", "profiles")
    
    def create_mouse_profile(self, name: str) -> MouseProfile:
        """Create a new mouse profile.
        
        Args:
            name: Name for the profile.
        
        Returns:
            The new MouseProfile.
        """
        profile = MouseProfile(name=name)
        self._profiles[name] = profile.to_dict()
        return profile
    
    def create_keyboard_profile(self, name: str) -> KeyboardProfile:
        """Create a new keyboard profile.
        
        Args:
            name: Name for the profile.
        
        Returns:
            The new KeyboardProfile.
        """
        profile = KeyboardProfile(name=name)
        self._profiles[name] = profile.to_dict()
        return profile
    
    def get_mouse_profile(self, name: str) -> Optional[MouseProfile]:
        """Get a mouse profile by name."""
        if name in self._profiles:
            return MouseProfile.from_dict(self._profiles[name])
        return None
    
    def get_keyboard_profile(self, name: str) -> Optional[KeyboardProfile]:
        """Get a keyboard profile by name."""
        if name in self._profiles:
            return KeyboardProfile.from_dict(self._profiles[name])
        return None
    
    def save_profile(self, name: str) -> bool:
        """Save a profile to disk.
        
        Args:
            name: Name of the profile to save.
        
        Returns:
            True if saved successfully.
        """
        import os
        if name not in self._profiles:
            return False
        
        try:
            os.makedirs(self.config_dir, exist_ok=True)
            filepath = os.path.join(self.config_dir, f"{name}.json")
            with open(filepath, "w") as f:
                json.dump(self._profiles[name], f, indent=2)
            return True
        except IOError:
            return False
    
    def load_profile(self, name: str) -> bool:
        """Load a profile from disk.
        
        Args:
            name: Name of the profile to load.
        
        Returns:
            True if loaded successfully.
        """
        import os
        filepath = os.path.join(self.config_dir, f"{name}.json")
        try:
            with open(filepath, "r") as f:
                self._profiles[name] = json.load(f)
            return True
        except IOError:
            return False
    
    def set_active_mouse_profile(self, profile: MouseProfile) -> None:
        """Set the active mouse profile.
        
        Args:
            profile: MouseProfile to activate.
        """
        self._active_mouse_profile = profile
    
    def set_active_keyboard_profile(self, profile: KeyboardProfile) -> None:
        """Set the active keyboard profile.
        
        Args:
            profile: KeyboardProfile to activate.
        """
        self._active_keyboard_profile = profile
    
    def get_active_mouse_profile(self) -> Optional[MouseProfile]:
        """Get the currently active mouse profile."""
        return self._active_mouse_profile
    
    def get_active_keyboard_profile(self) -> Optional[KeyboardProfile]:
        """Get the currently active keyboard profile."""
        return self._active_keyboard_profile


# Global profile manager
_global_manager: Optional[InputProfileManager] = None


def get_profile_manager() -> InputProfileManager:
    """Get the global input profile manager."""
    global _global_manager
    if _global_manager is None:
        _global_manager = InputProfileManager()
    return _global_manager
