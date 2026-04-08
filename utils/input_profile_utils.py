"""Input profile utilities.

This module provides utilities for managing input profiles
that define device characteristics and input behaviors.
"""

from __future__ import annotations

import time
from typing import Any, Callable, Dict, List, Optional
from dataclasses import dataclass, field
from enum import Enum, auto


class ProfileType(Enum):
    """Input profile types."""
    MOUSE = auto()
    KEYBOARD = auto()
    TOUCH = auto()
    COMBINED = auto()


@dataclass
class MouseProfile:
    """Mouse device profile."""
    dpi: int = 800
    polling_rate_hz: int = 125
    acceleration: float = 1.0
    scroll_speed: float = 1.0
    double_click_delay_ms: float = 300.0
    pointer_speed: float = 1.0


@dataclass
class KeyboardProfile:
    """Keyboard device profile."""
    key_repeat_delay_ms: float = 250.0
    key_repeat_rate_hz: float = 30.0
    modifier_swap: Dict[str, str] = field(default_factory=dict)
    layout: str = "qwerty-us"


@dataclass
class TouchProfile:
    """Touch device profile."""
    touch_resolution: int = 100
    palm_rejection: bool = True
    touch_feedback: bool = True
    multi_touch_max: int = 10


@dataclass
class InputProfile:
    """Complete input profile."""
    name: str
    profile_type: ProfileType
    mouse: Optional[MouseProfile] = None
    keyboard: Optional[KeyboardProfile] = None
    touch: Optional[TouchProfile] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    def get_mouse(self) -> MouseProfile:
        if self.mouse is None:
            self.mouse = MouseProfile()
        return self.mouse

    def get_keyboard(self) -> KeyboardProfile:
        if self.keyboard is None:
            self.keyboard = KeyboardProfile()
        return self.keyboard

    def get_touch(self) -> TouchProfile:
        if self.touch is None:
            self.touch = TouchProfile()
        return self.touch


class ProfileManager:
    """Manages input profiles."""

    def __init__(self) -> None:
        self._profiles: Dict[str, InputProfile] = {}
        self._active_profile: Optional[str] = None
        self._on_profile_change: Optional[Callable[[str, str], None]] = None

    def add_profile(self, profile: InputProfile) -> None:
        self._profiles[profile.name] = profile

    def remove_profile(self, name: str) -> bool:
        if name in self._profiles:
            del self._profiles[name]
            if self._active_profile == name:
                self._active_profile = None
            return True
        return False

    def set_active(self, name: str) -> bool:
        if name not in self._profiles:
            return False
        old = self._active_profile
        self._active_profile = name
        if self._on_profile_change and old != name:
            self._on_profile_change(old or "", name)
        return True

    def get_active(self) -> Optional[InputProfile]:
        if self._active_profile:
            return self._profiles.get(self._active_profile)
        return None

    @property
    def profiles(self) -> Dict[str, InputProfile]:
        return self._profiles.copy()

    def on_profile_change(self, handler: Callable[[str, str], None]) -> None:
        self._on_profile_change = handler


PRESET_PROFILES: Dict[str, InputProfile] = {}


def create_default_mouse_profile() -> MouseProfile:
    return MouseProfile()


def create_default_keyboard_profile() -> KeyboardProfile:
    return KeyboardProfile()


def create_default_touch_profile() -> TouchProfile:
    return TouchProfile()


def create_combined_profile(name: str) -> InputProfile:
    return InputProfile(
        name=name,
        profile_type=ProfileType.COMBINED,
        mouse=create_default_mouse_profile(),
        keyboard=create_default_keyboard_profile(),
        touch=create_default_touch_profile(),
    )


__all__ = [
    "ProfileType",
    "MouseProfile",
    "KeyboardProfile",
    "TouchProfile",
    "InputProfile",
    "ProfileManager",
    "create_default_mouse_profile",
    "create_default_keyboard_profile",
    "create_default_touch_profile",
    "create_combined_profile",
]
