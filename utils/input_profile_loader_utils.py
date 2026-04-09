"""Input Profile Loader Utilities.

Loads and manages input profiles for different devices and use cases.
Profiles contain calibration data, sensitivity settings, and behavior configs.

Example:
    >>> from input_profile_loader_utils import InputProfileLoader, InputProfile
    >>> loader = InputProfileLoader()
    >>> profile = loader.load_profile("gaming")
    >>> print(profile.mouse_sensitivity)
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Optional


DEFAULT_PROFILES_DIR = Path("~/.input_profiles").expanduser()


@dataclass
class InputProfile:
    """Input device profile."""
    name: str
    mouse_sensitivity: float = 1.0
    mouse_acceleration: float = 0.0
    scroll_speed: float = 1.0
    key_repeat_delay: float = 0.5
    key_repeat_rate: float = 30.0
    touch_sensitivity: float = 1.0
    trackpad_sensitivity: float = 1.0
    natural_scrolling: bool = True
    tap_to_click: bool = True
    extra: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Convert profile to dictionary."""
        return {
            "name": self.name,
            "mouse_sensitivity": self.mouse_sensitivity,
            "mouse_acceleration": self.mouse_acceleration,
            "scroll_speed": self.scroll_speed,
            "key_repeat_delay": self.key_repeat_delay,
            "key_repeat_rate": self.key_repeat_rate,
            "touch_sensitivity": self.touch_sensitivity,
            "trackpad_sensitivity": self.trackpad_sensitivity,
            "natural_scrolling": self.natural_scrolling,
            "tap_to_click": self.tap_to_click,
            "extra": self.extra,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> InputProfile:
        """Create profile from dictionary."""
        return cls(
            name=data.get("name", "unknown"),
            mouse_sensitivity=data.get("mouse_sensitivity", 1.0),
            mouse_acceleration=data.get("mouse_acceleration", 0.0),
            scroll_speed=data.get("scroll_speed", 1.0),
            key_repeat_delay=data.get("key_repeat_delay", 0.5),
            key_repeat_rate=data.get("key_repeat_rate", 30.0),
            touch_sensitivity=data.get("touch_sensitivity", 1.0),
            trackpad_sensitivity=data.get("trackpad_sensitivity", 1.0),
            natural_scrolling=data.get("natural_scrolling", True),
            tap_to_click=data.get("tap_to_click", True),
            extra=data.get("extra", {}),
        )


class InputProfileLoader:
    """Loads input profiles from disk."""

    def __init__(self, profiles_dir: Optional[Path] = None):
        """Initialize loader.

        Args:
            profiles_dir: Directory containing profile JSON files.
        """
        self.profiles_dir = profiles_dir or DEFAULT_PROFILES_DIR

    def load_profile(self, name: str) -> InputProfile:
        """Load a profile by name.

        Args:
            name: Profile name (without .json extension).

        Returns:
            InputProfile instance.

        Raises:
            FileNotFoundError: If profile doesn't exist.
        """
        path = self.profiles_dir / f"{name}.json"
        if not path.exists():
            raise FileNotFoundError(f"Profile not found: {name}")
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return InputProfile.from_dict(data)

    def save_profile(self, profile: InputProfile) -> None:
        """Save a profile to disk.

        Args:
            profile: InputProfile to save.
        """
        self.profiles_dir.mkdir(parents=True, exist_ok=True)
        path = self.profiles_dir / f"{profile.name}.json"
        with open(path, "w", encoding="utf-8") as f:
            json.dump(profile.to_dict(), f, indent=2)

    def list_profiles(self) -> list[str]:
        """List available profile names.

        Returns:
            List of profile names (without .json extension).
        """
        if not self.profiles_dir.exists():
            return []
        return [p.stem for p in self.profiles_dir.glob("*.json")]

    def delete_profile(self, name: str) -> None:
        """Delete a profile.

        Args:
            name: Profile name to delete.
        """
        path = self.profiles_dir / f"{name}.json"
        if path.exists():
            path.unlink()
