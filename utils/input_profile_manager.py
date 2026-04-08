"""Input profile manager for UI automation.

Manages different input profiles (speed, accuracy settings)
for different automation scenarios.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Callable, Optional


class ProfileType(Enum):
    """Types of input profiles."""
    FAST = auto()       # Speed-optimized
    ACCURATE = auto()   # Accuracy-optimized
    STEALTH = auto()    # Human-like, avoids detection
    PRECISE = auto()    # High precision for small targets
    BALANCED = auto()   # Balanced speed/accuracy


@dataclass
class InputProfile:
    """Configuration profile for input behavior.

    Attributes:
        profile_type: The type of profile.
        mouse_speed: Movement speed (pixels per second).
        click_delay: Delay between clicks in seconds.
        type_delay: Delay between keystrokes in seconds.
        jitter: Random jitter amount for mouse movement.
        curve_movement: Whether to use curved mouse paths.
        double_click_speed: Speed for double-clicks.
        scroll_amount: Amount per scroll tick.
        gesture_duration: Base duration for gestures.
    """
    profile_type: ProfileType
    name: str = ""
    mouse_speed: float = 1000.0
    click_delay: float = 0.05
    type_delay: float = 0.05
    jitter: float = 2.0
    curve_movement: bool = True
    double_click_speed: float = 0.1
    scroll_amount: float = 3.0
    gesture_duration: float = 0.3
    id: str = field(default_factory=lambda: str(uuid.uuid4()))


# Preset profiles
FAST_PROFILE = InputProfile(
    profile_type=ProfileType.FAST,
    name="Fast",
    mouse_speed=2000.0,
    click_delay=0.01,
    type_delay=0.01,
    jitter=0.0,
    curve_movement=False,
    gesture_duration=0.15,
)

ACCURATE_PROFILE = InputProfile(
    profile_type=ProfileType.ACCURATE,
    name="Accurate",
    mouse_speed=400.0,
    click_delay=0.15,
    type_delay=0.1,
    jitter=1.0,
    curve_movement=True,
    gesture_duration=0.5,
)

STEALTH_PROFILE = InputProfile(
    profile_type=ProfileType.STEALTH,
    name="Stealth",
    mouse_speed=300.0,
    click_delay=0.2,
    type_delay=0.15,
    jitter=5.0,
    curve_movement=True,
    gesture_duration=0.6,
)

PRECISE_PROFILE = InputProfile(
    profile_type=ProfileType.PRECISE,
    name="Precise",
    mouse_speed=200.0,
    click_delay=0.1,
    type_delay=0.08,
    jitter=0.5,
    curve_movement=True,
    gesture_duration=0.4,
)

BALANCED_PROFILE = InputProfile(
    profile_type=ProfileType.BALANCED,
    name="Balanced",
    mouse_speed=800.0,
    click_delay=0.08,
    type_delay=0.06,
    jitter=2.0,
    curve_movement=True,
    gesture_duration=0.3,
)


class InputProfileManager:
    """Manages input profiles for automation scenarios."""

    def __init__(self) -> None:
        """Initialize with default profiles."""
        self._profiles: dict[str, InputProfile] = {
            p.id: p for p in [
                FAST_PROFILE, ACCURATE_PROFILE,
                STEALTH_PROFILE, PRECISE_PROFILE, BALANCED_PROFILE,
            ]
        }
        self._active_id: Optional[str] = BALANCED_PROFILE.id

    def add_profile(self, profile: InputProfile) -> str:
        """Add a custom profile."""
        self._profiles[profile.id] = profile
        return profile.id

    def get_profile(self, profile_id: str) -> Optional[InputProfile]:
        """Get a profile by ID."""
        return self._profiles.get(profile_id)

    def set_active(self, profile_id: str) -> bool:
        """Set the active profile."""
        if profile_id in self._profiles:
            self._active_id = profile_id
            return True
        return False

    def get_active(self) -> InputProfile:
        """Get the currently active profile."""
        if self._active_id:
            profile = self._profiles.get(self._active_id)
            if profile:
                return profile
        return BALANCED_PROFILE

    def apply_profile(
        self,
        profile_id: str,
        target: Callable[[InputProfile], None],
    ) -> bool:
        """Apply a profile to a target callable."""
        profile = self.get_profile(profile_id)
        if not profile:
            return False
        target(profile)
        return True

    @property
    def all_profiles(self) -> list[InputProfile]:
        """Return all available profiles."""
        return list(self._profiles.values())
