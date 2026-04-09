"""
Haptic Feedback Utilities for UI Automation.

This module provides utilities for managing haptic feedback
on macOS devices during UI automation workflows.

Author: AI Assistant
License: MIT
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any, Callable
from enum import Enum


class HapticIntensity(Enum):
    """Haptic feedback intensity levels."""
    LIGHT = "light"
    MEDIUM = "medium"
    HEAVY = "heavy"
    RIGID = "rigid"
    SOFT = "soft"


class HapticPattern(Enum):
    """Predefined haptic feedback patterns."""
    TAP = "tap"
    DOUBLE_TAP = "double_tap"
    SUCCESS = "success"
    WARNING = "warning"
    ERROR = "error"
    SELECTION = "selection"
    IMPACT = "impact"


@dataclass
class HapticEvent:
    """Represents a haptic feedback event."""
    pattern: HapticPattern
    intensity: HapticIntensity = HapticIntensity.MEDIUM
    duration_ms: float = 100.0
    delay_ms: float = 0.0
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class HapticSequence:
    """A sequence of haptic events."""
    name: str
    events: List[HapticEvent] = field(default_factory=list)
    repeat: int = 1


class HapticFeedbackManager:
    """Manages haptic feedback for UI automation."""

    def __init__(self) -> None:
        self._enabled: bool = True
        self._sequence_cache: Dict[str, HapticSequence] = {}
        self._last_feedback_time: float = 0.0

    def enable(self) -> None:
        """Enable haptic feedback."""
        self._enabled = True

    def disable(self) -> None:
        """Disable haptic feedback."""
        self._enabled = False

    @property
    def is_enabled(self) -> bool:
        """Check if haptic feedback is enabled."""
        return self._enabled

    def register_sequence(self, sequence: HapticSequence) -> None:
        """Register a haptic sequence for later use."""
        self._sequence_cache[sequence.name] = sequence

    def get_sequence(self, name: str) -> Optional[HapticSequence]:
        """Retrieve a registered haptic sequence."""
        return self._sequence_cache.get(name)

    def play_event(self, event: HapticEvent) -> bool:
        """Play a single haptic event."""
        if not self._enabled:
            return False

        if event.delay_ms > 0:
            time.sleep(event.delay_ms / 1000.0)

        self._last_feedback_time = time.time()
        return True

    def play_sequence(self, sequence: HapticSequence) -> None:
        """Play a sequence of haptic events."""
        for _ in range(sequence.repeat):
            for event in sequence.events:
                self.play_event(event)

    def create_tap_sequence(
        self,
        intensity: HapticIntensity = HapticIntensity.MEDIUM,
    ) -> HapticSequence:
        """Create a simple tap haptic sequence."""
        return HapticSequence(
            name="custom_tap",
            events=[
                HapticEvent(
                    pattern=HapticPattern.TAP,
                    intensity=intensity,
                    duration_ms=100.0,
                )
            ],
        )

    def create_success_sequence(self) -> HapticSequence:
        """Create a success haptic sequence."""
        return HapticSequence(
            name="success",
            events=[
                HapticEvent(pattern=HapticPattern.TAP, intensity=HapticIntensity.LIGHT),
                HapticEvent(
                    pattern=HapticPattern.TAP,
                    intensity=HapticIntensity.MEDIUM,
                    delay_ms=100.0,
                ),
                HapticEvent(
                    pattern=HapticPattern.TAP,
                    intensity=HapticIntensity.HEAVY,
                    delay_ms=200.0,
                ),
            ],
        )

    def create_error_sequence(self) -> HapticSequence:
        """Create an error haptic sequence."""
        return HapticSequence(
            name="error",
            events=[
                HapticEvent(pattern=HapticPattern.ERROR, intensity=HapticIntensity.HEAVY),
                HapticEvent(
                    pattern=HapticPattern.ERROR,
                    intensity=HapticIntensity.MEDIUM,
                    delay_ms=150.0,
                ),
            ],
        )

    def get_last_feedback_time(self) -> float:
        """Get the timestamp of the last haptic feedback."""
        return self._last_feedback_time


def create_haptic_event(
    pattern: HapticPattern,
    intensity: HapticIntensity = HapticIntensity.MEDIUM,
    duration_ms: float = 100.0,
) -> HapticEvent:
    """Create a haptic event with the specified parameters."""
    return HapticEvent(
        pattern=pattern,
        intensity=intensity,
        duration_ms=duration_ms,
    )
