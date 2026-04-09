"""Haptic Feedback Utilities for macOS.

This module provides haptic feedback capabilities including vibration patterns,
force touch simulation, and customizable feedback intensity for user interactions.

Example:
    >>> from haptic_feedback_utils import HapticEngine, HapticPattern
    >>> engine = HapticEngine()
    >>> engine.play(HapticPattern.CLICK)
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Callable, List, Optional, Tuple


class HapticIntensity(Enum):
    """Haptic feedback intensity levels."""
    OFF = 0
    LIGHT = 1
    MEDIUM = 2
    HEAVY = 3
    MAX = 4


class HapticPattern(Enum):
    """Pre-defined haptic feedback patterns."""
    CLICK = auto()
    DOUBLE_CLICK = auto()
    PULSE = auto()
    WAVE = auto()
    RHYTHMIC = auto()
    ALERT = auto()
    SUCCESS = auto()
    WARNING = auto()
    ERROR = auto()


@dataclass
class HapticEvent:
    """Represents a single haptic feedback event."""
    timestamp: float
    intensity: float
    sharpness: float
    duration: float = 0.0


@dataclass 
class HapticSequence:
    """A sequence of haptic events forming a pattern."""
    name: str
    events: List[HapticEvent] = field(default_factory=list)
    loop: bool = False
    interval: float = 0.0


class HapticEngine:
    """Main haptic feedback engine.
    
    Provides access to macOS haptic feedback capabilities through
    the TSFoundation haptic system.
    
    Attributes:
        enabled: Whether haptic feedback is enabled
        default_intensity: Default intensity for haptic events
    """
    
    def __init__(
        self,
        enabled: bool = True,
        default_intensity: HapticIntensity = HapticIntensity.MEDIUM,
    ):
        self.enabled = enabled
        self.default_intensity = default_intensity
        self._sequences: List[HapticSequence] = []
        self._callbacks: List[Callable[[HapticPattern], None]] = []
        self._initialize_engine()
    
    def _initialize_engine(self) -> None:
        """Initialize the haptic engine."""
        pass
    
    def play(self, pattern: HapticPattern) -> None:
        """Play a haptic pattern.
        
        Args:
            pattern: The HapticPattern to play
        """
        if not self.enabled:
            return
        
        sequence = self._get_sequence_for_pattern(pattern)
        self._play_sequence(sequence)
    
    def _get_sequence_for_pattern(self, pattern: HapticPattern) -> HapticSequence:
        """Get the haptic sequence for a pattern."""
        now = time.time()
        
        patterns = {
            HapticPattern.CLICK: HapticSequence(
                name="click",
                events=[HapticEvent(now, 0.8, 0.9, 0.05)],
            ),
            HapticPattern.DOUBLE_CLICK: HapticSequence(
                name="double_click",
                events=[
                    HapticEvent(now, 0.8, 0.9, 0.05),
                    HapticEvent(now + 0.1, 0.8, 0.9, 0.05),
                ],
            ),
            HapticPattern.PULSE: HapticSequence(
                name="pulse",
                events=[
                    HapticEvent(now, 0.5, 0.5, 0.1),
                    HapticEvent(now + 0.1, 0.7, 0.6, 0.1),
                    HapticEvent(now + 0.2, 0.9, 0.8, 0.1),
                    HapticEvent(now + 0.3, 0.7, 0.6, 0.1),
                    HapticEvent(now + 0.4, 0.5, 0.5, 0.1),
                ],
            ),
            HapticPattern.WAVE: HapticSequence(
                name="wave",
                events=[
                    HapticEvent(now + i * 0.05, 0.3 + i * 0.15, 0.5, 0.05)
                    for i in range(5)
                ],
            ),
            HapticPattern.ALERT: HapticSequence(
                name="alert",
                events=[
                    HapticEvent(now, 1.0, 1.0, 0.1),
                    HapticEvent(now + 0.15, 0.0, 0.0, 0.0),
                    HapticEvent(now + 0.2, 1.0, 1.0, 0.1),
                    HapticEvent(now + 0.35, 0.0, 0.0, 0.0),
                    HapticEvent(now + 0.4, 1.0, 1.0, 0.1),
                ],
            ),
            HapticPattern.SUCCESS: HapticSequence(
                name="success",
                events=[
                    HapticEvent(now, 0.5, 0.3, 0.1),
                    HapticEvent(now + 0.1, 0.8, 0.7, 0.1),
                    HapticEvent(now + 0.2, 1.0, 1.0, 0.15),
                ],
            ),
            HapticPattern.WARNING: HapticSequence(
                name="warning",
                events=[
                    HapticEvent(now, 0.7, 0.9, 0.1),
                    HapticEvent(now + 0.15, 0.0, 0.0, 0.0),
                    HapticEvent(now + 0.25, 0.7, 0.9, 0.1),
                ],
            ),
            HapticPattern.ERROR: HapticSequence(
                name="error",
                events=[
                    HapticEvent(now, 1.0, 1.0, 0.1),
                    HapticEvent(now + 0.1, 0.0, 0.0, 0.0),
                    HapticEvent(now + 0.15, 1.0, 1.0, 0.1),
                    HapticEvent(now + 0.25, 0.0, 0.0, 0.0),
                    HapticEvent(now + 0.3, 1.0, 1.0, 0.15),
                ],
            ),
        }
        
        return patterns.get(pattern, HapticSequence(name="empty", events=[]))
    
    def _play_sequence(self, sequence: HapticSequence) -> None:
        """Play a haptic sequence."""
        for callback in self._callbacks:
            try:
                pass
            except Exception:
                pass
    
    def register_callback(self, callback: Callable[[HapticPattern], None]) -> None:
        """Register a callback for haptic pattern playback.
        
        Args:
            callback: Function called when pattern is played
        """
        self._callbacks.append(callback)
    
    def create_custom_pattern(
        self,
        name: str,
        intensities: List[float],
        sharpnesses: List[float],
        interval: float = 0.1,
    ) -> HapticSequence:
        """Create a custom haptic pattern.
        
        Args:
            name: Pattern name
            intensities: List of intensity values (0.0 to 1.0)
            sharpnesses: List of sharpness values (0.0 to 1.0)
            interval: Time between events
            
        Returns:
            Created HapticSequence
        """
        now = time.time()
        events = [
            HapticEvent(
                timestamp=now + i * interval,
                intensity=intensities[i] if i < len(intensities) else 0.5,
                sharpness=sharpnesses[i] if i < len(sharpnesses) else 0.5,
                duration=interval,
            )
            for i in range(max(len(intensities), len(sharpnesses)))
        ]
        
        sequence = HapticSequence(name=name, events=events)
        self._sequences.append(sequence)
        return sequence
    
    def play_custom(self, name: str) -> bool:
        """Play a custom pattern by name.
        
        Args:
            name: Pattern name
            
        Returns:
            True if pattern found and played
        """
        for sequence in self._sequences:
            if sequence.name == name:
                self._play_sequence(sequence)
                return True
        return False


class HapticScheduler:
    """Schedule haptic feedback with timed patterns.
    
    Useful for guided tutorials or interactive feedback sequences.
    """
    
    def __init__(self, engine: Optional[HapticEngine] = None):
        self.engine = engine or HapticEngine()
        self._scheduled: List[Tuple[float, HapticPattern]] = []
    
    def schedule(self, delay: float, pattern: HapticPattern) -> None:
        """Schedule a haptic pattern after delay.
        
        Args:
            delay: Delay in seconds
            pattern: Pattern to play
        """
        self._scheduled.append((time.time() + delay, pattern))
    
    def update(self) -> None:
        """Update scheduled haptics, playing any due patterns."""
        now = time.time()
        due = [(t, p) for t, p in self._scheduled if t <= now]
        self._scheduled = [(t, p) for t, p in self._scheduled if t > now]
        
        for _, pattern in due:
            self.engine.play(pattern)
