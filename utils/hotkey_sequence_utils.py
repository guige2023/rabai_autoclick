"""
Hotkey Sequence Utilities for Keyboard Automation.

This module provides utilities for recording, replaying, and
comparing hotkey sequences in UI automation.

Author: AI Assistant
License: MIT
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any, Callable
from enum import Enum
import time


class KeyAction(Enum):
    """Key action types."""
    PRESS = "press"
    RELEASE = "release"
    HOLD = "hold"
    TYPE = "type"


@dataclass
class KeyEvent:
    """Single key event."""
    key: str
    action: KeyAction
    timestamp: float
    modifiers: List[str] = field(default_factory=list)


@dataclass
class HotkeySequence:
    """A sequence of hotkey events."""
    name: str
    events: List[KeyEvent]
    description: str = ""
    metadata: Dict[str, Any] = field(default_factory=dict)

    @property
    def duration(self) -> float:
        """Total duration of the sequence."""
        if not self.events:
            return 0.0
        return self.events[-1].timestamp - self.events[0].timestamp

    @property
    def key_count(self) -> int:
        """Number of unique keys in sequence."""
        return len(set(e.key for e in self.events))


@dataclass
class SequenceMatch:
    """Result of sequence matching."""
    is_match: bool
    similarity: float
    matched_events: int
    total_events: int
    mismatches: List[str] = field(default_factory=list)


class HotkeySequenceRecorder:
    """
    Record hotkey sequences for replay.
    """

    def __init__(self, name: str):
        """
        Initialize recorder.

        Args:
            name: Sequence name
        """
        self.name = name
        self._events: List[KeyEvent] = []
        self._start_time: Optional[float] = None
        self._recording = False

    def start(self) -> None:
        """Start recording."""
        self._events.clear()
        self._start_time = time.time()
        self._recording = True

    def stop(self) -> HotkeySequence:
        """
        Stop recording.

        Returns:
            Recorded HotkeySequence
        """
        self._recording = False
        return HotkeySequence(
            name=self.name,
            events=self._events.copy()
        )

    def record_event(
        self,
        key: str,
        action: KeyAction,
        modifiers: Optional[List[str]] = None
    ) -> None:
        """
        Record a key event.

        Args:
            key: Key identifier
            action: Key action
            modifiers: Active modifiers
        """
        if not self._recording:
            return

        timestamp = time.time() - (self._start_time or time.time())
        self._events.append(KeyEvent(
            key=key,
            action=action,
            timestamp=timestamp,
            modifiers=modifiers or []
        ))

    @property
    def event_count(self) -> int:
        """Number of recorded events."""
        return len(self._events)


class HotkeySequencePlayer:
    """
    Play back recorded hotkey sequences.
    """

    def __init__(self):
        """Initialize player."""
        self._sequences: Dict[str, HotkeySequence] = {}
        self._key_handler: Optional[Callable] = None
        self._speed: float = 1.0

    def register_sequence(self, sequence: HotkeySequence) -> None:
        """Register a sequence for playback."""
        self._sequences[sequence.name] = sequence

    def set_key_handler(self, handler: Callable[[KeyEvent], None]) -> None:
        """
        Set key event handler.

        Args:
            handler: Function to call for each key event
        """
        self._key_handler = handler

    def set_speed(self, speed: float) -> None:
        """Set playback speed multiplier."""
        self._speed = speed

    def play(
        self,
        name: str,
        on_complete: Optional[Callable] = None
    ) -> bool:
        """
        Play a registered sequence.

        Args:
            name: Sequence name
            on_complete: Callback when playback completes

        Returns:
            True if sequence was found and played
        """
        if name not in self._sequences:
            return False

        sequence = self._sequences[name]
        last_time = 0.0

        for event in sequence.events:
            adjusted_delay = (event.timestamp - last_time) / self._speed

            if adjusted_delay > 0:
                time.sleep(adjusted_delay)

            if self._key_handler:
                self._key_handler(event)

            last_time = event.timestamp

        if on_complete:
            on_complete()

        return True


class HotkeySequenceMatcher:
    """
    Match recorded sequences against live input.
    """

    def __init__(self, tolerance: float = 0.1):
        """
        Initialize matcher.

        Args:
            tolerance: Time tolerance for event matching (seconds)
        """
        self.tolerance = tolerance

    def match(
        self,
        recorded: HotkeySequence,
        live_events: List[KeyEvent]
    ) -> SequenceMatch:
        """
        Match live events against recorded sequence.

        Args:
            recorded: Recorded sequence
            live_events: Live key events

        Returns:
            SequenceMatch result
        """
        if not recorded.events or not live_events:
            return SequenceMatch(
                is_match=False,
                similarity=0.0,
                matched_events=0,
                total_events=len(recorded.events)
            )

        matched = 0
        mismatches = []
        live_idx = 0

        for recorded_event in recorded.events:
            found = False
            for i in range(live_idx, len(live_events)):
                live_event = live_events[i]
                if self._events_match(recorded_event, live_event):
                    matched += 1
                    live_idx = i + 1
                    found = True
                    break

            if not found:
                mismatches.append(f"Missing: {recorded_event.key} @ {recorded_event.timestamp}")

        similarity = matched / len(recorded.events) if recorded.events else 0.0

        return SequenceMatch(
            is_match=similarity >= 0.8,
            similarity=similarity,
            matched_events=matched,
            total_events=len(recorded.events),
            mismatches=mismatches
        )

    def _events_match(self, expected: KeyEvent, actual: KeyEvent) -> bool:
        """Check if two events match."""
        if expected.key != actual.key:
            return False
        if expected.action != actual.action:
            return False
        time_diff = abs(expected.timestamp - actual.timestamp)
        return time_diff <= self.tolerance


def normalize_key_name(key: str) -> str:
    """
    Normalize key name for consistency.

    Args:
        key: Raw key name

    Returns:
        Normalized key name
    """
    key = key.lower().strip()

    replacements = {
        "control": "ctrl",
        "escape": "esc",
        "arrow_up": "up",
        "arrow_down": "down",
        "arrow_left": "left",
        "arrow_right": "right",
        " ": "space",
    }

    return replacements.get(key, key)


def hotkey_to_string(keys: List[str]) -> str:
    """
    Convert list of keys to human-readable string.

    Args:
        keys: List of key names

    Returns:
        Human-readable string (e.g., "Ctrl+C")
    """
    modifiers = []
    regular = []

    modifier_keys = {"ctrl", "shift", "alt", "cmd", "meta"}

    for key in keys:
        k = normalize_key_name(key)
        if k in modifier_keys:
            modifiers.append(k.upper())
        else:
            regular.append(k.upper())

    parts = modifiers + regular
    return "+".join(parts)
