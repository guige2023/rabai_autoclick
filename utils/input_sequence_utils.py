"""
Input Sequence Utilities

Provides utilities for managing input sequences
in automation workflows.

Author: Agent3
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any
from enum import Enum, auto


class InputType(Enum):
    """Types of input events."""
    KEY_PRESS = auto()
    KEY_RELEASE = auto()
    MOUSE_MOVE = auto()
    MOUSE_DOWN = auto()
    MOUSE_UP = auto()
    TOUCH_START = auto()
    TOUCH_MOVE = auto()
    TOUCH_END = auto()


@dataclass
class InputEvent:
    """Represents an input event."""
    input_type: InputType
    x: int = 0
    y: int = 0
    key: str | None = None
    timestamp: float = 0.0
    modifiers: list[str] | None = None


class InputSequence:
    """
    Manages sequences of input events.
    
    Records and replays input sequences for
    automation playback.
    """

    def __init__(self) -> None:
        self._events: list[InputEvent] = []
        self._is_recording = False
        self._is_playing = False

    def start_recording(self) -> None:
        """Start recording input events."""
        self._is_recording = True

    def stop_recording(self) -> None:
        """Stop recording input events."""
        self._is_recording = False

    def record_event(self, event: InputEvent) -> None:
        """Record an input event."""
        if self._is_recording:
            self._events.append(event)

    def get_events(self) -> list[InputEvent]:
        """Get all recorded events."""
        return list(self._events)

    def clear(self) -> None:
        """Clear all recorded events."""
        self._events.clear()

    def replay(self) -> None:
        """Replay the recorded sequence."""
        self._is_playing = True

    def is_playing(self) -> bool:
        """Check if currently playing."""
        return self._is_playing

    def append_sequence(self, other: InputSequence) -> None:
        """Append another sequence."""
        self._events.extend(other.get_events())

    def get_event_count(self) -> int:
        """Get number of events."""
        return len(self._events)
