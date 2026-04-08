"""Input sequence recording utilities.

This module provides utilities for recording and playing back
input sequences such as mouse movements and key presses.
"""

from __future__ import annotations

from typing import Any, Callable, Dict, List, Optional
from dataclasses import dataclass, field
from enum import Enum, auto
import time
import json


class InputEventType(Enum):
    """Types of input events that can be recorded."""
    MOUSE_MOVE = auto()
    MOUSE_CLICK = auto()
    MOUSE_RELEASE = auto()
    MOUSE_SCROLL = auto()
    KEY_PRESS = auto()
    KEY_RELEASE = auto()
    TOUCH_START = auto()
    TOUCH_MOVE = auto()
    TOUCH_END = auto()


@dataclass
class InputEvent:
    """A single input event."""
    event_type: InputEventType
    timestamp: float
    x: int = 0
    y: int = 0
    button: int = 0
    key_code: int = 0
    key_name: str = ""
    scroll_delta: int = 0
    modifiers: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "event_type": self.event_type.name,
            "timestamp": self.timestamp,
            "x": self.x,
            "y": self.y,
            "button": self.button,
            "key_code": self.key_code,
            "key_name": self.key_name,
            "scroll_delta": self.scroll_delta,
            "modifiers": self.modifiers,
        }

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "InputEvent":
        return cls(
            event_type=InputEventType[d["event_type"]],
            timestamp=d["timestamp"],
            x=d.get("x", 0),
            y=d.get("y", 0),
            button=d.get("button", 0),
            key_code=d.get("key_code", 0),
            key_name=d.get("key_name", ""),
            scroll_delta=d.get("scroll_delta", 0),
            modifiers=d.get("modifiers", []),
        )


@dataclass
class RecordedSequence:
    """A recorded sequence of input events."""
    name: str
    events: List[InputEvent] = field(default_factory=list)
    created_at: float = field(default_factory=time.time)
    metadata: Dict[str, Any] = field(default_factory=dict)

    def add(self, event: InputEvent) -> None:
        self.events.append(event)

    def duration(self) -> float:
        if len(self.events) < 2:
            return 0.0
        return self.events[-1].timestamp - self.events[0].timestamp

    def to_json(self) -> str:
        data = {
            "name": self.name,
            "created_at": self.created_at,
            "metadata": self.metadata,
            "events": [e.to_dict() for e in self.events],
        }
        return json.dumps(data)

    @classmethod
    def from_json(cls, json_str: str) -> "RecordedSequence":
        data = json.loads(json_str)
        return cls(
            name=data["name"],
            created_at=data["created_at"],
            metadata=data.get("metadata", {}),
            events=[InputEvent.from_dict(e) for e in data["events"]],
        )


class SequenceRecorder:
    """Records input sequences."""

    def __init__(self, name: str = "unnamed") -> None:
        self.name = name
        self._events: List[InputEvent] = []
        self._recording = False
        self._start_time: Optional[float] = None

    def start(self) -> None:
        """Start recording."""
        self._events.clear()
        self._recording = True
        self._start_time = time.perf_counter()

    def stop(self) -> RecordedSequence:
        """Stop recording and return the sequence."""
        self._recording = False
        return RecordedSequence(name=self.name, events=self._events[:])

    def record_event(self, event: InputEvent) -> None:
        """Record a single event."""
        if self._recording:
            if self._start_time is not None:
                event.timestamp = time.perf_counter() - self._start_time
            self._events.append(event)

    @property
    def is_recording(self) -> bool:
        return self._recording

    @property
    def event_count(self) -> int:
        return len(self._events)


class SequencePlayer:
    """Plays back recorded input sequences."""

    def __init__(self) -> None:
        self._handlers: Dict[InputEventType, Callable[[InputEvent], None]] = {}
        self._on_playback_end: Optional[Callable[[], None]] = None

    def set_handler(
        self,
        event_type: InputEventType,
        handler: Callable[[InputEvent], None],
    ) -> None:
        self._handlers[event_type] = handler

    def set_playback_end_handler(self, handler: Callable[[], None]) -> None:
        self._on_playback_end = handler

    def play(self, sequence: RecordedSequence, speed: float = 1.0) -> None:
        """Play back a recorded sequence.

        Args:
            sequence: RecordedSequence to play.
            speed: Playback speed multiplier (1.0 = normal).
        """
        for event in sequence.events:
            handler = self._handlers.get(event.event_type)
            if handler:
                handler(event)
            time.sleep(0.001 / speed if speed > 0 else 0.001)

        if self._on_playback_end:
            self._on_playback_end()


__all__ = [
    "InputEventType",
    "InputEvent",
    "RecordedSequence",
    "SequenceRecorder",
    "SequencePlayer",
]
