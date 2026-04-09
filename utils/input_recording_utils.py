"""
Input recording and playback utilities.

Records raw input events (mouse, keyboard) and provides
replay functionality with timing preservation.

Author: Auto-generated
"""

from __future__ import annotations

import json
import time
from collections import deque
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Callable


class InputEventKind(Enum):
    """Kind of input event."""
    MOUSE_DOWN = auto()
    MOUSE_UP = auto()
    MOUSE_MOVE = auto()
    MOUSE_WHEEL = auto()
    KEY_DOWN = auto()
    KEY_UP = auto()
    KEY_CHAR = auto()


@dataclass
class InputEvent:
    """A single input event with timing."""
    kind: InputEventKind
    timestamp: float
    x: float = 0
    y: float = 0
    button: int = 0
    delta: int = 0
    keycode: int = 0
    char: str = ""
    modifiers: int = 0
    
    def to_dict(self) -> dict:
        """Serialize to dictionary."""
        return {
            "kind": self.kind.name,
            "timestamp": self.timestamp,
            "x": self.x,
            "y": self.y,
            "button": self.button,
            "delta": self.delta,
            "keycode": self.keycode,
            "char": self.char,
            "modifiers": self.modifiers,
        }
    
    @classmethod
    def from_dict(cls, data: dict) -> InputEvent:
        """Deserialize from dictionary."""
        return cls(
            kind=InputEventKind[data["kind"]],
            timestamp=data["timestamp"],
            x=data.get("x", 0),
            y=data.get("y", 0),
            button=data.get("button", 0),
            delta=data.get("delta", 0),
            keycode=data.get("keycode", 0),
            char=data.get("char", ""),
            modifiers=data.get("modifiers", 0),
        )


@dataclass
class Recording:
    """A recording of input events."""
    name: str
    events: list[InputEvent] = field(default_factory=list)
    start_time: float = field(default_factory=time.time)
    end_time: float = 0
    
    def duration(self) -> float:
        """Duration in seconds."""
        if not self.events:
            return 0.0
        return self.events[-1].timestamp - self.events[0].timestamp
    
    def save(self, filepath: str) -> None:
        """Save recording to file."""
        data = {
            "name": self.name,
            "events": [e.to_dict() for e in self.events],
            "start_time": self.start_time,
            "end_time": self.end_time,
        }
        with open(filepath, "w") as f:
            json.dump(data, f)
    
    @classmethod
    def load(cls, filepath: str) -> Recording:
        """Load recording from file."""
        with open(filepath, "r") as f:
            data = json.load(f)
        return cls(
            name=data["name"],
            events=[InputEvent.from_dict(e) for e in data["events"]],
            start_time=data["start_time"],
            end_time=data.get("end_time", 0),
        )


class InputRecorder:
    """
    Records input events.
    
    Example:
        recorder = InputRecorder("my_recording")
        recorder.start()
        # ... perform inputs ...
        recording = recorder.stop()
    """
    
    def __init__(self, name: str, max_events: int = 100000):
        self._name = name
        self._events: deque[InputEvent] = deque(maxlen=max_events)
        self._recording = False
        self._start_time: float = 0
    
    def start(self) -> None:
        """Start recording."""
        self._events.clear()
        self._recording = True
        self._start_time = time.perf_counter()
    
    def stop(self) -> Recording:
        """Stop recording and return Recording object."""
        self._recording = False
        recording = Recording(
            name=self._name,
            events=list(self._events),
            start_time=self._start_time,
            end_time=time.perf_counter(),
        )
        return recording
    
    def is_recording(self) -> bool:
        """Check if recording."""
        return self._recording
    
    def record(self, event: InputEvent) -> None:
        """Record an input event."""
        if not self._recording:
            return
        
        rel_time = time.perf_counter() - self._start_time
        event.timestamp = rel_time
        self._events.append(event)


class InputPlayer:
    """
    Plays back recorded input events.
    
    Example:
        player = InputPlayer(recording)
        player.play(speed=2.0)
    """
    
    def __init__(
        self,
        recording: Recording,
        mouse_handler: Callable[[InputEvent], None] | None = None,
        keyboard_handler: Callable[[InputEvent], None] | None = None,
    ):
        self._recording = recording
        self._mouse_handler = mouse_handler
        self._keyboard_handler = keyboard_handler
        self._playing = False
        self._canceled = False
    
    def play(self, speed: float = 1.0) -> None:
        """Play recording at given speed."""
        if not self._recording.events:
            return
        
        self._playing = True
        self._canceled = False
        
        prev_time = 0.0
        
        for event in self._recording.events:
            if self._canceled:
                break
            
            delay = (event.timestamp - prev_time) / speed
            if delay > 0:
                time.sleep(delay)
            
            self._dispatch(event)
            prev_time = event.timestamp
        
        self._playing = False
    
    def _dispatch(self, event: InputEvent) -> None:
        """Dispatch event to appropriate handler."""
        if event.kind in (
            InputEventKind.MOUSE_DOWN,
            InputEventKind.MOUSE_UP,
            InputEventKind.MOUSE_MOVE,
            InputEventKind.MOUSE_WHEEL,
        ):
            if self._mouse_handler:
                self._mouse_handler(event)
        elif event.kind in (
            InputEventKind.KEY_DOWN,
            InputEventKind.KEY_UP,
            InputEventKind.KEY_CHAR,
        ):
            if self._keyboard_handler:
                self._keyboard_handler(event)
    
    def cancel(self) -> None:
        """Cancel playback."""
        self._canceled = True
    
    def is_playing(self) -> bool:
        """Check if playing."""
        return self._playing


def create_synthetic_recording(
    name: str,
    clicks: list[tuple[float, float]],
    interval: float = 0.5,
) -> Recording:
    """
    Create a synthetic recording from click positions.
    
    Args:
        name: Recording name
        clicks: List of (x, y) click positions
        interval: Time between clicks in seconds
        
    Returns:
        Recording object
    """
    recording = Recording(name=name)
    timestamp = 0.0
    
    for x, y in clicks:
        recording.events.append(InputEvent(
            kind=InputEventKind.MOUSE_DOWN,
            timestamp=timestamp,
            x=x,
            y=y,
        ))
        recording.events.append(InputEvent(
            kind=InputEventKind.MOUSE_UP,
            timestamp=timestamp + 0.05,
            x=x,
            y=y,
        ))
        timestamp += interval
    
    return recording
