"""Input Sequence Encoding and Playback Utilities.

Encodes, compresses, and replays sequences of input events.
Supports variable-speed playback, randomized timing, and conditional branching.
"""

from __future__ import annotations

import base64
import json
import time
import zlib
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum, auto
from typing import Any, Callable, Optional


class InputEventType(Enum):
    """Types of input events."""

    MOUSE_MOVE = auto()
    MOUSE_DOWN = auto()
    MOUSE_UP = auto()
    MOUSE_CLICK = auto()
    MOUSE_SCROLL = auto()
    KEY_DOWN = auto()
    KEY_UP = auto()
    KEY_PRESS = auto()
    TOUCH_START = auto()
    TOUCH_MOVE = auto()
    TOUCH_END = auto()
    WAIT = auto()
    MARKER = auto()


@dataclass
class InputEvent:
    """Single input event in a sequence.

    Attributes:
        event_type: Type of input event.
        timestamp: Milliseconds since sequence start.
        x: X coordinate (for mouse/touch events).
        y: Y coordinate (for mouse/touch events).
        button: Button identifier (for mouse events).
        key_code: Virtual key code (for keyboard events).
        key_char: Character representation (for keyboard events).
        scroll_delta: Scroll amount (for scroll events).
        pressure: Touch pressure (0.0 to 1.0).
        metadata: Additional event data.
    """

    event_type: InputEventType
    timestamp: int = 0
    x: float = 0.0
    y: float = 0.0
    button: int = 0
    key_code: int = 0
    key_char: str = ""
    scroll_delta: float = 0.0
    pressure: float = 1.0
    metadata: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        """Convert to dictionary representation."""
        return {
            "type": self.event_type.name,
            "timestamp": self.timestamp,
            "x": self.x,
            "y": self.y,
            "button": self.button,
            "key_code": self.key_code,
            "key_char": self.key_char,
            "scroll_delta": self.scroll_delta,
            "pressure": self.pressure,
            "metadata": self.metadata,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "InputEvent":
        """Create from dictionary representation."""
        return cls(
            event_type=InputEventType[data["type"]],
            timestamp=data.get("timestamp", 0),
            x=data.get("x", 0.0),
            y=data.get("y", 0.0),
            button=data.get("button", 0),
            key_code=data.get("key_code", 0),
            key_char=data.get("key_char", ""),
            scroll_delta=data.get("scroll_delta", 0.0),
            pressure=data.get("pressure", 1.0),
            metadata=data.get("metadata", {}),
        )


@dataclass
class InputSequence:
    """A sequence of input events with metadata.

    Attributes:
        name: Human-readable name of the sequence.
        events: List of input events in order.
        created_at: When the sequence was created.
        duration_ms: Total duration of the sequence.
        loop_count: Number of times to loop during playback.
    """

    name: str = ""
    events: list[InputEvent] = field(default_factory=list)
    created_at: datetime = field(default_factory=datetime.now)
    duration_ms: int = 0
    loop_count: int = 1

    def add_event(self, event: InputEvent) -> None:
        """Add an event to the sequence.

        Args:
            event: InputEvent to add.
        """
        self.events.append(event)
        if self.events:
            self.duration_ms = max(e.timestamp for e in self.events)

    def get_events_in_range(
        self,
        start_ms: int,
        end_ms: int,
    ) -> list[InputEvent]:
        """Get events within a time range.

        Args:
            start_ms: Start time in milliseconds.
            end_ms: End time in milliseconds.

        Returns:
            List of events in the range.
        """
        return [e for e in self.events if start_ms <= e.timestamp <= end_ms]

    def to_json(self) -> str:
        """Serialize to JSON string.

        Returns:
            JSON representation of the sequence.
        """
        data = {
            "name": self.name,
            "created_at": self.created_at.isoformat(),
            "duration_ms": self.duration_ms,
            "loop_count": self.loop_count,
            "events": [e.to_dict() for e in self.events],
        }
        return json.dumps(data)

    @classmethod
    def from_json(cls, json_str: str) -> "InputSequence":
        """Deserialize from JSON string.

        Args:
            json_str: JSON string to parse.

        Returns:
            InputSequence instance.
        """
        data = json.loads(json_str)
        events = [InputEvent.from_dict(e) for e in data.get("events", [])]
        return cls(
            name=data.get("name", ""),
            events=events,
            created_at=datetime.fromisoformat(data.get("created_at", datetime.now().isoformat())),
            duration_ms=data.get("duration_ms", 0),
            loop_count=data.get("loop_count", 1),
        )

    def to_compressed_base64(self) -> str:
        """Serialize and compress to base64 string.

        Returns:
            Compressed base64-encoded sequence.
        """
        json_bytes = self.to_json().encode("utf-8")
        compressed = zlib.compress(json_bytes, level=9)
        return base64.b64encode(compressed).decode("ascii")

    @classmethod
    def from_compressed_base64(cls, encoded: str) -> "InputSequence":
        """Deserialize from compressed base64 string.

        Args:
            encoded: Compressed base64 string.

        Returns:
            InputSequence instance.
        """
        compressed = base64.b64decode(encoded.encode("ascii"))
        json_bytes = zlib.decompress(compressed)
        return cls.from_json(json_bytes.decode("utf-8"))


class InputSequenceRecorder:
    """Records input event sequences.

    Example:
        recorder = InputSequenceRecorder()
        recorder.start()
        # ... perform actions ...
        sequence = recorder.stop()
    """

    def __init__(self):
        """Initialize the recorder."""
        self._events: list[InputEvent] = []
        self._start_time: Optional[float] = None
        self._recording = False

    def start(self) -> None:
        """Start recording input events."""
        self._events = []
        self._start_time = time.time()
        self._recording = True

    def stop(self) -> InputSequence:
        """Stop recording and return the sequence.

        Returns:
            InputSequence with recorded events.
        """
        self._recording = False
        return InputSequence(
            name=f"Recording_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
            events=self._events,
            duration_ms=self._events[-1].timestamp if self._events else 0,
        )

    def record_event(self, event: InputEvent) -> None:
        """Record a single input event.

        Args:
            event: InputEvent to record.
        """
        if not self._recording:
            return

        if self._start_time is not None:
            elapsed_ms = int((time.time() - self._start_time) * 1000)
            event.timestamp = elapsed_ms

        self._events.append(event)


class InputSequencePlayer:
    """Plays back input event sequences.

    Example:
        player = InputSequencePlayer(executor=my_executor)
        player.play(sequence, speed=1.5)
    """

    def __init__(
        self,
        executor: Optional[Callable[[InputEvent], None]] = None,
        on_event: Optional[Callable[[InputEvent], None]] = None,
    ):
        """Initialize the player.

        Args:
            executor: Function to execute each input event.
            on_event: Callback for each event played.
        """
        self.executor = executor
        self.on_event = on_event
        self._stopped = False
        self._paused = False
        self._pause_time: float = 0

    def play(
        self,
        sequence: InputSequence,
        speed: float = 1.0,
        loop: bool = False,
        randomized_timing: float = 0.0,
    ) -> None:
        """Play an input sequence.

        Args:
            sequence: InputSequence to play.
            speed: Playback speed multiplier (1.0 = normal).
            loop: Whether to loop the sequence.
            randomized_timing: Amount of timing randomization (0.0 to 1.0).
        """
        self._stopped = False
        self._paused = False
        loop_count = sequence.loop_count if loop else 1

        for _ in range(loop_count):
            if self._stopped:
                break
            self._play_single(sequence, speed, randomized_timing)

    def _play_single(
        self,
        sequence: InputSequence,
        speed: float,
        randomized_timing: float,
    ) -> None:
        """Play a single iteration of the sequence.

        Args:
            sequence: InputSequence to play.
            speed: Playback speed multiplier.
            randomized_timing: Timing randomization factor.
        """
        last_timestamp = 0

        for event in sequence.events:
            if self._stopped:
                break

            while self._paused and not self._stopped:
                time.sleep(0.01)

            # Calculate delay
            delay_ms = (event.timestamp - last_timestamp) / speed
            if randomized_timing > 0:
                import random
                factor = 1.0 + random.uniform(-randomized_timing, randomized_timing)
                delay_ms *= factor

            if delay_ms > 0:
                time.sleep(delay_ms / 1000.0)

            last_timestamp = event.timestamp

            # Execute the event
            if self.executor:
                self.executor(event)
            if self.on_event:
                self.on_event(event)

    def stop(self) -> None:
        """Stop playback."""
        self._stopped = True

    def pause(self) -> None:
        """Pause playback."""
        self._paused = True
        self._pause_time = time.time()

    def resume(self) -> None:
        """Resume playback."""
        self._paused = False


class InputSequenceAnalyzer:
    """Analyzes input sequences for patterns and statistics."""

    @staticmethod
    def get_event_counts(sequence: InputSequence) -> dict[str, int]:
        """Count events by type.

        Args:
            sequence: InputSequence to analyze.

        Returns:
            Dictionary of event_type -> count.
        """
        counts: dict[str, int] = {}
        for event in sequence.events:
            name = event.event_type.name
            counts[name] = counts.get(name, 0) + 1
        return counts

    @staticmethod
    def get_total_distance(sequence: InputSequence) -> float:
        """Calculate total mouse/touch movement distance.

        Args:
            sequence: InputSequence to analyze.

        Returns:
            Total distance traveled in pixels.
        """
        total = 0.0
        prev_x, prev_y = 0.0, 0.0

        for event in sequence.events:
            if event.event_type in (
                InputEventType.MOUSE_MOVE,
                InputEventType.TOUCH_MOVE,
            ):
                dx = event.x - prev_x
                dy = event.y - prev_y
                total += (dx * dx + dy * dy) ** 0.5
                prev_x, prev_y = event.x, event.y

        return total

    @staticmethod
    def get_average_velocity(sequence: InputSequence) -> float:
        """Calculate average movement velocity.

        Args:
            sequence: InputSequence to analyze.

        Returns:
            Average velocity in pixels per second.
        """
        distance = InputSequenceAnalyzer.get_total_distance(sequence)
        duration_s = sequence.duration_ms / 1000.0
        if duration_s > 0:
            return distance / duration_s
        return 0.0
