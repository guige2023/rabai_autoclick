"""
Input sequence utilities for recording and replaying input sequences.

Provides input sequence recording, storage, and playback
capabilities with support for timing and synchronization.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Callable, Optional


@dataclass
class InputEvent:
    """A single input event in a sequence."""
    event_type: str  # "key", "click", "scroll", "move"
    x: float = 0
    y: float = 0
    data: any = None  # key char, button state, scroll delta
    timestamp_ms: float = 0.0
    delay_ms: float = 0.0  # Delay before this event


@dataclass
class InputSequence:
    """A recorded sequence of input events."""
    name: str
    events: list[InputEvent] = field(default_factory=list)
    duration_ms: float = 0.0
    created_at: float = field(default_factory=time.time)
    metadata: dict = field(default_factory=dict)

    def total_duration_ms(self) -> float:
        """Get total duration of the sequence."""
        if not self.events:
            return 0.0
        return self.events[-1].timestamp_ms - self.events[0].timestamp_ms


class InputSequenceRecorder:
    """Records input sequences for later replay."""

    def __init__(self, name: str = ""):
        self.name = name
        self._events: list[InputEvent] = []
        self._start_time: Optional[float] = None
        self._is_recording = False

    def start(self) -> None:
        """Start recording."""
        self._events.clear()
        self._start_time = time.time() * 1000
        self._is_recording = True

    def stop(self) -> InputSequence:
        """Stop recording and return the sequence."""
        self._is_recording = False
        return InputSequence(name=self.name, events=self._events[:])

    def record_key(self, key: str, delay_ms: float = 0.0) -> None:
        """Record a key press event."""
        if not self._is_recording:
            return
        ts = (time.time() * 1000) - (self._start_time or 0)
        self._events.append(InputEvent(event_type="key", data=key, timestamp_ms=ts, delay_ms=delay_ms))

    def record_click(self, x: float, y: float, button: int = 0, delay_ms: float = 0.0) -> None:
        """Record a click event."""
        if not self._is_recording:
            return
        ts = (time.time() * 1000) - (self._start_time or 0)
        self._events.append(InputEvent(event_type="click", x=x, y=y, data=button, timestamp_ms=ts, delay_ms=delay_ms))

    def record_scroll(self, delta_x: int, delta_y: int, x: float = 0, y: float = 0) -> None:
        """Record a scroll event."""
        if not self._is_recording:
            return
        ts = (time.time() * 1000) - (self._start_time or 0)
        self._events.append(InputEvent(event_type="scroll", x=x, y=y, data=(delta_x, delta_y), timestamp_ms=ts))

    def record_move(self, x: float, y: float) -> None:
        """Record a mouse move event."""
        if not self._is_recording:
            return
        ts = (time.time() * 1000) - (self._start_time or 0)
        self._events.append(InputEvent(event_type="move", x=x, y=y, timestamp_ms=ts))

    def get_sequence(self) -> InputSequence:
        """Get the current recording as a sequence."""
        return InputSequence(name=self.name, events=self._events[:])


class InputSequenceReplayer:
    """Replays recorded input sequences."""

    def __init__(
        self,
        executor: Optional[Callable[[InputEvent], bool]] = None,
    ):
        self._executor = executor or self._default_executor
        self._is_playing = False
        self._current_sequence: Optional[InputSequence] = None

    def replay(
        self,
        sequence: InputSequence,
        speed: float = 1.0,
        on_event: Optional[Callable[[InputEvent], None]] = None,
    ) -> bool:
        """Replay an input sequence.

        Args:
            sequence: The sequence to replay
            speed: Playback speed multiplier (1.0 = normal)
            on_event: Optional callback for each event

        Returns:
            True if successful, False otherwise
        """
        if not sequence.events:
            return False

        self._is_playing = True
        start_time = time.time() * 1000
        first_ts = sequence.events[0].timestamp_ms

        try:
            for event in sequence.events:
                if not self._is_playing:
                    return False

                # Calculate wait time
                event_start = (event.timestamp_ms - first_ts) / speed
                elapsed = (time.time() * 1000) - start_time
                wait_time = (event_start - elapsed) / 1000.0

                if wait_time > 0:
                    time.sleep(wait_time)

                if on_event:
                    on_event(event)

                if not self._executor(event):
                    return False

            return True
        finally:
            self._is_playing = False

    def stop(self) -> None:
        """Stop playback."""
        self._is_playing = False

    def _default_executor(self, event: InputEvent) -> bool:
        """Default event executor (logs the event)."""
        return True


__all__ = ["InputSequenceRecorder", "InputSequenceReplayer", "InputSequence", "InputEvent"]
