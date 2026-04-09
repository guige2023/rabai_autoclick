"""
Input Sequence Buffer Utilities

A ring-buffer implementation for storing and replaying input event sequences.
Used for input recording, playback, and analysis.

Author: rabai_autoclick-agent3
"""

from __future__ import annotations

import time
from collections import deque
from dataclasses import dataclass, field
from typing import Optional, Callable, List, Any


@dataclass
class InputEvent:
    """A single input event in the sequence buffer."""
    event_type: str  # 'mouse', 'keyboard', 'touch'
    action: str  # 'down', 'up', 'move', 'press', 'release'
    x: float = 0.0
    y: float = 0.0
    key: Optional[str] = None
    timestamp_ms: float = field(default_factory=lambda: time.time() * 1000)
    metadata: dict = field(default_factory=dict)


class InputSequenceBuffer:
    """A ring-buffer for storing input event sequences."""

    def __init__(self, max_size: int = 10000):
        self.max_size = max_size
        self._buffer: deque[InputEvent] = deque(maxlen=max_size)
        self._start_time_ms: Optional[float] = None

    def append(self, event: InputEvent) -> None:
        """Append an input event to the buffer."""
        if self._start_time_ms is None:
            self._start_time_ms = event.timestamp_ms
        self._buffer.append(event)

    def append_raw(
        self,
        event_type: str,
        action: str,
        x: float = 0.0,
        y: float = 0.0,
        key: Optional[str] = None,
        **metadata,
    ) -> None:
        """Append a raw event without creating an InputEvent first."""
        event = InputEvent(
            event_type=event_type,
            action=action,
            x=x,
            y=y,
            key=key,
            metadata=metadata,
        )
        self.append(event)

    def get_events(
        self,
        start_ms: Optional[float] = None,
        end_ms: Optional[float] = None,
        event_type: Optional[str] = None,
    ) -> List[InputEvent]:
        """Get events within a time range, optionally filtered by type."""
        events = list(self._buffer)
        if start_ms is not None:
            events = [e for e in events if e.timestamp_ms >= start_ms]
        if end_ms is not None:
            events = [e for e in events if e.timestamp_ms <= end_ms]
        if event_type is not None:
            events = [e for e in events if e.event_type == event_type]
        return events

    def get_duration_ms(self) -> float:
        """Get the total duration of the buffered sequence."""
        if len(self._buffer) < 2:
            return 0.0
        return self._buffer[-1].timestamp_ms - self._buffer[0].timestamp_ms

    def clear(self) -> None:
        """Clear the buffer."""
        self._buffer.clear()
        self._start_time_ms = None

    def __len__(self) -> int:
        return len(self._buffer)

    def __bool__(self) -> bool:
        return len(self._buffer) > 0


def replay_sequence(
    buffer: InputSequenceBuffer,
    executor: Callable[[InputEvent], None],
    speed: float = 1.0,
    start_offset_ms: float = 0.0,
) -> None:
    """
    Replay a buffered input sequence through an executor function.

    Args:
        buffer: The InputSequenceBuffer containing events.
        executor: A function that executes a single InputEvent.
        speed: Playback speed multiplier (1.0 = real-time, 2.0 = 2x speed).
        start_offset_ms: Start replay from this offset into the sequence.
    """
    if speed <= 0:
        speed = 1.0

    base_time = buffer._buffer[0].timestamp_ms if buffer._buffer else time.time() * 1000
    last_time = base_time + start_offset_ms

    for event in buffer._buffer:
        if event.timestamp_ms < last_time:
            continue

        wait_ms = (event.timestamp_ms - last_time) / speed
        if wait_ms > 0:
            time.sleep(wait_ms / 1000.0)

        executor(event)
        last_time = event.timestamp_ms
