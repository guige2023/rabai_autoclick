"""
Input Buffer Utility

Buffers and throttles input events (keyboard/mouse) for automation.
Ensures consistent timing and prevents event flooding.

Example:
    >>> buffer = InputBuffer(capacity=100, flush_interval=0.05)
    >>> buffer.push(InputEvent(type='key', key='a'))
    >>> buffer.flush()
"""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional, Callable


class InputEventType(Enum):
    """Types of input events."""
    KEY_DOWN = "key_down"
    KEY_UP = "key_up"
    MOUSE_MOVE = "mouse_move"
    MOUSE_DOWN = "mouse_down"
    MOUSE_UP = "mouse_up"
    MOUSE_WHEEL = "mouse_wheel"
    TOUCH_START = "touch_start"
    TOUCH_MOVE = "touch_move"
    TOUCH_END = "touch_end"


@dataclass
class InputEvent:
    """
    A single input event.

    Attributes:
        type: Event type enum.
        key: Key name (for keyboard events).
        x: X coordinate (for mouse/touch events).
        y: Y coordinate (for mouse/touch events).
        button: Mouse button number.
        delta: Wheel delta (for mouse wheel events).
        timestamp: Event timestamp (uses time.time() if not provided).
        modifiers: List of active modifier keys.
    """
    type: InputEventType
    key: Optional[str] = None
    x: float = 0.0
    y: float = 0.0
    button: int = 0
    delta: float = 0.0
    timestamp: float = field(default_factory=time.time)
    modifiers: list[str] = field(default_factory=list)

    def delay_since(self, other: InputEvent) -> float:
        """Return seconds since another event."""
        return self.timestamp - other.timestamp


class InputBuffer:
    """
    Thread-safe buffer for input events.

    Args:
        capacity: Maximum number of events to buffer.
        flush_interval: Auto-flush interval in seconds (0 = manual only).
    """

    def __init__(
        self,
        capacity: int = 100,
        flush_interval: float = 0.0,
    ) -> None:
        self.capacity = capacity
        self.flush_interval = flush_interval
        self._buffer: list[InputEvent] = []
        self._lock = threading.Lock()
        self._flush_callback: Optional[Callable[[list[InputEvent]], None]] = None
        self._last_flush_time = time.time()

    def set_flush_callback(
        self,
        callback: Callable[[list[InputEvent]], None],
    ) -> None:
        """Set callback invoked when buffer is flushed."""
        self._flush_callback = callback

    def push(self, event: InputEvent) -> bool:
        """
        Add an event to the buffer.

        Args:
            event: InputEvent to add.

        Returns:
            True if buffered, False if buffer was full and auto-flushed.
        """
        with self._lock:
            if len(self._buffer) >= self.capacity:
                self._do_flush()
                return False
            self._buffer.append(event)
            if self.flush_interval > 0:
                now = time.time()
                if now - self._last_flush_time >= self.flush_interval:
                    self._do_flush()
            return True

    def flush(self) -> list[InputEvent]:
        """
        Manually flush and return all buffered events.

        Returns:
            List of buffered events (buffer is cleared).
        """
        with self._lock:
            return self._do_flush()

    def _do_flush(self) -> list[InputEvent]:
        """Internal flush without lock."""
        events = list(self._buffer)
        self._buffer.clear()
        self._last_flush_time = time.time()
        if events and self._flush_callback:
            try:
                self._flush_callback(events)
            except Exception:
                pass
        return events

    def size(self) -> int:
        """Return current buffer size."""
        with self._lock:
            return len(self._buffer)

    def clear(self) -> None:
        """Clear the buffer without flushing."""
        with self._lock:
            self._buffer.clear()

    def get_events(
        self,
        event_type: Optional[InputEventType] = None,
        since: Optional[float] = None,
    ) -> list[InputEvent]:
        """Get a copy of buffered events, optionally filtered."""
        with self._lock:
            events = list(self._buffer)
        if event_type:
            events = [e for e in events if e.type == event_type]
        if since is not None:
            events = [e for e in events if e.timestamp >= since]
        return events


class ThrottledInputBuffer(InputBuffer):
    """
    InputBuffer that enforces minimum delay between same-type events.

    Args:
        min_interval: Minimum seconds between flushes.
        capacity: Maximum buffer size.
    """

    def __init__(
        self,
        min_interval: float = 0.016,  # ~60fps
        capacity: int = 100,
    ) -> None:
        super().__init__(capacity=capacity, flush_interval=0.0)
        self.min_interval = min_interval
        self._last_flush: dict[InputEventType, float] = {}

    def push(self, event: InputEvent) -> bool:
        """Push with throttle enforcement."""
        with self._lock:
            last = self._last_flush.get(event.type, 0.0)
            now = time.time()
            if now - last < self.min_interval:
                # Coalesce with last event of same type
                if self._buffer and self._buffer[-1].type == event.type:
                    self._buffer[-1] = event
                    return True
                return False
            self._last_flush[event.type] = now
            return super().push(event)


class InputSequenceRecorder:
    """
    Records input sequences for replay.

    Args:
        buffer: InputBuffer to record into.
    """

    def __init__(self, buffer: Optional[InputBuffer] = None) -> None:
        self.buffer = buffer or InputBuffer()
        self._recording = False
        self._start_time: Optional[float] = None

    def start(self) -> None:
        """Start recording."""
        self._recording = True
        self._start_time = time.time()
        self.buffer.clear()

    def stop(self) -> list[InputEvent]:
        """
        Stop recording.

        Returns:
            List of recorded events.
        """
        self._recording = False
        return self.buffer.flush()

    def is_recording(self) -> bool:
        """Return whether currently recording."""
        return self._recording

    def record_event(self, event: InputEvent) -> None:
        """Record a single event (only if recording)."""
        if self._recording:
            rel_time = event.timestamp - (self._start_time or event.timestamp)
            adjusted = InputEvent(
                type=event.type,
                key=event.key,
                x=event.x,
                y=event.y,
                button=event.button,
                delta=event.delta,
                timestamp=rel_time,
                modifiers=event.modifiers,
            )
            self.buffer.push(adjusted)

    def get_relative_time(self) -> float:
        """Get time since recording started."""
        if self._start_time is None:
            return 0.0
        return time.time() - self._start_time
