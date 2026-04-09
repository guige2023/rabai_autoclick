"""Input Synchronization Utilities.

Synchronizes input events across multiple input sources and devices.

Example:
    >>> from input_synchronization_utils import InputSynchronizer
    >>> sync = InputSynchronizer()
    >>> sync.register_device("keyboard")
    >>> sync.register_device("mouse")
    >>> sync.sync_events()
"""

from __future__ import annotations

import time
from collections import deque
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Callable, Deque, Dict, List, Optional


class EventSource(Enum):
    """Input event source types."""
    KEYBOARD = auto()
    MOUSE = auto()
    TOUCH = auto()
    PEN = auto()
    CUSTOM = auto()


@dataclass
class SyncEvent:
    """A synchronized input event."""
    source: EventSource
    event_type: str
    data: Dict[str, Any]
    timestamp: float
    sequence: int


class InputSynchronizer:
    """Synchronizes events from multiple input sources."""

    def __init__(self, buffer_size: int = 100):
        """Initialize synchronizer.

        Args:
            buffer_size: Maximum events per source to buffer.
        """
        self.buffer_size = buffer_size
        self._buffers: Dict[str, Deque[SyncEvent]] = {}
        self._sequence = 0
        self._handlers: List[Callable[[SyncEvent], None]] = []

    def register_device(self, device_id: str) -> None:
        """Register an input device.

        Args:
            device_id: Unique device identifier.
        """
        if device_id not in self._buffers:
            self._buffers[device_id] = deque(maxlen=self.buffer_size)

    def unregister_device(self, device_id: str) -> None:
        """Unregister an input device.

        Args:
            device_id: Device to remove.
        """
        self._buffers.pop(device_id, None)

    def add_event(
        self,
        device_id: str,
        source: EventSource,
        event_type: str,
        data: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Add an event from a device.

        Args:
            device_id: Source device.
            source: Event source type.
            event_type: Event type name.
            data: Event data.
        """
        if device_id not in self._buffers:
            self.register_device(device_id)

        self._sequence += 1
        event = SyncEvent(
            source=source,
            event_type=event_type,
            data=data or {},
            timestamp=time.time(),
            sequence=self._sequence,
        )
        self._buffers[device_id].append(event)

    def sync_events(self) -> List[SyncEvent]:
        """Synchronize and return all buffered events sorted by timestamp.

        Returns:
            List of SyncEvent objects sorted by sequence.
        """
        all_events: List[SyncEvent] = []
        for buffer in self._buffers.values():
            all_events.extend(buffer)

        all_events.sort(key=lambda e: e.timestamp)
        return all_events

    def on_sync(self, handler: Callable[[SyncEvent], None]) -> None:
        """Register a synchronization handler.

        Args:
            handler: Callback for synchronized events.
        """
        self._handlers.append(handler)

    def process_sync(self) -> None:
        """Process all synchronized events through handlers."""
        events = self.sync_events()
        for event in events:
            for handler in self._handlers:
                handler(event)
