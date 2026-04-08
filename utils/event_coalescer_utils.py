"""
Event Coalescer Utilities

Provides utilities for coalescing multiple events
into single operations in automation workflows.

Author: Agent3
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable
from datetime import datetime
import time


@dataclass
class CoalescedEvent:
    """An event with coalescing applied."""
    event_type: str
    data: dict[str, Any]
    count: int = 1
    first_timestamp: float = 0.0
    last_timestamp: float = 0.0


class EventCoalescer:
    """
    Coalesces multiple similar events into single events.
    
    Reduces event storm by merging rapid successive
    events of the same type.
    """

    def __init__(
        self,
        window_ms: float = 100.0,
        max_count: int = 100,
    ) -> None:
        self._window_ms = window_ms
        self._max_count = max_count
        self._pending: dict[str, CoalescedEvent] = {}
        self._handlers: dict[str, list[Callable[[CoalescedEvent], None]]] = {}

    def add_event(
        self,
        event_type: str,
        data: dict[str, Any] | None = None,
    ) -> CoalescedEvent | None:
        """
        Add an event for coalescing.
        
        Args:
            event_type: Type of event.
            data: Event data.
            
        Returns:
            CoalescedEvent if flushed, None otherwise.
        """
        now = time.time()
        data = data or {}

        if event_type not in self._pending:
            self._pending[event_type] = CoalescedEvent(
                event_type=event_type,
                data=data,
                count=1,
                first_timestamp=now,
                last_timestamp=now,
            )
            return None

        coalesced = self._pending[event_type]
        if now - coalesced.last_timestamp < self._window_ms / 1000.0:
            coalesced.count += 1
            coalesced.last_timestamp = now
            coalesced.data.update(data)
            if coalesced.count >= self._max_count:
                return self._flush(event_type)
            return None
        else:
            self._flush(event_type)
            self._pending[event_type] = CoalescedEvent(
                event_type=event_type,
                data=data,
                count=1,
                first_timestamp=now,
                last_timestamp=now,
            )
            return None

    def _flush(self, event_type: str) -> CoalescedEvent | None:
        """Flush a pending coalesced event."""
        if event_type in self._pending:
            coalesced = self._pending.pop(event_type)
            self._notify_handlers(coalesced)
            return coalesced
        return None

    def flush_all(self) -> list[CoalescedEvent]:
        """Flush all pending events."""
        results = []
        for event_type in list(self._pending.keys()):
            coalesced = self._flush(event_type)
            if coalesced:
                results.append(coalesced)
        return results

    def register_handler(
        self,
        event_type: str,
        handler: Callable[[CoalescedEvent], None],
    ) -> None:
        """Register a handler for coalesced events."""
        if event_type not in self._handlers:
            self._handlers[event_type] = []
        self._handlers[event_type].append(handler)

    def _notify_handlers(self, event: CoalescedEvent) -> None:
        """Notify handlers of a coalesced event."""
        if event.event_type in self._handlers:
            for handler in self._handlers[event.event_type]:
                handler(event)
