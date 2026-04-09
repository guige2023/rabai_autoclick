"""
Accessibility Event Coalescing Utilities

Coalesce rapid accessibility events into meaningful state changes,
reducing event noise while preserving important transitions.

Author: rabai_autoclick-agent3
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Callable, Optional, Dict, Any, List
from collections import deque


@dataclass
class CoalescedEvent:
    """An event that may represent multiple raw events."""
    event_type: str
    element_id: str
    accumulated_count: int
    latest_value: Any
    first_seen_ms: float
    last_seen_ms: float


class AccessibilityEventCoalescer:
    """
    Coalesce rapid accessibility events into single meaningful events.

    For example, a focus event that fires rapidly during layout
    transitions is coalesced into one event with the final focus state.
    """

    def __init__(
        self,
        coalescing_window_ms: float = 100.0,
        max_accumulation: int = 50,
    ):
        self.coalescing_window_ms = coalescing_window_ms
        self.max_accumulation = max_accumulation
        self._pending: Dict[str, CoalescedEvent] = {}
        self._handlers: Dict[str, Callable[[CoalescedEvent], None]] = {}
        self._last_flush_ms = time.time() * 1000

    def register_handler(
        self,
        event_type: str,
        handler: Callable[[CoalescedEvent], None],
    ) -> None:
        """Register a handler for a coalesced event type."""
        self._handlers[event_type] = handler

    def add_event(
        self,
        event_type: str,
        element_id: str,
        value: Any,
        timestamp_ms: Optional[float] = None,
    ) -> Optional[CoalescedEvent]:
        """
        Add a raw accessibility event.

        Returns a CoalescedEvent if one was just flushed, otherwise None.
        """
        now = timestamp_ms or time.time() * 1000
        key = f"{event_type}:{element_id}"

        existing = self._pending.get(key)

        if existing:
            existing.accumulated_count += 1
            existing.latest_value = value
            existing.last_seen_ms = now
            # Check if we should flush due to accumulation limit
            if existing.accumulated_count >= self.max_accumulation:
                return self._flush(key)
        else:
            self._pending[key] = CoalescedEvent(
                event_type=event_type,
                element_id=element_id,
                accumulated_count=1,
                latest_value=value,
                first_seen_ms=now,
                last_seen_ms=now,
            )

        # Check if coalescing window has elapsed for any pending event
        self._check_flush_by_time(now)

        return None

    def _flush(self, key: str) -> Optional[CoalescedEvent]:
        """Flush a specific pending event."""
        event = self._pending.pop(key, None)
        if event and event.event_type in self._handlers:
            self._handlers[event.event_type](event)
        return event

    def _check_flush_by_time(self, now_ms: float) -> List[CoalescedEvent]:
        """Flush any events whose coalescing window has elapsed."""
        flushed = []
        for key in list(self._pending.keys()):
            event = self._pending[key]
            if now_ms - event.first_seen_ms >= self.coalescing_window_ms:
                flushed.append(self._flush(key))
        return [f for f in flushed if f]

    def flush_all(self) -> List[CoalescedEvent]:
        """Force flush all pending events."""
        flushed = []
        for key in list(self._pending.keys()):
            flushed.append(self._flush(key))
        return [f for f in flushed if f]

    def get_pending_count(self) -> int:
        """Get the number of pending (not yet flushed) events."""
        return len(self._pending)
