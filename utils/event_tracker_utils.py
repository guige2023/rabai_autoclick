"""
Event Tracker Utilities

Provides utilities for tracking event sequences
and patterns in automation workflows.

Author: Agent3
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any
from datetime import datetime
from collections import deque


@dataclass
class TrackedEvent:
    """Represents a tracked event."""
    event_type: str
    timestamp: datetime
    data: dict[str, Any] | None = None


class EventTracker:
    """
    Tracks sequences of events for pattern detection.
    
    Maintains event history and supports
    sequence matching.
    """

    def __init__(self, max_history: int = 100) -> None:
        self._events: deque[TrackedEvent] = deque(maxlen=max_history)
        self._sequences: list[list[str]] = []

    def track(
        self,
        event_type: str,
        data: dict[str, Any] | None = None,
    ) -> TrackedEvent:
        """
        Track a new event.
        
        Args:
            event_type: Type of event.
            data: Optional event data.
            
        Returns:
            Created TrackedEvent.
        """
        event = TrackedEvent(
            event_type=event_type,
            timestamp=datetime.now(),
            data=data,
        )
        self._events.append(event)
        return event

    def get_events(
        self,
        event_type: str | None = None,
        limit: int | None = None,
    ) -> list[TrackedEvent]:
        """
        Get tracked events.
        
        Args:
            event_type: Filter by event type.
            limit: Maximum number of events to return.
            
        Returns:
            List of tracked events.
        """
        events = list(self._events)
        if event_type:
            events = [e for e in events if e.event_type == event_type]
        if limit:
            events = events[-limit:]
        return events

    def register_sequence(self, sequence: list[str]) -> None:
        """Register a sequence pattern to watch for."""
        self._sequences.append(sequence)

    def detect_sequence(self, sequence: list[str]) -> bool:
        """
        Detect if a sequence has occurred.
        
        Args:
            sequence: Event types to look for.
            
        Returns:
            True if sequence is found in recent history.
        """
        if len(sequence) > len(self._events):
            return False
        event_types = [e.event_type for e in self._events]
        for i in range(len(event_types) - len(sequence) + 1):
            if event_types[i:i + len(sequence)] == sequence:
                return True
        return False

    def get_last_sequence(
        self,
        sequence: list[str],
    ) -> list[TrackedEvent] | None:
        """Get events matching a sequence."""
        if len(sequence) > len(self._events):
            return None
        event_types = [e.event_type for e in self._events]
        for i in range(len(event_types) - len(sequence) + 1):
            if event_types[i:i + len(sequence)] == sequence:
                return list(self._events)[i:i + len(sequence)]
        return None

    def clear(self) -> None:
        """Clear all tracked events."""
        self._events.clear()

    def get_event_count(self, event_type: str | None = None) -> int:
        """Get count of tracked events."""
        if event_type:
            return sum(1 for e in self._events if e.event_type == event_type)
        return len(self._events)
