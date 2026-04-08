"""
Event Filter Utilities

Provides utilities for filtering and processing
events in automation workflows.

Author: Agent3
"""
from __future__ import annotations

from typing import Any, Callable
from dataclasses import dataclass


@dataclass
class EventFilter:
    """Filter criteria for events."""
    event_types: set[str] | None = None
    predicate: Callable[[dict[str, Any]], bool] | None = None
    rate_limit_per_second: float | None = None


class EventFilter:
    """
    Filters events based on criteria.
    
    Provides selective event processing
    based on type, content, or rate.
    """

    def __init__(self) -> None:
        self._filters: list[EventFilter] = []

    def add_filter(self, event_filter: EventFilter) -> None:
        """Add a filter to the chain."""
        self._filters.append(event_filter)

    def should_process(self, event: dict[str, Any]) -> bool:
        """
        Check if event should be processed.
        
        Args:
            event: Event to check.
            
        Returns:
            True if event passes all filters.
        """
        for f in self._filters:
            if f.event_types:
                if event.get("type") not in f.event_types:
                    continue
            if f.predicate:
                if not f.predicate(event):
                    continue
            return True
        return len(self._filters) == 0

    def process(
        self,
        event: dict[str, Any],
    ) -> dict[str, Any] | None:
        """
        Process an event through filters.
        
        Returns:
            Processed event or None if filtered out.
        """
        if self.should_process(event):
            return event
        return None

    def clear_filters(self) -> None:
        """Remove all filters."""
        self._filters.clear()


def create_type_filter(*event_types: str) -> EventFilter:
    """Create a filter for specific event types."""
    return EventFilter(event_types=set(event_types))


def create_predicate_filter(
    predicate: Callable[[dict[str, Any]], bool]
) -> EventFilter:
    """Create a filter using a predicate function."""
    return EventFilter(predicate=predicate)
