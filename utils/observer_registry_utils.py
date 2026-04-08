"""
Observer Registry Utilities

Provides utilities for managing observer patterns
in automation workflows.

Author: Agent3
"""
from __future__ import annotations

from typing import Any, Callable


class ObserverRegistry:
    """
    Registry for observer pattern implementation.
    
    Manages observers and notification
    for state changes.
    """

    def __init__(self) -> None:
        self._observers: dict[str, list[Callable[[Any], None]]] = {}

    def register(
        self,
        event_type: str,
        observer: Callable[[Any], None],
    ) -> None:
        """
        Register an observer for an event type.
        
        Args:
            event_type: Event to observe.
            observer: Callback function.
        """
        if event_type not in self._observers:
            self._observers[event_type] = []
        if observer not in self._observers[event_type]:
            self._observers[event_type].append(observer)

    def unregister(
        self,
        event_type: str,
        observer: Callable[[Any], None],
    ) -> None:
        """Unregister an observer."""
        if event_type in self._observers:
            self._observers[event_type] = [
                o for o in self._observers[event_type] if o != observer
            ]

    def notify(self, event_type: str, data: Any = None) -> None:
        """Notify all observers of an event."""
        if event_type in self._observers:
            for observer in self._observers[event_type]:
                observer(data)

    def clear(self, event_type: str | None = None) -> None:
        """Clear observers."""
        if event_type:
            self._observers.pop(event_type, None)
        else:
            self._observers.clear()

    def get_observer_count(self, event_type: str) -> int:
        """Get number of observers for an event type."""
        return len(self._observers.get(event_type, []))
