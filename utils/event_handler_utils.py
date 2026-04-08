"""
Event Handler Utilities

Provides utilities for managing event handlers
in automation workflows.

Author: Agent3
"""
from __future__ import annotations

from typing import Any, Callable
from dataclasses import dataclass
import weakref


@dataclass
class HandlerRegistration:
    """Registration info for an event handler."""
    event_type: str
    handler: Callable[..., Any]
    priority: int = 0
    once: bool = False


class EventHandler:
    """
    Manages event handlers with registration and cleanup.
    
    Supports handler priorities, one-time handlers,
    and automatic cleanup of deleted objects.
    """

    def __init__(self) -> None:
        self._handlers: dict[str, list[HandlerRegistration]] = {}

    def on(
        self,
        event_type: str,
        handler: Callable[..., Any],
        priority: int = 0,
    ) -> HandlerRegistration:
        """
        Register an event handler.
        
        Args:
            event_type: Type of event to listen for.
            handler: Handler function.
            priority: Handler priority (higher = called first).
            
        Returns:
            HandlerRegistration for cleanup.
        """
        reg = HandlerRegistration(
            event_type=event_type,
            handler=handler,
            priority=priority,
        )
        if event_type not in self._handlers:
            self._handlers[event_type] = []
        self._handlers[event_type].append(reg)
        self._handlers[event_type].sort(key=lambda r: -r.priority)
        return reg

    def once(
        self,
        event_type: str,
        handler: Callable[..., Any],
        priority: int = 0,
    ) -> HandlerRegistration:
        """Register a one-time event handler."""
        reg = HandlerRegistration(
            event_type=event_type,
            handler=handler,
            priority=priority,
            once=True,
        )
        if event_type not in self._handlers:
            self._handlers[event_type] = []
        self._handlers[event_type].append(reg)
        self._handlers[event_type].sort(key=lambda r: -r.priority)
        return reg

    def off(
        self,
        event_type: str,
        handler: Callable[..., Any] | None = None,
    ) -> None:
        """
        Unregister event handlers.
        
        Args:
            event_type: Type of event.
            handler: Specific handler to remove (None = all).
        """
        if handler is None:
            self._handlers.pop(event_type, None)
        elif event_type in self._handlers:
            self._handlers[event_type] = [
                r for r in self._handlers[event_type]
                if r.handler != handler
            ]

    def emit(
        self,
        event_type: str,
        *args: Any,
        **kwargs: Any,
    ) -> list[Any]:
        """
        Emit an event to all registered handlers.
        
        Returns:
            List of handler return values.
        """
        results = []
        if event_type in self._handlers:
            to_remove = []
            for reg in self._handlers[event_type]:
                try:
                    result = reg.handler(*args, **kwargs)
                    results.append(result)
                except Exception:
                    pass
                if reg.once:
                    to_remove.append(reg)
            for reg in to_remove:
                self._handlers[event_type].remove(reg)
        return results

    def has_handlers(self, event_type: str) -> bool:
        """Check if event type has registered handlers."""
        return bool(self._handlers.get(event_type))


def create_event_handler() -> EventHandler:
    """Create a new EventHandler instance."""
    return EventHandler()
