"""
Event Router Utilities

Provides utilities for routing events to
appropriate handlers in automation workflows.

Author: Agent3
"""
from __future__ import annotations

from typing import Any, Callable
from dataclasses import dataclass


@dataclass
class Route:
    """Represents an event route."""
    pattern: str
    handler: Callable[..., Any]
    priority: int = 0


class EventRouter:
    """
    Routes events to appropriate handlers based on patterns.
    
    Supports wildcard patterns and priority-based
    handler selection.
    """

    def __init__(self) -> None:
        self._routes: list[Route] = []
        self._default_handler: Callable[..., Any] | None = None

    def add_route(
        self,
        pattern: str,
        handler: Callable[..., Any],
        priority: int = 0,
    ) -> None:
        """Add a route for an event pattern."""
        route = Route(pattern=pattern, handler=handler, priority=priority)
        self._routes.append(route)
        self._routes.sort(key=lambda r: -r.priority)

    def set_default(self, handler: Callable[..., Any]) -> None:
        """Set default handler for unmatched events."""
        self._default_handler = handler

    def route(self, event_type: str, *args: Any, **kwargs: Any) -> Any | None:
        """
        Route an event to matching handlers.
        
        Args:
            event_type: Type of event to route.
            *args: Positional arguments to pass to handlers.
            **kwargs: Keyword arguments to pass to handlers.
            
        Returns:
            Result from first matching handler.
        """
        for route in self._routes:
            if self._match(route.pattern, event_type):
                return route.handler(*args, **kwargs)
        if self._default_handler:
            return self._default_handler(event_type, *args, **kwargs)
        return None

    def _match(self, pattern: str, event_type: str) -> bool:
        """Match event type against pattern."""
        if pattern == "*":
            return True
        if pattern.endswith("*"):
            prefix = pattern[:-1]
            return event_type.startswith(prefix)
        if pattern.startswith("*"):
            suffix = pattern[1:]
            return event_type.endswith(suffix)
        return pattern == event_type

    def remove_route(self, pattern: str) -> bool:
        """Remove routes matching a pattern."""
        original_len = len(self._routes)
        self._routes = [r for r in self._routes if r.pattern != pattern]
        return len(self._routes) < original_len

    def clear_routes(self) -> None:
        """Remove all routes."""
        self._routes.clear()
