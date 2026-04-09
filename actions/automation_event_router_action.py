"""Automation event router action for event-driven automation.

Routes events to appropriate handlers based on event type,
topic, and filtering rules with priority support.
"""

import asyncio
import logging
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Optional

logger = logging.getLogger(__name__)


class EventPriority(Enum):
    """Event priority levels."""
    LOW = 0
    NORMAL = 1
    HIGH = 2
    CRITICAL = 3


@dataclass
class Event:
    """An automation event."""
    type: str
    data: Any
    timestamp: float = field(default_factory=time.time)
    priority: EventPriority = EventPriority.NORMAL
    source: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class EventHandler:
    """Handler registration for events."""
    name: str
    handler_fn: Callable[[Event], Any]
    event_types: list[str] = field(default_factory=list)
    topics: list[str] = field(default_factory=list)
    priority: EventPriority = EventPriority.NORMAL
    filter_fn: Optional[Callable[[Event], bool]] = None


@dataclass
class RouteResult:
    """Result of routing an event."""
    event: Event
    handlers_invoked: int
    processing_time_ms: float
    errors: list[str] = field(default_factory=list)


class AutomationEventRouterAction:
    """Route events to appropriate handlers.

    Args:
        async_execution: If True, execute handlers asynchronously.

    Example:
        >>> router = AutomationEventRouterAction()
        >>> router.register_handler("click", handle_click, event_types=["click"])
        >>> result = await router.route_event(Event(type="click", data={}))
    """

    def __init__(self, async_execution: bool = True) -> None:
        self.async_execution = async_execution
        self._handlers: list[EventHandler] = []
        self._event_history: list[Event] = []
        self._max_history = 1000

    def register_handler(
        self,
        name: str,
        handler_fn: Callable[[Event], Any],
        event_types: Optional[list[str]] = None,
        topics: Optional[list[str]] = None,
        priority: EventPriority = EventPriority.NORMAL,
        filter_fn: Optional[Callable[[Event], bool]] = None,
    ) -> "AutomationEventRouterAction":
        """Register an event handler.

        Args:
            name: Unique name for this handler.
            handler_fn: Function to handle matching events.
            event_types: List of event types to match.
            topics: List of topics to match.
            priority: Handler priority (higher runs first).
            filter_fn: Additional filter function.

        Returns:
            Self for method chaining.
        """
        handler = EventHandler(
            name=name,
            handler_fn=handler_fn,
            event_types=event_types or [],
            topics=topics or [],
            priority=priority,
            filter_fn=filter_fn,
        )
        self._handlers.append(handler)
        self._handlers.sort(key=lambda h: -h.priority.value)
        return self

    def unregister_handler(self, name: str) -> bool:
        """Remove a handler by name.

        Args:
            name: Handler name to remove.

        Returns:
            True if handler was found and removed.
        """
        for i, handler in enumerate(self._handlers):
            if handler.name == name:
                del self._handlers[i]
                return True
        return False

    async def route_event(self, event: Event) -> RouteResult:
        """Route an event to matching handlers.

        Args:
            event: Event to route.

        Returns:
            Route result with processing details.
        """
        start_time = time.time()
        errors: list[str] = []
        handlers_invoked = 0

        self._add_to_history(event)

        matching_handlers = self._get_matching_handlers(event)

        logger.debug(
            f"Routing event {event.type} to {len(matching_handlers)} handlers"
        )

        for handler in matching_handlers:
            try:
                if self.async_execution:
                    if asyncio.iscoroutinefunction(handler.handler_fn):
                        await handler.handler_fn(event)
                    else:
                        asyncio.create_task(
                            self._run_sync_handler(handler.handler_fn, event)
                        )
                else:
                    handler.handler_fn(event)

                handlers_invoked += 1

            except Exception as e:
                error_msg = f"Handler {handler.name} failed: {e}"
                logger.error(error_msg)
                errors.append(error_msg)

        return RouteResult(
            event=event,
            handlers_invoked=handlers_invoked,
            processing_time_ms=(time.time() - start_time) * 1000,
            errors=errors,
        )

    async def _run_sync_handler(
        self,
        handler_fn: Callable[[Event], Any],
        event: Event,
    ) -> None:
        """Run a sync handler in async context.

        Args:
            handler_fn: Handler function.
            event: Event to handle.
        """
        try:
            handler_fn(event)
        except Exception as e:
            logger.error(f"Sync handler failed: {e}")

    def _get_matching_handlers(self, event: Event) -> list[EventHandler]:
        """Find handlers that match the event.

        Args:
            event: Event to match.

        Returns:
            List of matching handlers.
        """
        matching = []

        for handler in self._handlers:
            if self._handler_matches(handler, event):
                matching.append(handler)

        return matching

    def _handler_matches(self, handler: EventHandler, event: Event) -> bool:
        """Check if a handler matches an event.

        Args:
            handler: Handler to check.
            event: Event to match.

        Returns:
            True if handler should receive this event.
        """
        if handler.event_types:
            if event.type not in handler.event_types:
                return False

        if handler.topics:
            topic = event.metadata.get("topic", "")
            if topic not in handler.topics:
                return False

        if handler.filter_fn:
            try:
                if not handler.filter_fn(event):
                    return False
            except Exception:
                return False

        return True

    def _add_to_history(self, event: Event) -> None:
        """Add event to history.

        Args:
            event: Event to add.
        """
        self._event_history.append(event)
        if len(self._event_history) > self._max_history:
            self._event_history.pop(0)

    def get_history(
        self,
        event_type: Optional[str] = None,
        limit: int = 100,
    ) -> list[Event]:
        """Get event history.

        Args:
            event_type: Optional filter by event type.
            limit: Maximum events to return.

        Returns:
            List of historical events.
        """
        events = self._event_history
        if event_type:
            events = [e for e in events if e.type == event_type]
        return events[-limit:]

    def clear_history(self) -> None:
        """Clear event history."""
        self._event_history.clear()

    def get_handler_count(self) -> int:
        """Get number of registered handlers.

        Returns:
            Handler count.
        """
        return len(self._handlers)
