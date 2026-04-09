"""
Accessibility Bridge Module.

Provides a bridge between accessibility APIs and the automation framework,
translating system accessibility events into framework-agnostic actions.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Callable, Protocol


logger = logging.getLogger(__name__)


class AccessibilityEvent(Enum):
    """Enumeration of accessibility event types."""
    ELEMENT_FOCUSED = auto()
    ELEMENT_UNFOCUSED = auto()
    ELEMENT_SELECTED = auto()
    ELEMENT_EXPANDED = auto()
    ELEMENT_COLLAPSED = auto()
    ELEMENT_MOVED = auto()
    ELEMENT_RESIZED = auto()
    WINDOW_ACTIVATED = auto()
    WINDOW_DEACTIVATED = auto()
    WINDOW_MINIMIZED = auto()
    WINDOW_MAXIMIZED = auto()
    WINDOW_RESTORED = auto()
    VALUE_CHANGED = auto()
    TEXT_CHANGED = auto()
    STRUCTURE_CHANGED = auto()


@dataclass
class AccessibilityEventData:
    """Data container for accessibility events."""
    event_type: AccessibilityEvent
    element: Any = None
    window: Any = None
    old_value: Any = None
    new_value: Any = None
    timestamp: float = 0.0
    metadata: dict[str, Any] = field(default_factory=dict)


class AccessibilityEventHandler(Protocol):
    """Protocol for handling accessibility events."""

    def handle_event(self, event: AccessibilityEventData) -> None:
        """Handle an accessibility event."""
        ...


class AccessibilityBridge:
    """
    Bridge class connecting system accessibility APIs to the automation framework.

    This class translates raw accessibility events into structured, framework-
    agnostic actions that can be processed by the automation system.

    Example:
        >>> bridge = AccessibilityBridge()
        >>> bridge.register_handler(handler)
        >>> bridge.start_listening()
    """

    def __init__(self) -> None:
        """Initialize the accessibility bridge."""
        self._handlers: list[AccessibilityEventHandler] = []
        self._event_queue: asyncio.Queue[AccessibilityEventData] = asyncio.Queue()
        self._listening: bool = False
        self._task: asyncio.Task[None] | None = None
        self._event_buffer: list[AccessibilityEventData] = []
        self._buffer_size: int = 100

    def register_handler(self, handler: AccessibilityEventHandler) -> None:
        """
        Register an event handler with the bridge.

        Args:
            handler: An object implementing AccessibilityEventHandler protocol.
        """
        if handler not in self._handlers:
            self._handlers.append(handler)
            logger.debug(f"Registered handler: {handler.__class__.__name__}")

    def unregister_handler(self, handler: AccessibilityEventHandler) -> None:
        """
        Unregister an event handler from the bridge.

        Args:
            handler: The handler to remove.
        """
        if handler in self._handlers:
            self._handlers.remove(handler)
            logger.debug(f"Unregistered handler: {handler.__class__.__name__}")

    async def start_listening(self) -> None:
        """Start listening for accessibility events."""
        if self._listening:
            logger.warning("Already listening for accessibility events")
            return

        self._listening = True
        self._task = asyncio.create_task(self._event_processor())
        logger.info("Accessibility bridge started listening")

    async def stop_listening(self) -> None:
        """Stop listening for accessibility events."""
        self._listening = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info("Accessibility bridge stopped listening")

    async def emit_event(self, event: AccessibilityEventData) -> None:
        """
        Emit an accessibility event to all registered handlers.

        Args:
            event: The accessibility event to emit.
        """
        await self._event_queue.put(event)

    async def _event_processor(self) -> None:
        """Process events from the queue and dispatch to handlers."""
        while self._listening:
            try:
                event = await asyncio.wait_for(
                    self._event_queue.get(),
                    timeout=1.0
                )
                await self._dispatch_event(event)
            except asyncio.TimeoutError:
                continue
            except Exception as e:
                logger.error(f"Error processing accessibility event: {e}")

    async def _dispatch_event(self, event: AccessibilityEventData) -> None:
        """
        Dispatch an event to all registered handlers.

        Args:
            event: The event to dispatch.
        """
        self._buffer_event(event)

        for handler in self._handlers:
            try:
                handler.handle_event(event)
            except Exception as e:
                logger.error(
                    f"Error in handler {handler.__class__.__name__}: {e}"
                )

    def _buffer_event(self, event: AccessibilityEventData) -> None:
        """
        Buffer an event for later replay.

        Args:
            event: The event to buffer.
        """
        self._event_buffer.append(event)
        if len(self._event_buffer) > self._buffer_size:
            self._event_buffer.pop(0)

    def get_event_history(self) -> list[AccessibilityEventData]:
        """
        Get the buffered event history.

        Returns:
            List of recent accessibility events.
        """
        return list(self._event_buffer)

    def clear_event_history(self) -> None:
        """Clear the buffered event history."""
        self._event_buffer.clear()
        logger.debug("Event history cleared")

    def set_buffer_size(self, size: int) -> None:
        """
        Set the maximum buffer size for event history.

        Args:
            size: Maximum number of events to buffer.
        """
        self._buffer_size = max(1, size)
        while len(self._event_buffer) > self._buffer_size:
            self._event_buffer.pop(0)
        logger.debug(f"Buffer size set to {size}")


class AccessibilityEventFilter:
    """
    Filter for accessibility events based on criteria.

    Allows filtering events by type, source, or custom predicates.
    """

    def __init__(self) -> None:
        """Initialize the event filter."""
        self._event_types: set[AccessibilityEvent] = set()
        self._element_predicates: list[Callable[[Any], bool]] = []
        self._custom_filters: list[Callable[[AccessibilityEventData], bool]] = []

    def add_event_type(self, event_type: AccessibilityEvent) -> AccessibilityEventFilter:
        """
        Add an event type to the filter.

        Args:
            event_type: The event type to include.

        Returns:
            Self for chaining.
        """
        self._event_types.add(event_type)
        return self

    def add_element_predicate(
        self,
        predicate: Callable[[Any], bool]
    ) -> AccessibilityEventFilter:
        """
        Add a predicate for filtering elements.

        Args:
            predicate: Function that returns True if element should pass.

        Returns:
            Self for chaining.
        """
        self._element_predicates.append(predicate)
        return self

    def add_custom_filter(
        self,
        filter_func: Callable[[AccessibilityEventData], bool]
    ) -> AccessibilityEventFilter:
        """
        Add a custom filter function.

        Args:
            filter_func: Function that returns True if event should pass.

        Returns:
            Self for chaining.
        """
        self._custom_filters.append(filter_func)
        return self

    def should_emit(self, event: AccessibilityEventData) -> bool:
        """
        Determine if an event should be emitted based on filters.

        Args:
            event: The event to evaluate.

        Returns:
            True if the event passes all filters.
        """
        if self._event_types and event.event_type not in self._event_types:
            return False

        if self._element_predicates and event.element is not None:
            if not any(p(event.element) for p in self._element_predicates):
                return False

        if self._custom_filters:
            if not all(f(event) for f in self._custom_filters):
                return False

        return True


class AccessibilityBridgeBuilder:
    """
    Builder for creating configured AccessibilityBridge instances.
    """

    def __init__(self) -> None:
        """Initialize the builder."""
        self._handlers: list[AccessibilityEventHandler] = []
        self._filters: list[AccessibilityEventFilter] = []
        self._buffer_size: int = 100

    def add_handler(self, handler: AccessibilityEventHandler) -> AccessibilityBridgeBuilder:
        """
        Add a handler to the bridge.

        Args:
            handler: The handler to add.

        Returns:
            Self for chaining.
        """
        self._handlers.append(handler)
        return self

    def add_filter(self, filter_impl: AccessibilityEventFilter) -> AccessibilityBridgeBuilder:
        """
        Add a filter to the bridge.

        Args:
            filter_impl: The filter to add.

        Returns:
            Self for chaining.
        """
        self._filters.append(filter_impl)
        return self

    def set_buffer_size(self, size: int) -> AccessibilityBridgeBuilder:
        """
        Set the event buffer size.

        Args:
            size: Maximum buffer size.

        Returns:
            Self for chaining.
        """
        self._buffer_size = size
        return self

    def build(self) -> AccessibilityBridge:
        """
        Build the configured AccessibilityBridge instance.

        Returns:
            Configured AccessibilityBridge.
        """
        bridge = AccessibilityBridge()
        bridge.set_buffer_size(self._buffer_size)

        for handler in self._handlers:
            bridge.register_handler(handler)

        logger.info(
            f"Built AccessibilityBridge with {len(self._handlers)} handlers, "
            f"{len(self._filters)} filters"
        )

        return bridge
