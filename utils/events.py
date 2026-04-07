"""Event handling utilities for RabAI AutoClick.

Provides:
- Event dispatcher
- Event types
- Event handlers
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, List, Optional


class EventType(Enum):
    """Event types."""
    WORKFLOW_START = "workflow_start"
    WORKFLOW_STOP = "workflow_stop"
    WORKFLOW_PAUSE = "workflow_pause"
    WORKFLOW_RESUME = "workflow_resume"
    ACTION_EXECUTE = "action_execute"
    ACTION_COMPLETE = "action_complete"
    ACTION_ERROR = "action_error"
    SCREENSHOT_TAKEN = "screenshot_taken"
    IMAGE_MATCH_FOUND = "image_match_found"
    KEYBOARD_PRESS = "keyboard_press"
    MOUSE_CLICK = "mouse_click"
    CUSTOM = "custom"


@dataclass
class Event:
    """Base event class."""
    type: EventType
    timestamp: datetime
    data: Dict[str, Any] = None

    def __post_init__(self) -> None:
        if self.data is None:
            self.data = {}


class EventHandler(ABC):
    """Base event handler."""

    @abstractmethod
    def handle(self, event: Event) -> None:
        """Handle an event.

        Args:
            event: Event to handle.
        """
        pass


class EventDispatcher:
    """Central event dispatcher."""

    def __init__(self) -> None:
        """Initialize dispatcher."""
        self._handlers: Dict[EventType, List[EventHandler]] = {}
        self._callbacks: Dict[EventType, List[Callable]] = {}
        self._event_history: List[Event] = []
        self._max_history: int = 100

    def subscribe(
        self,
        event_type: EventType,
        handler: EventHandler,
    ) -> None:
        """Subscribe to event type.

        Args:
            event_type: Type of event.
            handler: Handler to receive events.
        """
        if event_type not in self._handlers:
            self._handlers[event_type] = []
        self._handlers[event_type].append(handler)

    def unsubscribe(
        self,
        event_type: EventType,
        handler: EventHandler,
    ) -> bool:
        """Unsubscribe from event type.

        Args:
            event_type: Type of event.
            handler: Handler to remove.

        Returns:
            True if handler was found and removed.
        """
        if event_type in self._handlers:
            if handler in self._handlers[event_type]:
                self._handlers[event_type].remove(handler)
                return True
        return False

    def on(
        self,
        event_type: EventType,
        callback: Callable[[Event], None],
    ) -> None:
        """Register callback for event type.

        Args:
            event_type: Type of event.
            callback: Function to call.
        """
        if event_type not in self._callbacks:
            self._callbacks[event_type] = []
        self._callbacks[event_type].append(callback)

    def off(
        self,
        event_type: EventType,
        callback: Callable[[Event], None],
    ) -> bool:
        """Unregister callback.

        Args:
            event_type: Type of event.
            callback: Function to remove.

        Returns:
            True if callback was found and removed.
        """
        if event_type in self._callbacks:
            if callback in self._callbacks[event_type]:
                self._callbacks[event_type].remove(callback)
                return True
        return False

    def dispatch(self, event: Event) -> None:
        """Dispatch event to handlers.

        Args:
            event: Event to dispatch.
        """
        # Store in history
        self._event_history.append(event)
        if len(self._event_history) > self._max_history:
            self._event_history.pop(0)

        # Call handlers
        if event.type in self._handlers:
            for handler in self._handlers[event.type]:
                try:
                    handler.handle(event)
                except Exception:
                    pass

        # Call callbacks
        if event.type in self._callbacks:
            for callback in self._callbacks[event.type]:
                try:
                    callback(event)
                except Exception:
                    pass

    def emit(
        self,
        event_type: EventType,
        data: Optional[Dict[str, Any]] = None,
    ) -> Event:
        """Create and dispatch event.

        Args:
            event_type: Type of event.
            data: Optional event data.

        Returns:
            Created event.
        """
        event = Event(
            type=event_type,
            timestamp=datetime.now(),
            data=data or {},
        )
        self.dispatch(event)
        return event

    def get_history(
        self,
        event_type: Optional[EventType] = None,
        limit: int = 50,
    ) -> List[Event]:
        """Get event history.

        Args:
            event_type: Filter by type (None for all).
            limit: Maximum events to return.

        Returns:
            List of events.
        """
        history = self._event_history
        if event_type:
            history = [e for e in history if e.type == event_type]
        return history[-limit:]

    def clear_history(self) -> None:
        """Clear event history."""
        self._event_history.clear()


class EventBus:
    """Global event bus (singleton)."""

    _instance: Optional["EventBus"] = None

    def __new__(cls) -> "EventBus":
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._dispatcher = EventDispatcher()
        return cls._instance

    def __init__(self) -> None:
        if not hasattr(self, "_dispatcher"):
            self._dispatcher = EventDispatcher()

    def subscribe(
        self,
        event_type: EventType,
        handler: EventHandler,
    ) -> None:
        """Subscribe to event type."""
        self._dispatcher.subscribe(event_type, handler)

    def unsubscribe(
        self,
        event_type: EventType,
        handler: EventHandler,
    ) -> bool:
        """Unsubscribe from event type."""
        return self._dispatcher.unsubscribe(event_type, handler)

    def on(
        self,
        event_type: EventType,
        callback: Callable[[Event], None],
    ) -> None:
        """Register callback."""
        self._dispatcher.on(event_type, callback)

    def off(
        self,
        event_type: EventType,
        callback: Callable[[Event], None],
    ) -> bool:
        """Unregister callback."""
        return self._dispatcher.off(event_type, callback)

    def dispatch(self, event: Event) -> None:
        """Dispatch event."""
        self._dispatcher.dispatch(event)

    def emit(
        self,
        event_type: EventType,
        data: Optional[Dict[str, Any]] = None,
    ) -> Event:
        """Emit event."""
        return self._dispatcher.emit(event_type, data)

    def get_history(
        self,
        event_type: Optional[EventType] = None,
        limit: int = 50,
    ) -> List[Event]:
        """Get event history."""
        return self._dispatcher.get_history(event_type, limit)

    def clear_history(self) -> None:
        """Clear history."""
        self._dispatcher.clear_history()


class EventFilter:
    """Filter events by criteria."""

    def __init__(self) -> None:
        """Initialize filter."""
        self._type_filter: Optional[EventType] = None
        self._since_filter: Optional[datetime] = None
        self._data_filter: Optional[Callable[[Dict], bool]] = None

    def type(self, event_type: EventType) -> "EventFilter":
        """Filter by type.

        Args:
            event_type: Event type.

        Returns:
            Self for chaining.
        """
        self._type_filter = event_type
        return self

    def since(self, timestamp: datetime) -> "EventFilter":
        """Filter by timestamp.

        Args:
            timestamp: Events after this time.

        Returns:
            Self for chaining.
        """
        self._since_filter = timestamp
        return self

    def data(self, filter_func: Callable[[Dict], bool]) -> "EventFilter":
        """Filter by data content.

        Args:
            filter_func: Function that returns True if event matches.

        Returns:
            Self for chaining.
        """
        self._data_filter = filter_func
        return self

    def matches(self, event: Event) -> bool:
        """Check if event matches filter.

        Args:
            event: Event to check.

        Returns:
            True if matches.
        """
        if self._type_filter and event.type != self._type_filter:
            return False

        if self._since_filter and event.timestamp < self._since_filter:
            return False

        if self._data_filter and not self._data_filter(event.data):
            return False

        return True


def create_event(
    event_type: EventType,
    **data: Any,
) -> Event:
    """Create a new event.

    Args:
        event_type: Type of event.
        **data: Event data.

    Returns:
        Created event.
    """
    return Event(
        type=event_type,
        timestamp=datetime.now(),
        data=data,
    )
