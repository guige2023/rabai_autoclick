"""Event system utilities for RabAI AutoClick.

Provides:
- EventBus: Central event dispatcher
- Event: Base event class
- Subscriber: Decorator for event handlers
"""

import threading
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set, Type, TypeVar


T = TypeVar("T", bound="Event")


class EventPriority(Enum):
    """Event handler priority."""
    LOW = 0
    NORMAL = 50
    HIGH = 100


@dataclass
class Event:
    """Base event class.

    Usage:
        event = Event(type="click", data={"x": 100, "y": 200})
    """
    type: str
    data: Dict[str, Any] = field(default_factory=dict)
    source: Optional[str] = None
    timestamp: datetime = field(default_factory=datetime.now)
    propagation_stopped: bool = field(default=False, init=False)

    def stop_propagation(self) -> None:
        """Stop event from being propagated to further handlers."""
        self.propagation_stopped = True


@dataclass
class Subscription:
    """Represents an event subscription."""
    event_type: str
    handler: Callable[[Event], None]
    priority: EventPriority = EventPriority.NORMAL
    filter: Optional[Callable[[Event], bool]] = None


class EventBus:
    """Central event dispatcher.

    Usage:
        bus = EventBus()

        # Subscribe
        @bus.subscribe("click")
        def on_click(event):
            print(f"Clicked at {event.data}")

        # Publish
        bus.publish(Event("click", {"x": 100, "y": 200}))

        # Unsubscribe
        bus.unsubscribe("click", on_click)
    """

    def __init__(self) -> None:
        self._subscriptions: Dict[str, List[Subscription]] = {}
        self._lock = threading.RLock()
        self._event_history: List[Event] = []
        self._max_history = 100

    def subscribe(
        self,
        event_type: str,
        priority: EventPriority = EventPriority.NORMAL,
    ) -> Callable[[Callable[[Event], None]], Callable[[Event], None]]:
        """Decorator to subscribe to an event type.

        Args:
            event_type: Type of event to subscribe to.
            priority: Handler priority (higher = called first).

        Returns:
            Decorator function.

        Usage:
            @bus.subscribe("click", priority=EventPriority.HIGH)
            def on_click_high_priority(event):
                pass
        """
        def decorator(handler: Callable[[Event], None]) -> Callable[[Event], None]:
            self.add_subscription(event_type, handler, priority)
            return handler
        return decorator

    def add_subscription(
        self,
        event_type: str,
        handler: Callable[[Event], None],
        priority: EventPriority = EventPriority.NORMAL,
        filter: Optional[Callable[[Event], bool]] = None,
    ) -> None:
        """Add an event subscription.

        Args:
            event_type: Type of event.
            handler: Handler function.
            priority: Handler priority.
            filter: Optional filter function for event data.
        """
        with self._lock:
            if event_type not in self._subscriptions:
                self._subscriptions[event_type] = []

            sub = Subscription(
                event_type=event_type,
                handler=handler,
                priority=priority,
                filter=filter,
            )
            self._subscriptions[event_type].append(sub)
            self._subscriptions[event_type].sort(
                key=lambda s: s.priority.value, reverse=True
            )

    def unsubscribe(
        self,
        event_type: str,
        handler: Callable[[Event], None],
    ) -> bool:
        """Remove an event subscription.

        Args:
            event_type: Type of event.
            handler: Handler function to remove.

        Returns:
            True if handler was found and removed.
        """
        with self._lock:
            if event_type not in self._subscriptions:
                return False

            subs = self._subscriptions[event_type]
            for i, sub in enumerate(subs):
                if sub.handler == handler:
                    subs.pop(i)
                    return True

            return False

    def publish(self, event: Event) -> None:
        """Publish an event to all subscribers.

        Args:
            event: Event to publish.
        """
        with self._lock:
            subscriptions = self._subscriptions.get(event.type, []).copy()
            self._add_to_history(event)

        for sub in subscriptions:
            if event.propagation_stopped:
                break

            if sub.filter and not sub.filter(event):
                continue

            try:
                sub.handler(event)
            except Exception:
                pass

    def publish_async(self, event: Event) -> None:
        """Publish an event asynchronously in a new thread.

        Args:
            event: Event to publish.
        """
        thread = threading.Thread(target=self.publish, args=(event,))
        thread.start()

    def _add_to_history(self, event: Event) -> None:
        """Add event to history, maintaining max size."""
        self._event_history.append(event)
        if len(self._event_history) > self._max_history:
            self._event_history.pop(0)

    def get_history(
        self,
        event_type: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> List[Event]:
        """Get event history.

        Args:
            event_type: Filter by event type.
            limit: Maximum number of events to return.

        Returns:
            List of events.
        """
        with self._lock:
            events = self._event_history.copy()

        if event_type:
            events = [e for e in events if e.type == event_type]

        if limit:
            events = events[-limit:]

        return events

    def clear_history(self) -> None:
        """Clear event history."""
        with self._lock:
            self._event_history.clear()

    def get_subscriber_count(self, event_type: str) -> int:
        """Get number of subscribers for an event type."""
        with self._lock:
            return len(self._subscriptions.get(event_type, []))


class EventBusManager:
    """Manages multiple event buses.

    Provides isolated event buses for different components.
    """

    _instance: Optional['EventBusManager'] = None
    _lock = threading.Lock()

    def __new__(cls) -> 'EventBusManager':
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialized = False
        return cls._instance

    def __init__(self) -> None:
        if self._initialized:
            return

        self._initialized = True
        self._buses: Dict[str, EventBus] = {}
        self._default_bus = EventBus()

    def get_bus(self, name: str = "default") -> EventBus:
        """Get or create a named event bus.

        Args:
            name: Name of the event bus.

        Returns:
            EventBus instance.
        """
        with self._lock:
            if name not in self._buses:
                self._buses[name] = EventBus()
            return self._buses[name]

    @property
    def default_bus(self) -> EventBus:
        """Get the default event bus."""
        return self._default_bus

    def list_buses(self) -> List[str]:
        """List all event bus names."""
        with self._lock:
            return list(self._buses.keys())

    def remove_bus(self, name: str) -> bool:
        """Remove a named event bus.

        Args:
            name: Name of bus to remove.

        Returns:
            True if bus existed and was removed.
        """
        with self._lock:
            if name in self._buses:
                del self._buses[name]
                return True
            return False


# Global event bus
event_bus = EventBusManager().default_bus


# Predefined event types
class Events:
    """Predefined event types for RabAI AutoClick."""
    # Workflow events
    WORKFLOW_STARTED = "workflow.started"
    WORKFLOW_STOPPED = "workflow.stopped"
    WORKFLOW_PAUSED = "workflow.paused"
    WORKFLOW_RESUMED = "workflow.resumed"
    WORKFLOW_ERROR = "workflow.error"
    WORKFLOW_COMPLETED = "workflow.completed"

    # Action events
    ACTION_STARTED = "action.started"
    ACTION_COMPLETED = "action.completed"
    ACTION_FAILED = "action.failed"

    # UI events
    UI_CLICK = "ui.click"
    UI_KEYPRESS = "ui.keypress"
    UI_MOUSE_MOVE = "ui.mouse_move"

    # System events
    SYSTEM_START = "system.start"
    SYSTEM_STOP = "system.stop"
    SYSTEM_ERROR = "system.error"
    CONFIG_CHANGED = "config.changed"