"""Observer pattern and event handling utilities.

Provides event subscription, notification,
and observer pattern implementation.
"""

from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional


@dataclass
class Event:
    """Base event class."""
    name: str
    source: Any = None
    data: Dict[str, Any] = None

    def __post_init__(self) -> None:
        if self.data is None:
            self.data = {}


class Subscriber:
    """Event subscriber with callback."""

    def __init__(
        self,
        callback: Callable[[Event], None],
        name: Optional[str] = None,
    ) -> None:
        self.callback = callback
        self.name = name or f"subscriber_{id(self)}"


class EventBus:
    """Central event bus for pub/sub messaging.

    Example:
        bus = EventBus()
        bus.subscribe("click", lambda e: handle_click(e))
        bus.publish(Event("click", data={"x": 100, "y": 200}))
    """

    def __init__(self) -> None:
        self._subscribers: Dict[str, List[Subscriber]] = {}
        self._global_subscribers: List[Subscriber] = []

    def subscribe(
        self,
        event_name: str,
        callback: Callable[[Event], None],
        name: Optional[str] = None,
    ) -> Subscriber:
        """Subscribe to event.

        Args:
            event_name: Name of event to subscribe to.
            callback: Function to call when event fires.
            name: Optional subscriber name.

        Returns:
            Subscriber object.
        """
        subscriber = Subscriber(callback, name)

        if event_name not in self._subscribers:
            self._subscribers[event_name] = []

        self._subscribers[event_name].append(subscriber)
        return subscriber

    def subscribe_all(
        self,
        callback: Callable[[Event], None],
        name: Optional[str] = None,
    ) -> Subscriber:
        """Subscribe to all events.

        Args:
            callback: Function to call for any event.
            name: Optional subscriber name.

        Returns:
            Subscriber object.
        """
        subscriber = Subscriber(callback, name)
        self._global_subscribers.append(subscriber)
        return subscriber

    def unsubscribe(
        self,
        event_name: str,
        subscriber: Subscriber,
    ) -> bool:
        """Unsubscribe from event.

        Args:
            event_name: Event name.
            subscriber: Subscriber to remove.

        Returns:
            True if subscriber was found and removed.
        """
        if event_name in self._subscribers:
            subs = self._subscribers[event_name]
            if subscriber in subs:
                subs.remove(subscriber)
                return True
        return False

    def unsubscribe_all(self, subscriber: Subscriber) -> int:
        """Unsubscribe from all events.

        Args:
            subscriber: Subscriber to remove.

        Returns:
            Number of subscriptions removed.
        """
        count = 0

        for event_name in list(self._subscribers.keys()):
            if self.unsubscribe(event_name, subscriber):
                count += 1

        if subscriber in self._global_subscribers:
            self._global_subscribers.remove(subscriber)
            count += 1

        return count

    def publish(self, event: Event) -> None:
        """Publish event to all subscribers.

        Args:
            event: Event to publish.
        """
        if event.name in self._subscribers:
            for subscriber in self._subscribers[event.name]:
                try:
                    subscriber.callback(event)
                except Exception:
                    pass

        for subscriber in self._global_subscribers:
            try:
                subscriber.callback(event)
            except Exception:
                pass

    def clear(self, event_name: Optional[str] = None) -> None:
        """Clear subscribers.

        Args:
            event_name: Specific event to clear. All if None.
        """
        if event_name:
            self._subscribers.pop(event_name, None)
        else:
            self._subscribers.clear()
            self._global_subscribers.clear()

    def list_subscriptions(self) -> Dict[str, int]:
        """List subscription counts by event name."""
        return {name: len(subs) for name, subs in self._subscribers.items()}


class EventEmitter:
    """Event emitter with method-based subscriptions.

    Example:
        emitter = EventEmitter()
        @emitter.on("change")
        def handle_change(value):
            print(f"Changed: {value}")
        emitter.emit("change", value=42)
    """

    def __init__(self) -> None:
        self._handlers: Dict[str, List[Callable]] = {}

    def on(self, event: str) -> Callable:
        """Decorator to register event handler.

        Example:
            @emitter.on("data")
            def handle(data):
                print(data)
        """
        def decorator(func: Callable) -> Callable:
            self.add_listener(event, func)
            return func
        return decorator

    def add_listener(self, event: str, handler: Callable) -> None:
        """Add event handler."""
        if event not in self._handlers:
            self._handlers[event] = []
        self._handlers[event].append(handler)

    def remove_listener(self, event: str, handler: Callable) -> bool:
        """Remove event handler."""
        if event in self._handlers:
            try:
                self._handlers[event].remove(handler)
                return True
            except ValueError:
                pass
        return False

    def emit(self, event: str, **kwargs: Any) -> None:
        """Emit event to all handlers.

        Args:
            event: Event name.
            **kwargs: Event data passed to handlers.
        """
        if event in self._handlers:
            for handler in self._handlers[event]:
                try:
                    handler(**kwargs)
                except Exception:
                    pass

    def clear(self, event: Optional[str] = None) -> None:
        """Clear handlers."""
        if event:
            self._handlers.pop(event, None)
        else:
            self._handlers.clear()


class EventHistory:
    """Track event history for debugging.

    Example:
        history = EventHistory(max_events=100)
        history.record(Event("click"))
        for event in history.get("click"):
            print(event)
    """

    def __init__(self, max_events: int = 100) -> None:
        self.max_events = max_events
        self._events: List[Event] = []
        self._by_type: Dict[str, List[Event]] = {}

    def record(self, event: Event) -> None:
        """Record an event."""
        self._events.append(event)

        if event.name not in self._by_type:
            self._by_type[event.name] = []

        self._by_type[event.name].append(event)

        if len(self._events) > self.max_events:
            removed = self._events.pop(0)
            if removed.name in self._by_type:
                try:
                    self._by_type[removed.name].remove(removed)
                except ValueError:
                    pass

    def get(
        self,
        event_name: Optional[str] = None,
        limit: int = 10,
    ) -> List[Event]:
        """Get recent events.

        Args:
            event_name: Filter by event name. All if None.
            limit: Maximum events to return.

        Returns:
            List of events, newest first.
        """
        events = self._by_type.get(event_name, self._events) if event_name else self._events
        return list(reversed(events))[:limit]

    def clear(self) -> None:
        """Clear all events."""
        self._events.clear()
        self._by_type.clear()

    def count(self, event_name: Optional[str] = None) -> int:
        """Count events."""
        if event_name:
            return len(self._by_type.get(event_name, []))
        return len(self._events)


class Observable:
    """Base class for observable objects.

    Example:
        class Button(Observable):
            def click(self):
                self.notify("click", x=100, y=200)

        button = Button()
        button.observe(lambda e: print("Clicked!"))
    """

    def __init__(self) -> None:
        self._observers: List[Callable] = []

    def observe(self, callback: Callable) -> None:
        """Register observer callback."""
        self._observers.append(callback)

    def unobserve(self, callback: Callable) -> bool:
        """Unregister observer."""
        try:
            self._observers.remove(callback)
            return True
        except ValueError:
            return False

    def notify(self, event_name: str, **data: Any) -> None:
        """Notify all observers."""
        event = Event(name=event_name, source=self, data=data)
        for observer in self._observers:
            try:
                observer(event)
            except Exception:
                pass
