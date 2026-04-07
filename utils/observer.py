"""Observer pattern utilities for RabAI AutoClick.

Provides:
- Observable: Subject that notifies observers
- Observer: Base observer class
- PropertyObserver: Observe property changes
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Set, TypeVar


T = TypeVar("T")


class Observer(ABC):
    """Abstract base observer class."""

    @abstractmethod
    def update(self, event: Dict[str, Any]) -> None:
        """Receive update from subject.

        Args:
            event: Event data from subject.
        """
        pass


class Observable:
    """Subject that notifies observers.

    Usage:
        class MyObserver(Observer):
            def update(self, event):
                print(f"Received: {event}")

        subject = Observable()
        subject.attach(MyObserver())
        subject.notify({"message": "hello"})
    """

    def __init__(self) -> None:
        self._observers: List[Observer] = []
        self._event_handlers: Dict[str, List[Callable]] = {}

    def attach(self, observer: Observer) -> None:
        """Attach an observer.

        Args:
            observer: Observer to attach.
        """
        if observer not in self._observers:
            self._observers.append(observer)

    def detach(self, observer: Observer) -> None:
        """Detach an observer.

        Args:
            observer: Observer to detach.
        """
        if observer in self._observers:
            self._observers.remove(observer)

    def notify(self, event: Dict[str, Any]) -> None:
        """Notify all observers.

        Args:
            event: Event data to send.
        """
        for observer in self._observers:
            try:
                observer.update(event)
            except Exception:
                pass

    def on(self, event_type: str, handler: Callable[[Dict[str, Any]], None]) -> None:
        """Register event handler.

        Args:
            event_type: Type of event.
            handler: Handler function.
        """
        if event_type not in self._event_handlers:
            self._event_handlers[event_type] = []
        self._event_handlers[event_type].append(handler)

    def off(self, event_type: str, handler: Optional[Callable] = None) -> None:
        """Unregister event handler.

        Args:
            event_type: Type of event.
            handler: Handler to remove (if None, removes all for type).
        """
        if event_type not in self._event_handlers:
            return

        if handler is None:
            self._event_handlers[event_type].clear()
        else:
            self._event_handlers[event_type] = [
                h for h in self._event_handlers[event_type] if h != handler
            ]

    def emit(self, event_type: str, data: Optional[Dict[str, Any]] = None) -> None:
        """Emit an event to registered handlers.

        Args:
            event_type: Type of event.
            data: Event data.
        """
        event = {"type": event_type, "data": data or {}}

        if event_type in self._event_handlers:
            for handler in self._event_handlers[event_type]:
                try:
                    handler(event)
                except Exception:
                    pass

        # Also notify all observers
        self.notify(event)


@dataclass
class PropertyChange:
    """Represents a property change."""
    name: str
    old_value: Any
    new_value: Any
    source: Any = None


class PropertyObserver(Observable):
    """Observable that tracks property changes.

    Usage:
        class Person(PropertyObserver):
            @property
            def name(self):
                return self._name

            @name.setter
            def name(self, value):
                old = self._name
                self._name = value
                self.notify_property_change("name", old, value)
    """

    def __init__(self) -> None:
        super().__init__()
        self._property_handlers: Dict[str, List[Callable[[PropertyChange], None]]] = {}

    def observe_property(
        self,
        property_name: str,
        handler: Callable[[PropertyChange], None],
    ) -> None:
        """Register handler for property changes.

        Args:
            property_name: Name of property to observe.
            handler: Handler function.
        """
        if property_name not in self._property_handlers:
            self._property_handlers[property_name] = []
        self._property_handlers[property_name].append(handler)

    def unobserve_property(
        self,
        property_name: str,
        handler: Optional[Callable] = None,
    ) -> None:
        """Unregister property change handler.

        Args:
            property_name: Name of property.
            handler: Handler to remove.
        """
        if property_name not in self._property_handlers:
            return

        if handler is None:
            self._property_handlers[property_name].clear()
        else:
            self._property_handlers[property_name] = [
                h for h in self._property_handlers[property_name] if h != handler
            ]

    def notify_property_change(
        self,
        property_name: str,
        old_value: Any,
        new_value: Any,
    ) -> None:
        """Notify listeners of property change.

        Args:
            property_name: Name of changed property.
            old_value: Previous value.
            new_value: New value.
        """
        change = PropertyChange(
            name=property_name,
            old_value=old_value,
            new_value=new_value,
            source=self,
        )

        # Call property-specific handlers
        if property_name in self._property_handlers:
            for handler in self._property_handlers[property_name]:
                try:
                    handler(change)
                except Exception:
                    pass

        # General notification
        self.notify({
            "type": "property_change",
            "property": property_name,
            "old_value": old_value,
            "new_value": new_value,
        })


class EventEmitter:
    """Simple event emitter pattern.

    Usage:
        emitter = EventEmitter()

        @emitter.on("data")
        def handle_data(data):
            print(data)

        emitter.emit("data", {"value": 42})
    """

    def __init__(self) -> None:
        self._listeners: Dict[str, Set[Callable]] = {}

    def on(self, event: str, handler: Callable) -> Callable:
        """Register event handler.

        Args:
            event: Event name.
            handler: Handler function.

        Returns:
            Handler for decorator use.
        """
        if event not in self._listeners:
            self._listeners[event] = set()
        self._listeners[event].add(handler)
        return handler

    def off(self, event: str, handler: Callable) -> None:
        """Unregister event handler.

        Args:
            event: Event name.
            handler: Handler to remove.
        """
        if event in self._listeners:
            self._listeners[event].discard(handler)

    def once(self, event: str, handler: Callable) -> Callable:
        """Register one-time event handler.

        Args:
            event: Event name.
            handler: Handler function.

        Returns:
            Wrapped handler.
        """
        def wrapper(*args: Any, **kwargs: Any) -> None:
            handler(*args, **kwargs)
            self.off(event, wrapper)

        return self.on(event, wrapper)

    def emit(self, event: str, *args: Any, **kwargs: Any) -> None:
        """Emit event to all handlers.

        Args:
            event: Event name.
            *args: Positional arguments for handlers.
            **kwargs: Keyword arguments for handlers.
        """
        if event in self._listeners:
            for handler in list(self._listeners[event]):
                try:
                    handler(*args, **kwargs)
                except Exception:
                    pass

    def clear(self, event: Optional[str] = None) -> None:
        """Clear handlers.

        Args:
            event: Specific event to clear (None = clear all).
        """
        if event is None:
            self._listeners.clear()
        elif event in self._listeners:
            self._listeners[event].clear()

    def listener_count(self, event: str) -> int:
        """Get number of listeners for event."""
        return len(self._listeners.get(event, set()))


class WeakObserver:
    """Observer that holds weak reference to subscriber.

    Useful for observer pattern without preventing garbage collection.
    """

    def __init__(self) -> None:
        import weakref
        self._observers: List[weakref.ref] = []

    def attach(self, observer: Observer) -> None:
        """Attach observer using weak reference."""
        import weakref
        if observer not in [ref() for ref in self._observers]:
            self._observers.append(weakref.ref(observer))

    def detach(self, observer: Observer) -> None:
        """Detach observer."""
        import weakref
        self._observers = [
            ref for ref in self._observers
            if ref() is not observer and ref() is not None
        ]

    def notify(self, event: Dict[str, Any]) -> None:
        """Notify all living observers."""
        dead_refs = []
        for ref in self._observers:
            observer = ref()
            if observer is None:
                dead_refs.append(ref)
                continue
            try:
                observer.update(event)
            except Exception:
                pass

        for ref in dead_refs:
            self._observers.remove(ref)