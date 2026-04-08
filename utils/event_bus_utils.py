"""Event bus and pub/sub utilities for decoupled communication."""

from typing import Callable, Dict, List, Any, Optional
import threading
import uuid


class Event:
    """Event object passed to subscribers."""

    def __init__(self, name: str, data: Any = None):
        """Initialize event.
        
        Args:
            name: Event name.
            data: Optional event data payload.
        """
        self.name = name
        self.data = data
        self.timestamp = threading.current_thread().name
        self.id = str(uuid.uuid4())


class EventBus:
    """Centralized event bus for publish/subscribe communication."""

    def __init__(self):
        """Initialize event bus."""
        self._subscribers: Dict[str, List[Callable[[Event], None]]] = {}
        self._lock = threading.RLock()
        self._global_handlers: List[Callable[[Event], None]] = []

    def subscribe(
        self,
        event_name: str,
        handler: Callable[[Event], None]
    ) -> str:
        """Subscribe to an event.
        
        Args:
            event_name: Name of event to subscribe to.
            handler: Callback function.
        
        Returns:
            Subscription ID for later unsubscription.
        """
        with self._lock:
            if event_name not in self._subscribers:
                self._subscribers[event_name] = []
            self._subscribers[event_name].append(handler)
            return f"{event_name}:{uuid.uuid4()}"

    def subscribe_all(
        self,
        handler: Callable[[Event], None]
    ) -> str:
        """Subscribe to all events.
        
        Args:
            handler: Callback function for all events.
        
        Returns:
            Subscription ID.
        """
        with self._lock:
            self._global_handlers.append(handler)
            return f"*:{uuid.uuid4()}"

    def unsubscribe(self, subscription_id: str) -> bool:
        """Unsubscribe a handler.
        
        Args:
            subscription_id: Subscription ID from subscribe().
        
        Returns:
            True if unsubscribed, False if not found.
        """
        with self._lock:
            parts = subscription_id.split(":", 1)
            if len(parts) != 2:
                return False
            event_name, _ = parts
            if event_name == "*":
                for h in self._global_handlers:
                    if str(id(h)) in subscription_id:
                        self._global_handlers.remove(h)
                        return True
                return False
            if event_name in self._subscribers:
                for h in self._subscribers[event_name]:
                    if str(id(h)) in subscription_id:
                        self._subscribers[event_name].remove(h)
                        return True
            return False

    def publish(self, event_name: str, data: Any = None) -> None:
        """Publish an event.
        
        Args:
            event_name: Name of event.
            data: Optional event data.
        """
        event = Event(event_name, data)
        with self._lock:
            handlers = self._subscribers.get(event_name, [])[:]
            global_handlers = self._global_handlers[:]
        for handler in handlers + global_handlers:
            try:
                handler(event)
            except Exception:
                pass

    def unsubscribe_all(self) -> None:
        """Unsubscribe all handlers."""
        with self._lock:
            self._subscribers.clear()
            self._global_handlers.clear()


_global_event_bus: Optional[EventBus] = None


def get_event_bus() -> EventBus:
    """Get the global event bus instance."""
    global _global_event_bus
    if _global_event_bus is None:
        _global_event_bus = EventBus()
    return _global_event_bus
