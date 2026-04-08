"""Event bus action module for RabAI AutoClick.

Provides event bus utilities:
- EventBus: Pub/sub event bus
- Event: Event object
- EventHandler: Handle events
"""

from typing import Any, Callable, Dict, List, Optional
import threading
import time
import uuid

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class Event:
    """Event object."""

    def __init__(
        self,
        event_type: str,
        data: Optional[Dict[str, Any]] = None,
        event_id: Optional[str] = None,
    ):
        self.event_id = event_id or str(uuid.uuid4())
        self.event_type = event_type
        self.data = data or {}
        self.timestamp = time.time()


class EventHandler:
    """Event handler callback."""

    def __init__(self, handler: Callable, event_type: str, subscriber_id: str):
        self.handler = handler
        self.event_type = event_type
        self.subscriber_id = subscriber_id
        self.filter_func: Optional[Callable] = None

    def set_filter(self, filter_func: Callable) -> None:
        """Set filter function."""
        self.filter_func = filter_func

    def can_handle(self, event: Event) -> bool:
        """Check if handler can handle event."""
        if self.filter_func:
            return self.filter_func(event)
        return True

    def handle(self, event: Event) -> Any:
        """Handle event."""
        if self.can_handle(event):
            return self.handler(event)
        return None


class EventBus:
    """Thread-safe event bus."""

    def __init__(self):
        self._handlers: Dict[str, List[EventHandler]] = {}
        self._global_handlers: List[EventHandler] = []
        self._lock = threading.RLock()
        self._event_history: List[Event] = []
        self._max_history = 1000

    def subscribe(self, event_type: str, handler: Callable, filter_func: Optional[Callable] = None) -> str:
        """Subscribe to events."""
        subscriber_id = str(uuid.uuid4())

        with self._lock:
            event_handler = EventHandler(handler, event_type, subscriber_id)
            if filter_func:
                event_handler.set_filter(filter_func)

            if event_type not in self._handlers:
                self._handlers[event_type] = []
            self._handlers[event_type].append(event_handler)

        return subscriber_id

    def subscribe_global(self, handler: Callable) -> str:
        """Subscribe to all events."""
        subscriber_id = str(uuid.uuid4())

        with self._lock:
            event_handler = EventHandler(handler, "*", subscriber_id)
            self._global_handlers.append(event_handler)

        return subscriber_id

    def unsubscribe(self, subscriber_id: str) -> bool:
        """Unsubscribe from events."""
        with self._lock:
            for event_type, handlers in self._handlers.items():
                for i, handler in enumerate(handlers):
                    if handler.subscriber_id == subscriber_id:
                        self._handlers[event_type].pop(i)
                        return True

            for i, handler in enumerate(self._global_handlers):
                if handler.subscriber_id == subscriber_id:
                    self._global_handlers.pop(i)
                    return True

        return False

    def publish(self, event: Event) -> List[Any]:
        """Publish an event."""
        with self._lock:
            results = []

            handlers = self._handlers.get(event.event_type, []) + self._global_handlers

            for handler in handlers:
                try:
                    result = handler.handle(event)
                    if result is not None:
                        results.append(result)
                except Exception:
                    pass

            self._event_history.append(event)
            if len(self._event_history) > self._max_history:
                self._event_history = self._event_history[-self._max_history:]

        return results

    def emit(self, event_type: str, data: Optional[Dict[str, Any]] = None) -> List[Any]:
        """Emit an event."""
        event = Event(event_type, data)
        return self.publish(event)

    def get_history(self, event_type: Optional[str] = None, limit: int = 100) -> List[Event]:
        """Get event history."""
        with self._lock:
            history = self._event_history
            if event_type:
                history = [e for e in history if e.event_type == event_type]
            return history[-limit:]

    def get_subscribers(self, event_type: Optional[str] = None) -> List[str]:
        """Get subscriber IDs."""
        with self._lock:
            if event_type:
                return [h.subscriber_id for h in self._handlers.get(event_type, [])]
            all_subs = [h.subscriber_id for h in self._global_handlers]
            for handlers in self._handlers.values():
                all_subs.extend([h.subscriber_id for h in handlers])
            return all_subs


class EventBusAction(BaseAction):
    """Event bus management action."""
    action_type = "eventbus"
    display_name = "事件总线"
    description = "发布订阅"

    def __init__(self):
        super().__init__()
        self._bus = EventBus()

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "publish")

            if operation == "publish":
                return self._publish(params)
            elif operation == "subscribe":
                return self._subscribe(params)
            elif operation == "unsubscribe":
                return self._unsubscribe(params)
            elif operation == "history":
                return self._history(params)
            elif operation == "subscribers":
                return self._subscribers(params)
            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"EventBus error: {str(e)}")

    def _publish(self, params: Dict[str, Any]) -> ActionResult:
        """Publish an event."""
        event_type = params.get("event_type")
        data = params.get("data", {})

        if not event_type:
            return ActionResult(success=False, message="event_type is required")

        results = self._bus.emit(event_type, data)

        return ActionResult(success=True, message=f"Published: {event_type}", data={"event_type": event_type, "handlers_called": len(results)})

    def _subscribe(self, params: Dict[str, Any]) -> ActionResult:
        """Subscribe to events."""
        event_type = params.get("event_type", "*")

        def handler(event):
            return {"received": True}

        subscriber_id = self._bus.subscribe(event_type, handler)

        return ActionResult(success=True, message=f"Subscribed: {subscriber_id}", data={"subscriber_id": subscriber_id})

    def _unsubscribe(self, params: Dict[str, Any]) -> ActionResult:
        """Unsubscribe from events."""
        subscriber_id = params.get("subscriber_id")

        if not subscriber_id:
            return ActionResult(success=False, message="subscriber_id is required")

        success = self._bus.unsubscribe(subscriber_id)

        return ActionResult(success=success, message="Unsubscribed" if success else "Subscriber not found")

    def _history(self, params: Dict[str, Any]) -> ActionResult:
        """Get event history."""
        event_type = params.get("event_type")
        limit = params.get("limit", 100)

        history = self._bus.get_history(event_type, limit)

        events = [
            {
                "event_id": e.event_id,
                "event_type": e.event_type,
                "data": e.data,
                "timestamp": e.timestamp,
            }
            for e in history
        ]

        return ActionResult(success=True, message=f"{len(events)} events", data={"events": events})

    def _subscribers(self, params: Dict[str, Any]) -> ActionResult:
        """Get subscribers."""
        event_type = params.get("event_type")

        subscribers = self._bus.get_subscribers(event_type)

        return ActionResult(success=True, message=f"{len(subscribers)} subscribers", data={"subscribers": subscribers})
