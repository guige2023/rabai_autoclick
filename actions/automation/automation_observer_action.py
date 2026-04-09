"""Observer pattern implementation for automation event handling.

Provides a publish-subscribe event system for decoupling
automation components and enabling reactive workflows.
"""

from __future__ import annotations

import threading
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set

import copy


class EventPriority(Enum):
    """Priority levels for event handlers."""
    LOW = 0
    NORMAL = 50
    HIGH = 100


@dataclass
class Event:
    """An event in the system."""
    event_id: str
    event_type: str
    payload: Dict[str, Any]
    source: Optional[str] = None
    timestamp: float = field(default_factory=time.time)
    correlation_id: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class EventHandler:
    """A registered event handler."""
    handler_id: str
    event_type: str
    callback: Callable[..., Any]
    priority: EventPriority = EventPriority.NORMAL
    name: Optional[str] = None
    enabled: bool = True
    once: bool = False
    filter_fn: Optional[Callable[[Event], bool]] = None
    created_at: float = field(default_factory=time.time)
    call_count: int = 0
    error_count: int = 0
    last_error: Optional[str] = None
    tags: Set[str] = field(default_factory=set)


@dataclass
class Subscription:
    """A subscription to events."""
    subscription_id: str
    handler_ids: List[str]
    event_types: List[str]
    created_at: float = field(default_factory=time.time)
    active: bool = True


class EventBus:
    """Central event bus for publish-subscribe messaging."""

    def __init__(self):
        self._handlers: Dict[str, List[EventHandler]] = {}
        self._subscriptions: Dict[str, Subscription] = {}
        self._lock = threading.RLock()
        self._event_history: List[Event] = []
        self._max_history = 1000
        self._wildcard_handlers: List[EventHandler] = []

    def subscribe(
        self,
        event_types: List[str],
        handler: Callable[..., Any],
        priority: EventPriority = EventPriority.NORMAL,
        name: Optional[str] = None,
        once: bool = False,
        filter_fn: Optional[Callable[[Event], bool]] = None,
        tags: Optional[Set[str]] = None,
    ) -> str:
        """Subscribe to one or more event types."""
        handler_id = str(uuid.uuid4())[:12]

        event_handler = EventHandler(
            handler_id=handler_id,
            event_type=",".join(event_types),
            callback=handler,
            priority=priority,
            name=name,
            once=once,
            filter_fn=filter_fn,
            tags=tags or set(),
        )

        with self._lock:
            for event_type in event_types:
                if event_type == "*":
                    self._wildcard_handlers.append(event_handler)
                else:
                    if event_type not in self._handlers:
                        self._handlers[event_type] = []
                    self._handlers[event_type].append(event_handler)
                    self._handlers[event_type].sort(
                        key=lambda h: h.priority.value, reverse=True
                    )

        return handler_id

    def unsubscribe(self, handler_id: str) -> bool:
        """Unsubscribe a handler by ID."""
        with self._lock:
            for handlers in self._handlers.values():
                for i, h in enumerate(handlers):
                    if h.handler_id == handler_id:
                        handlers.pop(i)
                        return True

            for i, h in enumerate(self._wildcard_handlers):
                if h.handler_id == handler_id:
                    self._wildcard_handlers.pop(i)
                    return True

        return False

    def publish(
        self,
        event_type: str,
        payload: Dict[str, Any],
        source: Optional[str] = None,
        correlation_id: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> List[Any]:
        """Publish an event to all matching handlers."""
        event_id = str(uuid.uuid4())[:12]

        event = Event(
            event_id=event_id,
            event_type=event_type,
            payload=payload,
            source=source,
            correlation_id=correlation_id,
            metadata=metadata or {},
        )

        with self._lock:
            self._event_history.append(event)
            if len(self._event_history) > self._max_history:
                self._event_history = self._event_history[-self._max_history:]

        results = []

        handlers_to_call = []

        with self._lock:
            handlers_to_call.extend(self._handlers.get(event_type, []))
            handlers_to_call.extend(self._wildcard_handlers)

        handlers_to_call.sort(key=lambda h: h.priority.value, reverse=True)

        for handler in handlers_to_call:
            if not handler.enabled:
                continue

            if handler.filter_fn and not handler.filter_fn(event):
                continue

            try:
                result = handler.callback(event)
                handler.call_count += 1
                results.append(result)

                if handler.once:
                    self.unsubscribe(handler.handler_id)

            except Exception as e:
                handler.error_count += 1
                handler.last_error = str(e)
                results.append({"error": str(e), "handler_id": handler.handler_id})

        return results

    def create_subscription(
        self,
        event_types: List[str],
        handlers: List[Callable[..., Any]],
    ) -> str:
        """Create a subscription group for multiple handlers."""
        subscription_id = str(uuid.uuid4())[:12]
        handler_ids = []

        for handler in handlers:
            handler_id = self.subscribe(event_types, handler)
            handler_ids.append(handler_id)

        subscription = Subscription(
            subscription_id=subscription_id,
            handler_ids=handler_ids,
            event_types=event_types,
        )

        with self._lock:
            self._subscriptions[subscription_id] = subscription

        return subscription_id

    def cancel_subscription(self, subscription_id: str) -> bool:
        """Cancel a subscription and remove all its handlers."""
        with self._lock:
            subscription = self._subscriptions.get(subscription_id)
            if not subscription:
                return False

            for handler_id in subscription.handler_ids:
                self.unsubscribe(handler_id)

            subscription.active = False
            return True

    def get_history(
        self,
        event_type: Optional[str] = None,
        limit: int = 100,
    ) -> List[Dict[str, Any]]:
        """Get event history."""
        with self._lock:
            history = list(reversed(self._event_history))
            if event_type:
                history = [e for e in history if e.event_type == event_type]
            return [
                {
                    "event_id": e.event_id,
                    "event_type": e.event_type,
                    "payload": e.payload,
                    "source": e.source,
                    "timestamp": datetime.fromtimestamp(e.timestamp).isoformat(),
                    "correlation_id": e.correlation_id,
                }
                for e in history[:limit]
            ]

    def get_handlers(self, event_type: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get registered handlers."""
        with self._lock:
            if event_type:
                handlers = self._handlers.get(event_type, [])
            else:
                handlers = []
                for h_list in self._handlers.values():
                    handlers.extend(h_list)
                handlers.extend(self._wildcard_handlers)

            return [
                {
                    "handler_id": h.handler_id,
                    "event_type": h.event_type,
                    "name": h.name,
                    "priority": h.priority.name,
                    "enabled": h.enabled,
                    "once": h.once,
                    "call_count": h.call_count,
                    "error_count": h.error_count,
                    "last_error": h.last_error,
                    "tags": list(h.tags),
                }
                for h in handlers
            ]


class AutomationObserverAction:
    """Action providing observer/event handling for automation workflows."""

    def __init__(self, event_bus: Optional[EventBus] = None):
        self._event_bus = event_bus or EventBus()

    def on(
        self,
        event_type: str,
        handler: Callable[..., Any],
        priority: str = "normal",
        name: Optional[str] = None,
        once: bool = False,
    ) -> str:
        """Register an event handler."""
        try:
            priority_enum = EventPriority[priority.upper()]
        except ValueError:
            priority_enum = EventPriority.NORMAL

        return self._event_bus.subscribe(
            event_types=[event_type],
            handler=handler,
            priority=priority_enum,
            name=name,
            once=once,
        )

    def once(
        self,
        event_type: str,
        handler: Callable[..., Any],
        name: Optional[str] = None,
    ) -> str:
        """Register a one-time event handler."""
        return self.on(event_type, handler, name=name, once=True)

    def emit(
        self,
        event_type: str,
        payload: Dict[str, Any],
        source: Optional[str] = None,
        correlation_id: Optional[str] = None,
    ) -> List[Any]:
        """Emit an event to all registered handlers."""
        return self._event_bus.publish(
            event_type=event_type,
            payload=payload,
            source=source,
            correlation_id=correlation_id,
        )

    def execute(
        self,
        context: Dict[str, Any],
        params: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Execute an observer/event operation.

        Required params:
            operation: str - 'subscribe', 'unsubscribe', 'emit', or 'create_subscription'
            event_type: str - For subscribe/emit operations

        For subscribe operation:
            handler: callable - Handler function
            priority: str - Handler priority
            once: bool - Whether handler fires once only

        For emit operation:
            payload: dict - Event payload

        For create_subscription operation:
            handlers: list - List of handler functions
        """
        operation = params.get("operation")
        event_type = params.get("event_type")
        handler = params.get("handler")
        priority = params.get("priority", "normal")
        once = params.get("once", False)
        payload = params.get("payload", {})
        handlers = params.get("handlers", [])

        if operation == "subscribe":
            if not event_type:
                raise ValueError("event_type is required for subscribe")
            if not callable(handler):
                raise ValueError("handler must be a callable")
            handler_id = self.on(event_type, handler, priority, once=once)
            return {"handler_id": handler_id, "event_type": event_type}

        elif operation == "unsubscribe":
            if not event_type and not handler:
                raise ValueError("handler_id or event_type required for unsubscribe")
            if handler:
                success = self._event_bus.unsubscribe(handler)
            else:
                handlers_list = self._event_bus.get_handlers(event_type)
                for h in handlers_list:
                    self._event_bus.unsubscribe(h["handler_id"])
                success = True
            return {"success": success}

        elif operation == "emit":
            if not event_type:
                raise ValueError("event_type is required for emit")
            results = self.emit(event_type, payload)
            return {
                "event_type": event_type,
                "handlers_called": len(results),
                "results": results,
            }

        elif operation == "create_subscription":
            if not event_type:
                raise ValueError("event_type is required")
            if not handlers:
                raise ValueError("handlers list is required")
            subscription_id = self._event_bus.create_subscription(
                [event_type], handlers
            )
            return {"subscription_id": subscription_id}

        elif operation == "cancel_subscription":
            subscription_id = params.get("subscription_id")
            if not subscription_id:
                raise ValueError("subscription_id is required")
            success = self._event_bus.cancel_subscription(subscription_id)
            return {"success": success}

        else:
            raise ValueError(f"Unknown operation: {operation}")

    def get_history(
        self,
        event_type: Optional[str] = None,
        limit: int = 100,
    ) -> List[Dict[str, Any]]:
        """Get event history."""
        return self._event_bus.get_history(event_type, limit)

    def get_handlers(self, event_type: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get registered handlers."""
        return self._event_bus.get_handlers(event_type)
