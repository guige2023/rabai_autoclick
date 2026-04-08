"""Automation Event Action Module.

Provides event-driven automation with pub/sub messaging,
event routing, and reactive workflow execution.
"""

import time
import hashlib
import asyncio
from typing import Any, Dict, List, Optional, Callable, Set
from dataclasses import dataclass, field
from enum import Enum
from collections import defaultdict
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class EventType(Enum):
    """Event type classification."""
    SYSTEM = "system"
    USER = "user"
    DATA = "data"
    TIMER = "timer"
    CUSTOM = "custom"


class EventPriority(Enum):
    """Event priority level."""
    LOW = "low"
    NORMAL = "normal"
    HIGH = "high"
    CRITICAL = "critical"


@dataclass
class Event:
    """An event in the system."""
    event_id: str
    event_type: EventType
    name: str
    payload: Any
    priority: EventPriority = EventPriority.NORMAL
    timestamp: float = field(default_factory=time.time)
    source: Optional[str] = None
    correlation_id: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class EventSubscription:
    """Subscription to an event or pattern."""
    subscription_id: str
    event_name: str
    handler: Callable
    filter_func: Optional[Callable[[Event], bool]] = None
    priority: EventPriority = EventPriority.NORMAL
    async_handler: bool = False
    enabled: bool = True


@dataclass
class EventRoute:
    """Route for event forwarding."""
    route_id: str
    source_event: str
    target_event: str
    transformer: Optional[Callable] = None
    filter_func: Optional[Callable[[Event], bool]] = None
    enabled: bool = True


@dataclass
class EventMetrics:
    """Event processing metrics."""
    total_events: int = 0
    processed_events: int = 0
    failed_events: int = 0
    events_by_type: Dict[str, int] = field(default_factory=dict)
    events_by_priority: Dict[str, int] = field(default_factory=dict)


class EventBus:
    """Event bus for pub/sub messaging."""

    def __init__(self):
        self._subscriptions: Dict[str, List[EventSubscription]] = defaultdict(list)
        self._routes: Dict[str, List[EventRoute]] = defaultdict(list)
        self._event_history: List[Event] = []
        self._metrics = EventMetrics()
        self._pending_events: List[Event] = []

    def subscribe(
        self,
        event_name: str,
        handler: Callable,
        filter_func: Optional[Callable[[Event], bool]] = None,
        priority: EventPriority = EventPriority.NORMAL
    ) -> str:
        """Subscribe to an event."""
        subscription_id = hashlib.md5(
            f"{event_name}{time.time()}".encode()
        ).hexdigest()[:8]

        subscription = EventSubscription(
            subscription_id=subscription_id,
            event_name=event_name,
            handler=handler,
            filter_func=filter_func,
            priority=priority
        )

        self._subscriptions[event_name].append(subscription)
        self._subscriptions[event_name].sort(
            key=lambda s: s.priority.value,
            reverse=True
        )

        return subscription_id

    def unsubscribe(self, subscription_id: str) -> bool:
        """Unsubscribe from event."""
        for event_name, subs in self._subscriptions.items():
            for i, sub in enumerate(subs):
                if sub.subscription_id == subscription_id:
                    subs.pop(i)
                    return True
        return False

    def publish(self, event: Event) -> List[Any]:
        """Publish an event to all subscribers."""
        self._event_history.append(event)
        self._metrics.total_events += 1

        event_type_key = event.event_type.value
        self._metrics.events_by_type[event_type_key] = \
            self._metrics.events_by_type.get(event_type_key, 0) + 1

        priority_key = event.priority.value
        self._metrics.events_by_priority[priority_key] = \
            self._metrics.events_by_priority.get(priority_key, 0) + 1

        results = []
        subscriptions = self._subscriptions.get(event.name, [])
        subscriptions.extend(self._subscriptions.get("*", []))

        for sub in subscriptions:
            if not sub.enabled:
                continue

            if sub.filter_func and not sub.filter_func(event):
                continue

            try:
                if sub.async_handler:
                    result = asyncio.create_task(
                        sub.handler(event, event.payload)
                    )
                else:
                    result = sub.handler(event, event.payload)
                results.append(result)
                self._metrics.processed_events += 1

            except Exception as e:
                self._metrics.failed_events += 1

        self._process_routes(event)

        if len(self._event_history) > 10000:
            self._event_history = self._event_history[-5000:]

        return results

    def _process_routes(self, event: Event) -> None:
        """Process event routes."""
        routes = self._routes.get(event.name, [])

        for route in routes:
            if not route.enabled:
                continue

            if route.filter_func and not route.filter_func(event):
                continue

            payload = event.payload
            if route.transformer:
                try:
                    payload = route.transformer(event.payload)
                except Exception:
                    continue

            new_event = Event(
                event_id=hashlib.md5(
                    f"{route.target_event}{time.time()}".encode()
                ).hexdigest()[:8],
                event_type=event.event_type,
                name=route.target_event,
                payload=payload,
                priority=event.priority,
                source=event.name,
                correlation_id=event.correlation_id
            )

            self.publish(new_event)

    def add_route(
        self,
        source_event: str,
        target_event: str,
        transformer: Optional[Callable] = None,
        filter_func: Optional[Callable[[Event], bool]] = None
    ) -> str:
        """Add event route."""
        route_id = hashlib.md5(
            f"{source_event}{target_event}{time.time()}".encode()
        ).hexdigest()[:8]

        route = EventRoute(
            route_id=route_id,
            source_event=source_event,
            target_event=target_event,
            transformer=transformer,
            filter_func=filter_func
        )

        self._routes[source_event].append(route)
        return route_id

    def remove_route(self, route_id: str) -> bool:
        """Remove event route."""
        for routes in self._routes.values():
            for i, route in enumerate(routes):
                if route.route_id == route_id:
                    routes.pop(i)
                    return True
        return False

    def get_subscriptions(self, event_name: Optional[str] = None) -> List[Dict]:
        """Get subscriptions."""
        if event_name:
            return [
                {
                    "subscription_id": s.subscription_id,
                    "event_name": s.event_name,
                    "priority": s.priority.value,
                    "enabled": s.enabled
                }
                for s in self._subscriptions.get(event_name, [])
            ]

        return [
            {
                "subscription_id": s.subscription_id,
                "event_name": s.event_name,
                "priority": s.priority.value,
                "enabled": s.enabled
            }
            for subs in self._subscriptions.values()
            for s in subs
        ]

    def get_event_history(
        self,
        event_name: Optional[str] = None,
        limit: int = 100
    ) -> List[Dict]:
        """Get event history."""
        history = self._event_history

        if event_name:
            history = [e for e in history if e.name == event_name]

        history = history[-limit:]

        return [
            {
                "event_id": e.event_id,
                "name": e.name,
                "type": e.event_type.value,
                "priority": e.priority.value,
                "timestamp": e.timestamp,
                "source": e.source
            }
            for e in history
        ]

    def get_metrics(self) -> EventMetrics:
        """Get event metrics."""
        return self._metrics

    def clear_history(self) -> None:
        """Clear event history."""
        self._event_history.clear()


class AutomationEventAction(BaseAction):
    """Action for event-driven automation."""

    def __init__(self):
        super().__init__("automation_event")
        self._event_bus = EventBus()

    def execute(self, params: Dict[str, Any]) -> ActionResult:
        """Execute event automation action."""
        try:
            operation = params.get("operation", "publish")

            if operation == "publish":
                return self._publish_event(params)
            elif operation == "subscribe":
                return self._subscribe(params)
            elif operation == "unsubscribe":
                return self._unsubscribe(params)
            elif operation == "add_route":
                return self._add_route(params)
            elif operation == "remove_route":
                return self._remove_route(params)
            elif operation == "subscriptions":
                return self._get_subscriptions(params)
            elif operation == "history":
                return self._get_history(params)
            elif operation == "metrics":
                return self._get_metrics(params)
            else:
                return ActionResult(
                    success=False,
                    message=f"Unknown operation: {operation}"
                )

        except Exception as e:
            return ActionResult(success=False, message=str(e))

    def _publish_event(self, params: Dict[str, Any]) -> ActionResult:
        """Publish an event."""
        event = Event(
            event_id=hashlib.md5(
                f"{params.get('name', 'event')}{time.time()}".encode()
            ).hexdigest()[:8],
            event_type=EventType(params.get("event_type", "custom")),
            name=params.get("name", "unnamed"),
            payload=params.get("payload"),
            priority=EventPriority(params.get("priority", "normal")),
            source=params.get("source"),
            correlation_id=params.get("correlation_id"),
            metadata=params.get("metadata", {})
        )

        results = self._event_bus.publish(event)

        return ActionResult(
            success=True,
            data={
                "event_id": event.event_id,
                "subscribers_notified": len(results)
            }
        )

    def _subscribe(self, params: Dict[str, Any]) -> ActionResult:
        """Subscribe to an event."""
        event_name = params.get("event_name")
        if not event_name:
            return ActionResult(success=False, message="event_name required")

        def placeholder_handler(event, payload):
            return {"status": "handled"}

        subscription_id = self._event_bus.subscribe(
            event_name=event_name,
            handler=params.get("handler") or placeholder_handler,
            filter_func=params.get("filter_func"),
            priority=EventPriority(params.get("priority", "normal"))
        )

        return ActionResult(
            success=True,
            data={"subscription_id": subscription_id}
        )

    def _unsubscribe(self, params: Dict[str, Any]) -> ActionResult:
        """Unsubscribe from event."""
        subscription_id = params.get("subscription_id")
        if not subscription_id:
            return ActionResult(success=False, message="subscription_id required")

        success = self._event_bus.unsubscribe(subscription_id)

        return ActionResult(
            success=success,
            message="Unsubscribed" if success else "Subscription not found"
        )

    def _add_route(self, params: Dict[str, Any]) -> ActionResult:
        """Add event route."""
        source = params.get("source_event")
        target = params.get("target_event")

        if not source or not target:
            return ActionResult(
                success=False,
                message="source_event and target_event required"
            )

        route_id = self._event_bus.add_route(
            source_event=source,
            target_event=target,
            transformer=params.get("transformer"),
            filter_func=params.get("filter_func")
        )

        return ActionResult(
            success=True,
            data={"route_id": route_id}
        )

    def _remove_route(self, params: Dict[str, Any]) -> ActionResult:
        """Remove event route."""
        route_id = params.get("route_id")
        if not route_id:
            return ActionResult(success=False, message="route_id required")

        success = self._event_bus.remove_route(route_id)

        return ActionResult(
            success=success,
            message="Route removed" if success else "Route not found"
        )

    def _get_subscriptions(self, params: Dict[str, Any]) -> ActionResult:
        """Get event subscriptions."""
        event_name = params.get("event_name")

        subscriptions = self._event_bus.get_subscriptions(event_name)

        return ActionResult(
            success=True,
            data={"subscriptions": subscriptions}
        )

    def _get_history(self, params: Dict[str, Any]) -> ActionResult:
        """Get event history."""
        history = self._event_bus.get_event_history(
            event_name=params.get("event_name"),
            limit=params.get("limit", 100)
        )

        return ActionResult(
            success=True,
            data={"history": history}
        )

    def _get_metrics(self, params: Dict[str, Any]) -> ActionResult:
        """Get event metrics."""
        metrics = self._event_bus.get_metrics()

        return ActionResult(
            success=True,
            data={
                "total_events": metrics.total_events,
                "processed_events": metrics.processed_events,
                "failed_events": metrics.failed_events,
                "events_by_type": metrics.events_by_type,
                "events_by_priority": metrics.events_by_priority
            }
        )
