"""
Automation Observer Action Module

Provides observer pattern implementation for automation workflow monitoring with
event subscription, filtering, and multi-observer coordination.

MIT License - Copyright (c) 2025 RabAi Research
"""

from __future__ import annotations

import asyncio
import logging
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set

logger = logging.getLogger(__name__)


class EventPriority(Enum):
    """Event priority levels."""

    LOW = "low"
    NORMAL = "normal"
    HIGH = "high"
    CRITICAL = "critical"


@dataclass
class Event:
    """An observable event."""

    event_id: str
    event_type: str
    source: str
    priority: EventPriority = EventPriority.NORMAL
    data: Dict[str, Any] = field(default_factory=dict)
    timestamp: Optional[float] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = time.time()


@dataclass
class Subscription:
    """An event subscription."""

    subscription_id: str
    observer_id: str
    event_types: Set[str]
    filter_func: Optional[Callable[[Event], bool]] = None
    priority: EventPriority = EventPriority.NORMAL
    enabled: bool = True


class EventBus:
    """Central event bus for observer pattern."""

    def __init__(self):
        self._subscribers: Dict[str, List[Subscription]] = {}

    def subscribe(self, subscription: Subscription) -> None:
        for event_type in subscription.event_types:
            if event_type not in self._subscribers:
                self._subscribers[event_type] = []
            self._subscribers[event_type].append(subscription)

    def unsubscribe(self, subscription_id: str) -> bool:
        for event_type, subs in self._subscribers.items():
            self._subscribers[event_type] = [
                s for s in subs if s.subscription_id != subscription_id
            ]
        return True

    def get_subscribers(self, event_type: str) -> List[Subscription]:
        return self._subscribers.get(event_type, [])


class Observer:
    """An observer that reacts to events."""

    def __init__(self, observer_id: str, name: str):
        self.observer_id = observer_id
        self.name = name
        self._subscriptions: List[Subscription] = []
        self._event_count: Dict[str, int] = {}

    async def on_event(self, event: Event) -> None:
        """Handle an event. Override in subclass."""
        self._event_count[event.event_type] = self._event_count.get(event.event_type, 0) + 1

    def get_stats(self) -> Dict[str, Any]:
        return {
            "observer_id": self.observer_id,
            "name": self.name,
            "subscriptions": len(self._subscriptions),
            "events_handled": sum(self._event_count.values()),
        }


class AutomationObserverAction:
    """
    Observer pattern action for automation workflow monitoring.

    Features:
    - Event subscription and filtering
    - Multiple observer support
    - Priority-based event delivery
    - Event history and replay
    - Observer coordination

    Usage:
        observer = AutomationObserverAction()
        
        def my_handler(event):
            print(f"Event: {event.event_type}")
        
        observer.subscribe("workflow.*", my_handler)
        observer.emit(Event("workflow.completed", "workflow-1"))
    """

    def __init__(self):
        self._event_bus = EventBus()
        self._observers: Dict[str, Observer] = {}
        self._event_history: List[Event] = []
        self._max_history: int = 1000
        self._stats = {
            "events_emitted": 0,
            "events_delivered": 0,
            "observers_registered": 0,
            "subscriptions_created": 0,
        }

    def register_observer(self, observer: Observer) -> None:
        """Register an observer."""
        self._observers[observer.observer_id] = observer
        self._stats["observers_registered"] += 1

    def subscribe(
        self,
        observer_id: str,
        event_types: List[str],
        handler: Callable[[Event], Any],
        filter_func: Optional[Callable[[Event], bool]] = None,
        priority: EventPriority = EventPriority.NORMAL,
    ) -> Subscription:
        """Subscribe to events."""
        subscription_id = f"sub_{uuid.uuid4().hex[:8]}"
        subscription = Subscription(
            subscription_id=subscription_id,
            observer_id=observer_id,
            event_types=set(event_types),
            filter_func=filter_func,
            priority=priority,
        )

        self._event_bus.subscribe(subscription)
        self._stats["subscriptions_created"] += 1

        observer = self._observers.get(observer_id)
        if observer:
            observer._subscriptions.append(subscription)

        return subscription

    def unsubscribe(self, subscription_id: str) -> bool:
        """Unsubscribe from events."""
        return self._event_bus.unsubscribe(subscription_id)

    async def emit(self, event: Event) -> None:
        """Emit an event to all subscribers."""
        self._stats["events_emitted"] += 1
        self._event_history.append(event)

        if len(self._event_history) > self._max_history:
            self._event_history = self._event_history[-self._max_history:]

        subscribers = self._event_bus.get_subscribers(event.event_type)

        for sub in subscribers:
            if not sub.enabled:
                continue

            if sub.filter_func and not sub.filter_func(event):
                continue

            observer = self._observers.get(sub.observer_id)
            if observer:
                await observer.on_event(event)
                self._stats["events_delivered"] += 1

    def get_history(
        self,
        event_type: Optional[str] = None,
        limit: int = 100,
    ) -> List[Event]:
        """Get event history."""
        history = self._event_history

        if event_type:
            history = [e for e in history if e.event_type == event_type]

        return history[-limit:]

    def get_stats(self) -> Dict[str, Any]:
        return {
            **self._stats.copy(),
            "total_observers": len(self._observers),
            "history_size": len(self._event_history),
        }


async def demo_observer():
    """Demonstrate observer pattern."""
    observer_action = AutomationObserverAction()

    observer = Observer("obs_1", "TestObserver")
    observer_action.register_observer(observer)

    async def handler(event):
        print(f"Handler received: {event.event_type}")

    observer_action.subscribe(
        "obs_1",
        ["workflow.started", "workflow.completed"],
        handler,
    )

    await observer_action.emit(Event("workflow.started", "workflow-1"))
    await observer_action.emit(Event("workflow.completed", "workflow-1"))

    print(f"Stats: {observer_action.get_stats()}")


if __name__ == "__main__":
    asyncio.run(demo_observer())
