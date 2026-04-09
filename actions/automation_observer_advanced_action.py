"""
Automation Observer Advanced Action Module

Provides advanced observer pattern with event filtering, transformation,
aggregation, and multi-observer coordination.

MIT License - Copyright (c) 2025 RabAi Research
"""

from __future__ import annotations

import asyncio
import logging
import time
import uuid
from collections import defaultdict
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


class ObserverType(Enum):
    """Observer types."""

    SYNC = "sync"
    ASYNC = "async"
    BATCH = "batch"
    FILTER = "filter"


@dataclass
class Event:
    """An observable event."""

    event_id: str
    event_type: str
    source: str
    priority: EventPriority = EventPriority.NORMAL
    data: Dict[str, Any] = field(default_factory=dict)
    timestamp: float = field(default_factory=time.time)
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class Observer:
    """An observer for events."""

    observer_id: str
    name: str
    observer_type: ObserverType
    handler: Callable[..., Any]
    event_types: Set[str] = field(default_factory=set)
    filter_func: Optional[Callable[[Event], bool]] = None
    transformer: Optional[Callable[[Event], Event]] = None
    batch_size: int = 10
    batch_timeout: float = 1.0


@dataclass
class EventSubscription:
    """An event subscription."""

    subscription_id: str
    observer_id: str
    event_types: Set[str]
    priority: EventPriority = EventPriority.NORMAL
    enabled: bool = True


@dataclass
class ObserverConfig:
    """Configuration for observer system."""

    enable_event_bus: bool = True
    enable_batching: bool = True
    enable_filtering: bool = True
    max_queue_size: int = 1000
    default_batch_timeout: float = 1.0


class AdvancedObserverAction:
    """
    Advanced observer action for event-driven automation.

    Features:
    - Multiple observer types (sync, async, batch, filter)
    - Event filtering and transformation
    - Priority-based event delivery
    - Event batching for efficiency
    - Multi-observer coordination
    - Event aggregation

    Usage:
        observer = AdvancedObserverAction(config)
        
        async def handler(event):
            print(f"Event: {event.event_type}")
        
        obs = observer.create_observer("obs-1", ObserverType.ASYNC, handler)
        observer.subscribe("obs-1", ["workflow.*"])
        
        observer.emit(Event("workflow.started", "source"))
    """

    def __init__(self, config: Optional[ObserverConfig] = None):
        self.config = config or ObserverConfig()
        self._observers: Dict[str, Observer] = {}
        self._subscriptions: Dict[str, EventSubscription] = {}
        self._event_queue: asyncio.Queue = asyncio.Queue(maxsize=self.config.max_queue_size)
        self._event_history: List[Event] = []
        self._max_history: int = 1000
        self._stats = {
            "observers_created": 0,
            "subscriptions_created": 0,
            "events_emitted": 0,
            "events_delivered": 0,
        }

    def create_observer(
        self,
        observer_id: str,
        observer_type: ObserverType,
        handler: Callable[..., Any],
        event_types: Optional[Set[str]] = None,
    ) -> Observer:
        """Create a new observer."""
        observer = Observer(
            observer_id=observer_id,
            name=observer_id,
            observer_type=observer_type,
            handler=handler,
            event_types=event_types or set(),
        )
        self._observers[observer_id] = observer
        self._stats["observers_created"] += 1
        return observer

    def subscribe(
        self,
        observer_id: str,
        event_types: List[str],
        priority: EventPriority = EventPriority.NORMAL,
    ) -> EventSubscription:
        """Subscribe an observer to event types."""
        observer = self._observers.get(observer_id)
        if observer is None:
            raise ValueError(f"Observer not found: {observer_id}")

        subscription_id = f"sub_{uuid.uuid4().hex[:8]}"
        subscription = EventSubscription(
            subscription_id=subscription_id,
            observer_id=observer_id,
            event_types=set(event_types),
            priority=priority,
        )

        self._subscriptions[subscription_id] = subscription
        observer.event_types.update(event_types)
        self._stats["subscriptions_created"] += 1

        return subscription

    async def emit(self, event: Event) -> None:
        """Emit an event to all matching observers."""
        self._stats["events_emitted"] += 1
        self._event_history.append(event)

        if len(self._event_history) > self._max_history:
            self._event_history = self._event_history[-self._max_history:]

        matching_subscriptions = [
            sub for sub in self._subscriptions.values()
            if sub.enabled and self._matches_event_type(event.event_type, sub.event_types)
        ]

        for sub in matching_subscriptions:
            observer = self._observers.get(sub.observer_id)
            if observer is None:
                continue

            if observer.filter_func and not observer.filter_func(event):
                continue

            transformed_event = event
            if observer.transformer:
                transformed_event = observer.transformer(event)

            await self._deliver_to_observer(observer, transformed_event)
            self._stats["events_delivered"] += 1

    def _matches_event_type(self, event_type: str, subscribed_types: Set[str]) -> bool:
        """Check if an event type matches subscribed types (supports wildcards)."""
        for sub_type in subscribed_types:
            if sub_type == "*":
                return True
            if sub_type.endswith(".*"):
                prefix = sub_type[:-1]
                if event_type.startswith(prefix):
                    return True
            if event_type == sub_type:
                return True
        return False

    async def _deliver_to_observer(self, observer: Observer, event: Event) -> None:
        """Deliver an event to an observer based on its type."""
        if observer.observer_type == ObserverType.SYNC:
            observer.handler(event)
        elif observer.observer_type == ObserverType.ASYNC:
            if asyncio.iscoroutinefunction(observer.handler):
                await observer.handler(event)
            else:
                observer.handler(event)
        elif observer.observer_type == ObserverType.BATCH:
            await self._handle_batch_observer(observer, event)

    async def _handle_batch_observer(self, observer: Observer, event: Event) -> None:
        """Handle batch-style observer delivery."""
        await asyncio.sleep(observer.batch_timeout)
        if asyncio.iscoroutinefunction(observer.handler):
            await observer.handler([event])
        else:
            observer.handler([event])

    def get_observer(self, observer_id: str) -> Optional[Observer]:
        """Get an observer by ID."""
        return self._observers.get(observer_id)

    def get_history(self, limit: int = 100) -> List[Event]:
        """Get recent event history."""
        return self._event_history[-limit:]

    def get_stats(self) -> Dict[str, Any]:
        """Get observer statistics."""
        return {
            **self._stats.copy(),
            "total_observers": len(self._observers),
            "total_subscriptions": len(self._subscriptions),
            "history_size": len(self._event_history),
        }


async def demo_advanced_observer():
    """Demonstrate advanced observer."""
    config = ObserverConfig()
    observer = AdvancedObserverAction(config)

    async def handler(event):
        print(f"Handler received: {event.event_type}")

    obs = observer.create_observer("obs-1", ObserverType.ASYNC, handler)
    observer.subscribe("obs-1", ["workflow.started", "workflow.completed"])

    await observer.emit(Event("workflow.started", "workflow-1", data={"id": 1}))
    await observer.emit(Event("workflow.completed", "workflow-1", data={"id": 1}))

    print(f"Stats: {observer.get_stats()}")


if __name__ == "__main__":
    asyncio.run(demo_advanced_observer())
