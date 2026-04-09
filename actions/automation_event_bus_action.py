"""
Automation Event Bus Action Module

Provides an event bus system for automation workflows.
Supports pub/sub messaging, event filtering, and dead letter queues.

Author: rabai_autoclick team
Version: 1.0.0
"""

from __future__ import annotations

import asyncio
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Optional
import threading
import uuid


class EventPriority(Enum):
    """Event priority levels."""
    LOW = "low"
    NORMAL = "normal"
    HIGH = "high"
    CRITICAL = "critical"


@dataclass
class Event:
    """An event in the bus."""
    id: str
    event_type: str
    payload: Any
    priority: EventPriority = EventPriority.NORMAL
    timestamp: datetime = field(default_factory=datetime.now)
    metadata: dict[str, Any] = field(default_factory=dict)
    source: Optional[str] = None
    correlation_id: Optional[str] = None


@dataclass
class Subscription:
    """Event subscription."""
    id: str
    event_type: str
    handler: Callable
    filter_fn: Optional[Callable[[Event], bool]] = None
    priority: EventPriority = EventPriority.NORMAL
    dead_letter_handler: Optional[Callable] = None
    max_retries: int = 3


@dataclass
class EventDeliveryResult:
    """Result of event delivery."""
    event_id: str
    delivered: bool
    subscriber_id: Optional[str] = None
    error: Optional[str] = None
    duration_ms: float = 0.0


class DeadLetterQueue:
    """Dead letter queue for failed event deliveries."""
    
    def __init__(self, max_size: int = 1000):
        self.max_size = max_size
        self._queue: deque[Event] = deque(maxlen=max_size)
        self._stats = {
            "total_failed": 0,
            "total_retry": 0,
            "total_discarded": 0
        }
    
    def add(self, event: Event, error: str) -> None:
        """Add a failed event to the dead letter queue."""
        event.metadata["dlq_error"] = error
        event.metadata["dlq_timestamp"] = datetime.now().isoformat()
        self._queue.append(event)
        self._stats["total_failed"] += 1
    
    def retry(self) -> list[Event]:
        """Get events for retry."""
        events = list(self._queue)
        self._queue.clear()
        self._stats["total_retry"] += len(events)
        return events
    
    def discard_all(self) -> int:
        """Discard all events in the queue."""
        count = len(self._queue)
        self._queue.clear()
        self._stats["total_discarded"] += count
        return count
    
    def get_stats(self) -> dict:
        return self._stats.copy()


class AutomationEventBus:
    """
    Event bus for automation workflows.
    
    Example:
        event_bus = AutomationEventBus()
        
        event_bus.subscribe(
            event_type="task.completed",
            handler=on_task_completed
        )
        
        event_bus.publish(
            event_type="task.completed",
            payload={"task_id": "123", "status": "success"}
        )
    """
    
    def __init__(self):
        self._subscriptions: dict[str, list[Subscription]] = {}
        self._global_subscriptions: list[Subscription] = []
        self._dead_letter_queue = DeadLetterQueue()
        self._event_history: deque[Event] = deque(maxlen=1000)
        self._lock = threading.Lock()
        self._stats = {
            "total_published": 0,
            "total_delivered": 0,
            "total_failed": 0,
            "total_subscribers": 0
        }
    
    def subscribe(
        self,
        event_type: str,
        handler: Callable,
        filter_fn: Optional[Callable[[Event], bool]] = None,
        priority: EventPriority = EventPriority.NORMAL
    ) -> str:
        """
        Subscribe to an event type.
        
        Args:
            event_type: Event type to subscribe to
            handler: Handler function
            filter_fn: Optional filter function
            priority: Subscription priority
            
        Returns:
            Subscription ID
        """
        subscription_id = f"sub_{uuid.uuid4().hex[:8]}"
        
        subscription = Subscription(
            id=subscription_id,
            event_type=event_type,
            handler=handler,
            filter_fn=filter_fn,
            priority=priority
        )
        
        with self._lock:
            if event_type not in self._subscriptions:
                self._subscriptions[event_type] = []
            self._subscriptions[event_type].append(subscription)
            self._subscriptions[event_type].sort(
                key=lambda s: s.priority.value, reverse=True
            )
        
        self._stats["total_subscribers"] += 1
        return subscription_id
    
    def subscribe_all(
        self,
        handler: Callable,
        filter_fn: Optional[Callable[[Event], bool]] = None
    ) -> str:
        """Subscribe to all events."""
        subscription_id = f"sub_all_{uuid.uuid4().hex[:8]}"
        
        subscription = Subscription(
            id=subscription_id,
            event_type="*",
            handler=handler,
            filter_fn=filter_fn
        )
        
        with self._lock:
            self._global_subscriptions.append(subscription)
        
        self._stats["total_subscribers"] += 1
        return subscription_id
    
    def unsubscribe(self, subscription_id: str) -> bool:
        """Unsubscribe from events."""
        with self._lock:
            for event_type, subs in self._subscriptions.items():
                for i, sub in enumerate(subs):
                    if sub.id == subscription_id:
                        subs.pop(i)
                        return True
            
            for i, sub in enumerate(self._global_subscriptions):
                if sub.id == subscription_id:
                    self._global_subscriptions.pop(i)
                    return True
        
        return False
    
    async def publish(
        self,
        event_type: str,
        payload: Any,
        priority: EventPriority = EventPriority.NORMAL,
        source: Optional[str] = None,
        correlation_id: Optional[str] = None,
        metadata: Optional[dict[str, Any]] = None
    ) -> list[EventDeliveryResult]:
        """
        Publish an event.
        
        Args:
            event_type: Type of event
            payload: Event payload
            priority: Event priority
            source: Event source
            correlation_id: Correlation ID for tracing
            metadata: Additional metadata
            
        Returns:
            List of delivery results
        """
        event = Event(
            id=f"evt_{uuid.uuid4().hex[:12]}",
            event_type=event_type,
            payload=payload,
            priority=priority,
            source=source,
            correlation_id=correlation_id,
            metadata=metadata or {}
        )
        
        self._event_history.append(event)
        self._stats["total_published"] += 1
        
        results = []
        
        results.extend(await self._deliver_to_subscribers(event))
        results.extend(await self._deliver_to_global_subscribers(event))
        
        return results
    
    async def _deliver_to_subscribers(self, event: Event) -> list[EventDeliveryResult]:
        """Deliver event to type-specific subscribers."""
        results = []
        
        subscribers = self._subscriptions.get(event.event_type, [])
        
        for sub in subscribers:
            result = await self._deliver_to_subscriber(event, sub)
            results.append(result)
        
        return results
    
    async def _deliver_to_global_subscribers(self, event: Event) -> list[EventDeliveryResult]:
        """Deliver event to global subscribers."""
        results = []
        
        for sub in self._global_subscriptions:
            if sub.filter_fn is None or sub.filter_fn(event):
                result = await self._deliver_to_subscriber(event, sub)
                results.append(result)
        
        return results
    
    async def _deliver_to_subscriber(self, event: Event, sub: Subscription) -> EventDeliveryResult:
        """Deliver event to a single subscriber."""
        start_time = datetime.now()
        
        try:
            if sub.filter_fn and not sub.filter_fn(event):
                return EventDeliveryResult(
                    event_id=event.id,
                    delivered=False,
                    subscriber_id=sub.id,
                    error="Filter rejected event",
                    duration_ms=0.0
                )
            
            handler = sub.handler
            if asyncio.iscoroutinefunction(handler):
                await handler(event)
            else:
                handler(event)
            
            self._stats["total_delivered"] += 1
            duration_ms = (datetime.now() - start_time).total_seconds() * 1000
            
            return EventDeliveryResult(
                event_id=event.id,
                delivered=True,
                subscriber_id=sub.id,
                duration_ms=duration_ms
            )
        
        except Exception as e:
            self._stats["total_failed"] += 1
            duration_ms = (datetime.now() - start_time).total_seconds() * 1000
            
            self._dead_letter_queue.add(event, str(e))
            
            return EventDeliveryResult(
                event_id=event.id,
                delivered=False,
                subscriber_id=sub.id,
                error=str(e),
                duration_ms=duration_ms
            )
    
    def get_event_history(self, limit: int = 100) -> list[Event]:
        """Get recent events."""
        return list(self._event_history)[-limit:]
    
    def get_dead_letter_events(self) -> list[Event]:
        """Get events from dead letter queue."""
        return list(self._dead_letter_queue._queue)
    
    def retry_dead_letters(self) -> int:
        """Retry dead letter events."""
        events = self._dead_letter_queue.retry()
        return len(events)
    
    def get_stats(self) -> dict[str, Any]:
        """Get event bus statistics."""
        return {
            **self._stats,
            "total_subscriptions": self._stats["total_subscribers"],
            "event_types": len(self._subscriptions),
            "delivery_rate": (
                self._stats["total_delivered"] / max(1, self._stats["total_published"])
            ),
            "dlq_stats": self._dead_letter_queue.get_stats()
        }
