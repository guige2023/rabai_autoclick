"""API Event Bus Action Module.

Implements an event bus for API events with:
- Publish/subscribe patterns
- Event routing and filtering
- Dead letter queues
- Event replay capability
- Monitoring and metrics

Author: rabai_autoclick team
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum, auto
from typing import Any, Callable, Dict, List, Optional, Set

logger = logging.getLogger(__name__)


class EventPriority(Enum):
    """Event priority levels."""
    LOW = auto()
    NORMAL = auto()
    HIGH = auto()
    CRITICAL = auto()


@dataclass
class Event:
    """Represents an event in the bus."""
    id: str
    type: str
    payload: Dict[str, Any]
    priority: EventPriority = EventPriority.NORMAL
    timestamp: float = field(default_factory=time.time)
    source: Optional[str] = None
    correlation_id: Optional[str] = None
    headers: Dict[str, str] = field(default_factory=dict)
    metadata: Dict[str, Any] = field(default_factory=dict)
    delivery_count: int = 0


@dataclass
class Subscription:
    """Event subscription configuration."""
    id: str
    event_type: str
    handler: Callable[[Event], Any]
    filter_fn: Optional[Callable[[Event], bool]] = None
    priority: EventPriority = EventPriority.NORMAL
    auto_ack: bool = True
    max_retries: int = 3
    dead_letter_queue: Optional[str] = None


@dataclass
class EventMetrics:
    """Event bus metrics."""
    events_published: int = 0
    events_delivered: int = 0
    events_failed: int = 0
    events_dead_lettered: int = 0
    active_subscriptions: int = 0
    queue_depth: Dict[str, int] = field(default_factory=dict)


class APIEventBus:
    """Event bus for asynchronous API event distribution.
    
    Features:
    - Topic-based pub/sub
    - Event filtering
    - Priority queues
    - Dead letter handling
    - Event replay
    - Monitoring
    """
    
    def __init__(self, name: str = "default"):
        self.name = name
        self._subscriptions: Dict[str, List[Subscription]] = defaultdict(list)
        self._queues: Dict[str, asyncio.PriorityQueue] = {}
        self._dead_letter_queues: Dict[str, deque] = defaultdict(lambda: deque(maxlen=1000))
        self._running = False
        self._processors: Dict[str, asyncio.Task] = {}
        self._metrics = EventMetrics()
        self._lock = asyncio.Lock()
        self._event_history: deque = deque(maxlen=10000)
    
    async def publish(
        self,
        event_type: str,
        payload: Dict[str, Any],
        priority: EventPriority = EventPriority.NORMAL,
        source: Optional[str] = None,
        correlation_id: Optional[str] = None,
        headers: Optional[Dict[str, str]] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> Event:
        """Publish an event to the bus.
        
        Args:
            event_type: Type of event
            payload: Event payload
            priority: Event priority
            source: Event source
            correlation_id: Correlation ID for tracing
            headers: Event headers
            metadata: Additional metadata
            
        Returns:
            Published event
        """
        event = Event(
            id=f"evt_{int(time.time() * 1000000)}",
            type=event_type,
            payload=payload,
            priority=priority,
            source=source,
            correlation_id=correlation_id,
            headers=headers or {},
            metadata=metadata or {}
        )
        
        async with self._lock:
            self._event_history.append(event)
        
        self._metrics.events_published += 1
        
        subscriptions = self._subscriptions.get(event_type, [])
        
        for sub in subscriptions:
            if sub.filter_fn and not sub.filter_fn(event):
                continue
            
            await self._enqueue_event(sub, event)
        
        wildcard_subs = self._subscriptions.get("*", [])
        for sub in wildcard_subs:
            await self._enqueue_event(sub, event)
        
        return event
    
    async def subscribe(
        self,
        event_type: str,
        handler: Callable[[Event], Any],
        filter_fn: Optional[Callable[[Event], bool]] = None,
        priority: EventPriority = EventPriority.NORMAL,
        auto_ack: bool = True,
        max_retries: int = 3,
        dead_letter_queue: Optional[str] = None
    ) -> str:
        """Subscribe to an event type.
        
        Args:
            event_type: Event type to subscribe to
            handler: Async handler function
            filter_fn: Optional event filter
            priority: Subscription priority
            auto_ack: Auto acknowledge events
            max_retries: Max delivery retries
            dead_letter_queue: DLQ name
            
        Returns:
            Subscription ID
        """
        sub_id = f"sub_{event_type}_{int(time.time() * 1000000)}"
        
        subscription = Subscription(
            id=sub_id,
            event_type=event_type,
            handler=handler,
            filter_fn=filter_fn,
            priority=priority,
            auto_ack=auto_ack,
            max_retries=max_retries,
            dead_letter_queue=dead_letter_queue
        )
        
        async with self._lock:
            self._subscriptions[event_type].append(subscription)
            self._subscriptions[event_type].sort(
                key=lambda s: s.priority.value, reverse=True
            )
            
            if event_type not in self._queues:
                self._queues[event_type] = asyncio.PriorityQueue()
            
            self._metrics.active_subscriptions = sum(
                len(subs) for subs in self._subscriptions.values()
            )
        
        if event_type not in self._processors:
            self._processors[event_type] = asyncio.create_task(
                self._process_events(event_type)
            )
        
        logger.info(f"Subscribed to event type: {event_type}")
        return sub_id
    
    async def unsubscribe(self, subscription_id: str) -> bool:
        """Unsubscribe from an event type.
        
        Args:
            subscription_id: Subscription ID to remove
            
        Returns:
            True if unsubscribed
        """
        async with self._lock:
            for event_type, subs in self._subscriptions.items():
                for i, sub in enumerate(subs):
                    if sub.id == subscription_id:
                        subs.pop(i)
                        self._metrics.active_subscriptions = sum(
                            len(s) for s in self._subscriptions.values()
                        )
                        return True
        return False
    
    async def _enqueue_event(self, subscription: Subscription, event: Event) -> None:
        """Enqueue event for a subscription.
        
        Args:
            subscription: Target subscription
            event: Event to enqueue
        """
        event_type = subscription.event_type
        queue = self._queues.get(event_type)
        
        if queue:
            priority_value = 10 - subscription.priority.value
            await queue.put((priority_value, event))
            
            self._metrics.queue_depth[event_type] = queue.qsize()
    
    async def _process_events(self, event_type: str) -> None:
        """Process events for an event type.
        
        Args:
            event_type: Event type to process
        """
        queue = self._queues.get(event_type)
        if not queue:
            return
        
        while self._running or not queue.empty():
            try:
                _, event = await asyncio.wait_for(queue.get(), timeout=1.0)
                
                subscriptions = self._subscriptions.get(event_type, [])
                
                for sub in subscriptions:
                    if sub.filter_fn and not sub.filter_fn(event):
                        continue
                    
                    try:
                        if asyncio.iscoroutinefunction(sub.handler):
                            await sub.handler(event)
                        else:
                            sub.handler(event)
                        
                        self._metrics.events_delivered += 1
                        
                    except Exception as e:
                        logger.error(f"Event handler error: {e}")
                        event.delivery_count += 1
                        
                        if event.delivery_count >= sub.max_retries:
                            await self._dead_letter(sub, event, str(e))
                        else:
                            await queue.put((0, event))
                        
                        self._metrics.events_failed += 1
                
                self._metrics.queue_depth[event_type] = queue.qsize()
                
            except asyncio.TimeoutError:
                continue
            except Exception as e:
                logger.error(f"Event processing error: {e}")
    
    async def _dead_letter(self, subscription: Subscription, event: Event, error: str) -> None:
        """Move event to dead letter queue.
        
        Args:
            subscription: Subscription that failed
            event: Failed event
            error: Error message
        """
        dlq_name = subscription.dead_letter_queue or f"dlq_{subscription.event_type}"
        
        dlq = self._dead_letter_queues[dlq_name]
        dlq.append({
            "event": event,
            "error": error,
            "failed_at": datetime.now().isoformat(),
            "subscription_id": subscription.id
        })
        
        self._metrics.events_dead_lettered += 1
        logger.warning(f"Event {event.id} dead-lettered: {error}")
    
    async def start(self) -> None:
        """Start the event bus."""
        self._running = True
        
        for event_type, queue in self._queues.items():
            if event_type not in self._processors:
                self._processors[event_type] = asyncio.create_task(
                    self._process_events(event_type)
                )
        
        logger.info(f"Event bus '{self.name}' started")
    
    async def stop(self) -> None:
        """Stop the event bus."""
        self._running = False
        
        for task in self._processors.values():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        
        self._processors.clear()
        logger.info(f"Event bus '{self.name}' stopped")
    
    async def replay_events(
        self,
        event_type: str,
        from_timestamp: Optional[float] = None,
        to_timestamp: Optional[float] = None,
        limit: int = 1000
    ) -> List[Event]:
        """Replay historical events.
        
        Args:
            event_type: Event type to replay
            from_timestamp: Start timestamp
            to_timestamp: End timestamp
            limit: Max events to replay
            
        Returns:
            List of historical events
        """
        events = []
        
        async with self._lock:
            for event in self._event_history:
                if event.type != event_type:
                    continue
                
                if from_timestamp and event.timestamp < from_timestamp:
                    continue
                
                if to_timestamp and event.timestamp > to_timestamp:
                    continue
                
                events.append(event)
                
                if len(events) >= limit:
                    break
        
        return events
    
    def get_dead_letter_events(
        self,
        queue_name: Optional[str] = None,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """Get events from dead letter queue.
        
        Args:
            queue_name: Specific DLQ name
            limit: Max events to return
            
        Returns:
            List of dead-lettered events
        """
        if queue_name:
            return list(self._dead_letter_queues.get(queue_name, []))[-limit:]
        
        all_dlq = []
        for dlq in self._dead_letter_queues.values():
            all_dlq.extend(dlq)
        
        return all_dlq[-limit:]
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get event bus metrics."""
        return {
            "events_published": self._metrics.events_published,
            "events_delivered": self._metrics.events_delivered,
            "events_failed": self._metrics.events_failed,
            "events_dead_lettered": self._metrics.events_dead_lettered,
            "active_subscriptions": self._metrics.active_subscriptions,
            "queue_depth": dict(self._metrics.queue_depth),
            "history_size": len(self._event_history)
        }


class EventRouter:
    """Routes events to multiple handlers based on rules."""
    
    def __init__(self, event_bus: APIEventBus):
        self._bus = event_bus
        self._routes: Dict[str, str] = {}
        self._transformers: Dict[str, Callable] = {}
    
    def register_route(
        self,
        from_type: str,
        to_type: str,
        transformer: Optional[Callable[[Event], Event]] = None
    ) -> None:
        """Register a routing rule.
        
        Args:
            from_type: Source event type
            to_type: Target event type
            transformer: Optional event transformer
        """
        self._routes[from_type] = to_type
        
        if transformer:
            self._transformers[from_type] = transformer
        
        async def route_handler(event: Event) -> None:
            to_type = self._routes[event.type]
            transformed = event
            
            if event.type in self._transformers:
                transformed = self._transformers[event.type](event)
            
            await self._bus.publish(
                event_type=to_type,
                payload=transformed.payload,
                source=f"route:{event.type}->{to_type}",
                correlation_id=event.correlation_id
            )
        
        asyncio.create_task(
            self._bus.subscribe(from_type, route_handler)
        )
