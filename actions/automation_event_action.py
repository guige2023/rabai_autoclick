"""
Automation Event Action - Event-driven automation with pub/sub messaging.

This module provides an event system for automation workflows, supporting
event publishing, subscription, filtering, and propagation.
"""

from __future__ import annotations

import asyncio
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Callable, Awaitable
from enum import Enum
from collections import defaultdict
import re


class EventPriority(Enum):
    """Priority levels for event handlers."""
    LOW = 0
    NORMAL = 1
    HIGH = 2
    CRITICAL = 3


@dataclass
class Event:
    """Base event class for all automation events."""
    event_type: str
    data: dict[str, Any]
    timestamp: float = field(default_factory=time.time)
    event_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    source: str = "system"
    correlation_id: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class EventSubscription:
    """Subscription to events matching a pattern."""
    subscription_id: str
    pattern: str
    handler: Callable[[Event], Awaitable[Any]]
    priority: EventPriority = EventPriority.NORMAL
    filter_func: Callable[[Event], bool] | None = None
    max_invocations: int | None = None
    invocation_count: int = 0
    active: bool = True


@dataclass
class EventMetrics:
    """Metrics for event system performance."""
    total_events: int = 0
    handled_events: int = 0
    dropped_events: int = 0
    avg_handling_time_ms: float = 0.0


class EventBus:
    """
    Central event bus for pub/sub messaging in automation workflows.
    
    Example:
        bus = EventBus()
        await bus.subscribe("user.*", handle_user_event)
        await bus.publish(Event("user.login", {"user_id": "123"}))
    """
    
    def __init__(self, max_queue_size: int = 1000) -> None:
        self._subscriptions: dict[str, list[EventSubscription]] = defaultdict(list)
        self._subscription_meta: dict[str, EventSubscription] = {}
        self._metrics = EventMetrics()
        self._queue: asyncio.Queue[Event] = asyncio.Queue(maxsize=max_queue_size)
        self._processing = False
        self._lock = asyncio.Lock()
    
    async def subscribe(
        self,
        pattern: str,
        handler: Callable[[Event], Awaitable[Any]],
        priority: EventPriority = EventPriority.NORMAL,
        filter_func: Callable[[Event], bool] | None = None,
        max_invocations: int | None = None,
    ) -> str:
        """
        Subscribe to events matching a pattern.
        
        Args:
            pattern: Event type pattern (supports * and ? wildcards)
            handler: Async function to handle matching events
            priority: Handler priority (higher = called first)
            filter_func: Optional additional filter function
            max_invocations: Max number of times to invoke (-1 for unlimited)
            
        Returns:
            Subscription ID for later unsubscription
        """
        subscription_id = str(uuid.uuid4())
        subscription = EventSubscription(
            subscription_id=subscription_id,
            pattern=pattern,
            handler=handler,
            priority=priority,
            filter_func=filter_func,
            max_invocations=max_invocations,
        )
        
        async with self._lock:
            self._subscriptions[pattern].append(subscription)
            self._subscriptions[pattern].sort(key=lambda s: s.priority.value, reverse=True)
            self._subscription_meta[subscription_id] = subscription
        
        return subscription_id
    
    async def unsubscribe(self, subscription_id: str) -> bool:
        """
        Unsubscribe from events.
        
        Args:
            subscription_id: ID returned from subscribe()
            
        Returns:
            True if unsubscribed, False if not found
        """
        async with self._lock:
            if subscription_id in self._subscription_meta:
                sub = self._subscription_meta[subscription_id]
                sub.active = False
                for pattern_subs in self._subscriptions.values():
                    pattern_subs[:] = [s for s in pattern_subs if s.subscription_id != subscription_id]
                del self._subscription_meta[subscription_id]
                return True
        return False
    
    async def publish(self, event: Event) -> None:
        """
        Publish an event to all matching subscribers.
        
        Args:
            event: Event to publish
        """
        self._metrics.total_events += 1
        
        try:
            self._queue.put_nowait(event)
        except asyncio.QueueFull:
            self._metrics.dropped_events += 1
            return
        
        if not self._processing:
            self._processing = True
            asyncio.create_task(self._process_events())
    
    async def _process_events(self) -> None:
        """Process queued events."""
        try:
            while not self._queue.empty():
                event = self._queue.get_nowait()
                await self._dispatch_event(event)
        finally:
            self._processing = False
    
    async def _dispatch_event(self, event: Event) -> None:
        """Dispatch event to all matching subscriptions."""
        matching_subs = []
        
        async with self._lock:
            for pattern, subs in self._subscriptions.items():
                if self._matches_pattern(event.event_type, pattern):
                    matching_subs.extend([s for s in subs if s.active])
        
        for sub in matching_subs:
            if sub.filter_func and not sub.filter_func(event):
                continue
            
            if sub.max_invocations is not None:
                if sub.invocation_count >= sub.max_invocations:
                    sub.active = False
                    continue
                sub.invocation_count += 1
            
            start = time.time()
            try:
                result = await sub.handler(event)
                if asyncio.iscoroutine(result):
                    await result
                self._metrics.handled_events += 1
            except Exception:
                pass
            finally:
                handling_time = (time.time() - start) * 1000
                total_handled = self._metrics.handled_events
                self._metrics.avg_handling_time_ms = (
                    (self._metrics.avg_handling_time_ms * (total_handled - 1) + handling_time)
                    / total_handled
                )
    
    def _matches_pattern(self, event_type: str, pattern: str) -> bool:
        """Check if event type matches pattern."""
        regex_pattern = pattern.replace("*", ".*").replace("?", ".")
        regex_pattern = f"^{regex_pattern}$"
        return bool(re.match(regex_pattern, event_type))
    
    def get_metrics(self) -> EventMetrics:
        """Get current event system metrics."""
        return self._metrics
    
    async def wait_for_event(
        self,
        event_type: str,
        timeout: float = 30.0,
    ) -> Event | None:
        """
        Wait for a specific event type to be published.
        
        Args:
            event_type: Event type to wait for
            timeout: Maximum time to wait in seconds
            
        Returns:
            Event if published, None if timeout
        """
        future: asyncio.Future[Event] = asyncio.Future()
        
        async def handler(event: Event) -> None:
            if not future.done():
                future.set_result(event)
        
        sub_id = await self.subscribe(event_type, handler)
        try:
            return await asyncio.wait_for(future, timeout)
        except asyncio.TimeoutError:
            return None
        finally:
            await self.unsubscribe(sub_id)


class AutomationEventAction:
    """
    High-level event-driven automation action.
    
    Provides a simplified interface for event-based automation workflows.
    """
    
    def __init__(self, event_bus: EventBus | None = None) -> None:
        self.event_bus = event_bus or EventBus()
        self._workflows: dict[str, list[str]] = defaultdict(list)
    
    async def on(
        self,
        event_type: str,
        handler: Callable[[Event], Awaitable[Any]],
        priority: EventPriority = EventPriority.NORMAL,
    ) -> str:
        """Subscribe to events matching a pattern."""
        return await self.event_bus.subscribe(event_type, handler, priority)
    
    async def emit(
        self,
        event_type: str,
        data: dict[str, Any],
        source: str = "automation",
    ) -> None:
        """Publish an event."""
        event = Event(event_type=event_type, data=data, source=source)
        await self.event_bus.publish(event)
    
    async def create_workflow(
        self,
        workflow_id: str,
        triggers: list[tuple[str, Callable[[Event], Awaitable[Any]]]],
    ) -> None:
        """
        Create an event-driven workflow.
        
        Args:
            workflow_id: Unique workflow identifier
            triggers: List of (event_pattern, handler) tuples
        """
        for pattern, handler in triggers:
            sub_id = await self.on(pattern, handler)
            self._workflows[workflow_id].append(sub_id)
    
    async def remove_workflow(self, workflow_id: str) -> None:
        """Remove a workflow and all its subscriptions."""
        if workflow_id in self._workflows:
            for sub_id in self._workflows[workflow_id]:
                await self.event_bus.unsubscribe(sub_id)
            del self._workflows[workflow_id]
    
    def get_metrics(self) -> EventMetrics:
        """Get event system metrics."""
        return self.event_bus.get_metrics()


# Export public API
__all__ = [
    "Event",
    "EventSubscription",
    "EventBus",
    "EventMetrics",
    "EventPriority",
    "AutomationEventAction",
]
