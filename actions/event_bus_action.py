"""
Event Bus Action Module.

Provides a pub/sub event bus for decoupled communication between
components with support for async handlers, filtering, and event ordering.
"""

from typing import Optional, Dict, List, Any, Callable, TypeVar, Generic
from dataclasses import dataclass, field
from enum import Enum
import logging
import time
import threading
from collections import defaultdict
from concurrent.futures import Future, ThreadPoolExecutor

logger = logging.getLogger(__name__)

T = TypeVar("T")


class EventPriority(Enum):
    """Priority levels for event handlers."""
    LOW = 0
    NORMAL = 1
    HIGH = 2
    CRITICAL = 3


@dataclass
class Event:
    """Base event class."""
    event_type: str
    payload: Any = None
    timestamp: float = field(default_factory=time.time)
    source: Optional[str] = None
    correlation_id: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def __post_init__(self):
        if not self.timestamp:
            self.timestamp = time.time()


@dataclass
class EventSubscription:
    """Subscription to an event or pattern."""
    subscription_id: str
    handler: Callable[[Event], Any]
    event_pattern: str  # Exact type or wildcard pattern
    priority: EventPriority = EventPriority.NORMAL
    filter_fn: Optional[Callable[[Event], bool]] = None
    async_handler: bool = False
    subscription_time: float = field(default_factory=time.time)


@dataclass
class EventBusMetrics:
    """Metrics for event bus performance."""
    events_published: int = 0
    events_delivered: int = 0
    events_filtered: int = 0
    publish_errors: int = 0
    handler_errors: int = 0
    active_subscriptions: int = 0


class EventBus:
    """
    Central event bus for publish/subscribe messaging.
    
    Example:
        bus = EventBus()
        
        @bus.subscribe("user.created")
        def on_user_created(event):
            print(f"User created: {event.payload}")
            
        @bus.subscribe("user.*", priority=EventPriority.HIGH)
        def on_user_any(event):
            audit_log(event)
            
        bus.publish(Event("user.created", payload={"id": 123, "name": "Alice"}))
    """
    
    def __init__(
        self,
        async_executor: Optional[ThreadPoolExecutor] = None,
        ordered_delivery: bool = True,
        max_queue_size: int = 10000,
    ):
        self.async_executor = async_executor or ThreadPoolExecutor(max_workers=10)
        self.ordered_delivery = ordered_delivery
        self.max_queue_size = max_queue_size
        
        self._subscriptions: Dict[str, List[EventSubscription]] = defaultdict(list)
        self._subscription_counter: int = 0
        self._lock = threading.RLock()
        self._metrics = EventBusMetrics()
        
    def subscribe(
        self,
        event_pattern: str,
        priority: EventPriority = EventPriority.NORMAL,
        filter_fn: Optional[Callable[[Event], bool]] = None,
        async_handler: bool = True,
    ) -> Callable:
        """
        Decorator to subscribe to events.
        
        Args:
            event_pattern: Event type pattern (supports * wildcard)
            priority: Handler priority (higher = called first)
            filter_fn: Optional filter function
            async_handler: Run handler in thread pool
            
        Returns:
            Decorator function
        """
        def decorator(handler: Callable[[Event], Any]) -> Callable:
            self.add_subscription(
                event_pattern=event_pattern,
                handler=handler,
                priority=priority,
                filter_fn=filter_fn,
                async_handler=async_handler,
            )
            return handler
            
        return decorator
        
    def add_subscription(
        self,
        event_pattern: str,
        handler: Callable[[Event], Any],
        priority: EventPriority = EventPriority.NORMAL,
        filter_fn: Optional[Callable[[Event], bool]] = None,
        async_handler: bool = True,
    ) -> str:
        """
        Add a subscription to the event bus.
        
        Args:
            event_pattern: Event type pattern
            handler: Function to call when event matches
            priority: Handler priority
            filter_fn: Optional event filter
            async_handler: Run async in thread pool
            
        Returns:
            Subscription ID
        """
        with self._lock:
            self._subscription_counter += 1
            sub_id = f"sub_{self._subscription_counter}"
            
            subscription = EventSubscription(
                subscription_id=sub_id,
                handler=handler,
                event_pattern=event_pattern,
                priority=priority,
                filter_fn=filter_fn,
                async_handler=async_handler,
            )
            
            self._subscriptions[event_pattern].append(subscription)
            self._subscriptions[event_pattern].sort(
                key=lambda s: s.priority.value,
                reverse=True,
            )
            
            self._metrics.active_subscriptions = sum(
                len(subs) for subs in self._subscriptions.values()
            )
            
            logger.debug(f"Added subscription {sub_id} for pattern: {event_pattern}")
            return sub_id
            
    def unsubscribe(self, subscription_id: str) -> bool:
        """
        Remove a subscription by ID.
        
        Args:
            subscription_id: Subscription to remove
            
        Returns:
            True if removed, False if not found
        """
        with self._lock:
            for pattern, subs in self._subscriptions.items():
                for i, sub in enumerate(subs):
                    if sub.subscription_id == subscription_id:
                        subs.pop(i)
                        self._metrics.active_subscriptions = sum(
                            len(s) for s in self._subscriptions.values()
                        )
                        return True
        return False
        
    def publish(
        self,
        event: Event,
        wait: bool = False,
        timeout: Optional[float] = None,
    ) -> List[Any]:
        """
        Publish an event to all matching subscribers.
        
        Args:
            event: Event to publish
            wait: Wait for handlers to complete
            timeout: Max time to wait
            
        Returns:
            List of handler results
        """
        with self._lock:
            self._metrics.events_published += 1
            
        results = []
        matching_subs = self._get_matching_subscriptions(event)
        
        if not matching_subs:
            return results
            
        for sub in matching_subs:
            result = self._dispatch_event(sub, event, wait, timeout)
            if result is not None:
                results.append(result)
                
        return results
        
    def _get_matching_subscriptions(self, event: Event) -> List[EventSubscription]:
        """Find all subscriptions matching an event."""
        matching = []
        
        with self._lock:
            for pattern, subs in self._subscriptions.items():
                if self._pattern_matches(pattern, event.event_type):
                    for sub in subs:
                        if sub.filter_fn and not sub.filter_fn(event):
                            self._metrics.events_filtered += 1
                            continue
                        matching.append(sub)
                        
        return matching
        
    def _pattern_matches(self, pattern: str, event_type: str) -> bool:
        """Check if event type matches pattern."""
        if pattern == event_type:
            return True
        if pattern == "*":
            return True
        if "*" in pattern:
            import re
            regex_pattern = pattern.replace("*", ".*")
            return re.match(f"^{regex_pattern}$", event_type) is not None
        return False
        
    def _dispatch_event(
        self,
        sub: EventSubscription,
        event: Event,
        wait: bool,
        timeout: Optional[float],
    ) -> Any:
        """Dispatch event to subscription handler."""
        try:
            if sub.async_handler and not wait:
                future = self.async_executor.submit(sub.handler, event)
                return future
                
            result = sub.handler(event)
            
            with self._lock:
                self._metrics.events_delivered += 1
                
            return result
            
        except Exception as e:
            logger.error(f"Event handler error: {e}")
            with self._lock:
                self._metrics.handler_errors += 1
            return None
            
    def publish_sync(
        self,
        event_type: str,
        payload: Any = None,
        source: Optional[str] = None,
        correlation_id: Optional[str] = None,
        **metadata,
    ) -> List[Any]:
        """
        Synchronously publish an event.
        
        Args:
            event_type: Type of event
            payload: Event payload
            source: Source identifier
            correlation_id: For tracing related events
            **metadata: Additional metadata
            
        Returns:
            List of handler results
        """
        event = Event(
            event_type=event_type,
            payload=payload,
            source=source,
            correlation_id=correlation_id,
            metadata=metadata,
        )
        return self.publish(event, wait=True)
        
    def get_metrics(self) -> EventBusMetrics:
        """Get event bus metrics."""
        with self._lock:
            return EventBusMetrics(
                events_published=self._metrics.events_published,
                events_delivered=self._metrics.events_delivered,
                events_filtered=self._metrics.events_filtered,
                publish_errors=self._metrics.publish_errors,
                handler_errors=self._metrics.handler_errors,
                active_subscriptions=self._metrics.active_subscriptions,
            )
            
    def clear(self) -> None:
        """Clear all subscriptions."""
        with self._lock:
            self._subscriptions.clear()
            self._metrics.active_subscriptions = 0
            
    def get_subscription_count(self, event_pattern: Optional[str] = None) -> int:
        """Get number of subscriptions."""
        with self._lock:
            if event_pattern:
                return len(self._subscriptions.get(event_pattern, []))
            return sum(len(subs) for subs in self._subscriptions.values())


class EventBusBuilder:
    """
    Builder for configuring an event bus.
    
    Example:
        bus = EventBusBuilder() \
            .with_async_executor(max_workers=20) \
            .with_ordered_delivery(False) \
            .with_max_queue_size(50000) \
            .build()
    """
    
    def __init__(self):
        self._async_executor: Optional[ThreadPoolExecutor] = None
        self._ordered_delivery: bool = True
        self._max_queue_size: int = 10000
        self._default_subscriptions: List[tuple] = []
        
    def with_async_executor(
        self,
        max_workers: int = 10,
    ) -> "EventBusBuilder":
        """Configure async executor."""
        self._async_executor = ThreadPoolExecutor(max_workers=max_workers)
        return self
        
    def with_ordered_delivery(
        self,
        ordered: bool = True,
    ) -> "EventBusBuilder":
        """Configure ordered event delivery."""
        self._ordered_delivery = ordered
        return self
        
    def with_max_queue_size(
        self,
        size: int,
    ) -> "EventBusBuilder":
        """Configure maximum event queue size."""
        self._max_queue_size = size
        return self
        
    def with_default_subscription(
        self,
        pattern: str,
        handler: Callable[[Event], Any],
        **kwargs,
    ) -> "EventBusBuilder":
        """Add a default subscription to apply to built bus."""
        self._default_subscriptions.append((pattern, handler, kwargs))
        return self
        
    def build(self) -> EventBus:
        """Build the configured event bus."""
        bus = EventBus(
            async_executor=self._async_executor,
            ordered_delivery=self._ordered_delivery,
            max_queue_size=self._max_queue_size,
        )
        
        for pattern, handler, kwargs in self._default_subscriptions:
            bus.add_subscription(pattern, handler, **kwargs)
            
        return bus


# Global event bus instance
_global_bus: Optional[EventBus] = None


def get_event_bus() -> EventBus:
    """Get the global event bus instance."""
    global _global_bus
    if _global_bus is None:
        _global_bus = EventBus()
    return _global_bus


def publish_event(
    event_type: str,
    payload: Any = None,
    **kwargs,
) -> List[Any]:
    """Convenience function to publish to global bus."""
    return get_event_bus().publish_sync(event_type, payload, **kwargs)
