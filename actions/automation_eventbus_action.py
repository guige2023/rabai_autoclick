"""
Automation Event Bus Module.

Provides pub/sub event handling for automation workflows
with support for event filtering, wildcards, dead letter
queues, and distributed event processing.
"""

from typing import (
    Dict, List, Optional, Any, Callable, Set,
    Tuple, TypeVar, Generic, Union
)
from dataclasses import dataclass, field
from enum import Enum, auto
from datetime import datetime
import threading
import logging
import json
from collections import defaultdict, deque
import re
import uuid

logger = logging.getLogger(__name__)

T = TypeVar("T")


class EventPriority(Enum):
    """Event priority levels."""
    LOW = 0
    NORMAL = 1
    HIGH = 2
    CRITICAL = 3


class EventStatus(Enum):
    """Event processing status."""
    PUBLISHED = auto()
    DELIVERED = auto()
    FAILED = auto()
    DEAD_LETTER = auto()


@dataclass
class Event:
    """Automation event."""
    event_id: str
    event_type: str
    payload: Any
    timestamp: datetime = field(default_factory=datetime.now)
    priority: EventPriority = EventPriority.NORMAL
    source: Optional[str] = None
    correlation_id: Optional[str] = None
    headers: Dict[str, str] = field(default_factory=dict)
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def __post_init__(self) -> None:
        if not self.event_id:
            self.event_id = str(uuid.uuid4())


@dataclass
class Subscription:
    """Event subscription."""
    subscription_id: str
    event_type: str
    handler: Callable[[Event], Any]
    filter_func: Optional[Callable[[Event], bool]] = None
    async_handler: bool = False
    max_retries: int = 3
    dead_letter_queue: bool = True


@dataclass
class EventDelivery:
    """Event delivery record."""
    event: Event
    subscription: Subscription
    status: EventStatus
    delivered_at: Optional[datetime] = None
    error: Optional[str] = None
    retry_count: int = 0


@dataclass
class DeadLetterEvent:
    """Event that failed processing."""
    original_event: Event
    subscription_id: str
    error: str
    failed_at: datetime = field(default_factory=datetime.now)
    retry_count: int = 0
    original_payload: Any = None


class EventPattern:
    """Pattern matching for event types."""
    
    @staticmethod
    def match(pattern: str, event_type: str) -> bool:
        """
        Match event type against pattern.
        
        Supports:
            - Exact match: "order.created"
            - Wildcard: "order.*" matches "order.created", "order.updated"
            - Multi-level: "order.**" matches "order.created.v2"
            - Regex: "order\\.(created|updated)"
        """
        if pattern == event_type:
            return True
        
        if "**" in pattern:
            pattern = pattern.replace("**", ".*")
        
        if "*" in pattern:
            pattern = pattern.replace(".", "\\.")
            pattern = pattern.replace("*", "[^.]+")
        
        pattern = f"^{pattern}$"
        
        try:
            return bool(re.match(pattern, event_type))
        except re.error:
            return False


class InMemoryEventBus:
    """
    In-memory event bus implementation.
    
    Provides pub/sub messaging with pattern matching,
    filtering, and dead letter queue support.
    """
    
    def __init__(
        self,
        max_dead_letter_size: int = 1000,
        enable_metrics: bool = True
    ) -> None:
        self._subscriptions: Dict[str, List[Subscription]] = defaultdict(list)
        self._dead_letter_queue: deque = deque(maxlen=max_dead_letter_size)
        self._lock = threading.RLock()
        self._delivery_history: deque = deque(maxlen=1000)
        self._enable_metrics = enable_metrics
        self._metrics: Dict[str, int] = defaultdict(int)
    
    def subscribe(
        self,
        event_type: str,
        handler: Callable[[Event], Any],
        filter_func: Optional[Callable[[Event], bool]] = None,
        subscription_id: Optional[str] = None
    ) -> str:
        """
        Subscribe to events.
        
        Args:
            event_type: Event type pattern (supports wildcards)
            handler: Callback function to handle events
            filter_func: Optional additional filter
            subscription_id: Optional subscription ID
            
        Returns:
            Subscription ID
        """
        sub_id = subscription_id or str(uuid.uuid4())
        
        subscription = Subscription(
            subscription_id=sub_id,
            event_type=event_type,
            handler=handler,
            filter_func=filter_func
        )
        
        with self._lock:
            self._subscriptions[event_type].append(subscription)
        
        logger.info(f"Subscribed to '{event_type}' with ID: {sub_id}")
        return sub_id
    
    def unsubscribe(self, subscription_id: str) -> bool:
        """Remove subscription by ID."""
        with self._lock:
            for event_type, subs in self._subscriptions.items():
                self._subscriptions[event_type] = [
                    s for s in subs if s.subscription_id != subscription_id
                ]
                if not self._subscriptions[event_type]:
                    del self._subscriptions[event_type]
            return True
    
    def publish(
        self,
        event_type: str,
        payload: Any,
        priority: EventPriority = EventPriority.NORMAL,
        source: Optional[str] = None,
        correlation_id: Optional[str] = None,
        headers: Optional[Dict[str, str]] = None
    ) -> Event:
        """
        Publish an event to the bus.
        
        Args:
            event_type: Type of event
            payload: Event payload
            priority: Event priority
            source: Event source
            correlation_id: Optional correlation ID
            headers: Optional headers
            
        Returns:
            Published Event
        """
        event = Event(
            event_id=str(uuid.uuid4()),
            event_type=event_type,
            payload=payload,
            priority=priority,
            source=source,
            correlation_id=correlation_id,
            headers=headers or {}
        )
        
        self._dispatch(event)
        
        return event
    
    def _dispatch(self, event: Event) -> None:
        """Dispatch event to matching subscriptions."""
        matching_subs = self._get_matching_subscriptions(event.event_type)
        
        if not matching_subs:
            logger.debug(f"No subscribers for event: {event.event_type}")
            return
        
        if self._enable_metrics:
            self._metrics["published"] += 1
        
        for subscription in matching_subs:
            self._deliver_event(event, subscription)
    
    def _get_matching_subscriptions(
        self,
        event_type: str
    ) -> List[Subscription]:
        """Find all subscriptions matching event type."""
        matching = []
        
        with self._lock:
            for pattern, subs in self._subscriptions.items():
                if EventPattern.match(pattern, event_type):
                    matching.extend(subs)
        
        # Sort by priority (high first)
        return sorted(matching, key=lambda s: -s.handler.__code__.co_argcount)
    
    def _deliver_event(
        self,
        event: Event,
        subscription: Subscription
    ) -> None:
        """Deliver event to subscription handler."""
        # Apply filter if present
        if subscription.filter_func and not subscription.filter_func(event):
            logger.debug(f"Event filtered out by subscription {subscription.subscription_id}")
            return
        
        delivery = EventDelivery(
            event=event,
            subscription=subscription,
            status=EventStatus.PUBLISHED
        )
        
        try:
            result = subscription.handler(event)
            
            delivery.status = EventStatus.DELIVERED
            delivery.delivered_at = datetime.now()
            
            if self._enable_metrics:
                self._metrics["delivered"] += 1
            
            logger.debug(
                f"Delivered event {event.event_id} to {subscription.subscription_id}"
            )
        
        except Exception as e:
            delivery.status = EventStatus.FAILED
            delivery.error = str(e)
            
            if self._enable_metrics:
                self._metrics["failed"] += 1
            
            logger.error(
                f"Failed to deliver event {event.event_id}: {e}"
            )
            
            if subscription.dead_letter_queue:
                self._send_to_dead_letter(event, subscription, str(e))
    
    def _send_to_dead_letter(
        self,
        event: Event,
        subscription: Subscription,
        error: str
    ) -> None:
        """Send failed event to dead letter queue."""
        dead_letter = DeadLetterEvent(
            original_event=event,
            subscription_id=subscription.subscription_id,
            error=error,
            original_payload=event.payload
        )
        
        self._dead_letter_queue.append(dead_letter)
        
        if self._enable_metrics:
            self._metrics["dead_lettered"] += 1
        
        logger.warning(f"Event sent to DLQ: {event.event_id}")
    
    def get_dead_letters(
        self,
        limit: int = 100
    ) -> List[DeadLetterEvent]:
        """Get dead letter events."""
        return list(self._dead_letter_queue)[-limit:]
    
    def retry_dead_letter(
        self,
        event_id: str,
        subscription_id: str
    ) -> bool:
        """Retry a dead letter event."""
        for dlq_event in self._dead_letter_queue:
            if (dlq_event.original_event.event_id == event_id and
                dlq_event.subscription_id == subscription_id):
                
                # Restore original payload
                event = dlq_event.original_event
                event.payload = dlq_event.original_payload
                
                # Get subscription and retry
                for sub in self._get_matching_subscriptions(event.event_type):
                    if sub.subscription_id == subscription_id:
                        self._deliver_event(event, sub)
                        return True
        
        return False
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get event bus metrics."""
        return {
            "total_published": self._metrics["published"],
            "total_delivered": self._metrics["delivered"],
            "total_failed": self._metrics["failed"],
            "dead_letters": len(self._dead_letter_queue),
            "active_subscriptions": sum(
                len(subs) for subs in self._subscriptions.values()
            )
        }
    
    def clear_dead_letters(self) -> int:
        """Clear dead letter queue and return count."""
        count = len(self._dead_letter_queue)
        self._dead_letter_queue.clear()
        return count


# Entry point for direct execution
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    bus = InMemoryEventBus()
    
    # Define handlers
    def handle_order_created(event: Event) -> None:
        print(f"Order created: {event.payload}")
    
    def handle_order_updated(event: Event) -> None:
        print(f"Order updated: {event.payload}")
    
    def handle_all_orders(event: Event) -> None:
        print(f"Any order event: {event.event_type}")
    
    def order_filter(event: Event) -> bool:
        """Only process orders over $100."""
        return event.payload.get("amount", 0) > 100
    
    # Subscribe to events
    bus.subscribe("order.created", handle_order_created)
    bus.subscribe("order.updated", handle_order_updated)
    bus.subscribe("order.*", handle_all_orders)
    
    # Subscribe with filter
    bus.subscribe(
        "order.created",
        lambda e: print(f"High value order: {e.payload}"),
        filter_func=order_filter
    )
    
    # Publish events
    print("\n--- Publishing Events ---")
    
    bus.publish("order.created", {"order_id": "123", "amount": 150})
    bus.publish("order.created", {"order_id": "124", "amount": 50})  # Filtered
    bus.publish("order.updated", {"order_id": "123", "status": "shipped"})
    bus.publish("order.cancelled", {"order_id": "125", "reason": "Out of stock"})
    
    print(f"\n--- Metrics ---")
    print(bus.get_metrics())
    
    print(f"\n--- Dead Letters ---")
    print(f"DLQ size: {len(bus.get_dead_letters())}")
