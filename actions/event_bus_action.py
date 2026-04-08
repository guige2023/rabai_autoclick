"""
Event Bus Action.

Provides an in-memory event bus for pub/sub messaging.
Supports:
- Topic-based routing
- Wildcard subscriptions
- Dead letter queue
- Event filtering
"""

from typing import Dict, List, Optional, Any, Callable, Awaitable
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
import threading
import asyncio
import logging
import json
import uuid
import copy

logger = logging.getLogger(__name__)


class EventPriority(Enum):
    """Event priority levels."""
    LOW = "low"
    NORMAL = "normal"
    HIGH = "high"
    CRITICAL = "critical"


@dataclass
class Event:
    """Represents an event in the bus."""
    topic: str
    data: Any
    event_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    timestamp: datetime = field(default_factory=datetime.utcnow)
    priority: EventPriority = EventPriority.NORMAL
    headers: Dict[str, str] = field(default_factory=dict)
    source: Optional[str] = None
    correlation_id: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert event to dictionary."""
        return {
            "event_id": self.event_id,
            "topic": self.topic,
            "data": self.data,
            "timestamp": self.timestamp.isoformat(),
            "priority": self.priority.value,
            "headers": self.headers,
            "source": self.source,
            "correlation_id": self.correlation_id
        }


@dataclass
class Subscription:
    """Represents an event subscription."""
    subscription_id: str
    topic_pattern: str
    handler: Callable[[Event], Awaitable[None]]
    filter_fn: Optional[Callable[[Event], bool]] = None
    is_wildcard: bool = False
    created_at: datetime = field(default_factory=datetime.utcnow)
    event_count: int = 0
    last_event_at: Optional[datetime] = None


@dataclass
class DeadLetterEvent:
    """Event that failed processing."""
    original_event: Event
    error: str
    failed_at: datetime = field(default_factory=datetime.utcnow)
    retry_count: int = 0


class EventBusAction:
    """
    Event Bus Action.
    
    Provides an in-memory event bus with support for:
    - Topic-based pub/sub
    - Wildcard subscriptions (* and #)
    - Dead letter queue for failed events
    - Event filtering
    - Async and sync handlers
    """
    
    def __init__(
        self,
        max_subscribers_per_topic: int = 100,
        dead_letter_max_size: int = 1000,
        enable_wildcard: bool = True
    ):
        """
        Initialize the Event Bus Action.
        
        Args:
            max_subscribers_per_topic: Maximum subscribers per topic
            dead_letter_max_size: Maximum dead letter queue size
            enable_wildcard: Enable wildcard subscriptions
        """
        self.max_subscribers_per_topic = max_subscribers_per_topic
        self.enable_wildcard = enable_wildcard
        self._subscriptions: Dict[str, List[Subscription]] = {}
        self._dead_letter: List[DeadLetterEvent] = []
        self._dead_letter_max_size = dead_letter_max_size
        self._lock = threading.RLock()
        self._running = False
        self._event_queue: asyncio.PriorityQueue = None
        self._processor_task: Optional[asyncio.Task] = None
    
    def subscribe(
        self,
        topic_pattern: str,
        handler: Callable[[Event], Awaitable[None]],
        filter_fn: Optional[Callable[[Event], bool]] = None
    ) -> str:
        """
        Subscribe to events matching a topic pattern.
        
        Args:
            topic_pattern: Topic pattern (* for single level, # for multi-level)
            handler: Async handler function
            filter_fn: Optional filter function
        
        Returns:
            Subscription ID
        """
        is_wildcard = "#" in topic_pattern or "*" in topic_pattern
        
        subscription = Subscription(
            subscription_id=str(uuid.uuid4()),
            topic_pattern=topic_pattern,
            handler=handler,
            filter_fn=filter_fn,
            is_wildcard=is_wildcard
        )
        
        with self._lock:
            if topic_pattern not in self._subscriptions:
                self._subscriptions[topic_pattern] = []
            
            if len(self._subscriptions[topic_pattern]) >= self.max_subscribers_per_topic:
                raise RuntimeError(f"Max subscribers reached for topic '{topic_pattern}'")
            
            self._subscriptions[topic_pattern].append(subscription)
        
        logger.info(f"Subscribed to topic pattern: {topic_pattern} ({subscription.subscription_id})")
        return subscription.subscription_id
    
    def unsubscribe(self, subscription_id: str) -> bool:
        """
        Unsubscribe from events.
        
        Args:
            subscription_id: Subscription ID to remove
        
        Returns:
            True if unsubscribed, False if not found
        """
        with self._lock:
            for topic_pattern, subs in self._subscriptions.items():
                for i, sub in enumerate(subs):
                    if sub.subscription_id == subscription_id:
                        subs.pop(i)
                        logger.info(f"Unsubscribed: {subscription_id}")
                        return True
        return False
    
    async def publish(self, event: Event) -> int:
        """
        Publish an event to the bus.
        
        Args:
            event: Event to publish
        
        Returns:
            Number of subscribers that received the event
        """
        delivery_count = 0
        matching_subscriptions = self._get_matching_subscriptions(event.topic)
        
        for subscription in matching_subscriptions:
            # Apply filter if present
            if subscription.filter_fn and not subscription.filter_fn(event):
                continue
            
            try:
                await subscription.handler(event)
                delivery_count += 1
                
                # Update stats
                subscription.event_count += 1
                subscription.last_event_at = datetime.utcnow()
            
            except Exception as e:
                logger.error(f"Handler error for subscription {subscription.subscription_id}: {e}")
                self._add_to_dead_letter(event, str(e))
        
        logger.debug(f"Published event {event.event_id} to {delivery_count} subscribers")
        return delivery_count
    
    def publish_sync(self, event: Event) -> int:
        """Synchronous publish (runs in executor)."""
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            return loop.run_until_complete(self.publish(event))
        finally:
            loop.close()
    
    def _get_matching_subscriptions(self, topic: str) -> List[Subscription]:
        """Get all subscriptions matching a topic."""
        with self._lock:
            matching = []
            
            # Exact match
            if topic in self._subscriptions:
                matching.extend(self._subscriptions[topic])
            
            # Pattern matches
            if self.enable_wildcard:
                for pattern, subs in self._subscriptions.items():
                    if self._topic_matches(topic, pattern):
                        matching.extend(subs)
            
            return matching
    
    def _topic_matches(self, topic: str, pattern: str) -> bool:
        """Check if topic matches pattern."""
        if pattern == "#":
            return True
        
        if "#" in pattern:
            # Multi-level wildcard
            parts = pattern.split("#")
            if len(parts) == 2:
                prefix = parts[0].rstrip(".")
                suffix = parts[1].lstrip(".")
                
                if prefix and not topic.startswith(prefix):
                    return False
                if suffix and not topic.endswith(suffix):
                    return False
                return True
        
        if "*" in pattern:
            # Single-level wildcard
            topic_parts = topic.split(".")
            pattern_parts = pattern.split(".")
            
            if len(topic_parts) != len(pattern_parts):
                return False
            
            for tp, pp in zip(topic_parts, pattern_parts):
                if pp != "*" and tp != pp:
                    return False
            return True
        
        return False
    
    def _add_to_dead_letter(self, event: Event, error: str) -> None:
        """Add failed event to dead letter queue."""
        with self._lock:
            dle = DeadLetterEvent(original_event=event, error=error)
            self._dead_letter.append(dle)
            
            if len(self._dead_letter) > self._dead_letter_max_size:
                self._dead_letter.pop(0)
            
            logger.warning(f"Event {event.event_id} added to dead letter: {error}")
    
    def get_dead_letter_events(self) -> List[DeadLetterEvent]:
        """Get all dead letter events."""
        with self._lock:
            return copy.deepcopy(self._dead_letter)
    
    def retry_dead_letter(self, event_id: str) -> bool:
        """Retry a dead letter event."""
        with self._lock:
            for dle in self._dead_letter:
                if dle.original_event.event_id == event_id:
                    dle.retry_count += 1
                    # Would need to re-publish here
                    return True
        return False
    
    def clear_dead_letter(self) -> int:
        """Clear dead letter queue."""
        with self._lock:
            count = len(self._dead_letter)
            self._dead_letter = []
            return count
    
    def get_stats(self) -> Dict[str, Any]:
        """Get event bus statistics."""
        with self._lock:
            total_subs = sum(len(subs) for subs in self._subscriptions.values())
            
            return {
                "total_topic_patterns": len(self._subscriptions),
                "total_subscriptions": total_subs,
                "dead_letter_size": len(self._dead_letter),
                "wildcard_enabled": self.enable_wildcard,
                "subscriptions_by_topic": {
                    pattern: len(subs)
                    for pattern, subs in self._subscriptions.items()
                }
            }
    
    def get_subscriptions(self) -> List[Dict[str, Any]]:
        """Get all subscriptions."""
        with self._lock:
            result = []
            for pattern, subs in self._subscriptions.items():
                for sub in subs:
                    result.append({
                        "subscription_id": sub.subscription_id,
                        "topic_pattern": pattern,
                        "is_wildcard": sub.is_wildcard,
                        "event_count": sub.event_count,
                        "last_event_at": sub.last_event_at.isoformat() if sub.last_event_at else None
                    })
            return result


# Standalone execution
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    async def main():
        bus = EventBusAction()
        
        # Subscribe handlers
        async def user_handler(event: Event):
            print(f"User event received: {event.topic} - {event.data}")
        
        async def order_handler(event: Event):
            print(f"Order event received: {event.topic} - {event.data}")
        
        async def all_handler(event: Event):
            print(f"All events: {event.topic} - {event.data}")
        
        # Subscribe to topics
        bus.subscribe("users.*", user_handler)
        bus.subscribe("orders.*", order_handler)
        bus.subscribe("#", all_handler)
        
        # Publish events
        await bus.publish(Event(topic="users.created", data={"user_id": "123"}))
        await bus.publish(Event(topic="orders.created", data={"order_id": "456"}))
        await bus.publish(Event(topic="users.updated", data={"user_id": "789"}))
        
        print(f"\nStats: {json.dumps(bus.get_stats(), indent=2)}")
        print(f"\nSubscriptions: {json.dumps(bus.get_subscriptions(), indent=2, default=str)}")
    
    asyncio.run(main())
