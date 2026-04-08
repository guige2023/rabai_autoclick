"""
API Event Bus Action Module.

Provides event-driven architecture with pub/sub messaging,
event routing, filtering, and delivery guarantees.
"""

from typing import Any, Callable, Dict, List, Optional, Set, Union
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
import asyncio
import json
import logging
import uuid
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor

logger = logging.getLogger(__name__)


class EventDeliveryMode(Enum):
    """Event delivery modes."""
    AT_MOST_ONCE = "at_most_once"
    AT_LEAST_ONCE = "at_least_once"
    EXACTLY_ONCE = "exactly_once"


class EventPriority(Enum):
    """Event priority levels."""
    LOW = 1
    NORMAL = 2
    HIGH = 3
    CRITICAL = 4


@dataclass
class Event:
    """Represents an event in the system."""
    event_id: str
    event_type: str
    topic: str
    payload: Any
    priority: EventPriority = EventPriority.NORMAL
    timestamp: datetime = field(default_factory=datetime.now)
    metadata: Dict[str, Any] = field(default_factory=dict)
    correlation_id: Optional[str] = None
    reply_to: Optional[str] = None

    @classmethod
    def create(
        cls,
        event_type: str,
        topic: str,
        payload: Any,
        **kwargs
    ) -> "Event":
        """Create a new event."""
        return cls(
            event_id=str(uuid.uuid4()),
            event_type=event_type,
            topic=topic,
            payload=payload,
            **kwargs
        )

    def to_dict(self) -> Dict[str, Any]:
        """Serialize event to dictionary."""
        return {
            "event_id": self.event_id,
            "event_type": self.event_type,
            "topic": self.topic,
            "payload": self.payload,
            "priority": self.priority.value,
            "timestamp": self.timestamp.isoformat(),
            "metadata": self.metadata,
            "correlation_id": self.correlation_id,
            "reply_to": self.reply_to
        }


@dataclass
class Subscription:
    """Represents an event subscription."""
    subscription_id: str
    topic: str
    handler: Callable
    filter_func: Optional[Callable[[Event], bool]] = None
    priority: EventPriority = EventPriority.NORMAL
    dead_letter_handler: Optional[Callable] = None
    max_retries: int = 3
    retry_delay: float = 1.0

    def matches(self, event: Event) -> bool:
        """Check if event matches this subscription."""
        if event.topic != self.topic:
            return False
        if self.filter_func and not self.filter_func(event):
            return False
        return True


@dataclass
class DeliveryResult:
    """Result of event delivery attempt."""
    subscription_id: str
    event_id: str
    success: bool
    delivery_time: datetime
    error: Optional[str] = None
    retry_count: int = 0


@dataclass
class EventBusMetrics:
    """Metrics for event bus monitoring."""
    events_published: int = 0
    events_delivered: int = 0
    events_failed: int = 0
    events_dropped: int = 0
    subscriptions_count: int = 0
    topics_count: Set[str] = field(default_factory=set)

    @property
    def delivery_rate(self) -> float:
        """Get delivery success rate."""
        if self.events_published == 0:
            return 0.0
        return self.events_delivered / self.events_published


class EventFilter:
    """Filter for event subscription matching."""

    @staticmethod
    def by_metadata(key: str, value: Any) -> Callable[[Event], bool]:
        """Create metadata filter."""
        return lambda e: e.metadata.get(key) == value

    @staticmethod
    def by_priority(min_priority: EventPriority) -> Callable[[Event], bool]:
        """Create priority filter."""
        return lambda e: e.priority.value >= min_priority.value

    @staticmethod
    def by_correlation(correlation_id: str) -> Callable[[Event], bool]:
        """Create correlation ID filter."""
        return lambda e: e.correlation_id == correlation_id

    @staticmethod
    def by_payload_path(path: str, expected: Any) -> Callable[[Event], bool]:
        """Create payload path filter."""
        def get_path(data: Any, path: str) -> Any:
            keys = path.split(".")
            result = data
            for key in keys:
                if isinstance(result, dict):
                    result = result.get(key)
                else:
                    return None
            return result
        return lambda e: get_path(e.payload, path) == expected


class DeadLetterQueue:
    """Dead letter queue for failed event deliveries."""

    def __init__(self, max_size: int = 1000):
        self.max_size = max_size
        self._queue: List[Event] = []
        self._lock = asyncio.Lock()

    async def put(self, event: Event, error: str):
        """Add failed event to dead letter queue."""
        async with self._lock:
            event.metadata["dlq_error"] = error
            event.metadata["dlq_timestamp"] = datetime.now().isoformat()
            if len(self._queue) >= self.max_size:
                self._queue.pop(0)
            self._queue.append(event)

    async def get(self) -> Optional[Event]:
        """Get event from dead letter queue."""
        async with self._lock:
            if self._queue:
                return self._queue.pop(0)
            return None

    async def size(self) -> int:
        """Get queue size."""
        async with self._lock:
            return len(self._queue)


class EventBus:
    """Main event bus for pub/sub messaging."""

    def __init__(
        self,
        delivery_mode: EventDeliveryMode = EventDeliveryMode.AT_LEAST_ONCE,
        enable_metrics: bool = True
    ):
        self.delivery_mode = delivery_mode
        self.enable_metrics = enable_metrics
        self._subscriptions: Dict[str, List[Subscription]] = defaultdict(list)
        self._topicSubscriptions: Dict[str, Set[str]] = defaultdict(set)
        self._subscription_index: Dict[str, Subscription] = {}
        self._dead_letter_queue = DeadLetterQueue()
        self._metrics = EventBusMetrics()
        self._executor = ThreadPoolExecutor(max_workers=4)
        self._running = False
        self._event_queues: Dict[str, asyncio.Queue] = {}
        self._worker_tasks: List[asyncio.Task] = []

    def subscribe(
        self,
        topic: str,
        handler: Callable[[Event], Any],
        filter_func: Optional[Callable[[Event], bool]] = None,
        priority: EventPriority = EventPriority.NORMAL,
        subscription_id: Optional[str] = None
    ) -> str:
        """Subscribe to events on a topic."""
        sub_id = subscription_id or str(uuid.uuid4())
        subscription = Subscription(
            subscription_id=sub_id,
            topic=topic,
            handler=handler,
            filter_func=filter_func,
            priority=priority
        )

        self._subscriptions[sub_id] = subscription
        self._topicSubscriptions[topic].add(sub_id)
        self._subscription_index[sub_id] = subscription

        if self.enable_metrics:
            self._metrics.subscriptions_count += 1
            self._metrics.topics_count.add(topic)

        logger.info(f"Subscribed to topic '{topic}' with ID {sub_id}")
        return sub_id

    def unsubscribe(self, subscription_id: str):
        """Unsubscribe from events."""
        subscription = self._subscription_index.get(subscription_id)
        if subscription:
            topic = subscription.topic
            self._subscriptions[subscription_id] = []
            self._topicSubscriptions[topic].discard(subscription_id)
            del self._subscription_index[subscription_id]

            if self.enable_metrics:
                self._metrics.subscriptions_count -= 1

    async def publish(self, event: Event) -> List[DeliveryResult]:
        """Publish an event to all matching subscriptions."""
        if self.enable_metrics:
            self._metrics.events_published += 1

        results = []
        matching_subs = self._get_matching_subscriptions(event)

        if not matching_subs:
            if self.enable_metrics:
                self._metrics.events_dropped += 1
            return results

        delivery_tasks = [
            self._deliver_with_retry(sub, event)
            for sub in matching_subs
        ]

        results = await asyncio.gather(*delivery_tasks, return_exceptions=True)

        return [r for r in results if isinstance(r, DeliveryResult)]

    def _get_matching_subscriptions(self, event: Event) -> List[Subscription]:
        """Get all subscriptions matching an event."""
        matching = []
        for sub_id in self._topicSubscriptions.get(event.topic, []):
            sub = self._subscription_index.get(sub_id)
            if sub and sub.matches(event):
                matching.append(sub)

        matching.sort(key=lambda s: s.priority.value, reverse=True)
        return matching

    async def _deliver_with_retry(
        self,
        subscription: Subscription,
        event: Event
    ) -> DeliveryResult:
        """Deliver event to subscription with retry logic."""
        result = DeliveryResult(
            subscription_id=subscription.subscription_id,
            event_id=event.event_id,
            success=False,
            delivery_time=datetime.now()
        )

        for attempt in range(subscription.max_retries + 1):
            try:
                if asyncio.iscoroutinefunction(subscription.handler):
                    await asyncio.wait_for(
                        subscription.handler(event),
                        timeout=30.0
                    )
                else:
                    loop = asyncio.get_event_loop()
                    await loop.run_in_executor(
                        self._executor,
                        subscription.handler,
                        event
                    )

                result.success = True
                if self.enable_metrics:
                    self._metrics.events_delivered += 1
                return result

            except asyncio.TimeoutError:
                result.error = "Delivery timeout"
                result.retry_count = attempt

            except Exception as e:
                result.error = str(e)
                result.retry_count = attempt

            if attempt < subscription.max_retries:
                await asyncio.sleep(subscription.retry_delay * (attempt + 1))

        if self.enable_metrics:
            self._metrics.events_failed += 1

        if subscription.dead_letter_handler:
            await asyncio.to_thread(subscription.dead_letter_handler, event, result.error)
        else:
            await self._dead_letter_queue.put(event, result.error or "Unknown error")

        return result

    async def start(self):
        """Start the event bus."""
        self._running = True
        logger.info("Event bus started")

    async def stop(self):
        """Stop the event bus."""
        self._running = False
        for task in self._worker_tasks:
            task.cancel()
        self._executor.shutdown(wait=False)
        logger.info("Event bus stopped")

    def get_metrics(self) -> EventBusMetrics:
        """Get event bus metrics."""
        return self._metrics

    def get_subscription_count(self, topic: Optional[str] = None) -> int:
        """Get number of subscriptions."""
        if topic:
            return len(self._topicSubscriptions.get(topic, set()))
        return len(self._subscription_index)


async def handle_user_created(event: Event):
    """Sample event handler."""
    print(f"User created: {event.payload}")
    await asyncio.sleep(0.01)


async def handle_order_placed(event: Event):
    """Sample event handler."""
    print(f"Order placed: {event.payload}")


def main():
    """Synchronous main for basic demonstration."""
    bus = EventBus(delivery_mode=EventDeliveryMode.AT_LEAST_ONCE)

    bus.subscribe(
        topic="user.created",
        handler=handle_user_created,
        filter_func=EventFilter.by_priority(EventPriority.NORMAL)
    )

    bus.subscribe(
        topic="order.placed",
        handler=handle_order_placed
    )

    event = Event.create(
        event_type="user.created",
        topic="user.created",
        payload={"user_id": "123", "name": "John Doe"},
        metadata={"source": "test"}
    )

    asyncio.run(bus.publish(event))
    print(f"Metrics: {bus.get_metrics()}")


if __name__ == "__main__":
    main()
