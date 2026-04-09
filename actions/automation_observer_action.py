"""
Automation Observer Action Module.

Implements the Observer pattern for automation event
notification with subscription management.
"""

from __future__ import annotations

import asyncio
import logging
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Optional

logger = logging.getLogger(__name__)


class EventType(Enum):
    """Automation event types."""

    TASK_STARTED = "task_started"
    TASK_COMPLETED = "task_completed"
    TASK_FAILED = "task_failed"
    TASK_CANCELLED = "task_cancelled"
    STATE_CHANGED = "state_changed"
    ERROR_OCCURRED = "error_occurred"
    METRIC_UPDATED = "metric_updated"
    CUSTOM = "custom"


@dataclass
class AutomationEvent:
    """Represents an automation event."""

    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    event_type: EventType = EventType.CUSTOM
    source: str = ""
    timestamp: float = field(default_factory=time.time)
    data: dict[str, Any] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class Subscription:
    """Represents an event subscription."""

    id: str
    event_type: EventType
    callback: Callable
    filter_func: Optional[Callable] = None
    created_at: float = field(default_factory=time.time)
    is_active: bool = True
    receive_count: int = 0


class AutomationObserverAction:
    """
    Manages observer pattern for automation events.

    Features:
    - Event subscription and filtering
    - Synchronous and asynchronous notifications
    - Event buffering for high-frequency events
    - Dead letter queue for failed notifications

    Example:
        observer = AutomationObserverAction()
        observer.subscribe(EventType.TASK_COMPLETED, on_complete_handler)
        observer.emit(AutomationEvent(event_type=EventType.TASK_COMPLETED))
    """

    def __init__(
        self,
        buffer_size: int = 1000,
        enable_dlq: bool = True,
        max_retries: int = 3,
    ) -> None:
        """
        Initialize observer action.

        Args:
            buffer_size: Event buffer size.
            enable_dlq: Enable dead letter queue.
            max_retries: Max notification retries.
        """
        self.buffer_size = buffer_size
        self.enable_dlq = enable_dlq
        self.max_retries = max_retries
        self._subscriptions: dict[str, Subscription] = {}
        self._event_buffer: list[AutomationEvent] = []
        self._dlq: list[AutomationEvent] = []
        self._event_count = 0
        self._notification_errors = 0
        self._lock = asyncio.Lock()

    def subscribe(
        self,
        event_type: EventType,
        callback: Callable[[AutomationEvent], None],
        filter_func: Optional[Callable] = None,
    ) -> str:
        """
        Subscribe to an event type.

        Args:
            event_type: Event type to subscribe to.
            callback: Callback function.
            filter_func: Optional event filter.

        Returns:
            Subscription ID.
        """
        sub_id = str(uuid.uuid4())
        subscription = Subscription(
            id=sub_id,
            event_type=event_type,
            callback=callback,
            filter_func=filter_func,
        )
        self._subscriptions[sub_id] = subscription
        logger.info(f"Subscribed to {event_type.value}: {sub_id}")
        return sub_id

    def unsubscribe(self, subscription_id: str) -> bool:
        """
        Unsubscribe from an event.

        Args:
            subscription_id: Subscription ID.

        Returns:
            True if unsubscribed.
        """
        if subscription_id in self._subscriptions:
            del self._subscriptions[subscription_id]
            logger.info(f"Unsubscribed: {subscription_id}")
            return True
        return False

    async def emit(
        self,
        event: AutomationEvent,
        immediate: bool = False,
    ) -> int:
        """
        Emit an event to all subscribers.

        Args:
            event: Event to emit.
            immediate: Process immediately instead of buffering.

        Returns:
            Number of subscribers notified.
        """
        self._event_count += 1

        if not immediate:
            self._event_buffer.append(event)
            if len(self._event_buffer) > self.buffer_size:
                self._event_buffer.pop(0)

        notified = 0
        for subscription in self._subscriptions.values():
            if not subscription.is_active:
                continue

            if subscription.event_type != event.event_type:
                continue

            if subscription.filter_func and not subscription.filter_func(event):
                continue

            try:
                if asyncio.iscoroutinefunction(subscription.callback):
                    await subscription.callback(event)
                else:
                    subscription.callback(event)

                subscription.receive_count += 1
                notified += 1

            except Exception as e:
                self._notification_errors += 1
                logger.error(f"Notification error: {e}")

                if self.enable_dlq:
                    self._dlq.append(event)

        logger.debug(f"Emitted event {event.id}: {notified} notified")
        return notified

    async def process_buffer(self) -> int:
        """
        Process all buffered events.

        Returns:
            Number of events processed.
        """
        processed = 0
        async with self._lock:
            while self._event_buffer:
                event = self._event_buffer.pop(0)
                await self.emit(event, immediate=True)
                processed += 1

        return processed

    def get_subscriptions(
        self,
        event_type: Optional[EventType] = None,
    ) -> list[Subscription]:
        """
        Get active subscriptions.

        Args:
            event_type: Filter by event type.

        Returns:
            List of subscriptions.
        """
        subs = [s for s in self._subscriptions.values() if s.is_active]
        if event_type:
            subs = [s for s in subs if s.event_type == event_type]
        return subs

    def get_dlq(self) -> list[AutomationEvent]:
        """
        Get dead letter queue events.

        Returns:
            List of DLQ events.
        """
        return self._dlq.copy()

    def clear_dlq(self) -> int:
        """
        Clear dead letter queue.

        Returns:
            Number of events cleared.
        """
        count = len(self._dlq)
        self._dlq.clear()
        return count

    def get_stats(self) -> dict[str, Any]:
        """
        Get observer statistics.

        Returns:
            Statistics dictionary.
        """
        return {
            "total_subscriptions": len(self._subscriptions),
            "active_subscriptions": sum(1 for s in self._subscriptions.values() if s.is_active),
            "total_events": self._event_count,
            "buffered_events": len(self._event_buffer),
            "dlq_events": len(self._dlq),
            "notification_errors": self._notification_errors,
        }
