"""Event bus action for publish-subscribe messaging.

Provides event publishing, subscribing, and filtering
with support for wildcards and patterns.
"""

import logging
import re
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Optional

logger = logging.getLogger(__name__)


@dataclass
class Event:
    event_type: str
    payload: dict[str, Any]
    timestamp: float = field(default_factory=time.time)
    source: str = "unknown"
    correlation_id: Optional[str] = None
    headers: dict[str, str] = field(default_factory=dict)


@dataclass
class Subscription:
    subscriber_id: str
    event_pattern: str
    handler: Callable[[Event], None]
    created_at: float = field(default_factory=time.time)
    is_active: bool = True
    metadata: dict[str, Any] = field(default_factory=dict)


class EventBusAction:
    """Publish-subscribe event bus with pattern matching.

    Args:
        enable_wildcards: Enable wildcard pattern matching.
        max_queue_size: Maximum queue size per subscriber.
        enable_dead_letter: Enable dead letter queue for failed events.
    """

    def __init__(
        self,
        enable_wildcards: bool = True,
        max_queue_size: int = 1000,
        enable_dead_letter: bool = True,
    ) -> None:
        self._subscriptions: dict[str, list[Subscription]] = {}
        self._enable_wildcards = enable_wildcards
        self._max_queue_size = max_queue_size
        self._enable_dead_letter = enable_dead_letter
        self._dead_letter_queue: list[Event] = []
        self._event_history: list[Event] = []
        self._max_history = 10000
        self._compiled_patterns: dict[str, re.Pattern] = {}

    def subscribe(
        self,
        subscriber_id: str,
        event_pattern: str,
        handler: Callable[[Event], None],
        metadata: Optional[dict[str, Any]] = None,
    ) -> bool:
        """Subscribe to events matching a pattern.

        Args:
            subscriber_id: Unique subscriber ID.
            event_pattern: Event type pattern (supports * wildcards).
            handler: Event handler function.
            metadata: Optional subscriber metadata.

        Returns:
            True if subscribed successfully.
        """
        subscription = Subscription(
            subscriber_id=subscriber_id,
            event_pattern=event_pattern,
            handler=handler,
            metadata=metadata or {},
        )

        if event_pattern not in self._subscriptions:
            self._subscriptions[event_pattern] = []
        self._subscriptions[event_pattern].append(subscription)

        logger.debug(f"Subscribed {subscriber_id} to pattern: {event_pattern}")
        return True

    def unsubscribe(self, subscriber_id: str, event_pattern: str) -> bool:
        """Unsubscribe from an event pattern.

        Args:
            subscriber_id: Subscriber ID.
            event_pattern: Event pattern.

        Returns:
            True if unsubscribed.
        """
        subscriptions = self._subscriptions.get(event_pattern, [])
        for sub in subscriptions:
            if sub.subscriber_id == subscriber_id:
                sub.is_active = False
                return True
        return False

    def publish(
        self,
        event_type: str,
        payload: dict[str, Any],
        source: str = "unknown",
        correlation_id: Optional[str] = None,
        headers: Optional[dict[str, str]] = None,
    ) -> int:
        """Publish an event to all matching subscribers.

        Args:
            event_type: Type of event.
            payload: Event payload.
            source: Event source.
            correlation_id: Optional correlation ID.
            headers: Optional event headers.

        Returns:
            Number of subscribers that received the event.
        """
        event = Event(
            event_type=event_type,
            payload=payload,
            timestamp=time.time(),
            source=source,
            correlation_id=correlation_id,
            headers=headers or {},
        )

        self._event_history.append(event)
        if len(self._event_history) > self._max_history:
            self._event_history.pop(0)

        delivered_count = 0
        matched_patterns = self._get_matching_patterns(event_type)

        for pattern in matched_patterns:
            subscriptions = self._subscriptions.get(pattern, [])
            for subscription in subscriptions:
                if not subscription.is_active:
                    continue

                try:
                    subscription.handler(event)
                    delivered_count += 1
                except Exception as e:
                    logger.error(f"Handler error for {subscription.subscriber_id}: {e}")
                    if self._enable_dead_letter:
                        self._dead_letter_queue.append(event)

        logger.debug(f"Published {event_type} to {delivered_count} subscribers")
        return delivered_count

    def _get_matching_patterns(self, event_type: str) -> list[str]:
        """Get all patterns that match an event type.

        Args:
            event_type: Event type.

        Returns:
            List of matching patterns.
        """
        if not self._enable_wildcards:
            if event_type in self._subscriptions:
                return [event_type]
            return []

        matches = []
        for pattern in self._subscriptions.keys():
            if self._matches_pattern(event_type, pattern):
                matches.append(pattern)
        return matches

    def _matches_pattern(self, event_type: str, pattern: str) -> bool:
        """Check if event type matches a pattern.

        Args:
            event_type: Event type.
            pattern: Pattern with optional wildcards.

        Returns:
            True if matches.
        """
        if not self._enable_wildcards:
            return event_type == pattern

        regex_pattern = pattern.replace("*", ".*").replace(".", "\\.")
        if regex_pattern not in self._compiled_patterns:
            self._compiled_patterns[regex_pattern] = re.compile(f"^{regex_pattern}$")

        return bool(self._compiled_patterns[regex_pattern].match(event_type))

    def get_subscriptions(self, subscriber_id: Optional[str] = None) -> list[Subscription]:
        """Get subscriptions.

        Args:
            subscriber_id: Optional filter by subscriber ID.

        Returns:
            List of subscriptions.
        """
        all_subs = []
        for subs in self._subscriptions.values():
            all_subs.extend(subs)

        if subscriber_id:
            all_subs = [s for s in all_subs if s.subscriber_id == subscriber_id]

        return all_subs

    def get_dead_letter_queue(self, limit: int = 100) -> list[Event]:
        """Get dead letter queue events.

        Args:
            limit: Maximum events to return.

        Returns:
            List of dead letter events.
        """
        return self._dead_letter_queue[-limit:][::-1]

    def retry_dead_letter(self, event: Event) -> bool:
        """Retry a dead letter event.

        Args:
            event: Event to retry.

        Returns:
            True if retried successfully.
        """
        try:
            self._dead_letter_queue.remove(event)
            self.publish(
                event.event_type,
                event.payload,
                event.source,
                event.correlation_id,
                event.headers,
            )
            return True
        except ValueError:
            return False

    def clear_dead_letter_queue(self) -> int:
        """Clear the dead letter queue.

        Returns:
            Number of events cleared.
        """
        count = len(self._dead_letter_queue)
        self._dead_letter_queue.clear()
        return count

    def get_event_history(
        self,
        event_type_filter: Optional[str] = None,
        limit: int = 100,
    ) -> list[Event]:
        """Get event history.

        Args:
            event_type_filter: Filter by event type.
            limit: Maximum events to return.

        Returns:
            List of events (newest first).
        """
        events = self._event_history
        if event_type_filter:
            events = [e for e in events if e.event_type == event_type_filter]
        return events[-limit:][::-1]

    def get_stats(self) -> dict[str, Any]:
        """Get event bus statistics.

        Returns:
            Dictionary with stats.
        """
        total_subs = sum(len(subs) for subs in self._subscriptions.values())
        active_subs = sum(
            1 for subs in self._subscriptions.values()
            for s in subs if s.is_active
        )

        return {
            "total_patterns": len(self._subscriptions),
            "total_subscriptions": total_subs,
            "active_subscriptions": active_subs,
            "event_history_size": len(self._event_history),
            "dead_letter_size": len(self._dead_letter_queue),
            "wildcards_enabled": self._enable_wildcards,
        }
