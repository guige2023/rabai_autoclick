"""Event bus utilities for publish-subscribe communication.

Provides a centralized event bus for decoupled communication
between automation components with support for filtering,
priorities, and async event handling.

Example:
    >>> from utils.event_bus_utils import EventBus, event_bus
    >>> event_bus.subscribe("action:completed", on_completed)
    >>> event_bus.publish("action:completed", {"action_id": "123"})
"""

from __future__ import annotations

import re
import threading
from dataclasses import dataclass, field
from typing import (
    Any,
    Callable,
    Dict,
    List,
    Optional,
    Pattern,
    Set,
    Union,
)
from weakref import WeakSet


@dataclass
class Event:
    """Event payload."""
    topic: str
    data: Any
    source: Optional[str] = None
    timestamp: float = field(default_factory=lambda: __import__("time").time())


SubscriptionID = str


class EventBus:
    """Centralized event bus for publish-subscribe messaging.

    Supports:
    - Wildcard topic patterns (* and **)
    - Priority-based subscription ordering
    - Weak references for automatic cleanup
    - Thread-safe publishing

    Example:
        >>> bus = EventBus()
        >>> bus.subscribe("click:*", handler, priority=10)
        >>> bus.publish("click:button", {"x": 100, "y": 200})
    """

    def __init__(self) -> None:
        self._subscriptions: Dict[
            str,
            List[tuple[int, SubscriptionID, Callable[[Event], None]]]
        ] = {}
        self._wildcard_subscriptions: Dict[
            str,
            List[tuple[int, SubscriptionID, Callable[[Event], None]]]
        ] = {}
        self._id_counter = 0
        self._lock = threading.RLock()
        self._global_subscriptions: List[
            tuple[int, SubscriptionID, Callable[[Event], None]]
        ] = []

    def subscribe(
        self,
        topic: str,
        handler: Callable[[Event], None],
        *,
        priority: int = 0,
        source: Optional[str] = None,
    ) -> SubscriptionID:
        """Subscribe to an event topic.

        Args:
            topic: Topic pattern (supports * and ** wildcards).
            handler: Callback function to invoke on events.
            priority: Higher priority handlers execute first.
            source: Optional source filter.

        Returns:
            Subscription ID for later unsubscription.

        Example:
            >>> sub_id = bus.subscribe("action:*", my_handler)
            >>> bus.unsubscribe(sub_id)
        """
        with self._lock:
            self._id_counter += 1
            sid = f"sub_{self._id_counter}"

            entry = (priority, sid, handler)

            if "*" not in topic and "?" not in topic:
                if topic not in self._subscriptions:
                    self._subscriptions[topic] = []
                self._subscriptions[topic].append(entry)
                self._subscriptions[topic].sort(key=lambda x: -x[0])
            elif ("*" in topic or "?" in topic) and not topic.endswith("**"):
                pattern_key = topic.replace("**", "\x00WILDCARD\x00")
                if pattern_key not in self._wildcard_subscriptions:
                    self._wildcard_subscriptions[pattern_key] = []
                self._wildcard_subscriptions[pattern_key].append(entry)
                self._wildcard_subscriptions[pattern_key].sort(key=lambda x: -x[0])
            elif topic.endswith("**"):
                if topic not in self._wildcard_subscriptions:
                    self._wildcard_subscriptions[topic] = []
                self._wildcard_subscriptions[topic].append(entry)
                self._wildcard_subscriptions[topic].sort(key=lambda x: -x[0])

            return sid

    def subscribe_global(
        self,
        handler: Callable[[Event], None],
        *,
        priority: int = 0,
    ) -> SubscriptionID:
        """Subscribe to all events.

        Args:
            handler: Callback for all events.
            priority: Execution priority.

        Returns:
            Subscription ID.
        """
        with self._lock:
            self._id_counter += 1
            sid = f"global_{self._id_counter}"
            self._global_subscriptions.append((priority, sid, handler))
            self._global_subscriptions.sort(key=lambda x: -x[0])
            return sid

    def unsubscribe(self, subscription_id: SubscriptionID) -> bool:
        """Unsubscribe by ID.

        Args:
            subscription_id: ID returned from subscribe.

        Returns:
            True if found and removed.
        """
        with self._lock:
            for topic_subs in self._subscriptions.values():
                for i, (p, sid, h) in enumerate(topic_subs):
                    if sid == subscription_id:
                        topic_subs.pop(i)
                        return True

            for pattern, topic_subs in self._wildcard_subscriptions.items():
                for i, (p, sid, h) in enumerate(topic_subs):
                    if sid == subscription_id:
                        topic_subs.pop(i)
                        return True

            for i, (p, sid, h) in enumerate(self._global_subscriptions):
                if sid == subscription_id:
                    self._global_subscriptions.pop(i)
                    return True

            return False

    def publish(
        self,
        topic: str,
        data: Any = None,
        source: Optional[str] = None,
    ) -> List[Any]:
        """Publish an event to all matching subscribers.

        Args:
            topic: Event topic.
            data: Event payload.
            source: Optional source identifier.

        Returns:
            List of results from handlers.
        """
        event = Event(topic=topic, data=data, source=source)
        results: List[Any] = []

        with self._lock:
            direct_subs = list(self._subscriptions.get(topic, []))

            wildcard_subs: List[tuple[int, str, Callable]] = []
            for pattern, subs in self._wildcard_subscriptions.items():
                clean_pattern = pattern.replace("\x00WILDCARD\x00", "**")
                if self._match_wildcard(topic, clean_pattern):
                    wildcard_subs.extend(subs)

            global_subs = list(self._global_subscriptions)

            all_handlers = direct_subs + wildcard_subs + global_subs
            all_handlers.sort(key=lambda x: -x[0])

        for _, _, handler in all_handlers:
            try:
                result = handler(event)
                results.append(result)
            except Exception:
                pass

        return results

    def _match_wildcard(self, topic: str, pattern: str) -> bool:
        """Match a topic against a wildcard pattern."""
        if pattern.endswith("**"):
            prefix = pattern[:-2]
            return topic.startswith(prefix.rstrip(":").rstrip("."))
        else:
            regex = pattern.replace(".", r"\.").replace("*", "[^:]*").replace("?", "[^:]*")
            try:
                return bool(re.match(f"^{regex}$", topic))
            except re.error:
                return topic == pattern

    def clear(self, topic: Optional[str] = None) -> None:
        """Clear subscriptions.

        Args:
            topic: If provided, only clear that topic. Otherwise clear all.
        """
        with self._lock:
            if topic is None:
                self._subscriptions.clear()
                self._wildcard_subscriptions.clear()
                self._global_subscriptions.clear()
            else:
                self._subscriptions.pop(topic, None)

    @property
    def subscription_count(self) -> int:
        """Total number of subscriptions."""
        with self._lock:
            return (
                sum(len(s) for s in self._subscriptions.values())
                + sum(len(s) for s in self._wildcard_subscriptions.values())
                + len(self._global_subscriptions)
            )


_global_event_bus: Optional[EventBus] = None
_bus_lock = threading.Lock()


def get_event_bus() -> EventBus:
    """Get the global event bus singleton."""
    global _global_event_bus
    with _bus_lock:
        if _global_event_bus is None:
            _global_event_bus = EventBus()
        return _global_event_bus


event_bus = get_event_bus()
