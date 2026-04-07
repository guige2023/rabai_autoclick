"""
Observer Pattern & Pub/Sub Implementation

Provides observer pattern with support for:
- Subject-observer relationships
- Pub/sub messaging
- Event filtering and routing
- Async observer support
"""

from __future__ import annotations

import asyncio
import copy
import time
import uuid
from abc import ABC, abstractmethod
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Generic, TypeVar

T = TypeVar("T")


class EventPriority(Enum):
    """Priority levels for event handling."""
    LOW = auto()
    NORMAL = auto()
    HIGH = auto()
    CRITICAL = auto()


@dataclass
class Event:
    """Base event class."""
    id: str = field(default_factory=lambda: uuid.uuid4().hex[:8])
    timestamp: float = field(default_factory=time.time)
    topic: str = ""
    data: Any = None
    source: str = ""
    priority: EventPriority = EventPriority.NORMAL

    def __repr__(self) -> str:
        return f"<Event {self.topic} [{self.priority.name}] @{self.timestamp:.2f}>"


@dataclass
class Subscription:
    """Subscription details for an observer."""
    id: str = field(default_factory=lambda: uuid.uuid4().hex[:8])
    topic: str = ""
    callback: Callable[..., Any] | None = None
    priority: EventPriority = EventPriority.NORMAL
    filter_func: Callable[[Event], bool] | None = None
    once: bool = False
    active: bool = True

    def matches(self, event: Event) -> bool:
        """Check if this subscription matches an event."""
        if not self.active:
            return False
        if self.topic and self.topic != event.topic and self.topic != "*":
            return False
        if self.filter_func and not self.filter_func(event):
            return False
        return True


class Observer(ABC):
    """Abstract observer interface."""

    @abstractmethod
    def update(self, event: Event) -> None:
        """Called when an observed event occurs."""
        pass


class Subject:
    """
    Subject class that maintains a list of observers
    and notifies them of state changes.
    """

    def __init__(self):
        self._observers: dict[str, Observer] = {}
        self._subscriptions: dict[str, list[Subscription]] = {}
        self._lock: asyncio.Lock | None = None
        self._event_history: list[Event] = []
        self._max_history: int = 100

    def attach(self, observer: Observer, topic: str = "*") -> str:
        """
        Attach an observer to this subject.

        Args:
            observer: The observer to attach.
            topic: The topic to subscribe to (default: all topics).

        Returns:
            The subscription ID.
        """
        subscription = Subscription(topic=topic, callback=None)
        self._observers[subscription.id] = observer

        if topic not in self._subscriptions:
            self._subscriptions[topic] = []
        self._subscriptions[topic].append(subscription)

        return subscription.id

    def detach(self, observer: Observer | str) -> bool:
        """
        Detach an observer from this subject.

        Args:
            observer: The observer or subscription ID to detach.

        Returns:
            True if detachment was successful.
        """
        if isinstance(observer, str):
            # Detach by subscription ID
            if observer in self._observers:
                sub = self._observers[observer]
                for topic_subs in self._subscriptions.values():
                    if sub in topic_subs:
                        topic_subs.remove(sub)
                del self._observers[observer]
                return True
            return False

        # Detach by observer instance
        for obs_id, obs in list(self._observers.items()):
            if obs is observer:
                del self._observers[obs_id]
                for topic_subs in self._subscriptions.values():
                    if obs in [s for s in topic_subs]:
                        pass
                return True
        return False

    def subscribe(
        self,
        topic: str,
        callback: Callable[[Event], Any],
        priority: EventPriority = EventPriority.NORMAL,
        filter_func: Callable[[Event], bool] | None = None,
        once: bool = False,
    ) -> str:
        """
        Subscribe to events on a topic.

        Args:
            topic: The topic to subscribe to.
            callback: Function to call when event occurs.
            priority: Event priority level.
            filter_func: Optional filter function.
            once: If True, unsubscribe after first event.

        Returns:
            The subscription ID.
        """
        subscription = Subscription(
            topic=topic,
            callback=callback,
            priority=priority,
            filter_func=filter_func,
            once=once,
        )

        if topic not in self._subscriptions:
            self._subscriptions[topic] = []
        self._subscriptions[topic].append(subscription)
        self._observers[subscription.id] = subscription  # type: ignore

        return subscription.id

    def unsubscribe(self, subscription_id: str) -> bool:
        """Unsubscribe by subscription ID."""
        if subscription_id not in self._observers:
            return False

        sub = self._observers[subscription_id]
        if isinstance(sub, Subscription):
            for topic_subs in self._subscriptions.values():
                if sub in topic_subs:
                    topic_subs.remove(sub)
        del self._observers[subscription_id]
        return True

    def notify(self, event: Event | None = None, topic: str = "", data: Any = None) -> None:
        """
        Notify all observers subscribed to the event topic.

        Args:
            event: The event to notify about.
            topic: Alternative: notify by topic string.
            data: Alternative: notify with data.
        """
        if event is None:
            event = Event(topic=topic, data=data)

        # Store in history
        self._event_history.append(event)
        if len(self._event_history) > self._max_history:
            self._event_history.pop(0)

        # Get matching subscriptions
        subs = self._subscriptions.get(event.topic, [])
        subs.extend(self._subscriptions.get("*", []))

        # Sort by priority
        subs = sorted(subs, key=lambda s: s.priority.value, reverse=True)

        for sub in subs:
            if sub.matches(event):
                try:
                    if sub.callback:
                        sub.callback(event)
                    else:
                        # It's an observer
                        obs = self._observers.get(sub.id)
                        if isinstance(obs, Observer):
                            obs.update(event)
                except Exception:
                    pass  # Swallow observer exceptions

                if sub.once:
                    self.unsubscribe(sub.id)

    def get_subscription(self, subscription_id: str) -> Subscription | None:
        """Get a subscription by ID."""
        obs = self._observers.get(subscription_id)
        return obs if isinstance(obs, Subscription) else None

    def get_event_history(self, topic: str | None = None, limit: int = 10) -> list[Event]:
        """Get recent events, optionally filtered by topic."""
        events = self._event_history
        if topic:
            events = [e for e in events if e.topic == topic]
        return events[-limit:]

    def clear_history(self) -> None:
        """Clear event history."""
        self._event_history.clear()


class PubSub:
    """
    Pub/Sub message broker for decoupled communication.
    """

    def __init__(self, max_history: int = 1000):
        self._subjects: dict[str, Subject] = {}
        self._max_history = max_history
        self._global_history: list[Event] = []
        self._metrics: dict[str, int] = {}

    def get_or_create_subject(self, channel: str) -> Subject:
        """Get or create a subject for a channel."""
        if channel not in self._subjects:
            self._subjects[channel] = Subject()
        return self._subjects[channel]

    def publish(
        self,
        channel: str,
        data: Any = None,
        topic: str = "",
        priority: EventPriority = EventPriority.NORMAL,
    ) -> Event:
        """
        Publish an event to a channel.

        Args:
            channel: The channel to publish to.
            data: Event data.
            topic: Event topic within the channel.
            priority: Event priority.

        Returns:
            The published event.
        """
        event = Event(
            topic=topic or channel,
            data=data,
            source=channel,
            priority=priority,
        )

        self.get_or_create_subject(channel).notify(event)
        self._global_history.append(event)

        if len(self._global_history) > self._max_history:
            self._global_history.pop(0)

        self._metrics[channel] = self._metrics.get(channel, 0) + 1

        return event

    def subscribe(
        self,
        channel: str,
        callback: Callable[[Event], Any],
        topic: str = "*",
        priority: EventPriority = EventPriority.NORMAL,
        filter_func: Callable[[Event], bool] | None = None,
        once: bool = False,
    ) -> str:
        """
        Subscribe to a channel.

        Returns:
            Subscription ID.
        """
        subject = self.get_or_create_subject(channel)
        return subject.subscribe(
            topic=topic,
            callback=callback,
            priority=priority,
            filter_func=filter_func,
            once=once,
        )

    def unsubscribe(self, channel: str, subscription_id: str) -> bool:
        """Unsubscribe from a channel."""
        subject = self._subjects.get(channel)
        if subject:
            return subject.unsubscribe(subscription_id)
        return False

    def get_channels(self) -> list[str]:
        """List all channels."""
        return list(self._subjects.keys())

    def get_metrics(self) -> dict[str, int]:
        """Get publication metrics by channel."""
        return copy.copy(self._metrics)

    def get_global_history(self, limit: int = 100) -> list[Event]:
        """Get global event history."""
        return self._global_history[-limit:]


# Global default pub/sub instance
_default_pubsub: PubSub | None = None


def get_pubsub() -> PubSub:
    """Get the default global pub/sub instance."""
    global _default_pubsub
    if _default_pubsub is None:
        _default_pubsub = PubSub()
    return _default_pubsub


def publish(channel: str, data: Any = None, topic: str = "") -> Event:
    """Publish to the default pub/sub instance."""
    return get_pubsub().publish(channel=channel, data=data, topic=topic)


def subscribe(
    channel: str,
    callback: Callable[[Event], Any],
    topic: str = "*",
    **kwargs: Any,
) -> str:
    """Subscribe on the default pub/sub instance."""
    return get_pubsub().subscribe(channel=channel, callback=callback, topic=topic, **kwargs)


def unsubscribe(channel: str, subscription_id: str) -> bool:
    """Unsubscribe from the default pub/sub instance."""
    return get_pubsub().unsubscribe(channel=channel, subscription_id=subscription_id)
