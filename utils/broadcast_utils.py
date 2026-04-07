"""
Broadcast Channel Utilities

Provides multi-subscriber broadcast channels for
one-way communication and event distribution.
"""

from __future__ import annotations

import asyncio
import copy
import threading
import time
import uuid
from abc import ABC, abstractmethod
from collections.abc import Callable
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Generic, TypeVar

T = TypeVar("T")


class ChannelState(Enum):
    """Channel states."""
    OPEN = auto()
    CLOSED = auto()
    PAUSED = auto()


@dataclass
class Subscriber(Generic[T]):
    """A channel subscriber."""
    id: str = field(default_factory=lambda: uuid.uuid4().hex[:8])
    callback: Callable[[T], None] | None = None
    async_callback: Callable[[T], Any] | None = None
    filter_func: Callable[[T], bool] | None = None
    name: str = ""
    active: bool = True
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class Message(Generic[T]):
    """A broadcast message."""
    id: str = field(default_factory=lambda: uuid.uuid4().hex)
    data: T
    timestamp: float = field(default_factory=time.time)
    source: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)


class Channel(ABC, Generic[T]):
    """Abstract channel interface."""

    @abstractmethod
    def send(self, message: T) -> int:
        """Send a message to all subscribers. Returns number of recipients."""
        pass

    @abstractmethod
    def subscribe(
        self,
        callback: Callable[[T], Any],
        filter_func: Callable[[T], bool] | None = None,
    ) -> str:
        """Subscribe to the channel. Returns subscription ID."""
        pass

    @abstractmethod
    def unsubscribe(self, subscriber_id: str) -> bool:
        """Unsubscribe from the channel."""
        pass

    @abstractmethod
    def close(self) -> None:
        """Close the channel."""
        pass


class BroadcastChannel(Channel[T]):
    """
    Thread-safe broadcast channel with multiple subscribers.
    """

    def __init__(self, name: str = ""):
        self.name = name or f"channel_{uuid.uuid4().hex[:8]}"
        self._subscribers: dict[str, Subscriber[T]] = {}
        self._state = ChannelState.OPEN
        self._lock = threading.RLock()
        self._message_history: list[Message[T]] = []
        self._max_history = 1000
        self._metrics: dict[str, int] = {
            "messages_sent": 0,
            "messages_delivered": 0,
            "subscribers_added": 0,
            "subscribers_removed": 0,
        }

    def send(self, message: T) -> int:
        """Send a message to all active subscribers."""
        with self._lock:
            if self._state != ChannelState.OPEN:
                return 0

            msg = Message(data=message, source=self.name)
            self._message_history.append(msg)
            if len(self._message_history) > self._max_history:
                self._message_history.pop(0)

            self._metrics["messages_sent"] += 1
            delivered = 0

            for sub in self._subscribers.values():
                if not sub.active:
                    continue

                if sub.filter_func and not sub.filter_func(message):
                    continue

                try:
                    if sub.callback:
                        sub.callback(message)
                    elif sub.async_callback:
                        pass  # Async handled separately
                    delivered += 1
                except Exception:
                    pass

            self._metrics["messages_delivered"] += delivered
            return delivered

    def subscribe(
        self,
        callback: Callable[[T], Any],
        filter_func: Callable[[T], bool] | None = None,
        name: str = "",
    ) -> str:
        """Subscribe to the channel."""
        with self._lock:
            subscriber = Subscriber(
                callback=callback,
                filter_func=filter_func,
                name=name or f"subscriber_{len(self._subscribers) + 1}",
            )
            self._subscribers[subscriber.id] = subscriber
            self._metrics["subscribers_added"] += 1
            return subscriber.id

    def unsubscribe(self, subscriber_id: str) -> bool:
        """Unsubscribe from the channel."""
        with self._lock:
            if subscriber_id in self._subscribers:
                del self._subscribers[subscriber_id]
                self._metrics["subscribers_removed"] += 1
                return True
            return False

    def close(self) -> None:
        """Close the channel."""
        with self._lock:
            self._state = ChannelState.CLOSED
            self._subscribers.clear()

    def pause(self) -> None:
        """Pause the channel."""
        with self._lock:
            self._state = ChannelState.PAUSED

    def resume(self) -> None:
        """Resume the channel."""
        with self._lock:
            self._state = ChannelState.OPEN

    @property
    def state(self) -> ChannelState:
        """Get channel state."""
        return self._state

    @property
    def subscriber_count(self) -> int:
        """Get number of active subscribers."""
        with self._lock:
            return sum(1 for s in self._subscribers.values() if s.active)

    def get_subscriber(self, subscriber_id: str) -> Subscriber[T] | None:
        """Get subscriber details."""
        with self._lock:
            return copy.deepcopy(self._subscribers.get(subscriber_id))

    def get_history(self, limit: int = 100) -> list[Message[T]]:
        """Get message history."""
        with self._lock:
            return list(self._message_history[-limit:])

    @property
    def metrics(self) -> dict[str, int]:
        """Get channel metrics."""
        return copy.copy(self._metrics)


class AsyncBroadcastChannel(Channel[T]):
    """
    Async-compatible broadcast channel.
    """

    def __init__(self, name: str = ""):
        self._channel = BroadcastChannel[T](name)
        self._async_subscribers: dict[str, Subscriber[T]] = {}
        self._lock = threading.RLock()

    async def send(self, message: T) -> int:
        """Send a message to all subscribers (sync + async)."""
        delivered = self._channel.send(message)

        async_subs = []
        with self._lock:
            for sub in self._async_subscribers.values():
                if sub.filter_func and not sub.filter_func(message):
                    continue
                async_subs.append(sub)

        for sub in async_subs:
            if sub.async_callback:
                try:
                    await sub.async_callback(message)
                except Exception:
                    pass

        return delivered

    def subscribe(
        self,
        callback: Callable[[T], Any],
        filter_func: Callable[[T], bool] | None = None,
        name: str = "",
    ) -> str:
        """Subscribe (sync callback)."""
        return self._channel.subscribe(callback, filter_func, name)

    async def subscribe_async(
        self,
        callback: Callable[[T], Any],
        filter_func: Callable[[T], bool] | None = None,
        name: str = "",
    ) -> str:
        """Subscribe with async callback."""
        with self._lock:
            subscriber = Subscriber(
                async_callback=callback,
                filter_func=filter_func,
                name=name or f"async_subscriber_{len(self._async_subscribers) + 1}",
            )
            self._async_subscribers[subscriber.id] = subscriber
            return subscriber.id

    def unsubscribe(self, subscriber_id: str) -> bool:
        """Unsubscribe."""
        with self._lock:
            if subscriber_id in self._async_subscribers:
                del self._async_subscribers[subscriber_id]
                return True
        return self._channel.unsubscribe(subscriber_id)

    def close(self) -> None:
        """Close the channel."""
        self._channel.close()

    @property
    def metrics(self) -> dict[str, int]:
        return self._channel.metrics


class ChannelManager:
    """
    Manager for multiple broadcast channels.
    """

    def __init__(self):
        self._channels: dict[str, BroadcastChannel] = {}
        self._lock = threading.RLock()

    def create_channel(self, name: str) -> BroadcastChannel:
        """Create a new channel."""
        with self._lock:
            if name in self._channels:
                return self._channels[name]

            channel = BroadcastChannel(name)
            self._channels[name] = channel
            return channel

    def get_channel(self, name: str) -> BroadcastChannel | None:
        """Get a channel by name."""
        with self._lock:
            return self._channels.get(name)

    def delete_channel(self, name: str) -> bool:
        """Delete a channel."""
        with self._lock:
            if name in self._channels:
                self._channels[name].close()
                del self._channels[name]
                return True
            return False

    def list_channels(self) -> list[str]:
        """List all channel names."""
        with self._lock:
            return list(self._channels.keys())

    def close_all(self) -> None:
        """Close all channels."""
        with self._lock:
            for channel in self._channels.values():
                channel.close()
            self._channels.clear()


# Global default channel manager
_default_manager: ChannelManager | None = None


def get_channel_manager() -> ChannelManager:
    """Get the default channel manager."""
    global _default_manager
    if _default_manager is None:
        _default_manager = ChannelManager()
    return _default_manager


def create_channel(name: str) -> BroadcastChannel:
    """Create a named channel."""
    return get_channel_manager().create_channel(name)
