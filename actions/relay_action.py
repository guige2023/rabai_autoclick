"""
Relay Action Module.

Provides event/data relay pattern for forwarding data
between components with filtering and transformation.
"""

import time
import asyncio
import threading
from typing import Any, Callable, Dict, List, Optional, Set
from dataclasses import dataclass, field
from enum import Enum
from collections import defaultdict


class RelayMode(Enum):
    """Relay forwarding modes."""
    DIRECT = "direct"
    BROADCAST = "broadcast"
    ROUND_ROBIN = "round_robin"
    FIRST_MATCH = "first_match"


@dataclass
class RelayChannel:
    """A channel for relaying data."""
    name: str
    handlers: List[Callable] = field(default_factory=list)
    filters: List[Callable] = field(default_factory=list)
    transformers: List[Callable] = field(default_factory=list)
    enabled: bool = True


@dataclass
class RelayMessage:
    """Message being relayed."""
    channel: str
    data: Any
    timestamp: float = field(default_factory=time.time)
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class RelayStats:
    """Relay statistics."""
    messages_forwarded: int = 0
    messages_filtered: int = 0
    messages_error: int = 0
    channels: Dict[str, int] = field(default_factory=dict)


class RelayAction:
    """
    Action that relays data to multiple handlers.

    Example:
        relay = RelayAction("event_relay")
        relay.subscribe("events", handle_event)
        relay.send("events", {"type": "click", "x": 100})
    """

    def __init__(
        self,
        name: str,
        mode: RelayMode = RelayMode.DIRECT,
    ):
        self.name = name
        self.mode = mode
        self._channels: Dict[str, RelayChannel] = {}
        self._lock = threading.RLock()
        self._stats = RelayStats()
        self._round_robin_index: Dict[str, int] = defaultdict(int)

    def create_channel(self, name: str) -> RelayChannel:
        """Create a new relay channel."""
        with self._lock:
            channel = RelayChannel(name=name)
            self._channels[name] = channel
            return channel

    def subscribe(
        self,
        channel: str,
        handler: Callable,
        filter_fn: Optional[Callable] = None,
        transform_fn: Optional[Callable] = None,
    ) -> None:
        """Subscribe a handler to a channel."""
        with self._lock:
            if channel not in self._channels:
                self.create_channel(channel)

            ch = self._channels[channel]
            ch.handlers.append(handler)

            if filter_fn:
                ch.filters.append(filter_fn)

            if transform_fn:
                ch.transformers.append(transform_fn)

    def unsubscribe(self, channel: str, handler: Callable) -> bool:
        """Unsubscribe a handler from a channel."""
        with self._lock:
            if channel not in self._channels:
                return False

            ch = self._channels[channel]
            try:
                ch.handlers.remove(handler)
                return True
            except ValueError:
                return False

    def add_filter(self, channel: str, filter_fn: Callable) -> None:
        """Add a filter to a channel."""
        with self._lock:
            if channel in self._channels:
                self._channels[channel].filters.append(filter_fn)

    def add_transformer(self, channel: str, transform_fn: Callable) -> None:
        """Add a transformer to a channel."""
        with self._lock:
            if channel in self._channels:
                self._channels[channel].transformers.append(transform_fn)

    def _apply_filters(
        self,
        channel: RelayChannel,
        message: RelayMessage,
    ) -> bool:
        """Apply filters to a message."""
        for filter_fn in channel.filters:
            try:
                if not filter_fn(message):
                    self._stats.messages_filtered += 1
                    return False
            except Exception:
                pass
        return True

    def _apply_transformers(
        self,
        channel: RelayChannel,
        message: RelayMessage,
    ) -> Any:
        """Apply transformers to message data."""
        data = message.data
        for transformer in channel.transformers:
            try:
                data = transformer(data)
            except Exception:
                pass
        return data

    def _deliver_direct(
        self,
        channel: RelayChannel,
        message: RelayMessage,
    ) -> None:
        """Deliver to first matching handler."""
        for handler in channel.handlers:
            try:
                handler(message.data)
                self._stats.messages_forwarded += 1
                return
            except Exception:
                self._stats.messages_error += 1

    def _deliver_broadcast(
        self,
        channel: RelayChannel,
        message: RelayMessage,
    ) -> None:
        """Broadcast to all handlers."""
        for handler in channel.handlers:
            try:
                handler(message.data)
                self._stats.messages_forwarded += 1
            except Exception:
                self._stats.messages_error += 1

    def _deliver_round_robin(
        self,
        channel: RelayChannel,
        message: RelayMessage,
    ) -> None:
        """Deliver to handlers in round-robin order."""
        if not channel.handlers:
            return

        index = self._round_robin_index[channel.name] % len(channel.handlers)
        handler = channel.handlers[index]
        self._round_robin_index[channel.name] += 1

        try:
            handler(message.data)
            self._stats.messages_forwarded += 1
        except Exception:
            self._stats.messages_error += 1

    def _deliver_first_match(
        self,
        channel: RelayChannel,
        message: RelayMessage,
    ) -> None:
        """Deliver to first handler that accepts the message."""
        for handler in channel.handlers:
            try:
                if asyncio.iscoroutinefunction(handler):
                    asyncio.run(handler(message.data))
                else:
                    result = handler(message.data)
                self._stats.messages_forwarded += 1
                return
            except Exception:
                continue

        self._stats.messages_filtered += 1

    def send(
        self,
        channel: str,
        data: Any,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Send data to a channel."""
        with self._lock:
            if channel not in self._channels:
                return

            ch = self._channels[channel]
            if not ch.enabled:
                return

            message = RelayMessage(
                channel=channel,
                data=data,
                metadata=metadata or {},
            )

            if not self._apply_filters(ch, message):
                return

            message.data = self._apply_transformers(ch, message)

            if self.mode == RelayMode.DIRECT:
                self._deliver_direct(ch, message)
            elif self.mode == RelayMode.BROADCAST:
                self._deliver_broadcast(ch, message)
            elif self.mode == RelayMode.ROUND_ROBIN:
                self._deliver_round_robin(ch, message)
            elif self.mode == RelayMode.FIRST_MATCH:
                self._deliver_first_match(ch, message)

            self._stats.channels[channel] = (
                self._stats.channels.get(channel, 0) + 1
            )

    async def send_async(
        self,
        channel: str,
        data: Any,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Send data to a channel asynchronously."""
        await asyncio.sleep(0)
        self.send(channel, data, metadata)

    def broadcast(
        self,
        data: Any,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Broadcast data to all channels."""
        with self._lock:
            channels = list(self._channels.keys())

        for channel in channels:
            self.send(channel, data, metadata)

    def enable_channel(self, channel: str) -> None:
        """Enable a channel."""
        with self._lock:
            if channel in self._channels:
                self._channels[channel].enabled = True

    def disable_channel(self, channel: str) -> None:
        """Disable a channel."""
        with self._lock:
            if channel in self._channels:
                self._channels[channel].enabled = False

    def get_channels(self) -> List[str]:
        """Get list of channel names."""
        with self._lock:
            return list(self._channels.keys())

    def get_stats(self) -> Dict[str, Any]:
        """Get relay statistics."""
        return {
            "name": self.name,
            "mode": self.mode.value,
            "messages_forwarded": self._stats.messages_forwarded,
            "messages_filtered": self._stats.messages_filtered,
            "messages_error": self._stats.messages_error,
            "channels": self._stats.channels,
        }

    def reset_stats(self) -> None:
        """Reset statistics."""
        with self._lock:
            self._stats = RelayStats()
            self._round_robin_index.clear()
