"""
Event Stream Action Module.

Processes event streams with filtering, transformation, and aggregation.
"""

from __future__ import annotations

import asyncio
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Callable, Deque, Dict, List, Optional, Tuple


@dataclass
class Event:
    """A stream event."""
    event_type: str
    data: Dict[str, Any]
    timestamp: float = field(default_factory=time.time)
    source: str = ""
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class StreamConfig:
    """Configuration for event stream processing."""
    buffer_size: int = 1000
    flush_interval_seconds: float = 5.0
    max_batch_size: int = 100


FilterFunc = Callable[[Event], bool]
TransformFunc = Callable[[Event], Optional[Event]]


class EventStreamAction:
    """
    Event stream processing with buffering and batching.

    Supports filtering, transformation, and windowed aggregation.
    """

    def __init__(
        self,
        config: Optional[StreamConfig] = None,
    ) -> None:
        self.config = config or StreamConfig()
        self._buffer: Deque[Event] = deque(maxlen=self.config.buffer_size)
        self._filters: List[FilterFunc] = []
        self._transforms: List[TransformFunc] = []
        self._subscribers: Dict[str, List[Callable[[Event], None]]] = {}
        self._stats = {
            "total_received": 0,
            "total_processed": 0,
            "total_filtered": 0,
            "total_published": 0,
        }
        self._running = False
        self._flush_task: Optional[asyncio.Task] = None

    def add_filter(self, filter_func: FilterFunc) -> None:
        """Add a filter function."""
        self._filters.append(filter_func)

    def add_transform(self, transform_func: TransformFunc) -> None:
        """Add a transformation function."""
        self._transforms.append(transform_func)

    async def publish(
        self,
        event_type: str,
        data: Dict[str, Any],
        source: str = "",
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        Publish an event to the stream.

        Args:
            event_type: Type of event
            data: Event payload
            source: Event source
            metadata: Optional metadata
        """
        event = Event(
            event_type=event_type,
            data=data,
            timestamp=time.time(),
            source=source,
            metadata=metadata or {},
        )

        self._stats["total_received"] += 1

        filtered = await self._apply_filters(event)
        if filtered is None:
            self._stats["total_filtered"] += 1
            return

        transformed = await self._apply_transforms(filtered)
        if transformed is None:
            return

        self._buffer.append(transformed)
        self._stats["total_processed"] += 1

        await self._notify_subscribers(transformed)
        self._stats["total_published"] += 1

    async def _apply_filters(self, event: Event) -> Optional[Event]:
        """Apply all filters to event."""
        for f in self._filters:
            if not f(event):
                return None
        return event

    async def _apply_transforms(
        self,
        event: Event,
    ) -> Optional[Event]:
        """Apply all transforms to event."""
        result: Optional[Event] = event
        for t in self._transforms:
            result = t(result)
            if result is None:
                return None
        return result

    async def _notify_subscribers(self, event: Event) -> None:
        """Notify subscribers of event."""
        for event_type, subscribers in self._subscribers.items():
            if event_type == event.event_type or event_type == "*":
                for sub in subscribers:
                    try:
                        sub(event)
                    except Exception:
                        pass

    def subscribe(
        self,
        event_type: str,
        handler: Callable[[Event], None],
    ) -> None:
        """
        Subscribe to event type.

        Args:
            event_type: Event type to subscribe to (* for all)
            handler: Callback function
        """
        if event_type not in self._subscribers:
            self._subscribers[event_type] = []
        self._subscribers[event_type].append(handler)

    def unsubscribe(
        self,
        event_type: str,
        handler: Callable[[Event], None],
    ) -> bool:
        """Unsubscribe from event type."""
        if event_type in self._subscribers:
            try:
                self._subscribers[event_type].remove(handler)
                return True
            except ValueError:
                pass
        return False

    def get_batch(
        self,
        max_size: Optional[int] = None,
    ) -> List[Event]:
        """
        Get a batch of events from buffer.

        Args:
            max_size: Maximum batch size

        Returns:
            List of events
        """
        max_size = max_size or self.config.max_batch_size
        batch = []

        while self._buffer and len(batch) < max_size:
            batch.append(self._buffer.popleft())

        return batch

    def get_window(
        self,
        seconds: float,
        event_type: Optional[str] = None,
    ) -> List[Event]:
        """
        Get events within time window.

        Args:
            seconds: Window size in seconds
            event_type: Optional filter by type

        Returns:
            List of events in window
        """
        cutoff = time.time() - seconds
        result = []

        for event in self._buffer:
            if event.timestamp < cutoff:
                continue
            if event_type and event.event_type != event_type:
                continue
            result.append(event)

        return result

    def aggregate(
        self,
        event_type: str,
        window_seconds: float,
        aggregator: Callable[[List[Event]], Any],
    ) -> Any:
        """
        Aggregate events over a window.

        Args:
            event_type: Type to aggregate
            window_seconds: Window size
            aggregator: Aggregation function

        Returns:
            Aggregation result
        """
        events = self.get_window(window_seconds, event_type)
        return aggregator(events)

    def start_flush_task(self) -> None:
        """Start periodic flush task."""
        if self._running:
            return

        self._running = True
        self._flush_task = asyncio.create_task(self._flush_loop())

    async def _flush_loop(self) -> None:
        """Periodic flush loop."""
        while self._running:
            await asyncio.sleep(self.config.flush_interval_seconds)
            await self._flush()

    async def _flush(self) -> None:
        """Flush buffer (override in subclass)."""
        pass

    def stop(self) -> None:
        """Stop the stream processor."""
        self._running = False
        if self._flush_task:
            self._flush_task.cancel()
            self._flush_task = None

    def get_stats(self) -> Dict[str, Any]:
        """Get stream statistics."""
        return {
            **self._stats,
            "buffer_size": len(self._buffer),
            "buffer_max": self.config.buffer_size,
            "filter_count": len(self._filters),
            "transform_count": len(self._transforms),
            "subscriber_count": sum(len(s) for s in self._subscribers.values()),
        }

    def clear(self) -> None:
        """Clear the buffer."""
        self._buffer.clear()
