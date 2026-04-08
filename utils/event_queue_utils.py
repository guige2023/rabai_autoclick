"""
Event Queue Utilities

Provides utilities for queuing and processing
events in automation workflows.

Author: Agent3
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable
from collections import deque
import asyncio


@dataclass
class QueuedEvent:
    """An event in the queue."""
    event_type: str
    data: dict[str, Any]
    priority: int = 0
    timestamp: float = 0.0


class EventQueue:
    """
    Queue for managing and processing events.
    
    Supports priority-based ordering and
    asynchronous event processing.
    """

    def __init__(self, max_size: int = 1000) -> None:
        self._queue: deque[QueuedEvent] = deque(maxlen=max_size)
        self._processing = False
        self._handlers: dict[str, list[Callable[..., Any]]] = {}

    def enqueue(
        self,
        event_type: str,
        data: dict[str, Any] | None = None,
        priority: int = 0,
    ) -> bool:
        """
        Add an event to the queue.
        
        Args:
            event_type: Type of event.
            data: Event data.
            priority: Event priority (higher = processed first).
            
        Returns:
            True if event was enqueued.
        """
        if len(self._queue) >= self._queue.maxlen:
            return False
        import time
        event = QueuedEvent(
            event_type=event_type,
            data=data or {},
            priority=priority,
            timestamp=time.time(),
        )
        self._queue.append(event)
        return True

    def dequeue(self) -> QueuedEvent | None:
        """Remove and return the highest priority event."""
        if not self._queue:
            return None
        return self._queue.popleft()

    def peek(self) -> QueuedEvent | None:
        """View the next event without removing it."""
        if not self._queue:
            return None
        return self._queue[0]

    def size(self) -> int:
        """Get number of events in queue."""
        return len(self._queue)

    def is_empty(self) -> bool:
        """Check if queue is empty."""
        return len(self._queue) == 0

    def is_full(self) -> bool:
        """Check if queue is at capacity."""
        return len(self._queue) >= self._queue.maxlen

    def clear(self) -> None:
        """Clear all events from queue."""
        self._queue.clear()

    def register_handler(
        self,
        event_type: str,
        handler: Callable[[dict[str, Any]], Any],
    ) -> None:
        """Register a handler for an event type."""
        if event_type not in self._handlers:
            self._handlers[event_type] = []
        self._handlers[event_type].append(handler)

    async def process_next(self) -> Any | None:
        """Process the next event in queue."""
        event = self.dequeue()
        if event and event.event_type in self._handlers:
            for handler in self._handlers[event.event_type]:
                result = handler(event.data)
                if asyncio.iscoroutine(result):
                    result = await result
                return result
        return None

    async def process_all(self) -> list[Any]:
        """Process all events in queue."""
        results = []
        while not self.is_empty():
            result = await self.process_next()
            if result is not None:
                results.append(result)
        return results
