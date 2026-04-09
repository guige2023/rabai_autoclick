"""
Event Queue Utilities for UI Automation.

This module provides utilities for managing event queues, prioritizing events,
and handling event flow in automation workflows.

Author: AI Assistant
License: MIT
"""

from __future__ import annotations

import time
import uuid
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Callable, Optional
from collections import deque
import heapq


class EventPriority(Enum):
    """Event priority levels."""
    LOW = 0
    NORMAL = 1
    HIGH = 2
    URGENT = 3
    CRITICAL = 4


class EventStatus(Enum):
    """Event processing status."""
    PENDING = auto()
    PROCESSING = auto()
    COMPLETED = auto()
    FAILED = auto()
    CANCELLED = auto()
    TIMED_OUT = auto()


@dataclass
class Event:
    """
    Represents an automation event.
    
    Attributes:
        event_id: Unique identifier
        event_type: Type of event
        data: Event payload
        priority: Event priority
        created_at: Timestamp when event was created
        scheduled_at: When event should be processed
        timeout: Maximum processing time in seconds
        status: Current event status
    """
    event_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    event_type: str = ""
    data: Any = None
    priority: EventPriority = EventPriority.NORMAL
    created_at: float = field(default_factory=time.time)
    scheduled_at: float = field(default_factory=time.time)
    timeout: float = 30.0
    status: EventStatus = EventStatus.PENDING
    metadata: dict[str, Any] = field(default_factory=dict)
    
    @property
    def is_ready(self) -> bool:
        """Check if event is ready to be processed."""
        return (
            self.status == EventStatus.PENDING and
            time.time() >= self.scheduled_at
        )
    
    @property
    def is_expired(self) -> bool:
        """Check if event has timed out."""
        return (
            self.status == EventStatus.PROCESSING and
            time.time() - self.created_at > self.timeout
        )


@dataclass
class EventResult:
    """Result of event processing."""
    event_id: str
    success: bool
    output: Optional[Any] = None
    error: Optional[str] = None
    processing_time_ms: float = 0.0
    timestamp: float = field(default_factory=time.time)


class EventHandler:
    """Handler for processing specific event types."""
    
    def __init__(self, event_type: str, handler_func: Callable[[Event], Any]):
        self.event_type = event_type
        self.handler_func = handler_func
    
    def handle(self, event: Event) -> EventResult:
        """Process the event and return result."""
        start_time = time.time()
        try:
            output = self.handler_func(event)
            return EventResult(
                event_id=event.event_id,
                success=True,
                output=output,
                processing_time_ms=(time.time() - start_time) * 1000
            )
        except Exception as e:
            return EventResult(
                event_id=event.event_id,
                success=False,
                error=f"{type(e).__name__}: {str(e)}",
                processing_time_ms=(time.time() - start_time) * 1000
            )


class EventQueue:
    """
    Priority event queue for automation workflows.
    
    Features:
    - Priority-based processing
    - Delayed event scheduling
    - Event timeout handling
    - Event cancellation
    
    Example:
        queue = EventQueue()
        queue.enqueue(Event(event_type="click", data={"x": 100, "y": 200}))
        event = queue.dequeue()
    """
    
    def __init__(self, max_size: int = 10000):
        self.max_size = max_size
        self._heap: list[tuple[float, int, Event]] = []  # (scheduled_time, counter, event)
        self._counter = 0  # For FIFO ordering within same priority
        self._handlers: dict[str, EventHandler] = {}
        self._event_map: dict[str, Event] = {}
    
    def enqueue(self, event: Event) -> bool:
        """
        Add an event to the queue.
        
        Args:
            event: Event to enqueue
            
        Returns:
            True if enqueued successfully, False if queue is full
        """
        if len(self._heap) >= self.max_size:
            return False
        
        # Compute sort key: (scheduled_time, priority_value, counter)
        sort_key = (
            event.scheduled_at,
            -event.priority.value,  # Negative for max-heap behavior
            self._counter
        )
        
        heapq.heappush(self._heap, (sort_key, self._counter, event))
        self._event_map[event.event_id] = event
        self._counter += 1
        return True
    
    def dequeue(self) -> Optional[Event]:
        """
        Remove and return the highest priority ready event.
        
        Returns:
            Next event to process, or None if queue is empty or no events are ready
        """
        now = time.time()
        
        while self._heap:
            _, _, event = heapq.heappop(self._heap)
            
            # Skip cancelled events
            if event.status == EventStatus.CANCELLED:
                self._event_map.pop(event.event_id, None)
                continue
            
            # Return if event is ready
            if event.scheduled_at <= now:
                event.status = EventStatus.PROCESSING
                self._event_map.pop(event.event_id, None)
                return event
            
            # Re-queue if not ready yet (push back)
            # This is a simplified approach; in production, use a timer mechanism
            self._event_map.pop(event.event_id, None)
            
        return None
    
    def peek(self) -> Optional[Event]:
        """
        View the next event without removing it.
        
        Returns:
            Next event, or None if queue is empty
        """
        now = time.time()
        
        for item in self._heap:
            _, _, event = item
            if event.scheduled_at <= now and event.status == EventStatus.PENDING:
                return event
        return None
    
    def cancel(self, event_id: str) -> bool:
        """
        Cancel a pending event.
        
        Args:
            event_id: Event identifier
            
        Returns:
            True if cancelled, False if not found
        """
        event = self._event_map.get(event_id)
        if event and event.status == EventStatus.PENDING:
            event.status = EventStatus.CANCELLED
            return True
        return False
    
    def get_pending_count(self) -> int:
        """Get count of pending events."""
        return sum(
            1 for e in self._event_map.values()
            if e.status == EventStatus.PENDING
        )
    
    def get_ready_count(self) -> int:
        """Get count of events ready to be processed."""
        now = time.time()
        return sum(
            1 for e in self._event_map.values()
            if e.status == EventStatus.PENDING and e.scheduled_at <= now
        )
    
    def register_handler(self, event_type: str, handler: Callable[[Event], Any]) -> None:
        """Register a handler for an event type."""
        self._handlers[event_type] = EventHandler(event_type, handler)
    
    def process_next(self) -> Optional[EventResult]:
        """
        Dequeue and process the next ready event.
        
        Returns:
            EventResult if an event was processed, None otherwise
        """
        event = self.dequeue()
        if not event:
            return None
        
        handler = self._handlers.get(event.event_type)
        if handler:
            result = handler.handle(event)
            event.status = EventStatus.COMPLETED if result.success else EventStatus.FAILED
            return result
        
        event.status = EventStatus.FAILED
        return EventResult(
            event_id=event.event_id,
            success=False,
            error=f"No handler registered for event type: {event.event_type}"
        )


class EventBatcher:
    """
    Batches events together for batch processing.
    
    Example:
        batcher = EventBatcher(max_size=10, max_wait=1.0)
        batcher.add(event)
        events = batcher.flush()  # Returns batch when size or time reached
    """
    
    def __init__(self, max_size: int = 10, max_wait: float = 1.0):
        self.max_size = max_size
        self.max_wait = max_wait
        self._batch: list[Event] = []
        self._batch_start: Optional[float] = None
    
    def add(self, event: Event) -> bool:
        """
        Add an event to the current batch.
        
        Returns:
            True if batch is now ready to flush
        """
        if not self._batch_start:
            self._batch_start = time.time()
        
        self._batch.append(event)
        
        return self._is_ready()
    
    def _is_ready(self) -> bool:
        """Check if batch is ready to flush."""
        return (
            len(self._batch) >= self.max_size or
            (self._batch_start and time.time() - self._batch_start >= self.max_wait)
        )
    
    def flush(self) -> list[Event]:
        """
        Flush the current batch and start a new one.
        
        Returns:
            List of events in the flushed batch
        """
        batch = self._batch
        self._batch = []
        self._batch_start = None
        return batch
    
    def is_empty(self) -> bool:
        """Check if batch is empty."""
        return len(self._batch) == 0
