"""Input buffer utilities for UI automation.

Provides utilities for buffering input events, managing
input queues, and batch processing of input operations.
"""

from __future__ import annotations

import threading
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Callable, Deque, Dict, List, Optional, Any


@dataclass
class InputEvent:
    """Represents a buffered input event."""
    event_type: str
    x: float
    y: float
    timestamp_ms: float
    data: Dict[str, Any] = field(default_factory=dict)
    priority: int = 0


class InputBuffer:
    """Buffers input events for batch processing.
    
    Collects input events and processes them in batches
    for improved efficiency.
    """
    
    def __init__(
        self,
        max_size: int = 100,
        flush_interval_ms: float = 100.0,
        processor: Optional[Callable[[List[InputEvent]], None]] = None
    ) -> None:
        """Initialize the input buffer.
        
        Args:
            max_size: Maximum buffer size before auto-flush.
            flush_interval_ms: Time interval for auto-flush.
            processor: Function to process batched events.
        """
        self.max_size = max_size
        self.flush_interval_ms = flush_interval_ms
        self.processor = processor
        self._buffer: Deque[InputEvent] = deque(maxlen=max_size)
        self._lock = threading.Lock()
        self._last_flush_time = time.time() * 1000
        self._is_processing = False
    
    def add(self, event: InputEvent) -> None:
        """Add an event to the buffer.
        
        Args:
            event: Event to add.
        """
        with self._lock:
            self._buffer.append(event)
    
    def add_many(self, events: List[InputEvent]) -> None:
        """Add multiple events to the buffer.
        
        Args:
            events: Events to add.
        """
        with self._lock:
            for event in events:
                self._buffer.append(event)
    
    def should_flush(self) -> bool:
        """Check if buffer should be flushed.
        
        Returns:
            True if should flush.
        """
        if not self._buffer:
            return False
        
        if len(self._buffer) >= self.max_size:
            return True
        
        current_time = time.time() * 1000
        if current_time - self._last_flush_time >= self.flush_interval_ms:
            return True
        
        return False
    
    def flush(self) -> List[InputEvent]:
        """Flush all events from buffer.
        
        Returns:
            List of flushed events.
        """
        with self._lock:
            if not self._buffer:
                return []
            
            events = list(self._buffer)
            self._buffer.clear()
            self._last_flush_time = time.time() * 1000
            
            if self.processor:
                self._is_processing = True
                try:
                    self.processor(events)
                finally:
                    self._is_processing = False
            
            return events
    
    def get_pending_count(self) -> int:
        """Get number of pending events.
        
        Returns:
            Number of pending events.
        """
        with self._lock:
            return len(self._buffer)
    
    def clear(self) -> None:
        """Clear all events from buffer."""
        with self._lock:
            self._buffer.clear()


class PriorityInputBuffer(InputBuffer):
    """Input buffer with priority support.
    
    Events are processed in priority order,
    with higher priority events processed first.
    """
    
    def __init__(
        self,
        max_size: int = 100,
        flush_interval_ms: float = 100.0,
        processor: Optional[Callable[[List[InputEvent]], None]] = None
    ) -> None:
        """Initialize the priority input buffer.
        
        Args:
            max_size: Maximum buffer size.
            flush_interval_ms: Time interval for auto-flush.
            processor: Function to process batched events.
        """
        super().__init__(max_size, flush_interval_ms, processor)
        self._priority_queue: Deque[InputEvent] = deque(maxlen=max_size)
    
    def add(self, event: InputEvent) -> None:
        """Add an event to the buffer.
        
        Args:
            event: Event to add.
        """
        if event.priority != 0:
            with self._lock:
                self._priority_queue.append(event)
        else:
            super().add(event)
    
    def flush(self) -> List[InputEvent]:
        """Flush events in priority order.
        
        Returns:
            List of flushed events.
        """
        with self._lock:
            all_events = list(self._buffer) + list(self._priority_queue)
            self._buffer.clear()
            self._priority_queue.clear()
            
            all_events.sort(key=lambda e: -e.priority)
            
            self._last_flush_time = time.time() * 1000
            
            if self.processor:
                self._is_processing = True
                try:
                    self.processor(all_events)
                finally:
                    self._is_processing = False
            
            return all_events


class InputQueue:
    """Thread-safe queue for input events.
    
    Provides a blocking queue for input events
    with timeout support.
    """
    
    def __init__(self, max_size: int = 0) -> None:
        """Initialize the input queue.
        
        Args:
            max_size: Maximum queue size (0 for unlimited).
        """
        self.max_size = max_size
        self._queue: Deque[InputEvent] = deque()
        self._lock = threading.Lock()
        self._not_empty = threading.Condition(self._lock)
        self._not_full = threading.Condition(self._lock)
    
    def put(self, event: InputEvent, timeout: Optional[float] = None) -> bool:
        """Put an event in the queue.
        
        Args:
            event: Event to add.
            timeout: Timeout in seconds (None for blocking).
            
        Returns:
            True if added successfully.
        """
        with self._not_full:
            if self.max_size > 0:
                if timeout is None:
                    while len(self._queue) >= self.max_size:
                        self._not_full.wait()
                elif timeout > 0:
                    end_time = time.time() + timeout
                    while len(self._queue) >= self.max_size:
                        remaining = end_time - time.time()
                        if remaining <= 0:
                            return False
                        self._not_full.wait(remaining)
            
            self._queue.append(event)
            self._not_empty.notify()
            return True
    
    def get(self, timeout: Optional[float] = None) -> Optional[InputEvent]:
        """Get an event from the queue.
        
        Args:
            timeout: Timeout in seconds (None for blocking).
            
        Returns:
            Event or None if timeout.
        """
        with self._not_empty:
            if timeout is None:
                while not self._queue:
                    self._not_empty.wait()
            else:
                end_time = time.time() + timeout
                while not self._queue:
                    remaining = end_time - time.time()
                    if remaining <= 0:
                        return None
                    self._not_empty.wait(remaining)
            
            event = self._queue.popleft()
            self._not_full.notify()
            return event
    
    def get_batch(
        self,
        max_count: int,
        timeout: Optional[float] = None
    ) -> List[InputEvent]:
        """Get multiple events from the queue.
        
        Args:
            max_count: Maximum number of events.
            timeout: Timeout in seconds.
            
        Returns:
            List of events.
        """
        events = []
        deadline = None if timeout is None else time.time() + timeout
        
        while len(events) < max_count:
            remaining = None
            if deadline is not None:
                remaining = deadline - time.time()
                if remaining <= 0:
                    break
            
            event = self.get(timeout=remaining)
            if event is None:
                break
            
            events.append(event)
        
        return events
    
    def size(self) -> int:
        """Get current queue size.
        
        Returns:
            Number of events in queue.
        """
        with self._lock:
            return len(self._queue)
    
    def is_empty(self) -> bool:
        """Check if queue is empty.
        
        Returns:
            True if empty.
        """
        with self._lock:
            return len(self._queue) == 0
    
    def clear(self) -> None:
        """Clear all events from queue."""
        with self._lock:
            self._queue.clear()
            self._not_full.notify_all()


class InputBatchProcessor:
    """Processes input events in batches.
    
    Collects events and invokes callback with batch
    when batch is ready.
    """
    
    def __init__(
        self,
        batch_size: int = 10,
        time_window_ms: float = 100.0,
        on_batch: Optional[Callable[[List[InputEvent]], None]] = None
    ) -> None:
        """Initialize the batch processor.
        
        Args:
            batch_size: Number of events to trigger batch.
            time_window_ms: Time window to trigger batch.
            on_batch: Callback when batch is ready.
        """
        self.batch_size = batch_size
        self.time_window_ms = time_window_ms
        self.on_batch = on_batch
        self._batch: List[InputEvent] = []
        self._batch_start_time: Optional[float] = None
        self._lock = threading.Lock()
    
    def add(self, event: InputEvent) -> Optional[List[InputEvent]]:
        """Add event and check if batch is ready.
        
        Args:
            event: Event to add.
            
        Returns:
            Completed batch or None.
        """
        with self._lock:
            if self._batch_start_time is None:
                self._batch_start_time = event.timestamp_ms
            
            self._batch.append(event)
            
            elapsed = event.timestamp_ms - self._batch_start_time
            
            if len(self._batch) >= self.batch_size or elapsed >= self.time_window_ms:
                return self._complete_batch()
            
            return None
    
    def _complete_batch(self) -> List[InputEvent]:
        """Complete current batch.
        
        Returns:
            The completed batch.
        """
        batch = self._batch
        self._batch = []
        self._batch_start_time = None
        
        if self.on_batch:
            self.on_batch(batch)
        
        return batch
    
    def flush(self) -> List[InputEvent]:
        """Flush any pending events.
        
        Returns:
            Flushed events or empty list.
        """
        with self._lock:
            if not self._batch:
                return []
            return self._complete_batch()
    
    def get_pending_count(self) -> int:
        """Get number of pending events.
        
        Returns:
            Number of pending events.
        """
        with self._lock:
            return len(self._batch)


class CoalescingInputBuffer(InputBuffer):
    """Input buffer that coalesces similar events.
    
    Combines multiple similar events into single events
    to reduce processing overhead.
    """
    
    def __init__(
        self,
        max_size: int = 100,
        flush_interval_ms: float = 100.0,
        processor: Optional[Callable[[List[InputEvent]], None]] = None,
        coalesce_keys: Optional[List[str]] = None
    ) -> None:
        """Initialize the coalescing input buffer.
        
        Args:
            max_size: Maximum buffer size.
            flush_interval_ms: Time interval for auto-flush.
            processor: Function to process batched events.
            coalesce_keys: Keys to use for coalescing.
        """
        super().__init__(max_size, flush_interval_ms, processor)
        self.coalesce_keys = coalesce_keys or ["event_type", "x", "y"]
        self._coalesced: Dict[str, InputEvent] = {}
    
    def flush(self) -> List[InputEvent]:
        """Flush events, coalescing similar ones.
        
        Returns:
            List of flushed events.
        """
        with self._lock:
            events_to_coalesce = list(self._buffer)
            self._buffer.clear()
            
            for event in events_to_coalesce:
                key = self._get_coalesce_key(event)
                
                if key in self._coalesced:
                    existing = self._coalesced[key]
                    existing.data["coalesce_count"] = existing.data.get(
                        "coalesce_count", 1
                    ) + 1
                    existing.timestamp_ms = event.timestamp_ms
                else:
                    self._coalesced[key] = event
                    self._buffer.append(event)
            
            self._last_flush_time = time.time() * 1000
            
            if self.processor:
                self._is_processing = True
                try:
                    self.processor(list(self._buffer))
                finally:
                    self._is_processing = False
            
            result = list(self._buffer)
            self._buffer.clear()
            self._coalesced.clear()
            
            return result
    
    def _get_coalesce_key(self, event: InputEvent) -> str:
        """Get key for coalescing.
        
        Args:
            event: Event to get key for.
            
        Returns:
            Coalesce key string.
        """
        parts = []
        for key in self.coalesce_keys:
            if key == "event_type":
                parts.append(event.event_type)
            elif key == "x":
                parts.append(str(int(event.x)))
            elif key == "y":
                parts.append(str(int(event.y)))
            elif key in event.data:
                parts.append(str(event.data[key]))
        
        return ":".join(parts)
