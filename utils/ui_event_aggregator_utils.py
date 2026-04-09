"""UI event aggregation utilities for UI automation.

Provides utilities for aggregating multiple UI events into
coherent actions, batching events, and reducing event processing overhead.
"""

from __future__ import annotations

import time
from collections import deque
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional, Set, TypeVar, Deque


T = TypeVar('T')


@dataclass
class UIEvent:
    """Represents a UI event."""
    event_type: str
    element_id: str
    timestamp_ms: float
    data: Dict = field(default_factory=dict)
    source: str = "unknown"


@dataclass 
class AggregatedEvent:
    """Represents multiple aggregated UI events."""
    primary_event: UIEvent
    related_events: List[UIEvent] = field(default_factory=list)
    total_count: int = 1


@dataclass
class EventBatch:
    """A batch of UI events."""
    events: List[UIEvent]
    start_time_ms: float
    end_time_ms: float


class EventAggregator:
    """Aggregates related UI events into single actions.
    
    Reduces event processing overhead by combining rapid sequential
    events (e.g., multiple property changes) into a single event.
    """
    
    def __init__(
        self,
        time_window_ms: float = 100.0,
        max_batch_size: int = 50
    ) -> None:
        """Initialize the event aggregator.
        
        Args:
            time_window_ms: Time window for aggregating events.
            max_batch_size: Maximum number of events per batch.
        """
        self.time_window_ms = time_window_ms
        self.max_batch_size = max_batch_size
        self._pending_events: Deque[UIEvent] = deque()
        self._last_aggregate_time: float = 0.0
        self._handlers: Dict[str, Callable[[UIEvent], None]] = {}
    
    def add_event(self, event: UIEvent) -> Optional[List[UIEvent]]:
        """Add an event to the aggregator.
        
        Args:
            event: Event to add.
            
        Returns:
            List of aggregated events if batch is ready, None otherwise.
        """
        self._pending_events.append(event)
        
        if self._should_flush():
            return self.flush()
        
        return None
    
    def add_events(self, events: List[UIEvent]) -> Optional[List[UIEvent]]:
        """Add multiple events to the aggregator.
        
        Args:
            events: Events to add.
            
        Returns:
            List of aggregated events if batch is ready, None otherwise.
        """
        for event in events:
            self.add_event(event)
        
        if self._should_flush():
            return self.flush()
        
        return None
    
    def _should_flush(self) -> bool:
        """Check if the aggregator should flush.
        
        Returns:
            True if should flush, False otherwise.
        """
        if not self._pending_events:
            return False
        
        if len(self._pending_events) >= self.max_batch_size:
            return True
        
        oldest = self._pending_events[0]
        current_time = time.time() * 1000
        if current_time - oldest.timestamp_ms > self.time_window_ms:
            return True
        
        return False
    
    def flush(self) -> List[UIEvent]:
        """Flush all pending events.
        
        Returns:
            List of aggregated events.
        """
        if not self._pending_events:
            return []
        
        events = list(self._pending_events)
        self._pending_events.clear()
        
        aggregated = self._aggregate_events(events)
        self._last_aggregate_time = time.time() * 1000
        
        return aggregated
    
    def _aggregate_events(self, events: List[UIEvent]) -> List[UIEvent]:
        """Aggregate events by type and element.
        
        Args:
            events: Events to aggregate.
            
        Returns:
            List of aggregated events.
        """
        groups: Dict[str, List[UIEvent]] = {}
        
        for event in events:
            key = f"{event.event_type}:{event.element_id}"
            if key not in groups:
                groups[key] = []
            groups[key].append(event)
        
        result = []
        for group_events in groups.values():
            if len(group_events) == 1:
                result.append(group_events[0])
            else:
                aggregated = self._merge_events(group_events)
                result.append(aggregated)
        
        return result
    
    def _merge_events(self, events: List[UIEvent]) -> UIEvent:
        """Merge multiple events into one.
        
        Args:
            events: Events to merge.
            
        Returns:
            Merged event.
        """
        first = events[0]
        
        merged_data = dict(first.data)
        for event in events[1:]:
            for key, value in event.data.items():
                if key not in merged_data:
                    merged_data[key] = []
                if isinstance(merged_data[key], list):
                    merged_data[key].append(value)
                else:
                    merged_data[key] = [merged_data[key], value]
        
        return UIEvent(
            event_type=first.event_type,
            element_id=first.element_id,
            timestamp_ms=first.timestamp_ms,
            data={"merged": True, "count": len(events), "original_data": merged_data},
            source=first.source
        )
    
    def register_handler(
        self,
        event_type: str,
        handler: Callable[[UIEvent], None]
    ) -> None:
        """Register a handler for an event type.
        
        Args:
            event_type: Type of event to handle.
            handler: Handler function.
        """
        self._handlers[event_type] = handler
    
    def unregister_handler(self, event_type: str) -> None:
        """Unregister a handler for an event type.
        
        Args:
            event_type: Type of event to unregister.
        """
        self._handlers.pop(event_type, None)
    
    def process_pending(self) -> None:
        """Process all pending events through registered handlers."""
        while self._pending_events:
            event = self._pending_events.popleft()
            handler = self._handlers.get(event.event_type)
            if handler:
                handler(event)


class EventBatcher:
    """Batches UI events for efficient processing.
    
    Collects events over a time window and processes them
    as a batch, reducing overhead from individual event handling.
    """
    
    def __init__(
        self,
        batch_size: int = 100,
        time_window_ms: float = 500.0,
        processor: Optional[Callable[[List[UIEvent]], None]] = None
    ) -> None:
        """Initialize the event batcher.
        
        Args:
            batch_size: Maximum batch size.
            time_window_ms: Time window for batching.
            processor: Function to process batches.
        """
        self.batch_size = batch_size
        self.time_window_ms = time_window_ms
        self.processor = processor
        self._batch: Deque[UIEvent] = deque(maxlen=batch_size)
        self._batch_start_time: Optional[float] = None
    
    def add(self, event: UIEvent) -> Optional[List[UIEvent]]:
        """Add an event to the batch.
        
        Args:
            event: Event to add.
            
        Returns:
            Completed batch if ready, None otherwise.
        """
        if self._batch_start_time is None:
            self._batch_start_time = event.timestamp_ms
        
        self._batch.append(event)
        
        if self.is_ready():
            return self.complete_batch()
        
        return None
    
    def is_ready(self) -> bool:
        """Check if the batch is ready for processing.
        
        Returns:
            True if batch should be processed.
        """
        if len(self._batch) >= self.batch_size:
            return True
        
        if self._batch_start_time is None:
            return False
        
        current_time = time.time() * 1000
        if current_time - self._batch_start_time > self.time_window_ms:
            return True
        
        return False
    
    def complete_batch(self) -> Optional[List[UIEvent]]:
        """Complete the current batch.
        
        Returns:
            The completed batch or None if empty.
        """
        if not self._batch:
            return None
        
        batch = EventBatch(
            events=list(self._batch),
            start_time_ms=self._batch_start_time or 0,
            end_time_ms=self._batch[-1].timestamp_ms
        )
        
        self._batch.clear()
        self._batch_start_time = None
        
        if self.processor:
            self.processor(batch.events)
        
        return batch.events
    
    def get_pending_count(self) -> int:
        """Get the number of pending events.
        
        Returns:
            Number of pending events.
        """
        return len(self._batch)


class EventDeduplicator:
    """Deduplicates UI events based on type and content.
    
    Filters out duplicate events that arrive in rapid succession
    for the same element and event type.
    """
    
    def __init__(
        self,
        dedup_window_ms: float = 200.0,
        dedup_keys: Optional[Set[str]] = None
    ) -> None:
        """Initialize the event deduplicator.
        
        Args:
            dedup_window_ms: Time window for deduplication.
            dedup_keys: Keys to use for deduplication.
        """
        self.dedup_window_ms = dedup_window_ms
        self.dedup_keys = dedup_keys or {"event_type", "element_id"}
        self._seen_events: Deque[UIEvent] = deque(maxlen=1000)
    
    def is_duplicate(self, event: UIEvent) -> bool:
        """Check if an event is a duplicate.
        
        Args:
            event: Event to check.
            
        Returns:
            True if duplicate, False otherwise.
        """
        current_time = time.time() * 1000
        cutoff_time = current_time - self.dedup_window_ms
        
        while self._seen_events and self._seen_events[0].timestamp_ms < cutoff_time:
            self._seen_events.popleft()
        
        for seen in self._seen_events:
            if self._events_match(event, seen):
                return True
        
        self._seen_events.append(event)
        return False
    
    def _events_match(self, a: UIEvent, b: UIEvent) -> bool:
        """Check if two events match for deduplication.
        
        Args:
            a: First event.
            b: Second event.
            
        Returns:
            True if events match.
        """
        for key in self.dedup_keys:
            if key == "event_type":
                if a.event_type != b.event_type:
                    return False
            elif key == "element_id":
                if a.element_id != b.element_id:
                    return False
            else:
                if a.data.get(key) != b.data.get(key):
                    return False
        
        return True
    
    def filter(self, events: List[UIEvent]) -> List[UIEvent]:
        """Filter duplicates from a list of events.
        
        Args:
            events: Events to filter.
            
        Returns:
            Deduplicated events.
        """
        self._seen_events.clear()
        return [e for e in events if not self.is_duplicate(e)]
    
    def reset(self) -> None:
        """Reset the deduplicator."""
        self._seen_events.clear()


class EventThrottler:
    """Throttles UI events to a maximum rate.
    
    Ensures events are not processed faster than a specified rate,
    useful for rate-limiting rapid event streams.
    """
    
    def __init__(
        self,
        max_rate_per_second: float = 60.0,
        burst_allowance: int = 5
    ) -> None:
        """Initialize the event throttler.
        
        Args:
            max_rate_per_second: Maximum events per second.
            burst_allowance: Number of events allowed in burst.
        """
        self.max_rate_per_second = max_rate_per_second
        self.min_interval_ms = 1000.0 / max_rate_per_second
        self.burst_allowance = burst_allowance
        self._event_times: Deque[float] = deque(maxlen=burst_allowance)
        self._last_process_time: float = 0.0
    
    def should_process(self) -> bool:
        """Check if an event should be processed now.
        
        Returns:
            True if event should be processed.
        """
        current_time = time.time() * 1000
        
        while self._event_times and self._event_times[0] < current_time - 1000:
            self._event_times.popleft()
        
        if len(self._event_times) < self.burst_allowance:
            self._event_times.append(current_time)
            return True
        
        if current_time - self._last_process_time >= self.min_interval_ms:
            self._last_process_time = current_time
            return True
        
        return False
    
    def process(self, event: UIEvent) -> bool:
        """Process an event if throttling allows.
        
        Args:
            event: Event to process.
            
        Returns:
            True if processed, False if throttled.
        """
        if self.should_process():
            return True
        return False
    
    def wait_time_ms(self) -> float:
        """Get the time to wait before processing next event.
        
        Returns:
            Wait time in milliseconds.
        """
        current_time = time.time() * 1000
        elapsed = current_time - self._last_process_time
        return max(0, self.min_interval_ms - elapsed)
    
    def reset(self) -> None:
        """Reset the throttler."""
        self._event_times.clear()
        self._last_process_time = 0.0
