"""
UI event detection and filtering utilities.

Provides event filtering, deduplication, throttling,
and pattern detection for UI automation events.

Author: Auto-generated
"""

from __future__ import annotations

import time
from collections import deque
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Callable, Any


class EventType(Enum):
    """UI event types."""
    CLICK = auto()
    DOUBLE_CLICK = auto()
    LONG_PRESS = auto()
    HOVER = auto()
    FOCUS = auto()
    BLUR = auto()
    KEY_DOWN = auto()
    KEY_UP = auto()
    SCROLL = auto()
    DRAG = auto()
    CUSTOM = auto()


@dataclass
class UIEvent:
    """A UI event with metadata."""
    event_type: EventType
    timestamp: float
    x: float = 0
    y: float = 0
    key: str = ""
    target: str = ""
    metadata: dict = field(default_factory=dict)
    
    def age_ms(self) -> float:
        """Age of event in milliseconds."""
        return (time.time() - self.timestamp) * 1000


@dataclass
class EventFilter:
    """Filter criteria for events."""
    event_types: list[EventType] | None = None
    targets: list[str] | None = None
    min_interval_ms: float = 0
    dedup_window_ms: float = 0


class EventDeduplicator:
    """
    Deduplicates events based on type and position.
    
    Example:
        dedup = EventDeduplicator(window_ms=500)
        filtered = dedup.filter(events)
    """
    
    def __init__(self, window_ms: float = 500):
        self._window_ms = window_ms
        self._seen: deque[UIEvent] = deque(maxlen=1000)
    
    def filter(self, events: list[UIEvent]) -> list[UIEvent]:
        """
        Filter out duplicate events.
        
        Args:
            events: List of events to filter
            
        Returns:
            List of events with duplicates removed
        """
        result = []
        cutoff = time.time() - (self._window_ms / 1000)
        
        for event in events:
            if event.timestamp < cutoff:
                continue
            
            if not self._is_duplicate(event):
                result.append(event)
                self._seen.append(event)
        
        return result
    
    def _is_duplicate(self, event: UIEvent) -> bool:
        """Check if event is a duplicate of a recent event."""
        for seen in self._seen:
            if seen.event_type != event.event_type:
                continue
            if seen.timestamp < event.timestamp - (self._window_ms / 1000):
                continue
            
            # Check position similarity
            dx = abs(seen.x - event.x)
            dy = abs(seen.y - event.y)
            if dx < 5 and dy < 5:
                return True
        
        return False
    
    def reset(self) -> None:
        """Clear deduplication history."""
        self._seen.clear()


class EventThrottler:
    """
    Throttles event processing to a maximum rate.
    
    Example:
        throttler = EventThrottler(max_per_second=10)
        throttled = throttler.throttle(events)
    """
    
    def __init__(self, max_per_second: float = 10):
        self._max_per_second = max_per_second
        self._min_interval = 1.0 / max_per_second
        self._last_event_time: dict[EventType, float] = {}
    
    def should_process(self, event: UIEvent) -> bool:
        """
        Check if event should be processed.
        
        Args:
            event: Event to check
            
        Returns:
            True if event should be processed
        """
        last_time = self._last_event_time.get(event.event_type, 0)
        now = time.time()
        
        if now - last_time >= self._min_interval:
            self._last_event_time[event.event_type] = now
            return True
        
        return False
    
    def throttle(
        self,
        events: list[UIEvent],
    ) -> list[UIEvent]:
        """
        Filter events to maximum rate.
        
        Args:
            events: Events to throttle
            
        Returns:
            Filtered list of events
        """
        return [e for e in events if self.should_process(e)]
    
    def reset(self) -> None:
        """Reset throttling state."""
        self._last_event_time.clear()


class EventPatternDetector:
    """
    Detects patterns in event sequences.
    
    Example:
        detector = EventPatternDetector()
        detector.register_pattern("double_click", [EventType.CLICK, EventType.CLICK])
        patterns = detector.detect(event_sequence)
    """
    
    def __init__(self):
        self._patterns: dict[str, list[EventType]] = {}
        self._handlers: dict[str, Callable[[list[UIEvent]], None]] = {}
    
    def register_pattern(
        self,
        name: str,
        sequence: list[EventType],
    ) -> None:
        """
        Register an event pattern to detect.
        
        Args:
            name: Pattern name
            sequence: Sequence of event types
        """
        self._patterns[name] = sequence
    
    def register_handler(
        self,
        name: str,
        handler: Callable[[list[UIEvent]], None],
    ) -> None:
        """Register handler for pattern match."""
        self._handlers[name] = handler
    
    def detect(
        self,
        events: list[UIEvent],
        window_ms: float = 1000,
    ) -> list[tuple[str, list[UIEvent]]]:
        """
        Detect patterns in event sequence.
        
        Args:
            events: Event sequence
            window_ms: Time window for pattern matching
            
        Returns:
            List of (pattern_name, matched_events) tuples
        """
        results = []
        cutoff = time.time() - (window_ms / 1000)
        
        recent = [e for e in events if e.timestamp >= cutoff]
        
        for name, pattern in self._patterns.items():
            matches = self._find_pattern(recent, pattern)
            if matches:
                results.append((name, matches))
                if name in self._handlers:
                    self._handlers[name](matches)
        
        return results
    
    def _find_pattern(
        self,
        events: list[UIEvent],
        pattern: list[EventType],
    ) -> list[UIEvent] | None:
        """Find pattern in event sequence."""
        if len(events) < len(pattern):
            return None
        
        for i in range(len(events) - len(pattern) + 1):
            window = events[i:i + len(pattern)]
            if all(e.event_type == p for e, p in zip(window, pattern)):
                return window
        
        return None


class EventAccumulator:
    """
    Accumulates events over time for batch processing.
    
    Example:
        accumulator = EventAccumulator(max_events=100, max_age_ms=500)
        accumulator.add(event)
        batch = accumulator.flush()
    """
    
    def __init__(self, max_events: int = 100, max_age_ms: float = 500):
        self._events: deque[UIEvent] = deque(maxlen=max_events)
        self._max_events = max_events
        self._max_age_ms = max_age_ms
        self._last_flush = time.time()
    
    def add(self, event: UIEvent) -> None:
        """Add an event."""
        self._events.append(event)
    
    def should_flush(self) -> bool:
        """Check if accumulated events should be flushed."""
        if len(self._events) >= self._max_events:
            return True
        
        if not self._events:
            return False
        
        age_ms = self._events[0].age_ms()
        if age_ms >= self._max_age_ms:
            return True
        
        return False
    
    def flush(self) -> list[UIEvent]:
        """
        Flush and return accumulated events.
        
        Returns:
            List of accumulated events
        """
        events = list(self._events)
        self._events.clear()
        self._last_flush = time.time()
        return events
    
    def clear(self) -> None:
        """Clear accumulated events without returning."""
        self._events.clear()
    
    def get_events(self) -> list[UIEvent]:
        """Get events without flushing."""
        return list(self._events)


def filter_events(
    events: list[UIEvent],
    filter_config: EventFilter,
) -> list[UIEvent]:
    """
    Apply filter configuration to event list.
    
    Args:
        events: Events to filter
        filter_config: Filter configuration
        
    Returns:
        Filtered events
    """
    result = events
    
    if filter_config.event_types:
        result = [e for e in result if e.event_type in filter_config.event_types]
    
    if filter_config.targets:
        result = [e for e in result if e.target in filter_config.targets]
    
    if filter_config.min_interval_ms > 0:
        filtered: list[UIEvent] = []
        last_times: dict[EventType, float] = {}
        
        for event in result:
            last = last_times.get(event.event_type, 0)
            age_ms = event.age_ms()
            
            if age_ms - last >= filter_config.min_interval_ms:
                filtered.append(event)
                last_times[event.event_type] = age_ms
        
        result = filtered
    
    return result
