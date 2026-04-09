"""
Event Coalescing Utilities for UI Automation.

This module provides utilities for coalescing multiple similar
events into single events to reduce processing overhead.

Author: AI Assistant
License: MIT
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Callable, Any, Optional, Dict, List, Hashable
from enum import Enum
import threading


class CoalescingStrategy(Enum):
    """Event coalescing strategies."""
    LAST_ONLY = "last_only"
    FIRST_ONLY = "first_only"
    COUNT_BASED = "count_based"
    TIME_BASED = "time_based"
    MERGE_VALUES = "merge_values"


@dataclass
class CoalescedEvent:
    """Coalesced event result."""
    event_key: Hashable
    count: int
    last_value: Any
    first_timestamp: float
    last_timestamp: float
    merged_value: Optional[Any] = None


@dataclass
class CoalescingConfig:
    """Configuration for event coalescing."""
    strategy: CoalescingStrategy = CoalescingStrategy.LAST_ONLY
    time_window_ms: int = 100
    max_count: int = 100
    merge_func: Optional[Callable] = None


class EventCoalescer:
    """
    Coalesce multiple events into single events.
    """

    def __init__(self, config: Optional[CoalescingConfig] = None):
        """
        Initialize event coalescer.

        Args:
            config: Coalescing configuration
        """
        self.config = config or CoalescingConfig()
        self._pending: Dict[Hashable, Dict[str, Any]] = {}
        self._lock = threading.Lock()
        self._callbacks: Dict[Hashable, List[Callable]] = {}

    def add_event(
        self,
        key: Hashable,
        value: Any,
        timestamp: Optional[float] = None
    ) -> Optional[CoalescedEvent]:
        """
        Add an event for coalescing.

        Args:
            key: Event key
            value: Event value
            timestamp: Event timestamp (default: now)

        Returns:
            CoalescedEvent if coalescing occurred, None otherwise
        """
        timestamp = timestamp or time.time()

        with self._lock:
            if key not in self._pending:
                self._pending[key] = {
                    "count": 0,
                    "first_value": value,
                    "last_value": value,
                    "first_timestamp": timestamp,
                    "last_timestamp": timestamp,
                    "merged_value": value,
                }

            pending = self._pending[key]
            pending["count"] += 1
            pending["last_value"] = value
            pending["last_timestamp"] = timestamp

            if self.config.merge_func:
                pending["merged_value"] = self.config.merge_func(
                    pending.get("merged_value"), value
                )

            if self._should_coalesce(key):
                return self._emit(key)

            return None

    def _should_coalesce(self, key: Hashable) -> bool:
        """Check if event should be coalesced now."""
        pending = self._pending.get(key)
        if not pending:
            return False

        if self.config.strategy == CoalescingStrategy.LAST_ONLY:
            return True

        elif self.config.strategy == CoalescingStrategy.COUNT_BASED:
            return pending["count"] >= self.config.max_count

        elif self.config.strategy == CoalescingStrategy.TIME_BASED:
            elapsed_ms = (pending["last_timestamp"] - pending["first_timestamp"]) * 1000
            return elapsed_ms >= self.config.time_window_ms

        return False

    def _emit(self, key: Hashable) -> CoalescedEvent:
        """Emit coalesced event and clear pending."""
        pending = self._pending[key]

        result = CoalescedEvent(
            event_key=key,
            count=pending["count"],
            last_value=pending["last_value"],
            first_timestamp=pending["first_timestamp"],
            last_timestamp=pending["last_timestamp"],
            merged_value=pending.get("merged_value")
        )

        del self._pending[key]

        if key in self._callbacks:
            for callback in self._callbacks[key]:
                callback(result)

        return result

    def flush(self, key: Hashable) -> Optional[CoalescedEvent]:
        """
        Force flush pending events for a key.

        Args:
            key: Event key

        Returns:
            CoalescedEvent if pending events exist
        """
        with self._lock:
            if key in self._pending:
                return self._emit(key)
        return None

    def flush_all(self) -> List[CoalescedEvent]:
        """Flush all pending events."""
        results = []
        with self._lock:
            keys = list(self._pending.keys())
        for key in keys:
            result = self.flush(key)
            if result:
                results.append(result)
        return results

    def register_callback(self, key: Hashable, callback: Callable) -> None:
        """Register callback for coalesced events."""
        if key not in self._callbacks:
            self._callbacks[key] = []
        self._callbacks[key].append(callback)

    def get_pending_count(self, key: Hashable) -> int:
        """Get count of pending events for a key."""
        with self._lock:
            if key in self._pending:
                return self._pending[key]["count"]
        return 0


class MultiKeyCoalescer:
    """
    Manage multiple coalescers for different event types.
    """

    def __init__(self, default_config: Optional[CoalescingConfig] = None):
        """
        Initialize multi-key coalescer.

        Args:
            default_config: Default configuration
        """
        self.default_config = default_config or CoalescingConfig()
        self._coalescers: Dict[Hashable, EventCoalescer] = {}
        self._configs: Dict[Hashable, CoalescingConfig] = {}
        self._lock = threading.Lock()

    def add_event(
        self,
        key: Hashable,
        value: Any,
        timestamp: Optional[float] = None
    ) -> Optional[CoalescedEvent]:
        """Add event to appropriate coalescer."""
        with self._lock:
            if key not in self._coalescers:
                config = self._configs.get(key, self.default_config)
                self._coalescers[key] = EventCoalescer(config)

        return self._coalescers[key].add_event(key, value, timestamp)

    def get_config(self, key: Hashable) -> CoalescingConfig:
        """Get configuration for a key."""
        return self._configs.get(key, self.default_config)

    def set_config(self, key: Hashable, config: CoalescingConfig) -> None:
        """Set configuration for a key."""
        self._configs[key] = config


def merge_dict_values(existing: Optional[dict], new: dict) -> dict:
    """Merge dictionary values for coalescing."""
    if existing is None:
        return new.copy()
    result = existing.copy()
    result.update(new)
    return result


def merge_list_values(existing: Optional[list], new: list) -> list:
    """Merge list values for coalescing."""
    if existing is None:
        return new.copy()
    return existing + new


def coalesce_by_key(
    events: List[Dict[str, Any]],
    key_func: Callable[[Dict[str, Any]], Hashable],
    config: Optional[CoalescingConfig] = None
) -> List[CoalescedEvent]:
    """
    Coalesce a list of events by key.

    Args:
        events: List of event dictionaries
        key_func: Function to extract key from event
        config: Coalescing configuration

    Returns:
        List of coalesced events
    """
    coalescer = EventCoalescer(config)
    results = []

    for event in events:
        key = key_func(event)
        value = event.get("value", event)
        result = coalescer.add_event(key, value)
        if result:
            results.append(result)

    results.extend(coalescer.flush_all())
    return results
