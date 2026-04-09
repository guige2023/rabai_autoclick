"""
Data Timeline Action Module.

Provides temporal data modeling with event ordering,
time-window aggregation, and historical tracking.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Optional

logger = logging.getLogger(__name__)


class TimeGranularity(Enum):
    """Time granularity levels."""

    SECOND = "second"
    MINUTE = "minute"
    HOUR = "hour"
    DAY = "day"
    WEEK = "week"
    MONTH = "month"


@dataclass
class TimelineEvent:
    """Represents a single timeline event."""

    id: str
    timestamp: float
    event_type: str
    data: dict[str, Any]
    source: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class TimeWindow:
    """Represents a time window."""

    start: float
    end: float
    granularity: TimeGranularity


class DataTimelineAction:
    """
    Manages temporal data with timeline operations.

    Features:
    - Event ordering and deduplication
    - Time-window aggregation
    - Historical data retrieval
    - Temporal queries

    Example:
        timeline = DataTimelineAction()
        timeline.add_event("click", {"user": "u1"}, source="web")
        events = timeline.get_range(start_ts, end_ts)
    """

    def __init__(
        self,
        default_granularity: TimeGranularity = TimeGranularity.HOUR,
    ) -> None:
        """
        Initialize timeline action.

        Args:
            default_granularity: Default time granularity.
        """
        self.default_granularity = default_granularity
        self._events: list[TimelineEvent] = []
        self._event_index: dict[str, TimelineEvent] = {}
        self._type_index: dict[str, list[TimelineEvent]] = {}

    def add_event(
        self,
        event_type: str,
        data: dict[str, Any],
        timestamp: Optional[float] = None,
        event_id: Optional[str] = None,
        source: str = "",
        metadata: Optional[dict[str, Any]] = None,
    ) -> TimelineEvent:
        """
        Add an event to the timeline.

        Args:
            event_type: Type/category of the event.
            data: Event payload data.
            timestamp: Event timestamp (default: now).
            event_id: Optional event ID (auto-generated if not provided).
            source: Event source identifier.
            metadata: Optional metadata.

        Returns:
            Created TimelineEvent.
        """
        ts = timestamp if timestamp is not None else time.time()
        eid = event_id or f"{event_type}_{ts}_{len(self._events)}"

        event = TimelineEvent(
            id=eid,
            timestamp=ts,
            event_type=event_type,
            data=data,
            source=source,
            metadata=metadata or {},
        )

        self._events.append(event)

        if eid not in self._event_index:
            self._event_index[eid] = event

        if event_type not in self._type_index:
            self._type_index[event_type] = []
        self._type_index[event_type].append(event)

        self._events.sort(key=lambda e: e.timestamp)

        logger.debug(f"Added timeline event: {eid} ({event_type})")
        return event

    def get_range(
        self,
        start: float,
        end: float,
        event_type: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> list[TimelineEvent]:
        """
        Get events within a time range.

        Args:
            start: Start timestamp.
            end: End timestamp.
            event_type: Optional filter by event type.
            limit: Optional result limit.

        Returns:
            List of matching events.
        """
        events = [e for e in self._events if start <= e.timestamp <= end]

        if event_type:
            events = [e for e in events if e.event_type == event_type]

        if limit:
            events = events[:limit]

        return events

    def get_window(
        self,
        window_seconds: float,
        end: Optional[float] = None,
        event_type: Optional[str] = None,
    ) -> list[TimelineEvent]:
        """
        Get events within a sliding time window.

        Args:
            window_seconds: Window size in seconds.
            end: End timestamp (default: now).
            event_type: Optional event type filter.

        Returns:
            List of events in the window.
        """
        end_ts = end if end is not None else time.time()
        start_ts = end_ts - window_seconds
        return self.get_range(start_ts, end_ts, event_type)

    def aggregate_by_granularity(
        self,
        start: float,
        end: float,
        granularity: TimeGranularity,
        event_type: Optional[str] = None,
    ) -> dict[str, int]:
        """
        Aggregate events by time granularity.

        Args:
            start: Start timestamp.
            end: End timestamp.
            granularity: Time granularity.
            event_type: Optional event type filter.

        Returns:
            Dictionary mapping time buckets to counts.
        """
        events = self.get_range(start, end, event_type)
        buckets: dict[str, int] = {}

        for event in events:
            bucket_key = self._get_bucket_key(event.timestamp, granularity)
            buckets[bucket_key] = buckets.get(bucket_key, 0) + 1

        return buckets

    def _get_bucket_key(self, timestamp: float, granularity: TimeGranularity) -> str:
        """Get bucket key for a timestamp and granularity."""
        import datetime
        dt = datetime.datetime.fromtimestamp(timestamp)

        if granularity == TimeGranularity.MINUTE:
            return dt.strftime("%Y-%m-%d %H:%M")
        elif granularity == TimeGranularity.HOUR:
            return dt.strftime("%Y-%m-%d %H:00")
        elif granularity == TimeGranularity.DAY:
            return dt.strftime("%Y-%m-%d")
        elif granularity == TimeGranularity.WEEK:
            return dt.strftime("%Y-W%U")
        elif granularity == TimeGranularity.MONTH:
            return dt.strftime("%Y-%m")
        else:
            return dt.strftime("%Y-%m-%d %H:%M:%S")

    def get_latest(
        self,
        n: int = 10,
        event_type: Optional[str] = None,
    ) -> list[TimelineEvent]:
        """
        Get the N most recent events.

        Args:
            n: Number of events to return.
            event_type: Optional event type filter.

        Returns:
            List of most recent events.
        """
        events = self._events
        if event_type:
            events = self._type_index.get(event_type, [])

        return events[-n:] if n < len(events) else events

    def get_event(self, event_id: str) -> Optional[TimelineEvent]:
        """
        Get a specific event by ID.

        Args:
            event_id: Event identifier.

        Returns:
            TimelineEvent or None.
        """
        return self._event_index.get(event_id)

    def get_stats(self) -> dict[str, Any]:
        """
        Get timeline statistics.

        Returns:
            Statistics dictionary.
        """
        event_types = set(e.event_type for e in self._events)
        sources = set(e.source for e in self._events if e.source)

        return {
            "total_events": len(self._events),
            "unique_event_types": len(event_types),
            "unique_sources": len(sources),
            "earliest_event": self._events[0].timestamp if self._events else None,
            "latest_event": self._events[-1].timestamp if self._events else None,
            "event_types": list(event_types),
        }

    def clear(self) -> None:
        """Clear all events."""
        self._events.clear()
        self._event_index.clear()
        self._type_index.clear()
        logger.info("Timeline cleared")
