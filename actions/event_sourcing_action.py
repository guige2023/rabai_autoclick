"""
Event sourcing action for managing event-driven state management.

This module provides actions for implementing event sourcing patterns,
including event stores, event handlers, snapshots, and projections.

Author: rabai_autoclick team
Version: 1.0.0
"""

from __future__ import annotations

import json
import threading
import time
import uuid
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple, Type, Union


class EventType(Enum):
    """Base event types for event sourcing."""
    CREATED = "created"
    UPDATED = "updated"
    DELETED = "deleted"
    CUSTOM = "custom"


@dataclass
class Event:
    """A single event in the event store."""
    id: str
    aggregate_id: str
    event_type: str
    event_data: Dict[str, Any]
    metadata: Dict[str, Any] = field(default_factory=dict)
    timestamp: datetime = field(default_factory=datetime.now)
    version: int = 1
    correlation_id: Optional[str] = None
    causation_id: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert event to dictionary."""
        return {
            "id": self.id,
            "aggregate_id": self.aggregate_id,
            "event_type": self.event_type,
            "event_data": self.event_data,
            "metadata": self.metadata,
            "timestamp": self.timestamp.isoformat(),
            "version": self.version,
            "correlation_id": self.correlation_id,
            "causation_id": self.causation_id,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> Event:
        """Create event from dictionary."""
        data = data.copy()
        if isinstance(data.get("timestamp"), str):
            data["timestamp"] = datetime.fromisoformat(data["timestamp"])
        return cls(**data)


@dataclass
class Snapshot:
    """A point-in-time snapshot of aggregate state."""
    aggregate_id: str
    version: int
    state: Dict[str, Any]
    timestamp: datetime

    def to_dict(self) -> Dict[str, Any]:
        """Convert snapshot to dictionary."""
        return {
            "aggregate_id": self.aggregate_id,
            "version": self.version,
            "state": self.state,
            "timestamp": self.timestamp.isoformat(),
        }


class EventStore:
    """
    Event store for event sourcing pattern implementation.

    Stores events, manages snapshots, and supports event replay.
    """

    def __init__(self, storage_path: Optional[Union[str, Path]] = None):
        """
        Initialize the event store.

        Args:
            storage_path: Optional path for persisting events.
        """
        self._storage_path = Path(storage_path) if storage_path else None
        self._events: Dict[str, List[Event]] = defaultdict(list)
        self._snapshots: Dict[str, Snapshot] = {}
        self._lock = threading.RLock()
        self._event_handlers: Dict[str, List[Callable[[Event], None]]] = defaultdict(list)
        self._projections: Dict[str, Callable[[Event], None]] = {}
        self._version_cache: Dict[str, int] = {}

        if self._storage_path:
            self._storage_path.mkdir(parents=True, exist_ok=True)
            self._load_from_disk()

    def append(
        self,
        aggregate_id: str,
        event_type: str,
        event_data: Dict[str, Any],
        metadata: Optional[Dict[str, Any]] = None,
        correlation_id: Optional[str] = None,
        causation_id: Optional[str] = None,
    ) -> Event:
        """
        Append a new event to the store.

        Args:
            aggregate_id: ID of the aggregate this event belongs to.
            event_type: Type of the event.
            event_data: Event payload.
            metadata: Optional event metadata.
            correlation_id: Optional correlation ID for tracing.
            causation_id: Optional causation ID.

        Returns:
            The appended Event.
        """
        with self._lock:
            version = self._version_cache.get(aggregate_id, 0) + 1

            event = Event(
                id=str(uuid.uuid4()),
                aggregate_id=aggregate_id,
                event_type=event_type,
                event_data=event_data,
                metadata=metadata or {},
                timestamp=datetime.now(),
                version=version,
                correlation_id=correlation_id,
                causation_id=causation_id,
            )

            self._events[aggregate_id].append(event)
            self._version_cache[aggregate_id] = version

            self._notify_handlers(event)
            self._update_projections(event)

            if self._storage_path:
                self._save_event_to_disk(event)

            return event

    def get_events(
        self,
        aggregate_id: str,
        from_version: int = 0,
        to_version: Optional[int] = None,
        event_types: Optional[List[str]] = None,
    ) -> List[Event]:
        """
        Get events for an aggregate.

        Args:
            aggregate_id: ID of the aggregate.
            from_version: Start from this version (exclusive).
            to_version: End at this version (inclusive).
            event_types: Optional filter by event types.

        Returns:
            List of events.
        """
        with self._lock:
            events = self._events.get(aggregate_id, [])

            if from_version > 0:
                events = [e for e in events if e.version > from_version]

            if to_version is not None:
                events = [e for e in events if e.version <= to_version]

            if event_types:
                events = [e for e in events if e.event_type in event_types]

            return events

    def get_all_events(
        self,
        from_timestamp: Optional[datetime] = None,
        to_timestamp: Optional[datetime] = None,
        event_types: Optional[List[str]] = None,
        limit: Optional[int] = None,
    ) -> List[Event]:
        """
        Get all events across all aggregates.

        Args:
            from_timestamp: Filter events after this time.
            to_timestamp: Filter events before this time.
            event_types: Optional filter by event types.
            limit: Maximum number of events to return.

        Returns:
            List of events.
        """
        with self._lock:
            all_events = []
            for events in self._events.values():
                all_events.extend(events)

            all_events.sort(key=lambda e: e.timestamp)

            if from_timestamp:
                all_events = [e for e in all_events if e.timestamp >= from_timestamp]

            if to_timestamp:
                all_events = [e for e in all_events if e.timestamp <= to_timestamp]

            if event_types:
                all_events = [e for e in all_events if e.event_type in event_types]

            if limit:
                all_events = all_events[:limit]

            return all_events

    def get_current_version(self, aggregate_id: str) -> int:
        """Get the current version of an aggregate."""
        with self._lock:
            return self._version_cache.get(aggregate_id, 0)

    def get_snapshot(
        self,
        aggregate_id: str,
    ) -> Optional[Snapshot]:
        """
        Get the most recent snapshot for an aggregate.

        Args:
            aggregate_id: ID of the aggregate.

        Returns:
            Snapshot if exists, None otherwise.
        """
        with self._lock:
            return self._snapshots.get(aggregate_id)

    def save_snapshot(
        self,
        aggregate_id: str,
        state: Dict[str, Any],
    ) -> Snapshot:
        """
        Save a snapshot of aggregate state.

        Args:
            aggregate_id: ID of the aggregate.
            state: Current state to snapshot.

        Returns:
            The saved Snapshot.
        """
        with self._lock:
            version = self.get_current_version(aggregate_id)

            snapshot = Snapshot(
                aggregate_id=aggregate_id,
                version=version,
                state=state,
                timestamp=datetime.now(),
            )

            self._snapshots[aggregate_id] = snapshot

            if self._storage_path:
                self._save_snapshot_to_disk(snapshot)

            return snapshot

    def replay(
        self,
        aggregate_id: str,
        event_handlers: Dict[str, Callable[[Any, Event], Any]],
        from_version: int = 0,
    ) -> Tuple[Any, int]:
        """
        Replay events to reconstruct aggregate state.

        Args:
            aggregate_id: ID of the aggregate.
            event_handlers: Map of event types to handler functions.
            from_version: Start replay from this version (exclusive).

        Returns:
            Tuple of (reconstructed_state, final_version).
        """
        events = self.get_events(aggregate_id, from_version=from_version)

        state = None
        final_version = from_version

        for event in events:
            handler = event_handlers.get(event.event_type)
            if handler:
                state = handler(state, event)
            final_version = event.version

        return state, final_version

    def register_handler(
        self,
        event_type: str,
        handler: Callable[[Event], None],
    ) -> None:
        """
        Register an event handler for immediate notification.

        Args:
            event_type: Type of events to handle.
            handler: Handler function.
        """
        with self._lock:
            self._event_handlers[event_type].append(handler)

    def register_projection(
        self,
        name: str,
        projection: Callable[[Event], None],
    ) -> None:
        """
        Register a projection that processes all events.

        Args:
            name: Name of the projection.
            projection: Projection function.
        """
        with self._lock:
            self._projections[name] = projection

    def _notify_handlers(self, event: Event) -> None:
        """Notify registered handlers of an event."""
        handlers = self._event_handlers.get(event.event_type, [])
        for handler in handlers:
            try:
                handler(event)
            except Exception:
                pass

    def _update_projections(self, event: Event) -> None:
        """Update all registered projections with an event."""
        for projection in self._projections.values():
            try:
                projection(event)
            except Exception:
                pass

    def _load_from_disk(self) -> None:
        """Load events and snapshots from disk storage."""
        if not self._storage_path or not self._storage_path.exists():
            return

        events_file = self._storage_path / "events.json"
        if events_file.exists():
            try:
                with open(events_file, "r") as f:
                    data = json.load(f)
                    for agg_id, events in data.items():
                        self._events[agg_id] = [Event.from_dict(e) for e in events]
                        if self._events[agg_id]:
                            self._version_cache[agg_id] = max(
                                e.version for e in self._events[agg_id]
                            )
            except Exception:
                pass

        snapshots_file = self._storage_path / "snapshots.json"
        if snapshots_file.exists():
            try:
                with open(snapshots_file, "r") as f:
                    data = json.load(f)
                    for agg_id, snap_data in data.items():
                        snap_data = snap_data.copy()
                        if isinstance(snap_data.get("timestamp"), str):
                            snap_data["timestamp"] = datetime.fromisoformat(
                                snap_data["timestamp"]
                            )
                        self._snapshots[agg_id] = Snapshot(**snap_data)
            except Exception:
                pass

    def _save_event_to_disk(self, event: Event) -> None:
        """Save a single event to disk."""
        events_file = self._storage_path / "events.json"
        try:
            data = {}
            if events_file.exists():
                with open(events_file, "r") as f:
                    data = json.load(f)

            agg_id = event.aggregate_id
            if agg_id not in data:
                data[agg_id] = []
            data[agg_id].append(event.to_dict())

            with open(events_file, "w") as f:
                json.dump(data, f)
        except Exception:
            pass

    def _save_snapshot_to_disk(self, snapshot: Snapshot) -> None:
        """Save a snapshot to disk."""
        snapshots_file = self._storage_path / "snapshots.json"
        try:
            data = {}
            if snapshots_file.exists():
                with open(snapshots_file, "r") as f:
                    data = json.load(f)

            data[snapshot.aggregate_id] = snapshot.to_dict()

            with open(snapshots_file, "w") as f:
                json.dump(data, f)
        except Exception:
            pass

    def get_stats(self) -> Dict[str, Any]:
        """Get event store statistics."""
        with self._lock:
            total_events = sum(len(e) for e in self._events.values())
            return {
                "total_aggregates": len(self._events),
                "total_events": total_events,
                "total_snapshots": len(self._snapshots),
                "registered_projections": len(self._projections),
                "registered_handlers": {
                    et: len(hs) for et, hs in self._event_handlers.items()
                },
            }


def event_sourcing_append_action(
    aggregate_id: str,
    event_type: str,
    event_data: Dict[str, Any],
    metadata: Optional[Dict[str, Any]] = None,
    correlation_id: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Action function to append an event to the event store.

    Args:
        aggregate_id: ID of the aggregate.
        event_type: Type of the event.
        event_data: Event payload.
        metadata: Optional metadata.
        correlation_id: Optional correlation ID.

    Returns:
        Dictionary with appended event.
    """
    store = EventStore()
    event = store.append(
        aggregate_id=aggregate_id,
        event_type=event_type,
        event_data=event_data,
        metadata=metadata,
        correlation_id=correlation_id,
    )
    return event.to_dict()


def event_sourcing_replay_action(
    aggregate_id: str,
    event_handlers: Dict[str, str],
) -> Dict[str, Any]:
    """
    Action function to replay events for an aggregate.

    Args:
        aggregate_id: ID of the aggregate.
        event_handlers: Dict mapping event types to handler function names.

    Returns:
        Dictionary with reconstructed state and version.
    """
    store = EventStore()

    def create_handler(fn_str: str) -> Callable:
        """Create a simple state handler from event data."""
        def handler(state, event):
            if state is None:
                state = {}
            state[event.event_type] = event.event_data
            return state
        return handler

    handlers = {
        et: create_handler(fn_str)
        for et, fn_str in event_handlers.items()
    }

    state, version = store.replay(aggregate_id, handlers)
    return {
        "aggregate_id": aggregate_id,
        "state": state,
        "version": version,
    }


def event_sourcing_get_events_action(
    aggregate_id: str,
    from_version: int = 0,
) -> List[Dict[str, Any]]:
    """
    Action function to get events for an aggregate.

    Args:
        aggregate_id: ID of the aggregate.
        from_version: Start from this version.

    Returns:
        List of event dictionaries.
    """
    store = EventStore()
    events = store.get_events(aggregate_id, from_version=from_version)
    return [e.to_dict() for e in events]
