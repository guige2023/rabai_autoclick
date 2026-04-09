"""
Automation Event Sourcing Module.

Implements event sourcing pattern for automation workflows.
Stores all state changes as immutable events, supports event
replay, temporal queries, and snapshot management.
"""

from __future__ import annotations

import json
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Generic, TypeVar

T = TypeVar("T")


class EventType(Enum):
    """Standard event types."""
    CREATED = "created"
    UPDATED = "updated"
    DELETED = "deleted"
    STATE_CHANGED = "state_changed"
    ACTION_EXECUTED = "action_executed"
    ERROR = "error"
    SNAPSHOT = "snapshot"


@dataclass
class Event:
    """Base event class."""
    event_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    event_type: str = ""
    aggregate_id: str = ""
    aggregate_type: str = ""
    version: int = 1
    timestamp: float = field(default_factory=time.time)
    payload: dict[str, Any] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "event_id": self.event_id,
            "event_type": self.event_type,
            "aggregate_id": self.aggregate_id,
            "aggregate_type": self.aggregate_type,
            "version": self.version,
            "timestamp": self.timestamp,
            "payload": self.payload,
            "metadata": self.metadata
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Event:
        return cls(**{k: v for k, v in data.items() if k in cls.__dataclass_fields__})


class Snapshot:
    """State snapshot for an aggregate."""
    def __init__(
        self,
        aggregate_id: str,
        aggregate_type: str,
        version: int,
        state: dict[str, Any]
    ) -> None:
        self.aggregate_id = aggregate_id
        self.aggregate_type = aggregate_type
        self.version = version
        self.state = state
        self.timestamp = time.time()

    def to_dict(self) -> dict[str, Any]:
        return {
            "aggregate_id": self.aggregate_id,
            "aggregate_type": self.aggregate_type,
            "version": self.version,
            "state": self.state,
            "timestamp": self.timestamp
        }


class EventStore:
    """
    Event sourcing event store.

    Stores events and snapshots, supports replay, projection,
    and temporal queries.

    Example:
        store = EventStore()
        store.append(Event("order-1", "ORDER", "CREATED", {"item": "book"}))
        events = store.get_events("order-1")
        state = store.replay("order-1", order_reducer)
    """

    def __init__(self) -> None:
        self._events: dict[str, list[Event]] = {}
        self._snapshots: dict[str, Snapshot] = {}
        self._snapshots_enabled: bool = True
        self._snapshot_threshold: int = 100

    def append(self, event: Event) -> Event:
        """
        Append an event to the store.

        Args:
            event: Event to append

        Returns:
            The appended event
        """
        if event.aggregate_id not in self._events:
            self._events[event.aggregate_id] = []

        event.version = len(self._events[event.aggregate_id]) + 1
        self._events[event.aggregate_id].append(event)

        if self._snapshots_enabled:
            self._maybe_create_snapshot(event.aggregate_id)

        return event

    def _maybe_create_snapshot(self, aggregate_id: str) -> None:
        """Create snapshot if threshold reached."""
        events = self._events.get(aggregate_id, [])
        if len(events) >= self._snapshot_threshold:
            latest = events[-1]
            snapshot = Snapshot(
                aggregate_id=latest.aggregate_id,
                aggregate_type=latest.aggregate_type,
                version=latest.version,
                state=self._get_current_state(aggregate_id)
            )
            self._snapshots[aggregate_id] = snapshot

    def get_events(
        self,
        aggregate_id: str,
        from_version: int = 0
    ) -> list[Event]:
        """Get events for an aggregate from a version."""
        events = self._events.get(aggregate_id, [])
        if from_version > 0:
            events = [e for e in events if e.version > from_version]
        return events

    def get_events_of_type(
        self,
        event_type: str,
        from_time: float = 0,
        to_time: float = float("inf")
    ) -> list[Event]:
        """Get events of a specific type within a time range."""
        results = []
        for events in self._events.values():
            for event in events:
                if event.event_type == event_type:
                    if from_time <= event.timestamp <= to_time:
                        results.append(event)
        return results

    def replay(
        self,
        aggregate_id: str,
        reducer: Callable[[Any, Event], Any],
        initial_state: Any = None
    ) -> Any:
        """
        Replay events to reconstruct aggregate state.

        Args:
            aggregate_id: Aggregate ID to replay
            reducer: Reducer function (state, event) -> new_state
            initial_state: Initial state for reducer

        Returns:
            Final state after replay
        """
        events = self.get_events(aggregate_id)
        if not events:
            return initial_state

        snapshot = self._snapshots.get(aggregate_id)
        if snapshot and snapshot.version > 0:
            state = snapshot.state
            events = [e for e in events if e.version > snapshot.version]
        else:
            state = initial_state

        for event in events:
            state = reducer(state, event)

        return state

    def replay_from(
        self,
        aggregate_id: str,
        reducer: Callable[[Any, Event], Any],
        from_version: int,
        initial_state: Any = None
    ) -> Any:
        """Replay events from a specific version."""
        events = self.get_events(aggregate_id, from_version)
        state = initial_state
        for event in events:
            state = reducer(state, event)
        return state

    def _get_current_state(self, aggregate_id: str) -> dict[str, Any]:
        """Get current state dict for an aggregate."""
        events = self._events.get(aggregate_id, [])
        return {e.event_id: e.payload for e in events}

    def create_snapshot(self, aggregate_id: str, state: dict[str, Any]) -> Snapshot:
        """Manually create a snapshot for an aggregate."""
        events = self._events.get(aggregate_id, [])
        version = events[-1].version if events else 0

        snapshot = Snapshot(
            aggregate_id=aggregate_id,
            aggregate_type=events[-1].aggregate_type if events else "",
            version=version,
            state=state
        )
        self._snapshots[aggregate_id] = snapshot
        return snapshot

    def get_snapshot(self, aggregate_id: str) -> Snapshot | None:
        """Get the latest snapshot for an aggregate."""
        return self._snapshots.get(aggregate_id)

    def get_aggregate_ids(self) -> list[str]:
        """Get all aggregate IDs in the store."""
        return list(self._events.keys())

    def enable_snapshots(self, threshold: int = 100) -> None:
        """Enable automatic snapshot creation."""
        self._snapshots_enabled = True
        self._snapshot_threshold = threshold

    def disable_snapshots(self) -> None:
        """Disable automatic snapshot creation."""
        self._snapshots_enabled = False

    def clear(self) -> None:
        """Clear all events and snapshots."""
        self._events.clear()
        self._snapshots.clear()

    def export_events(self, aggregate_id: str) -> str:
        """Export events as JSON string."""
        events = self.get_events(aggregate_id)
        return json.dumps([e.to_dict() for e in events], indent=2)

    def import_events(self, data: str) -> int:
        """Import events from JSON string."""
        events_data = json.loads(data)
        count = 0
        for event_data in events_data:
            event = Event.from_dict(event_data)
            self.append(event)
            count += 1
        return count


class AggregateRoot(Generic[T]):
    """
    Base class for aggregate roots using event sourcing.

    Subclass this to create aggregates that automatically
    record events for state changes.

    Example:
        class Order(AggregateRoot[OrderState]):
            def __init__(self, order_id: str):
                super().__init__(order_id, "ORDER")
                self._state = OrderState()

            def add_item(self, item: str, qty: int):
                self.record_event("ITEM_ADDED", {"item": item, "qty": qty})

            def apply(self, event: Event):
                # Apply event to state
                pass
    """

    def __init__(
        self,
        aggregate_id: str,
        aggregate_type: str
    ) -> None:
        self.aggregate_id = aggregate_id
        self.aggregate_type = aggregate_type
        self._pending_events: list[Event] = []
        self._version: int = 0

    def record_event(
        self,
        event_type: str,
        payload: dict[str, Any],
        metadata: Optional[dict[str, Any]] = None
    ) -> Event:
        """Record a new pending event."""
        event = Event(
            event_type=event_type,
            aggregate_id=self.aggregate_id,
            aggregate_type=self.aggregate_type,
            version=self._version + 1,
            payload=payload,
            metadata=metadata or {}
        )
        self._pending_events.append(event)
        self._version += 1
        return event

    def apply(self, event: Event) -> None:
        """Apply an event to update internal state. Override in subclass."""
        raise NotImplementedError

    def replay(self, events: list[Event]) -> None:
        """Replay a list of events to reconstruct state."""
        for event in events:
            self.apply(event)
            self._version = event.version

    def get_pending_events(self) -> list[Event]:
        """Get all pending events and clear them."""
        pending = self._pending_events
        self._pending_events = []
        return pending

    def load_from_snapshot(self, snapshot: Snapshot) -> None:
        """Load state from a snapshot."""
        self._version = snapshot.version
