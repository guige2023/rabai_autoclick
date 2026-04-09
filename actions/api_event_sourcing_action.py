"""API Event Sourcing Action Module.

Provides event sourcing infrastructure for API operations
with event store, snapshots, and replay capabilities.

Author: RabAi Team
"""

from __future__ import annotations

import json
import time
import uuid
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Callable, Deque, Dict, List, Optional
from enum import Enum

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class EventType(Enum):
    """Event types in the system."""
    CREATED = "created"
    UPDATED = "updated"
    DELETED = "deleted"
    STATE_CHANGED = "state_changed"
    COMMAND_EXECUTED = "command_executed"
    QUERY_EXECUTED = "query_executed"


@dataclass
class Event:
    """Base event structure."""
    event_id: str
    event_type: EventType
    aggregate_id: str
    timestamp: float
    version: int
    data: Dict[str, Any]
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Convert event to dictionary."""
        return {
            "event_id": self.event_id,
            "event_type": self.event_type.value,
            "aggregate_id": self.aggregate_id,
            "timestamp": self.timestamp,
            "version": self.version,
            "data": self.data,
            "metadata": self.metadata
        }


@dataclass
class Snapshot:
    """Aggregate state snapshot."""
    aggregate_id: str
    version: int
    timestamp: float
    state: Dict[str, Any]


class EventHandler:
    """Handler for processing events."""

    def __init__(self):
        self._handlers: Dict[EventType, List[Callable]] = {}

    def register(
        self,
        event_type: EventType,
        handler: Callable[[Event], None]
    ) -> None:
        """Register event handler."""
        if event_type not in self._handlers:
            self._handlers[event_type] = []
        self._handlers[event_type].append(handler)

    def handle(self, event: Event) -> None:
        """Handle an event."""
        handlers = self._handlers.get(event.event_type, [])
        for handler in handlers:
            handler(event)


class EventStore:
    """Event store for persisting and retrieving events."""

    def __init__(self, max_events: int = 100000):
        self._events: Deque[Event] = deque(maxlen=max_events)
        self._by_aggregate: Dict[str, Deque[Event]] = {}
        self._snapshots: Dict[str, Snapshot] = {}
        self._snapshot_interval: int = 100

    def append(self, event: Event) -> None:
        """Append event to store."""
        self._events.append(event)

        if event.aggregate_id not in self._by_aggregate:
            self._by_aggregate[event.aggregate_id] = deque(maxlen=self._events.maxlen or 10000)

        self._by_aggregate[event.aggregate_id].append(event)

        if event.version % self._snapshot_interval == 0:
            self._take_snapshot(event.aggregate_id)

    def _take_snapshot(self, aggregate_id: str) -> None:
        """Take snapshot of aggregate state."""
        events = self._by_aggregate.get(aggregate_id, [])
        if not events:
            return

        latest = events[-1]
        state = latest.data.copy()
        state["_version"] = latest.version
        state["_last_modified"] = latest.timestamp

        snapshot = Snapshot(
            aggregate_id=aggregate_id,
            version=latest.version,
            timestamp=time.time(),
            state=state
        )

        self._snapshots[aggregate_id] = snapshot

    def get_events(
        self,
        aggregate_id: Optional[str] = None,
        event_type: Optional[EventType] = None,
        since_version: Optional[int] = None,
        limit: int = 1000
    ) -> List[Event]:
        """Get events from store."""
        events = list(self._events)

        if aggregate_id:
            events = [e for e in events if e.aggregate_id == aggregate_id]

        if event_type:
            events = [e for e in events if e.event_type == event_type]

        if since_version is not None:
            events = [e for e in events if e.version > since_version]

        return events[-limit:]

    def get_snapshot(self, aggregate_id: str) -> Optional[Snapshot]:
        """Get latest snapshot for aggregate."""
        return self._snapshots.get(aggregate_id)

    def rebuild_state(
        self,
        aggregate_id: str,
        apply_fn: Callable[[Dict[str, Any], Event], Dict[str, Any]]
    ) -> Dict[str, Any]:
        """Rebuild aggregate state from events."""
        snapshot = self.get_snapshot(aggregate_id)

        if snapshot:
            state = snapshot.state.copy()
            start_version = snapshot.version + 1
        else:
            state = {}
            start_version = 0

        events = [
            e for e in self._by_aggregate.get(aggregate_id, [])
            if e.version > start_version
        ]

        for event in events:
            state = apply_fn(state, event)

        return state

    def get_statistics(self) -> Dict[str, Any]:
        """Get event store statistics."""
        events_by_type: Dict[str, int] = {}
        for event in self._events:
            key = event.event_type.value
            events_by_type[key] = events_by_type.get(key, 0) + 1

        return {
            "total_events": len(self._events),
            "aggregates": len(self._by_aggregate),
            "snapshots": len(self._snapshots),
            "events_by_type": events_by_type
        }

    def clear(self) -> None:
        """Clear event store."""
        self._events.clear()
        self._by_aggregate.clear()
        self._snapshots.clear()


class Aggregate:
    """Base aggregate with event sourcing support."""

    def __init__(self, aggregate_id: str):
        self.aggregate_id = aggregate_id
        self.version = 0
        self._pending_events: Deque[Event] = deque()

    def _apply(self, event: Event) -> None:
        """Apply event to aggregate state."""
        self.version = event.version

    def _add_event(
        self,
        event_type: EventType,
        data: Dict[str, Any],
        metadata: Optional[Dict[str, Any]] = None
    ) -> Event:
        """Create and add pending event."""
        self.version += 1

        event = Event(
            event_id=str(uuid.uuid4()),
            event_type=event_type,
            aggregate_id=self.aggregate_id,
            timestamp=time.time(),
            version=self.version,
            data=data,
            metadata=metadata or {}
        )

        self._pending_events.append(event)
        return event

    def commit(self, event_store: EventStore) -> List[Event]:
        """Commit pending events to store."""
        committed = []

        while self._pending_events:
            event = self._pending_events.popleft()
            event_store.append(event)
            self._apply(event)
            committed.append(event)

        return committed


class APIEventSourcingAction(BaseAction):
    """Action for event sourcing operations."""

    def __init__(self):
        super().__init__("api_event_sourcing")
        self._store = EventStore()
        self._handler = EventHandler()

    def execute(self, params: Dict[str, Any]) -> ActionResult:
        """Execute event sourcing action."""
        try:
            operation = params.get("operation", "append")

            if operation == "append":
                return self._append(params)
            elif operation == "get_events":
                return self._get_events(params)
            elif operation == "get_snapshot":
                return self._get_snapshot(params)
            elif operation == "rebuild":
                return self._rebuild(params)
            elif operation == "stats":
                return self._get_stats(params)
            elif operation == "clear":
                return self._clear(params)
            else:
                return ActionResult(
                    success=False,
                    message=f"Unknown operation: {operation}"
                )

        except Exception as e:
            return ActionResult(success=False, message=str(e))

    def _append(self, params: Dict[str, Any]) -> ActionResult:
        """Append event to store."""
        event_type_str = params.get("event_type", "state_changed")
        aggregate_id = params.get("aggregate_id", str(uuid.uuid4()))
        data = params.get("data", {})
        metadata = params.get("metadata", {})

        try:
            event_type = EventType(event_type_str)
        except ValueError:
            return ActionResult(
                success=False,
                message=f"Invalid event type: {event_type_str}"
            )

        existing_events = self._store.get_events(aggregate_id)
        version = len(existing_events) + 1

        event = Event(
            event_id=str(uuid.uuid4()),
            event_type=event_type,
            aggregate_id=aggregate_id,
            timestamp=time.time(),
            version=version,
            data=data,
            metadata=metadata
        )

        self._store.append(event)

        return ActionResult(
            success=True,
            data={
                "event_id": event.event_id,
                "aggregate_id": aggregate_id,
                "version": version
            }
        )

    def _get_events(self, params: Dict[str, Any]) -> ActionResult:
        """Get events from store."""
        aggregate_id = params.get("aggregate_id")
        event_type_str = params.get("event_type")
        since_version = params.get("since_version")
        limit = params.get("limit", 1000)

        event_type = None
        if event_type_str:
            try:
                event_type = EventType(event_type_str)
            except ValueError:
                return ActionResult(
                    success=False,
                    message=f"Invalid event type: {event_type_str}"
                )

        events = self._store.get_events(
            aggregate_id=aggregate_id,
            event_type=event_type,
            since_version=since_version,
            limit=limit
        )

        return ActionResult(
            success=True,
            data={
                "events": [e.to_dict() for e in events],
                "count": len(events)
            }
        )

    def _get_snapshot(self, params: Dict[str, Any]) -> ActionResult:
        """Get snapshot for aggregate."""
        aggregate_id = params.get("aggregate_id", "")

        snapshot = self._store.get_snapshot(aggregate_id)

        if not snapshot:
            return ActionResult(
                success=True,
                data={"snapshot": None}
            )

        return ActionResult(
            success=True,
            data={
                "snapshot": {
                    "aggregate_id": snapshot.aggregate_id,
                    "version": snapshot.version,
                    "timestamp": snapshot.timestamp,
                    "state": snapshot.state
                }
            }
        )

    def _rebuild(self, params: Dict[str, Any]) -> ActionResult:
        """Rebuild aggregate state."""
        aggregate_id = params.get("aggregate_id", "")

        def apply_fn(state: Dict[str, Any], event: Event) -> Dict[str, Any]:
            new_state = state.copy()
            new_state.update(event.data)
            return new_state

        state = self._store.rebuild_state(aggregate_id, apply_fn)

        return ActionResult(
            success=True,
            data={"state": state}
        )

    def _get_stats(self, params: Dict[str, Any]) -> ActionResult:
        """Get event store statistics."""
        stats = self._store.get_statistics()
        return ActionResult(success=True, data=stats)

    def _clear(self, params: Dict[str, Any]) -> ActionResult:
        """Clear event store."""
        self._store.clear()
        return ActionResult(success=True, message="Event store cleared")
