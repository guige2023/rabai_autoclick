"""Automation Event Store Action.

Append-only event store for automation workflows: stores events,
supports replay, snapshots, and event sourcing patterns.
"""
from typing import Any, Callable, Dict, List, Optional, TypeVar
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
import json
import uuid


class EventType(Enum):
    COMMAND = "command"
    EVENT = "event"
    SNAPSHOT = "snapshot"
    MARKER = "marker"


@dataclass
class StoredEvent:
    event_id: str
    aggregate_id: str
    event_type: str
    event_data: Dict[str, Any]
    metadata: Dict[str, Any]
    timestamp: datetime
    sequence: int
    version: int = 1

    def to_json(self) -> str:
        return json.dumps({
            "event_id": self.event_id,
            "aggregate_id": self.aggregate_id,
            "event_type": self.event_type,
            "event_data": self.event_data,
            "metadata": self.metadata,
            "timestamp": self.timestamp.isoformat(),
            "sequence": self.sequence,
            "version": self.version,
        })

    @classmethod
    def from_json(cls, data: Dict[str, Any]) -> "StoredEvent":
        return cls(
            event_id=data["event_id"],
            aggregate_id=data["aggregate_id"],
            event_type=data["event_type"],
            event_data=data["event_data"],
            metadata=data["metadata"],
            timestamp=datetime.fromisoformat(data["timestamp"]),
            sequence=data["sequence"],
            version=data.get("version", 1),
        )


@dataclass
class Snapshot:
    aggregate_id: str
    version: int
    state: Dict[str, Any]
    timestamp: datetime
    sequence: int


class AutomationEventStoreAction:
    """Append-only event store with replay support."""

    def __init__(self) -> None:
        self._events: List[StoredEvent] = []
        self._snapshots: Dict[str, Snapshot] = {}
        self._sequences: Dict[str, int] = {}

    def append(
        self,
        aggregate_id: str,
        event_type: str,
        event_data: Dict[str, Any],
        metadata: Optional[Dict[str, Any]] = None,
    ) -> StoredEvent:
        seq = self._sequences.get(aggregate_id, 0) + 1
        self._sequences[aggregate_id] = seq
        event = StoredEvent(
            event_id=str(uuid.uuid4()),
            aggregate_id=aggregate_id,
            event_type=event_type,
            event_data=event_data,
            metadata=metadata or {},
            timestamp=datetime.now(),
            sequence=seq,
        )
        self._events.append(event)
        return event

    def get_events(
        self,
        aggregate_id: str,
        from_sequence: int = 0,
    ) -> List[StoredEvent]:
        return [
            e
            for e in self._events
            if e.aggregate_id == aggregate_id and e.sequence > from_sequence
        ]

    def replay(
        self,
        aggregate_id: str,
        event_handlers: Dict[str, Callable[[Dict[str, Any]], None]],
        from_sequence: int = 0,
    ) -> Dict[str, Any]:
        events = self.get_events(aggregate_id, from_sequence)
        state: Dict[str, Any] = {}
        for event in events:
            handler = event_handlers.get(event.event_type)
            if handler:
                state = handler(event.event_data) or state
            else:
                state.update(event.event_data)
        return state

    def take_snapshot(
        self,
        aggregate_id: str,
        state: Dict[str, Any],
    ) -> Snapshot:
        seq = self._sequences.get(aggregate_id, 0)
        snapshot = Snapshot(
            aggregate_id=aggregate_id,
            version=seq,
            state=dict(state),
            timestamp=datetime.now(),
            sequence=seq,
        )
        self._snapshots[aggregate_id] = snapshot
        return snapshot

    def get_snapshot(self, aggregate_id: str) -> Optional[Snapshot]:
        return self._snapshots.get(aggregate_id)

    def replay_from_snapshot(
        self,
        aggregate_id: str,
        event_handlers: Dict[str, Callable[[Dict[str, Any]], Dict[str, Any]]],
    ) -> Dict[str, Any]:
        snapshot = self.get_snapshot(aggregate_id)
        from_seq = 0
        state: Dict[str, Any] = {}
        if snapshot:
            state = dict(snapshot.state)
            from_seq = snapshot.sequence
        events = self.get_events(aggregate_id, from_seq)
        for event in events:
            handler = event_handlers.get(event.event_type)
            if handler:
                state = handler(event.event_data) or state
        return state

    def event_count(self, aggregate_id: Optional[str] = None) -> int:
        if aggregate_id:
            return sum(1 for e in self._events if e.aggregate_id == aggregate_id)
        return len(self._events)

    def mark(self, aggregate_id: str, marker: str, metadata: Optional[Dict[str, Any]] = None) -> StoredEvent:
        return self.append(
            aggregate_id=aggregate_id,
            event_type=EventType.MARKER.value,
            event_data={"marker": marker},
            metadata=metadata,
        )
