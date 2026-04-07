"""Event sourcing utilities: aggregate roots, event store, and replay."""

from __future__ import annotations

import json
import time
import uuid
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Callable

__all__ = [
    "DomainEvent",
    "EventStore",
    "AggregateRoot",
    "EventSourcedEntity",
]


@dataclass
class DomainEvent:
    """A domain event in the event sourcing pattern."""

    event_id: str
    event_type: str
    aggregate_id: str
    payload: dict[str, Any]
    timestamp: float = field(default_factory=time.time)
    version: int = 1

    @classmethod
    def create(
        cls,
        event_type: str,
        aggregate_id: str,
        payload: dict[str, Any],
        version: int = 1,
    ) -> "DomainEvent":
        return cls(
            event_id=uuid.uuid4().hex,
            event_type=event_type,
            aggregate_id=aggregate_id,
            payload=payload,
            version=version,
        )


class EventStore:
    """Event store for event sourcing."""

    def __init__(self) -> None:
        self._events: dict[str, list[DomainEvent]] = defaultdict(list)
        self._snapshots: dict[str, tuple[int, Any]] = {}

    def append(self, event: DomainEvent) -> None:
        self._events[event.aggregate_id].append(event)

    def get_events(self, aggregate_id: str, from_version: int = 0) -> list[DomainEvent]:
        events = self._events.get(aggregate_id, [])
        return [e for e in events if e.version > from_version]

    def get_all_events(self) -> list[DomainEvent]:
        all_events = []
        for events in self._events.values():
            all_events.extend(events)
        return sorted(all_events, key=lambda e: e.timestamp)

    def save_snapshot(self, aggregate_id: str, version: int, state: Any) -> None:
        self._snapshots[aggregate_id] = (version, state)

    def get_snapshot(self, aggregate_id: str) -> tuple[int, Any] | None:
        return self._snapshots.get(aggregate_id)


class AggregateRoot:
    """Base aggregate root for event sourcing."""

    def __init__(self, aggregate_id: str) -> None:
        self.aggregate_id = aggregate_id
        self._version = 0
        self._pending_events: list[DomainEvent] = []
        self._handlers: dict[str, Callable[[DomainEvent], None]] = {}

    def _apply(self, event: DomainEvent) -> None:
        handler = self._handlers.get(event.event_type)
        if handler:
            handler(event)

    def _raise(self, event_type: str, payload: dict[str, Any]) -> None:
        event = DomainEvent.create(
            event_type=event_type,
            aggregate_id=self.aggregate_id,
            payload=payload,
            version=self._version + 1,
        )
        self._pending_events.append(event)
        self._apply(event)
        self._version = event.version

    def commit(self, event_store: EventStore) -> None:
        for event in self._pending_events:
            event_store.append(event)
        self._pending_events.clear()

    def replay(self, event_store: EventStore) -> None:
        events = event_store.get_events(self.aggregate_id)
        for event in events:
            self._apply(event)
            self._version = event.version


class EventSourcedEntity:
    """Base class for event-sourced entities."""

    def __init__(self, entity_id: str) -> None:
        self.entity_id = entity_id
        self._version = 0
        self._pending_events: list[DomainEvent] = []

    def _raise(self, event_type: str, payload: dict[str, Any]) -> None:
        event = DomainEvent.create(
            event_type=event_type,
            aggregate_id=self.entity_id,
            payload=payload,
            version=self._version + 1,
        )
        self._pending_events.append(event)
        self._version = event.version
