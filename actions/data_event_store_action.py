"""
Data Event Store Action Module

Provides event sourcing and event store capabilities for data pipelines.
Supports event persistence, event replay, snapshots, CQRS patterns,
and event versioning with upcasting.

MIT License - Copyright (c) 2025 RabAi Research
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set

logger = logging.getLogger(__name__)


class EventType(Enum):
    """Event type classifications."""

    CREATE = "create"
    UPDATE = "update"
    DELETE = "delete"
    SNAPSHOT = "snapshot"
    SYSTEM = "system"


@dataclass
class Event:
    """A domain event in the event store."""

    event_id: str
    aggregate_id: str
    event_type: EventType
    event_name: str
    version: int
    payload: Dict[str, Any]
    metadata: Dict[str, Any] = field(default_factory=dict)
    timestamp: Optional[float] = None
    causation_id: Optional[str] = None
    correlation_id: Optional[str] = None

    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = time.time()

    def to_dict(self) -> Dict[str, Any]:
        """Serialize event to dictionary."""
        return {
            "event_id": self.event_id,
            "aggregate_id": self.aggregate_id,
            "event_type": self.event_type.value,
            "event_name": self.event_name,
            "version": self.version,
            "payload": self.payload,
            "metadata": self.metadata,
            "timestamp": self.timestamp,
            "causation_id": self.causation_id,
            "correlation_id": self.correlation_id,
        }


@dataclass
class Aggregate:
    """An aggregate root with event sourcing support."""

    aggregate_id: str
    aggregate_type: str
    version: int = 0
    events: List[Event] = field(default_factory=list)
    state: Dict[str, Any] = field(default_factory=dict)
    created_at: Optional[float] = None
    updated_at: Optional[float] = None

    def __post_init__(self):
        if self.created_at is None:
            self.created_at = time.time()

    def apply_event(self, event: Event) -> None:
        """Apply an event to the aggregate."""
        self.events.append(event)
        self.version = event.version
        self.updated_at = time.time()


@dataclass
class Snapshot:
    """A snapshot of aggregate state."""

    snapshot_id: str
    aggregate_id: str
    version: int
    state: Dict[str, Any]
    timestamp: float
    event_count: int


@dataclass
class EventStoreConfig:
    """Configuration for event store."""

    snapshot_interval: int = 100
    max_events_per_aggregate: int = 10000
    snapshot_enabled: bool = True
    snapshot_strategy: str = "interval"
    event_ttl_days: int = 0
    max_replay_events: int = 1000
    enable_event_upcasting: bool = True


class EventUpcaster:
    """Handles event versioning and upcasting."""

    def __init__(self):
        self._upcasters: Dict[str, Dict[int, Callable[[Dict], Dict]]] = {}

    def register_upcaster(
        self,
        event_name: str,
        from_version: int,
        upcaster: Callable[[Dict], Dict],
    ) -> None:
        """Register an upcaster for an event type and version."""
        if event_name not in self._upcasters:
            self._upcasters[event_name] = {}
        self._upcasters[event_name][from_version] = upcaster

    def upcast(
        self,
        event_name: str,
        version: int,
        payload: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Upcast an event payload to the current version."""
        upcasted = payload.copy()
        upcasters = self._upcasters.get(event_name, {})

        for from_ver, upcaster in sorted(upcasters.items()):
            if from_ver > version:
                upcasted = upcaster(upcasted)

        return upcasted


class EventRepository:
    """Repository for persisting and retrieving events."""

    def __init__(self, storage_path: Optional[str] = None):
        self._storage_path = storage_path
        self._events: Dict[str, List[Event]] = {}
        self._snapshots: Dict[str, Snapshot] = {}

    async def save_event(self, event: Event) -> None:
        """Save an event to the store."""
        aggregate_id = event.aggregate_id
        if aggregate_id not in self._events:
            self._events[aggregate_id] = []
        self._events[aggregate_id].append(event)
        logger.debug(f"Event saved: {event.event_id}")

    async def save_events(self, events: List[Event]) -> None:
        """Save multiple events in a batch."""
        for event in events:
            await self.save_event(event)

    async def get_events(
        self,
        aggregate_id: str,
        from_version: int = 0,
        to_version: Optional[int] = None,
    ) -> List[Event]:
        """Get events for an aggregate."""
        events = self._events.get(aggregate_id, [])
        filtered = [e for e in events if e.version > from_version]

        if to_version is not None:
            filtered = [e for e in filtered if e.version <= to_version]

        return filtered

    async def get_events_by_type(
        self,
        aggregate_id: str,
        event_name: str,
    ) -> List[Event]:
        """Get events of a specific type for an aggregate."""
        events = self._events.get(aggregate_id, [])
        return [e for e in events if e.event_name == event_name]

    async def get_all_events(
        self,
        from_timestamp: Optional[float] = None,
        to_timestamp: Optional[float] = None,
        event_types: Optional[Set[EventType]] = None,
        limit: int = 1000,
    ) -> List[Event]:
        """Get all events across aggregates."""
        all_events = []
        for events in self._events.values():
            all_events.extend(events)

        filtered = all_events

        if from_timestamp is not None:
            filtered = [e for e in filtered if e.timestamp >= from_timestamp]

        if to_timestamp is not None:
            filtered = [e for e in filtered if e.timestamp <= to_timestamp]

        if event_types:
            filtered = [e for e in filtered if e.event_type in event_types]

        filtered.sort(key=lambda e: e.timestamp)
        return filtered[:limit]

    async def save_snapshot(self, snapshot: Snapshot) -> None:
        """Save a snapshot for an aggregate."""
        self._snapshots[snapshot.aggregate_id] = snapshot
        logger.debug(f"Snapshot saved: {snapshot.snapshot_id}")

    async def get_snapshot(self, aggregate_id: str) -> Optional[Snapshot]:
        """Get the latest snapshot for an aggregate."""
        return self._snapshots.get(aggregate_id)

    async def get_event_count(self, aggregate_id: str) -> int:
        """Get event count for an aggregate."""
        return len(self._events.get(aggregate_id, []))


class DataEventStoreAction:
    """
    Event store action for event sourcing patterns.

    Features:
    - Event persistence with aggregate scoping
    - Event replay and state reconstruction
    - Snapshot support for performance optimization
    - Event upcasting for schema evolution
    - CQRS command/event separation
    - Event filtering and querying
    - Correlation and causation tracking

    Usage:
        store = DataEventStoreAction(config)
        
        # Create aggregate and apply events
        aggregate = store.create_aggregate("order-123", "Order")
        store.apply_event(aggregate, "OrderCreated", {"item": "book", "qty": 2})
        
        # Replay to reconstruct state
        state = await store.replay_aggregate("order-123")
    """

    def __init__(
        self,
        config: Optional[EventStoreConfig] = None,
        repository: Optional[EventRepository] = None,
    ):
        self.config = config or EventStoreConfig()
        self._repository = repository or EventRepository()
        self._upcaster = EventUpcaster()
        self._aggregates: Dict[str, Aggregate] = {}
        self._event_handlers: Dict[str, List[Callable[[Event], None]]] = {}
        self._stats = {
            "events_saved": 0,
            "events_replayed": 0,
            "snapshots_created": 0,
            "aggregates_created": 0,
        }

    def create_aggregate(
        self,
        aggregate_id: str,
        aggregate_type: str,
        initial_state: Optional[Dict[str, Any]] = None,
    ) -> Aggregate:
        """Create a new aggregate."""
        aggregate = Aggregate(
            aggregate_id=aggregate_id,
            aggregate_type=aggregate_type,
            state=initial_state or {},
        )
        self._aggregates[aggregate_id] = aggregate
        self._stats["aggregates_created"] += 1
        return aggregate

    def get_aggregate(self, aggregate_id: str) -> Optional[Aggregate]:
        """Get an aggregate by ID."""
        return self._aggregates.get(aggregate_id)

    def apply_event(
        self,
        aggregate: Aggregate,
        event_name: str,
        payload: Dict[str, Any],
        event_type: EventType = EventType.UPDATE,
        metadata: Optional[Dict[str, Any]] = None,
        correlation_id: Optional[str] = None,
        causation_id: Optional[str] = None,
    ) -> Event:
        """
        Apply an event to an aggregate.

        Args:
            aggregate: Target aggregate
            event_name: Name of the event
            payload: Event payload data
            event_type: Type of event
            metadata: Additional metadata
            correlation_id: Correlation ID for tracking
            causation_id: Causation ID

        Returns:
            Created event
        """
        aggregate.version += 1
        event = Event(
            event_id=f"{aggregate.aggregate_id}_{aggregate.version}",
            aggregate_id=aggregate.aggregate_id,
            event_type=event_type,
            event_name=event_name,
            version=aggregate.version,
            payload=payload,
            metadata=metadata or {},
            correlation_id=correlation_id,
            causation_id=causation_id,
        )

        aggregate.apply_event(event)
        self._aggregates[aggregate.aggregate_id] = aggregate

        # Save to repository
        asyncio.create_task(self._repository.save_event(event))
        self._stats["events_saved"] += 1

        # Trigger handlers
        self._notify_event_handlers(event)

        # Create snapshot if needed
        if self.config.snapshot_enabled and aggregate.version % self.config.snapshot_interval == 0:
            asyncio.create_task(self._create_snapshot(aggregate))

        return event

    async def _create_snapshot(self, aggregate: Aggregate) -> Snapshot:
        """Create a snapshot for an aggregate."""
        snapshot = Snapshot(
            snapshot_id=f"snap_{aggregate.aggregate_id}_{aggregate.version}",
            aggregate_id=aggregate.aggregate_id,
            version=aggregate.version,
            state=aggregate.state.copy(),
            timestamp=time.time(),
            event_count=len(aggregate.events),
        )
        await self._repository.save_snapshot(snapshot)
        self._stats["snapshots_created"] += 1
        return snapshot

    async def replay_aggregate(
        self,
        aggregate_id: str,
        from_version: int = 0,
        upcast: bool = True,
    ) -> Dict[str, Any]:
        """
        Replay events to reconstruct aggregate state.

        Args:
            aggregate_id: Aggregate ID
            from_version: Start from this version (0 = all)
            upcast: Whether to upcast event payloads

        Returns:
            Reconstructed state dictionary
        """
        events = await self._repository.get_events(aggregate_id, from_version)

        if not events:
            return {}

        # Apply events to rebuild state
        state: Dict[str, Any] = {}

        for event in events:
            if upcast and self.config.enable_event_upcasting:
                event.payload = self._upcaster.upcast(
                    event.event_name,
                    event.version,
                    event.payload,
                )

            state = self._apply_event_to_state(event, state)
            self._stats["events_replayed"] += 1

        return state

    def _apply_event_to_state(
        self,
        event: Event,
        state: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Apply an event payload to state."""
        new_state = state.copy()

        if event.event_type == EventType.CREATE:
            new_state.update(event.payload)
            new_state["_id"] = event.aggregate_id
            new_state["_created_at"] = event.timestamp
        elif event.event_type == EventType.UPDATE:
            for key, value in event.payload.items():
                if key.startswith("_"):
                    new_state[key] = value
                else:
                    if key in new_state:
                        old_value = new_state[key]
                        new_state[f"_prev_{key}"] = old_value
                    new_state[key] = value
        elif event.event_type == EventType.DELETE:
            for key in event.payload.get("deleted_fields", []):
                new_state.pop(key, None)

        new_state["_version"] = event.version
        new_state["_last_modified"] = event.timestamp

        return new_state

    def register_event_handler(
        self,
        event_name: str,
        handler: Callable[[Event], None],
    ) -> None:
        """Register an event handler."""
        if event_name not in self._event_handlers:
            self._event_handlers[event_name] = []
        self._event_handlers[event_name].append(handler)

    def _notify_event_handlers(self, event: Event) -> None:
        """Notify registered handlers of an event."""
        handlers = self._event_handlers.get(event.event_name, [])
        for handler in handlers:
            try:
                handler(event)
            except Exception as e:
                logger.error(f"Event handler error: {e}")

    def register_upcaster(
        self,
        event_name: str,
        from_version: int,
        upcaster: Callable[[Dict], Dict],
    ) -> None:
        """Register an event upcaster for schema migration."""
        self._upcaster.register_upcaster(event_name, from_version, upcaster)

    async def get_event_stream(
        self,
        aggregate_id: str,
    ) -> List[Event]:
        """Get the event stream for an aggregate."""
        return await self._repository.get_events(aggregate_id)

    async def get_all_events(
        self,
        from_timestamp: Optional[float] = None,
        to_timestamp: Optional[float] = None,
        limit: int = 1000,
    ) -> List[Event]:
        """Get all events across all aggregates."""
        return await self._repository.get_all_events(
            from_timestamp=from_timestamp,
            to_timestamp=to_timestamp,
            limit=limit,
        )

    async def create_snapshot(self, aggregate_id: str) -> Optional[Snapshot]:
        """Manually create a snapshot for an aggregate."""
        aggregate = self._aggregates.get(aggregate_id)
        if aggregate is None:
            events = await self._repository.get_events(aggregate_id)
            if not events:
                return None
            aggregate = Aggregate(
                aggregate_id=aggregate_id,
                aggregate_type="unknown",
            )

        return await self._create_snapshot(aggregate)

    def get_stats(self) -> Dict[str, Any]:
        """Get event store statistics."""
        return self._stats.copy()


async def demo_event_store():
    """Demonstrate event store usage."""
    config = EventStoreConfig(snapshot_interval=5)
    store = DataEventStoreAction(config)

    # Create aggregate
    order = store.create_aggregate("order-001", "Order")
    print(f"Created aggregate: {order.aggregate_id}")

    # Apply events
    store.apply_event(
        order,
        "OrderCreated",
        {"customer": "Alice", "item": "Book", "qty": 2},
        event_type=EventType.CREATE,
    )

    store.apply_event(
        order,
        "OrderUpdated",
        {"qty": 5},
        event_type=EventType.UPDATE,
    )

    # Replay
    state = await store.replay_aggregate("order-001")
    print(f"Replayed state: {state}")
    print(f"Stats: {store.get_stats()}")


if __name__ == "__main__":
    asyncio.run(demo_event_store())
