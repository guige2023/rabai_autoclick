"""
Data CQRS Action Module.

Implements Command Query Responsibility Segregation pattern for
event-driven data management with separate read/write models,
event sourcing support, and consistency guarantees.

Author: RabAi Team
"""

from __future__ import annotations

import asyncio
import json
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Callable, Generic, Optional, TypeVar

from collections import defaultdict


class EventType(Enum):
    """CQRS event types."""
    COMMAND_EXECUTED = "command_executed"
    QUERY_EXECUTED = "query_executed"
    SNAPSHOT_CREATED = "snapshot_created"
    REPLAY_COMPLETED = "replay_completed"


@dataclass
class CQRSConfig:
    """CQRS system configuration."""
    snapshot_threshold: int = 100
    event_store_enabled: bool = True
    read_model_update_mode: str = "synchronous"
    consistency_level: str = "eventual"
    snapshot_interval: int = 50


@dataclass
class Command:
    """CQRS command representation."""
    command_id: str
    aggregate_id: str
    command_type: str
    payload: dict[str, Any]
    timestamp: float = field(default_factory=time.time)
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class Query:
    """CQRS query representation."""
    query_id: str
    query_type: str
    parameters: dict[str, Any]
    timestamp: float = field(default_factory=time.time)


@dataclass
class Event:
    """CQRS event representation."""
    event_id: str
    aggregate_id: str
    event_type: str
    payload: dict[str, Any]
    sequence: int
    timestamp: float = field(default_factory=time.time)
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class Snapshot:
    """Aggregate snapshot."""
    aggregate_id: str
    version: int
    state: dict[str, Any]
    timestamp: float


@dataclass
class CommandResult:
    """Result of command execution."""
    command_id: str
    success: bool
    aggregate_id: str
    new_version: int
    events: list[Event] = field(default_factory=list)
    error: Optional[str] = None
    execution_time_ms: float = 0.0


@dataclass
class QueryResult(Generic[T]):
    """Result of query execution."""
    query_id: str
    data: Any
    from_cache: bool = False
    execution_time_ms: float = 0.0


class EventStore:
    """Persistent event store."""

    def __init__(self):
        self._events: dict[str, list[Event]] = defaultdict(list)
        self._snapshots: dict[str, Snapshot] = {}

    def append(self, event: Event) -> None:
        """Append event to store."""
        self._events[event.aggregate_id].append(event)

    def get_events(
        self,
        aggregate_id: str,
        from_sequence: int = 0,
    ) -> list[Event]:
        """Get events for aggregate from sequence."""
        events = self._events.get(aggregate_id, [])
        return [e for e in events if e.sequence > from_sequence]

    def get_all_events(self, aggregate_id: str) -> list[Event]:
        """Get all events for aggregate."""
        return self._events.get(aggregate_id, [])

    def save_snapshot(self, snapshot: Snapshot) -> None:
        """Save aggregate snapshot."""
        self._snapshots[snapshot.aggregate_id] = snapshot

    def get_snapshot(self, aggregate_id: str) -> Optional[Snapshot]:
        """Get latest snapshot for aggregate."""
        return self._snapshots.get(aggregate_id)

    def clear(self, aggregate_id: str) -> None:
        """Clear all events for aggregate."""
        if aggregate_id in self._events:
            del self._events[aggregate_id]


class Aggregate:
    """Base aggregate root."""

    def __init__(self, aggregate_id: str):
        self.aggregate_id = aggregate_id
        self.version = 0
        self._uncommitted_events: list[Event] = []

    def apply_event(self, event: Event) -> None:
        """Apply event to aggregate state."""
        self.version = event.sequence

    def mark_events_committed(self) -> None:
        """Clear uncommitted events."""
        self._uncommitted_events.clear()

    def get_uncommitted_events(self) -> list[Event]:
        """Get uncommitted events."""
        return self._uncommitted_events.copy()


class ReadModel:
    """Read model for queries."""

    def __init__(self, name: str):
        self.name = name
        self._data: dict[str, Any] = {}
        self._collections: dict[str, dict[str, Any]] = defaultdict(dict)

    def update(self, event: Event) -> None:
        """Update read model based on event."""
        pass

    def query(
        self,
        collection: str,
        filter_func: Optional[Callable[[dict], bool]] = None,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        """Query the read model."""
        items = list(self._collections.get(collection, {}).values())
        if filter_func:
            items = [item for item in items if filter_func(item)]
        return items[:limit]

    def get(self, collection: str, id: str) -> Optional[dict[str, Any]]:
        """Get single item from collection."""
        return self._collections.get(collection, {}).get(id)


class CQRSBus:
    """Command/Query bus."""

    def __init__(self, config: Optional[CQRSConfig] = None):
        self.config = config or CQRSConfig()
        self._event_store = EventStore()
        self._aggregates: dict[str, Aggregate] = {}
        self._read_models: dict[str, ReadModel] = {}
        self._command_handlers: dict[str, Callable] = {}
        self._query_handlers: dict[str, Callable] = {}
        self._sequence_numbers: dict[str, int] = defaultdict(int)

    def register_aggregate(self, aggregate: Aggregate) -> None:
        """Register an aggregate."""
        self._aggregates[aggregate.aggregate_id] = aggregate

    def register_read_model(self, read_model: ReadModel) -> None:
        """Register a read model."""
        self._read_models[read_model.name] = read_model

    def register_command_handler(
        self,
        command_type: str,
        handler: Callable[[Command], list[Event]],
    ) -> None:
        """Register command handler."""
        self._command_handlers[command_type] = handler

    def register_query_handler(
        self,
        query_type: str,
        handler: Callable[[Query], Any],
    ) -> None:
        """Register query handler."""
        self._query_handlers[query_type] = handler

    async def execute_command(self, command: Command) -> CommandResult:
        """Execute a command."""
        start = time.time()
        try:
            if command.command_type not in self._command_handlers:
                raise ValueError(f"Unknown command type: {command.command_type}")

            aggregate_id = command.aggregate_id
            if aggregate_id in self._aggregates:
                aggregate = self._aggregates[aggregate_id]
            else:
                aggregate = Aggregate(aggregate_id)
                self._aggregates[aggregate_id] = aggregate

            self._sequence_numbers[aggregate_id] += 1
            events = self._command_handlers[command.command_type](command)

            for i, event in enumerate(events):
                event.event_id = str(uuid.uuid4())
                event.aggregate_id = aggregate_id
                event.sequence = self._sequence_numbers[aggregate_id] + i
                aggregate.apply_event(event)
                self._event_store.append(event)

            self._sequence_numbers[aggregate_id] += len(events)

            for read_model in self._read_models.values():
                for event in events:
                    read_model.update(event)

            aggregate.mark_events_committed()

            return CommandResult(
                command_id=command.command_id,
                success=True,
                aggregate_id=aggregate_id,
                new_version=aggregate.version,
                events=events,
                execution_time_ms=(time.time() - start) * 1000,
            )

        except Exception as e:
            return CommandResult(
                command_id=command.command_id,
                success=False,
                aggregate_id=command.aggregate_id,
                new_version=0,
                error=str(e),
                execution_time_ms=(time.time() - start) * 1000,
            )

    async def execute_query(self, query: Query) -> QueryResult:
        """Execute a query."""
        start = time.time()
        if query.query_type not in self._query_handlers:
            raise ValueError(f"Unknown query type: {query.query_type}")

        data = self._query_handlers[query.query_type](query)
        return QueryResult(
            query_id=query.query_id,
            data=data,
            execution_time_ms=(time.time() - start) * 1000,
        )

    def replay_events(
        self,
        aggregate_id: str,
        from_sequence: int = 0,
    ) -> Aggregate:
        """Replay events for an aggregate."""
        aggregate = Aggregate(aggregate_id)
        events = self._event_store.get_events(aggregate_id, from_sequence)
        for event in events:
            aggregate.apply_event(event)
        return aggregate

    def get_event_history(self, aggregate_id: str) -> list[Event]:
        """Get full event history for aggregate."""
        return self._event_store.get_all_events(aggregate_id)


def create_command(
    aggregate_id: str,
    command_type: str,
    payload: dict[str, Any],
) -> Command:
    """Create a new command."""
    return Command(
        command_id=str(uuid.uuid4()),
        aggregate_id=aggregate_id,
        command_type=command_type,
        payload=payload,
    )


def create_query(
    query_type: str,
    parameters: dict[str, Any],
) -> Query:
    """Create a new query."""
    return Query(
        query_id=str(uuid.uuid4()),
        query_type=query_type,
        parameters=parameters,
    )


async def demo():
    """Demo CQRS pattern."""
    bus = CQRSBus()
    read_model = ReadModel("orders")
    bus.register_read_model(read_model)

    def create_order_handler(cmd: Command) -> list[Event]:
        return [Event(
            event_id="",
            aggregate_id=cmd.aggregate_id,
            event_type="OrderCreated",
            payload=cmd.payload,
            sequence=0,
        )]

    bus.register_command_handler("CreateOrder", create_order_handler)

    def get_order_handler(q: Query) -> Any:
        return read_model.query("orders")

    bus.register_query_handler("GetOrders", get_order_handler)

    cmd = create_command("order-1", "CreateOrder", {"customer": "Alice", "total": 100.0})
    result = await bus.execute_command(cmd)
    print(f"Command success: {result.success}, version: {result.new_version}")

    query = create_query("GetOrders", {})
    qresult = await bus.execute_query(query)
    print(f"Query returned: {qresult.data}")


if __name__ == "__main__":
    asyncio.run(demo())
