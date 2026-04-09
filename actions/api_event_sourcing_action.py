"""
API Event Sourcing Action Module.

Event sourcing pattern implementation with event store,
snapshot support, and event replay capabilities.
"""

import json
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Optional
from enum import Enum
import logging

logger = logging.getLogger(__name__)


class EventType(Enum):
    """Base event types."""
    UNKNOWN = "unknown"


@dataclass
class Event:
    """
    Event sourcing event.

    Attributes:
        event_id: Unique event identifier.
        event_type: Type of event.
        aggregate_id: ID of aggregate this event belongs to.
        data: Event payload.
        metadata: Event metadata.
        timestamp: When event occurred.
        version: Event version for ordering.
    """
    event_id: str
    event_type: str
    aggregate_id: str
    data: Any = None
    metadata: dict = field(default_factory=dict)
    timestamp: float = field(default_factory=time.time, init=False)
    version: int = 1


@dataclass
class Snapshot:
    """Snapshot of aggregate state."""
    aggregate_id: str
    version: int
    state: Any
    timestamp: float


class APIEventSourcingAction:
    """
    Event sourcing implementation for API state management.

    Example:
        event_store = APIEventSourcingAction()
        event_store.record_event("order-123", OrderCreatedEvent(data))
        events = event_store.get_events("order-123")
        state = event_store.rebuild_state("order-123", OrderAggregate())
    """

    def __init__(self, snapshot_threshold: int = 100):
        """
        Initialize event sourcing.

        Args:
            snapshot_threshold: Events before creating snapshot.
        """
        self.snapshot_threshold = snapshot_threshold
        self._events: list[Event] = []
        self._snapshots: dict[str, Snapshot] = {}
        self._handlers: dict[str, Callable] = {}

    def register_event_type(
        self,
        event_type: str,
        handler: Callable[[Any, Event], Any]
    ) -> None:
        """
        Register event handler.

        Args:
            event_type: Event type name.
            handler: Function to apply event to state.
        """
        self._handlers[event_type] = handler
        logger.debug(f"Registered handler for event type: {event_type}")

    def record_event(
        self,
        aggregate_id: str,
        event_type: str,
        data: Any,
        metadata: Optional[dict] = None
    ) -> Event:
        """
        Record a new event.

        Args:
            aggregate_id: Aggregate identifier.
            event_type: Type of event.
            data: Event payload.
            metadata: Optional metadata.

        Returns:
            Created Event.
        """
        import uuid

        aggregate_events = [e for e in self._events if e.aggregate_id == aggregate_id]
        version = max([e.version for e in aggregate_events], default=0) + 1

        event = Event(
            event_id=str(uuid.uuid4())[:12],
            event_type=event_type,
            aggregate_id=aggregate_id,
            data=data,
            metadata=metadata or {},
            version=version
        )

        self._events.append(event)

        if len(aggregate_events) >= self.snapshot_threshold:
            self._create_snapshot(aggregate_id)

        logger.debug(f"Recorded event: {event_type} for {aggregate_id} v{version}")
        return event

    def get_events(
        self,
        aggregate_id: str,
        from_version: int = 0
    ) -> list[Event]:
        """
        Get events for an aggregate.

        Args:
            aggregate_id: Aggregate identifier.
            from_version: Get events from this version onward.

        Returns:
            List of events.
        """
        return [
            e for e in self._events
            if e.aggregate_id == aggregate_id and e.version > from_version
        ]

    def rebuild_state(
        self,
        aggregate_id: str,
        initial_state: Any,
        until_version: Optional[int] = None
    ) -> Any:
        """
        Rebuild aggregate state from events.

        Args:
            aggregate_id: Aggregate identifier.
            initial_state: Initial state object.
            until_version: Rebuild until this version.

        Returns:
            Reconstructed state.
        """
        events = self.get_events(aggregate_id)

        if until_version:
            events = [e for e in events if e.version <= until_version]

        state = initial_state

        for event in events:
            handler = self._handlers.get(event.event_type)

            if handler:
                try:
                    state = handler(state, event)
                except Exception as e:
                    logger.error(f"Event handler failed for {event.event_type}: {e}")
                    raise
            else:
                logger.warning(f"No handler for event type: {event.event_type}")

        return state

    def _create_snapshot(self, aggregate_id: str) -> None:
        """Create snapshot for aggregate."""
        events = self.get_events(aggregate_id)

        if not events:
            return

        latest_version = max(e.version for e in events)
        latest_event = max(events, key=lambda e: e.version)

        snapshot = Snapshot(
            aggregate_id=aggregate_id,
            version=latest_version,
            state=latest_event.data,
            timestamp=time.time()
        )

        self._snapshots[aggregate_id] = snapshot
        logger.debug(f"Created snapshot for {aggregate_id} at v{latest_version}")

    def get_snapshot(self, aggregate_id: str) -> Optional[Snapshot]:
        """Get latest snapshot for aggregate."""
        return self._snapshots.get(aggregate_id)

    def replay_events(
        self,
        aggregate_id: str,
        from_version: int = 0,
        to_version: Optional[int] = None
    ) -> list[Event]:
        """
        Get events for replay (with optional version range).

        Args:
            aggregate_id: Aggregate identifier.
            from_version: Start from this version.
            to_version: End at this version.

        Returns:
            List of events to replay.
        """
        events = self.get_events(aggregate_id, from_version)

        if to_version:
            events = [e for e in events if e.version <= to_version]

        return events

    def get_event_statistics(self) -> dict:
        """Get event store statistics."""
        aggregates = set(e.aggregate_id for e in self._events)
        by_type: dict[str, int] = {}

        for event in self._events:
            by_type[event.event_type] = by_type.get(event.event_type, 0) + 1

        return {
            "total_events": len(self._events),
            "aggregates": len(aggregates),
            "snapshots": len(self._snapshots),
            "by_type": by_type
        }

    def export_events(
        self,
        aggregate_id: Optional[str] = None
    ) -> list[dict]:
        """
        Export events to serializable format.

        Args:
            aggregate_id: Export specific aggregate or all.

        Returns:
            List of event dictionaries.
        """
        events = self._events

        if aggregate_id:
            events = [e for e in events if e.aggregate_id == aggregate_id]

        return [
            {
                "event_id": e.event_id,
                "event_type": e.event_type,
                "aggregate_id": e.aggregate_id,
                "data": e.data,
                "metadata": e.metadata,
                "timestamp": e.timestamp,
                "version": e.version
            }
            for e in events
        ]

    def import_events(self, events_data: list[dict]) -> int:
        """
        Import events from serializable format.

        Args:
            events_data: List of event dictionaries.

        Returns:
            Number of events imported.
        """
        imported = 0

        for data in events_data:
            try:
                event = Event(
                    event_id=data["event_id"],
                    event_type=data["event_type"],
                    aggregate_id=data["aggregate_id"],
                    data=data.get("data"),
                    metadata=data.get("metadata", {}),
                    timestamp=data.get("timestamp", time.time()),
                    version=data.get("version", 1)
                )

                self._events.append(event)
                imported += 1

            except Exception as e:
                logger.error(f"Failed to import event: {e}")

        return imported

    def clear(self, before_timestamp: Optional[float] = None) -> int:
        """
        Clear events, optionally before timestamp.

        Args:
            before_timestamp: Clear events before this time.

        Returns:
            Number of events cleared.
        """
        if before_timestamp is None:
            cleared = len(self._events)
            self._events.clear()
            self._snapshots.clear()
        else:
            to_keep = [e for e in self._events if e.timestamp >= before_timestamp]
            cleared = len(self._events) - len(to_keep)
            self._events = to_keep

            to_keep_snaps = {k: v for k, v in self._snapshots.items() if v.timestamp >= before_timestamp}
            self._snapshots = to_keep_snaps

        logger.info(f"Cleared {cleared} events")
        return cleared
