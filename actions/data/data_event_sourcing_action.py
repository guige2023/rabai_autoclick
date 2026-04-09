"""Data Event Sourcing Action Module.

Provides event sourcing capabilities for data management,
storing state changes as a sequence of immutable events.

Example:
    >>> from actions.data.data_event_sourcing_action import EventSourcedAggregate
    >>> aggregate = EventSourcedAggregate("order-123")
    >>> await aggregate.load_from_events(event_store)
"""

from __future__ import annotations

import hashlib
import json
import time
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set, Type, TypeVar
import threading


T = TypeVar('T', bound='EventSourcedAggregate')


class EventType(Enum):
    """Base event types."""
    UNKNOWN = "unknown"
    CREATED = "created"
    UPDATED = "updated"
    DELETED = "deleted"


@dataclass
class DomainEvent:
    """Base class for domain events.
    
    Attributes:
        event_id: Unique event identifier
        event_type: Type of event
        aggregate_id: ID of the aggregate this event belongs to
        aggregate_type: Type of aggregate
        timestamp: When the event occurred
        version: Event version for ordering
        data: Event payload
        metadata: Event metadata ( causation, correlation, etc.)
        causation_id: ID of the command that caused this event
        correlation_id: ID for correlating related events
    """
    event_id: str
    event_type: str
    aggregate_id: str
    aggregate_type: str
    timestamp: datetime = field(default_factory=datetime.now)
    version: int = 1
    data: Dict[str, Any] = field(default_factory=dict)
    metadata: Dict[str, Any] = field(default_factory=dict)
    causation_id: Optional[str] = None
    correlation_id: Optional[str] = None


@dataclass
class Snapshot:
    """Aggregate state snapshot.
    
    Attributes:
        aggregate_id: Aggregate identifier
        aggregate_type: Aggregate type name
        version: Version at snapshot time
        state: Serialized state
        timestamp: Snapshot timestamp
    """
    aggregate_id: str
    aggregate_type: str
    version: int
    state: Dict[str, Any]
    timestamp: datetime = field(default_factory=datetime.now)


@dataclass
class EventSourcingConfig:
    """Configuration for event sourcing.
    
    Attributes:
        snapshot_threshold: Version gap before creating snapshot
        snapshot_interval: Time interval for snapshots
        max_events_per_aggregate: Maximum events to keep (0 = unlimited)
        enable_snapshots: Whether to use snapshots
        metadata_whitelist: Metadata keys to store
    """
    snapshot_threshold: int = 100
    snapshot_interval: float = 3600.0
    max_events_per_aggregate: int = 0
    enable_snapshots: bool = True
    metadata_whitelist: List[str] = field(default_factory=list)


class InMemoryEventStore:
    """In-memory event store implementation.
    
    Provides event storage for demonstration/testing.
    In production, replace with a persistent store.
    """
    
    def __init__(self):
        self._events: Dict[str, List[DomainEvent]] = defaultdict(list)
        self._snapshots: Dict[str, Snapshot] = {}
        self._lock = threading.RLock()
    
    async def append(self, event: DomainEvent) -> None:
        """Append an event to the store.
        
        Args:
            event: Event to append
        """
        with self._lock:
            self._events[event.aggregate_id].append(event)
    
    async def append_batch(self, events: List[DomainEvent]) -> None:
        """Append multiple events.
        
        Args:
            events: Events to append
        """
        with self._lock:
            for event in events:
                self._events[event.aggregate_id].append(event)
    
    async def get_events(
        self,
        aggregate_id: str,
        from_version: int = 0
    ) -> List[DomainEvent]:
        """Get events for an aggregate.
        
        Args:
            aggregate_id: Aggregate identifier
            from_version: Get events from this version onward
        
        Returns:
            List of events
        """
        with self._lock:
            events = self._events.get(aggregate_id, [])
            return [e for e in events if e.version > from_version]
    
    async def get_events_by_type(
        self,
        aggregate_id: str,
        event_type: str
    ) -> List[DomainEvent]:
        """Get events of a specific type.
        
        Args:
            aggregate_id: Aggregate identifier
            event_type: Type of events to retrieve
        
        Returns:
            List of matching events
        """
        with self._lock:
            events = self._events.get(aggregate_id, [])
            return [e for e in events if e.event_type == event_type]
    
    async def get_snapshot(self, aggregate_id: str) -> Optional[Snapshot]:
        """Get the latest snapshot for an aggregate.
        
        Args:
            aggregate_id: Aggregate identifier
        
        Returns:
            Snapshot or None
        """
        with self._lock:
            return self._snapshots.get(aggregate_id)
    
    async def save_snapshot(self, snapshot: Snapshot) -> None:
        """Save a snapshot.
        
        Args:
            snapshot: Snapshot to save
        """
        with self._lock:
            self._snapshots[snapshot.aggregate_id] = snapshot
    
    async def get_all_aggregate_ids(self) -> List[str]:
        """Get all aggregate IDs in the store.
        
        Returns:
            List of aggregate IDs
        """
        with self._lock:
            return list(self._events.keys())


class EventSourcedAggregate:
    """Base class for event-sourced aggregates.
    
    Handles event application and state management for
    aggregates using event sourcing pattern.
    
    Attributes:
        aggregate_id: Unique aggregate identifier
        aggregate_type: Type of this aggregate
        version: Current aggregate version
    
    Example:
        >>> class Order(EventSourcedAggregate):
        ...     def __init__(self, order_id: str):
        ...         super().__init__(order_id, "Order")
        ...         self.items = []
        ...     
        ...     def apply_event(self, event: DomainEvent):
        ...         if event.event_type == "OrderItemAdded":
        ...             self.items.append(event.data["item"])
    """
    
    # Override in subclasses
    aggregate_type: str = "EventSourcedAggregate"
    
    def __init__(self, aggregate_id: str, aggregate_type: Optional[str] = None):
        """Initialize the aggregate.
        
        Args:
            aggregate_id: Unique aggregate identifier
            aggregate_type: Optional type override
        """
        self.aggregate_id = aggregate_id
        self.aggregate_type = aggregate_type or self.__class__.aggregate_type
        self.version: int = 0
        self._uncommitted_events: List[DomainEvent] = []
        self._created = False
    
    def create_initial_state(self) -> None:
        """Initialize the aggregate's default state.
        
        Override in subclasses to set initial state.
        """
        pass
    
    def apply_event(self, event: DomainEvent) -> None:
        """Apply an event to update aggregate state.
        
        Override in subclasses to handle specific event types.
        
        Args:
            event: Event to apply
        """
        self.version = event.version
    
    def _mark_created(self) -> None:
        """Mark this aggregate as newly created."""
        self._created = True
    
    def is_created(self) -> bool:
        """Check if this aggregate was created in this session.
        
        Returns:
            True if newly created
        """
        return self._created
    
    def get_uncommitted_events(self) -> List[DomainEvent]:
        """Get events that haven't been committed.
        
        Returns:
            List of uncommitted events
        """
        events = list(self._uncommitted_events)
        self._uncommitted_events.clear()
        return events
    
    def clear_uncommitted_events(self) -> None:
        """Clear uncommitted events without returning them."""
        self._uncommitted_events.clear()
    
    def _add_uncommitted_event(self, event: DomainEvent) -> None:
        """Add an event to the uncommitted list.
        
        Args:
            event: Event to add
        """
        self._uncommitted_events.append(event)
    
    def to_snapshot_state(self) -> Dict[str, Any]:
        """Convert current state to snapshot format.
        
        Override in subclasses.
        
        Returns:
            State dictionary
        """
        return {"aggregate_id": self.aggregate_id, "version": self.version}
    
    def load_from_snapshot_state(self, state: Dict[str, Any]) -> None:
        """Load state from snapshot.
        
        Override in subclasses.
        
        Args:
            state: Snapshot state dictionary
        """
        self.aggregate_id = state.get("aggregate_id", self.aggregate_id)
        self.version = state.get("version", 0)


class EventStoreService:
    """Service for managing event-sourced aggregates.
    
    Handles loading, saving, and event application for
    event-sourced aggregates.
    
    Attributes:
        config: Event sourcing configuration
        event_store: Event storage backend
    
    Example:
        >>> service = EventStoreService(event_store)
        >>> order = await service.load(Order, "order-123")
        >>> order.add_item("Widget")
        >>> await service.save(order)
    """
    
    def __init__(
        self,
        event_store: InMemoryEventStore,
        config: Optional[EventSourcingConfig] = None
    ):
        """Initialize the event store service.
        
        Args:
            event_store: Event storage backend
            config: Event sourcing configuration
        """
        self.event_store = event_store
        self.config = config or EventSourcingConfig()
        self._event_handlers: Dict[str, Callable] = {}
    
    def register_event_type(
        self,
        event_type: str,
        handler: Callable[[EventSourcedAggregate, DomainEvent], None]
    ) -> None:
        """Register an event type handler.
        
        Args:
            event_type: Event type name
            handler: Handler function
        """
        self._event_handlers[event_type] = handler
    
    async def load(
        self,
        aggregate_class: Type[T],
        aggregate_id: str
    ) -> T:
        """Load an aggregate from the event store.
        
        Args:
            aggregate_class: Class of the aggregate to create
            aggregate_id: Aggregate identifier
        
        Returns:
            Loaded aggregate with current state
        """
        aggregate = aggregate_class.__new__(aggregate_class)
        aggregate.aggregate_id = aggregate_id
        aggregate.aggregate_type = aggregate_class.aggregate_type
        aggregate._uncommitted_events = []
        aggregate._created = False
        aggregate.version = 0
        
        # Try to load from snapshot first
        snapshot = None
        if self.config.enable_snapshots:
            snapshot = await self.event_store.get_snapshot(aggregate_id)
        
        if snapshot:
            aggregate.load_from_snapshot_state(snapshot.state)
        else:
            aggregate.create_initial_state()
        
        # Load and apply events
        from_version = aggregate.version
        events = await self.event_store.get_events(aggregate_id, from_version)
        
        for event in events:
            aggregate.apply_event(event)
        
        return aggregate
    
    async def save(self, aggregate: EventSourcedAggregate) -> List[DomainEvent]:
        """Save an aggregate's uncommitted events.
        
        Args:
            aggregate: Aggregate to save
        
        Returns:
            List of committed events
        """
        events = aggregate.get_uncommitted_events()
        
        if not events:
            return []
        
        # Assign versions and timestamps
        base_version = aggregate.version - len(events)
        for i, event in enumerate(events):
            event.version = base_version + i + 1
            event.aggregate_id = aggregate.aggregate_id
            event.aggregate_type = aggregate.aggregate_type
            event.timestamp = datetime.now()
        
        # Append to event store
        await self.event_store.append_batch(events)
        
        # Create snapshot if needed
        if self.config.enable_snapshots:
            current_version = aggregate.version
            if current_version % self.config.snapshot_threshold == 0:
                snapshot = Snapshot(
                    aggregate_id=aggregate.aggregate_id,
                    aggregate_type=aggregate.aggregate_type,
                    version=current_version,
                    state=aggregate.to_snapshot_state()
                )
                await self.event_store.save_snapshot(snapshot)
        
        return events
    
    async def create(
        self,
        aggregate_class: Type[T],
        aggregate_id: str,
        initial_data: Optional[Dict[str, Any]] = None
    ) -> T:
        """Create a new aggregate with initial state.
        
        Args:
            aggregate_class: Class of aggregate to create
            aggregate_id: Unique identifier
            initial_data: Optional initial state data
        
        Returns:
            Created aggregate
        """
        aggregate = aggregate_class.__new__(aggregate_class)
        aggregate.aggregate_id = aggregate_id
        aggregate.aggregate_type = aggregate_class.aggregate_type
        aggregate._uncommitted_events = []
        aggregate._created = True
        aggregate.version = 0
        
        aggregate.create_initial_state()
        
        # Create creation event
        creation_event = DomainEvent(
            event_id=f"{aggregate_id}_{int(time.time() * 1000)}",
            event_type="Created",
            aggregate_id=aggregate_id,
            aggregate_type=aggregate.aggregate_type,
            data=initial_data or {},
            version=1
        )
        
        aggregate._add_uncommitted_event(creation_event)
        aggregate.apply_event(creation_event)
        
        return aggregate
    
    async def get_event_history(
        self,
        aggregate_id: str,
        limit: Optional[int] = None
    ) -> List[DomainEvent]:
        """Get the full event history for an aggregate.
        
        Args:
            aggregate_id: Aggregate identifier
            limit: Optional limit on number of events
        
        Returns:
            List of events in chronological order
        """
        events = await self.event_store.get_events(aggregate_id)
        
        if limit:
            events = events[-limit:]
        
        return events
    
    async def replay_events(
        self,
        aggregate_id: str,
        from_version: int = 0
    ) -> List[DomainEvent]:
        """Replay events from a specific version.
        
        Args:
            aggregate_id: Aggregate identifier
            from_version: Version to replay from
        
        Returns:
            List of replayed events
        """
        return await self.event_store.get_events(aggregate_id, from_version)


class EventPublisher:
    """Publishes domain events to message bus.
    
    Integrates event sourcing with message publishing
    for event-driven architectures.
    """
    
    def __init__(self):
        self._subscribers: Dict[str, List[Callable]] = defaultdict(list)
        self._lock = threading.RLock()
    
    def subscribe(
        self,
        event_type: str,
        handler: Callable[[DomainEvent], Any]
    ) -> None:
        """Subscribe to an event type.
        
        Args:
            event_type: Event type to subscribe to
            handler: Handler function
        """
        with self._lock:
            self._subscribers[event_type].append(handler)
    
    def unsubscribe(
        self,
        event_type: str,
        handler: Callable[[DomainEvent], Any]
    ) -> None:
        """Unsubscribe from an event type.
        
        Args:
            event_type: Event type
            handler: Handler to remove
        """
        with self._lock:
            if handler in self._subscribers[event_type]:
                self._subscribers[event_type].remove(handler)
    
    async def publish(self, event: DomainEvent) -> None:
        """Publish an event to all subscribers.
        
        Args:
            event: Event to publish
        """
        handlers: List[Callable] = []
        
        with self._lock:
            handlers.extend(self._subscribers.get(event.event_type, []))
            # Also notify wildcard subscribers
            handlers.extend(self._subscribers.get("*", []))
        
        for handler in handlers:
            try:
                if asyncio.iscoroutinefunction(handler):
                    await handler(event)
                else:
                    handler(event)
            except Exception:
                pass  # Best effort publishing


import asyncio
