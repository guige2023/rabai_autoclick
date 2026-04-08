"""
Event Sourcing Action.

Provides event sourcing pattern implementation.
Supports:
- Event store
- Event replay
- Snapshot management
- Projection building
"""

from typing import Dict, List, Optional, Any, Callable, Awaitable
from dataclasses import dataclass, field
from datetime import datetime
import threading
import logging
import json
import uuid

logger = logging.getLogger(__name__)


@dataclass
class Event:
    """Represents a domain event."""
    event_id: str
    aggregate_id: str
    event_type: str
    payload: Dict[str, Any]
    timestamp: datetime
    version: int
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "event_id": self.event_id,
            "aggregate_id": self.aggregate_id,
            "event_type": self.event_type,
            "payload": self.payload,
            "timestamp": self.timestamp.isoformat(),
            "version": self.version,
            "metadata": self.metadata
        }


@dataclass
class Snapshot:
    """Represents an aggregate snapshot."""
    aggregate_id: str
    version: int
    state: Dict[str, Any]
    timestamp: datetime


class EventStore:
    """Event store for persisting events."""
    
    def __init__(self):
        self._events: Dict[str, List[Event]] = {}
        self._snapshots: Dict[str, Snapshot] = {}
        self._lock = threading.RLock()
    
    def append(self, event: Event) -> None:
        """Append an event to the store."""
        with self._lock:
            if event.aggregate_id not in self._events:
                self._events[event.aggregate_id] = []
            self._events[event.aggregate_id].append(event)
    
    def get_events(
        self,
        aggregate_id: str,
        from_version: int = 0
    ) -> List[Event]:
        """Get events for an aggregate."""
        with self._lock:
            events = self._events.get(aggregate_id, [])
            return [e for e in events if e.version > from_version]
    
    def save_snapshot(self, snapshot: Snapshot) -> None:
        """Save a snapshot."""
        with self._lock:
            self._snapshots[snapshot.aggregate_id] = snapshot
    
    def get_snapshot(self, aggregate_id: str) -> Optional[Snapshot]:
        """Get the latest snapshot for an aggregate."""
        return self._snapshots.get(aggregate_id)


class EventSourcingAction:
    """
    Event Sourcing Action.
    
    Provides event sourcing with support for:
    - Event storage and retrieval
    - Aggregate reconstruction
    - Snapshot management
    - Projection building
    """
    
    def __init__(
        self,
        snapshot_threshold: int = 100,
        snapshot_interval: int = 10
    ):
        """
        Initialize the Event Sourcing Action.
        
        Args:
            snapshot_threshold: Create snapshot every N events
            snapshot_interval: Snapshot version interval
        """
        self.event_store = EventStore()
        self.snapshot_threshold = snapshot_threshold
        self.snapshot_interval = snapshot_interval
        self._aggregates: Dict[str, Any] = {}
        self._handlers: Dict[str, Callable[[Any, Event], None]] = {}
    
    def register_handler(
        self,
        event_type: str,
        handler: Callable[[Any, Event], None]
    ) -> None:
        """Register an event handler."""
        self._handlers[event_type] = handler
    
    def load_aggregate(
        self,
        aggregate_id: str,
        initial_state: Any,
        apply_event: Callable[[Any, Event], None]
    ) -> Any:
        """Load an aggregate by replaying events."""
        # Try to get snapshot first
        snapshot = self.event_store.get_snapshot(aggregate_id)
        
        if snapshot:
            state = snapshot.state
            from_version = snapshot.version
        else:
            state = initial_state
            from_version = 0
        
        # Replay events
        events = self.event_store.get_events(aggregate_id, from_version)
        for event in events:
            apply_event(state, event)
        
        # Create snapshot if needed
        if len(events) >= self.snapshot_threshold:
            self._create_snapshot(aggregate_id, state, len(events))
        
        self._aggregates[aggregate_id] = state
        return state
    
    def _create_snapshot(
        self,
        aggregate_id: str,
        state: Dict[str, Any],
        version: int
    ) -> None:
        """Create a snapshot for an aggregate."""
        snapshot = Snapshot(
            aggregate_id=aggregate_id,
            version=version,
            state=copy.deepcopy(state),
            timestamp=datetime.utcnow()
        )
        self.event_store.save_snapshot(snapshot)
    
    def publish(
        self,
        aggregate_id: str,
        event_type: str,
        payload: Dict[str, Any],
        metadata: Optional[Dict[str, Any]] = None
    ) -> Event:
        """Publish a new event."""
        events = self.event_store.get_events(aggregate_id)
        version = len(events)
        
        event = Event(
            event_id=str(uuid.uuid4()),
            aggregate_id=aggregate_id,
            event_type=event_type,
            payload=payload,
            timestamp=datetime.utcnow(),
            version=version + 1,
            metadata=metadata or {}
        )
        
        self.event_store.append(event)
        return event
    
    def rebuild_projections(
        self,
        projection_handlers: Dict[str, Callable[[Any, Event], None]]
    ) -> Dict[str, Any]:
        """Rebuild all projections from events."""
        projections: Dict[str, Any] = {}
        
        for aggregate_id, events in self.event_store._events.items():
            for event in events:
                for event_type, handler in projection_handlers.items():
                    if event.event_type == event_type:
                        if event_type not in projections:
                            projections[event_type] = {}
                        handler(projections[event_type], event)
        
        return projections
    
    def get_stats(self) -> Dict[str, Any]:
        """Get event sourcing statistics."""
        total_events = sum(len(e) for e in self.event_store._events.values())
        return {
            "total_aggregates": len(self.event_store._events),
            "total_events": total_events,
            "total_snapshots": len(self.event_store._snapshots)
        }


if __name__ == "__main__":
    import copy
    logging.basicConfig(level=logging.INFO)
    
    es = EventSourcingAction(snapshot_threshold=5)
    
    # Apply event handler
    def apply_order_event(state: Dict, event: Event) -> None:
        if event.event_type == "OrderCreated":
            state["order_id"] = event.aggregate_id
            state["status"] = "created"
            state["items"] = event.payload.get("items", [])
        elif event.event_type == "OrderShipped":
            state["status"] = "shipped"
        elif event.event_type == "OrderDelivered":
            state["status"] = "delivered"
    
    # Publish events
    events = [
        ("order-1", "OrderCreated", {"items": [{"sku": "ITEM-1", "qty": 2}]}),
        ("order-1", "OrderShipped", {}),
        ("order-1", "OrderDelivered", {}),
    ]
    
    for agg_id, event_type, payload in events:
        es.publish(agg_id, event_type, payload)
    
    # Load and reconstruct
    state = es.load_aggregate("order-1", {}, apply_order_event)
    print(f"Order state: {state}")
    print(f"Stats: {json.dumps(es.get_stats(), indent=2, default=str)}")
