"""Event Sourcing action module for RabAI AutoClick.

Provides event sourcing infrastructure with event store,
aggregate reconstruction, snapshot support, and event replay.
"""

import sys
import os
import json
import time
import uuid
from typing import Any, Dict, List, Optional, Callable, Type
from dataclasses import dataclass, field
from enum import Enum
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class EventType(Enum):
    """Base event types."""
    DOMAIN_EVENT = "domain_event"
    COMMAND = "command"
    SNAPSHOT = "snapshot"
    SYSTEM_EVENT = "system_event"


@dataclass
class DomainEvent:
    """Represents a domain event in the event store."""
    event_id: str
    aggregate_id: str
    aggregate_type: str
    event_type: str
    event_type_short: str  # Short type name for display
    version: int
    timestamp: float
    data: Dict[str, Any]
    metadata: Dict[str, Any] = field(default_factory=dict)
    causation_id: Optional[str] = None
    correlation_id: Optional[str] = None
    user_id: Optional[str] = None
    
    def __post_init__(self):
        if not self.event_type_short:
            self.event_type_short = self.event_type.split(".")[-1] if "." in self.event_type else self.event_type


@dataclass
class AggregateSnapshot:
    """Snapshot of aggregate state at a point in time."""
    snapshot_id: str
    aggregate_id: str
    aggregate_type: str
    version: int
    timestamp: float
    state: Dict[str, Any]
    event_count: int  # Number of events since last snapshot


@dataclass
class AggregateConfig:
    """Configuration for an aggregate type."""
    aggregate_type: str
    event_handlers: Dict[str, str] = field(default_factory=dict)  # event_type -> method name
    snapshot_threshold: int = 100  # Create snapshot every N events
    snapshot_enabled: bool = True
    description: str = ""


class EventStore:
    """Event store with append-only event log."""
    
    def __init__(self, persistence_path: Optional[str] = None):
        self._events: Dict[str, List[DomainEvent]] = defaultdict(list)  # aggregate_id -> events
        self._snapshots: Dict[str, List[AggregateSnapshot]] = defaultdict(list)
        self._aggregate_configs: Dict[str, AggregateConfig] = {}
        self._event_handlers: Dict[str, Callable] = {}
        self._snapshots_by_aggregate: Dict[str, AggregateSnapshot] = {}  # Latest snapshot per aggregate
        self._persistence_path = persistence_path
        self._version_counter: Dict[str, int] = defaultdict(int)
        self._load()
    
    def _load(self) -> None:
        """Load event store from persistence."""
        if self._persistence_path and os.path.exists(self._persistence_path):
            try:
                with open(self._persistence_path, 'r') as f:
                    data = json.load(f)
                    for agg_id, events_data in data.get("events", {}).items():
                        events = []
                        for e_data in events_data:
                            e_data.pop('event_type_short', None)
                            events.append(DomainEvent(**e_data))
                        self._events[agg_id] = events
                        if events:
                            self._version_counter[agg_id] = max(e.version for e in events)
                    
                    for agg_id, snapshots_data in data.get("snapshots", {}).items():
                        for s_data in snapshots_data:
                            snap = AggregateSnapshot(**s_data)
                            self._snapshots[agg_id].append(snap)
                            if (agg_id not in self._snapshots_by_aggregate or
                                snap.version > self._snapshots_by_aggregate[agg_id].version):
                                self._snapshots_by_aggregate[agg_id] = snap
                    
                    for cfg_data in data.get("configs", []):
                        self._aggregate_configs[cfg_data["aggregate_type"]] = AggregateConfig(**cfg_data)
            except (json.JSONDecodeError, TypeError, KeyError):
                pass
    
    def _persist(self) -> None:
        """Persist event store."""
        if self._persistence_path:
            try:
                data = {
                    "events": {
                        agg_id: [
                            {
                                "event_id": e.event_id,
                                "aggregate_id": e.aggregate_id,
                                "aggregate_type": e.aggregate_type,
                                "event_type": e.event_type,
                                "event_type_short": e.event_type_short,
                                "version": e.version,
                                "timestamp": e.timestamp,
                                "data": e.data,
                                "metadata": e.metadata,
                                "causation_id": e.causation_id,
                                "correlation_id": e.correlation_id,
                                "user_id": e.user_id
                            }
                            for e in events
                        ]
                        for agg_id, events in self._events.items()
                    },
                    "snapshots": {
                        agg_id: [vars(s) for s in snaps]
                        for agg_id, snaps in self._snapshots.items()
                    },
                    "configs": [vars(c) for c in self._aggregate_configs.values()]
                }
                with open(self._persistence_path, 'w') as f:
                    json.dump(data, f, indent=2, default=str)
            except OSError:
                pass
    
    def register_aggregate(self, config: AggregateConfig) -> None:
        """Register an aggregate type configuration."""
        self._aggregate_configs[config.aggregate_type] = config
    
    def register_event_handler(self, event_type: str, handler: Callable) -> None:
        """Register a handler function for an event type."""
        self._event_handlers[event_type] = handler
    
    def append_event(
        self,
        aggregate_id: str,
        aggregate_type: str,
        event_type: str,
        data: Dict[str, Any],
        metadata: Optional[Dict[str, Any]] = None,
        causation_id: Optional[str] = None,
        correlation_id: Optional[str] = None,
        user_id: Optional[str] = None,
        expected_version: Optional[int] = None
    ) -> DomainEvent:
        """Append a new event to the aggregate's event stream.
        
        Args:
            aggregate_id: Unique identifier of the aggregate.
            aggregate_type: Type name of the aggregate.
            event_type: Full qualified event type name.
            data: Event payload data.
            metadata: Optional event metadata.
            causation_id: ID of the command that caused this event.
            correlation_id: ID for correlating related events.
            user_id: ID of user who triggered the event.
            expected_version: Expected version for optimistic concurrency.
        
        Returns:
            The appended DomainEvent.
        """
        # Optimistic concurrency check
        if expected_version is not None:
            current_version = self._version_counter[aggregate_id]
            if current_version != expected_version:
                raise ValueError(
                    f"Version conflict: expected {expected_version}, current {current_version}"
                )
        
        version = self._version_counter[aggregate_id] + 1
        
        event = DomainEvent(
            event_id=str(uuid.uuid4()),
            aggregate_id=aggregate_id,
            aggregate_type=aggregate_type,
            event_type=event_type,
            event_type_short=event_type.split(".")[-1] if "." in event_type else event_type,
            version=version,
            timestamp=time.time(),
            data=data,
            metadata=metadata or {},
            causation_id=causation_id,
            correlation_id=correlation_id,
            user_id=user_id
        )
        
        self._events[aggregate_id].append(event)
        self._version_counter[aggregate_id] = version
        
        # Trigger registered handlers
        if event_type in self._event_handlers:
            try:
                self._event_handlers[event_type](event)
            except Exception:
                pass  # Handler errors don't prevent event storage
        
        # Check if snapshot needed
        config = self._aggregate_configs.get(aggregate_type)
        if config and config.snapshot_enabled and version % config.snapshot_threshold == 0:
            self._create_snapshot(aggregate_id, aggregate_type, version)
        
        self._persist()
        return event
    
    def _create_snapshot(self, aggregate_id: str, aggregate_type: str, 
                         version: int) -> None:
        """Create a snapshot of aggregate state."""
        state = self.reconstruct_state(aggregate_id)
        
        events = self._events.get(aggregate_id, [])
        snapshot = AggregateSnapshot(
            snapshot_id=str(uuid.uuid4()),
            aggregate_id=aggregate_id,
            aggregate_type=aggregate_type,
            version=version,
            timestamp=time.time(),
            state=state,
            event_count=len(events)
        )
        
        self._snapshots[aggregate_id].append(snapshot)
        self._snapshots_by_aggregate[aggregate_id] = snapshot
        self._persist()
    
    def get_events(
        self,
        aggregate_id: str,
        from_version: int = 0,
        to_version: Optional[int] = None,
        event_types: Optional[List[str]] = None
    ) -> List[DomainEvent]:
        """Get events for an aggregate.
        
        Args:
            aggregate_id: Aggregate identifier.
            from_version: Starting version (exclusive).
            to_version: Ending version (inclusive), None for all.
            event_types: Filter to specific event types.
        
        Returns:
            List of DomainEvents.
        """
        events = self._events.get(aggregate_id, [])
        
        # Filter by version
        if from_version > 0:
            events = [e for e in events if e.version > from_version]
        if to_version is not None:
            events = [e for e in events if e.version <= to_version]
        
        # Filter by event types
        if event_types:
            events = [e for e in events if e.event_type in event_types]
        
        return events
    
    def get_events_since_snapshot(
        self,
        aggregate_id: str,
        snapshot_version: Optional[int] = None
    ) -> List[DomainEvent]:
        """Get events since a snapshot (or from beginning)."""
        if snapshot_version is None:
            latest_snapshot = self._snapshots_by_aggregate.get(aggregate_id)
            snapshot_version = latest_snapshot.version if latest_snapshot else 0
        
        return self.get_events(aggregate_id, from_version=snapshot_version)
    
    def reconstruct_state(
        self,
        aggregate_id: str,
        from_version: int = 0
    ) -> Dict[str, Any]:
        """Reconstruct aggregate state by replaying events.
        
        Args:
            aggregate_id: Aggregate identifier.
            from_version: Start replaying from this version.
        
        Returns:
            Reconstructed state as a dict.
        """
        events = self.get_events(aggregate_id, from_version=from_version)
        state: Dict[str, Any] = {}
        
        for event in events:
            # Apply event data to state (simplified apply)
            if event.event_type_short.startswith("Created") or "Created" in event.event_type:
                state["created"] = True
                state["id"] = aggregate_id
            
            # Merge event data into state
            if isinstance(event.data, dict):
                state.update(event.data)
            state["_last_event_version"] = event.version
            state["_last_event_type"] = event.event_type_short
        
        return state
    
    def reconstruct_from_snapshot(
        self,
        aggregate_id: str
    ) -> tuple[Dict[str, Any], int]:
        """Reconstruct state using latest snapshot + events.
        
        Returns:
            Tuple of (state, version).
        """
        latest_snapshot = self._snapshots_by_aggregate.get(aggregate_id)
        
        if latest_snapshot:
            state = self.reconstruct_state(aggregate_id, from_version=latest_snapshot.version)
            return state, latest_snapshot.version
        
        state = self.reconstruct_state(aggregate_id)
        return state, self._version_counter[aggregate_id]
    
    def get_snapshot(
        self,
        aggregate_id: str,
        version: Optional[int] = None
    ) -> Optional[AggregateSnapshot]:
        """Get a snapshot by aggregate ID and optionally version."""
        if version is not None:
            snapshots = self._snapshots.get(aggregate_id, [])
            for snap in reversed(snapshots):
                if snap.version == version:
                    return snap
            return None
        
        return self._snapshots_by_aggregate.get(aggregate_id)
    
    def get_aggregate_version(self, aggregate_id: str) -> int:
        """Get current version of an aggregate."""
        return self._version_counter.get(aggregate_id, 0)
    
    def list_aggregates(
        self,
        aggregate_type: Optional[str] = None
    ) -> List[str]:
        """List all aggregate IDs, optionally filtered by type."""
        if aggregate_type:
            return [
                agg_id for agg_id, events in self._events.items()
                if events and events[0].aggregate_type == aggregate_type
            ]
        return list(self._events.keys())
    
    def get_event_stream_info(
        self,
        aggregate_id: str
    ) -> Dict[str, Any]:
        """Get information about an aggregate's event stream."""
        events = self._events.get(aggregate_id, [])
        snapshot = self._snapshots_by_aggregate.get(aggregate_id)
        
        return {
            "aggregate_id": aggregate_id,
            "event_count": len(events),
            "current_version": self._version_counter.get(aggregate_id, 0),
            "has_snapshot": snapshot is not None,
            "snapshot_version": snapshot.version if snapshot else None,
            "first_event_time": events[0].timestamp if events else None,
            "last_event_time": events[-1].timestamp if events else None,
            "event_types": list(set(e.event_type_short for e in events))
        }


class EventSourcingAction(BaseAction):
    """Event sourcing infrastructure for aggregate state management.
    
    Supports event appending, state reconstruction, snapshots,
    event replay, and aggregate version tracking.
    """
    action_type = "event_sourcing"
    display_name = "事件溯源"
    description = "事件溯源系统，支持聚合体重建、快照和事件回放"
    
    def __init__(self):
        super().__init__()
        self._store: Optional[EventStore] = None
    
    def _get_store(self, params: Dict[str, Any]) -> EventStore:
        """Get or create the event store."""
        if self._store is None:
            persistence_path = params.get("persistence_path")
            self._store = EventStore(persistence_path)
        return self._store
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute event sourcing operation.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - operation: "register_aggregate", "append_event", "get_events",
                  "reconstruct_state", "reconstruct_from_snapshot", "get_snapshot",
                  "get_version", "list_aggregates", "get_stream_info"
                - For register: aggregate_type, config
                - For append: aggregate_id, aggregate_type, event_type, data
                - For reconstruct/get: aggregate_id
        
        Returns:
            ActionResult with operation result.
        """
        operation = params.get("operation", "")
        
        try:
            if operation == "register_aggregate":
                return self._register_aggregate(params)
            elif operation == "append_event":
                return self._append_event(params)
            elif operation == "get_events":
                return self._get_events(params)
            elif operation == "reconstruct_state":
                return self._reconstruct_state(params)
            elif operation == "reconstruct_from_snapshot":
                return self._reconstruct_from_snapshot(params)
            elif operation == "get_snapshot":
                return self._get_snapshot(params)
            elif operation == "get_version":
                return self._get_version(params)
            elif operation == "list_aggregates":
                return self._list_aggregates(params)
            elif operation == "get_stream_info":
                return self._get_stream_info(params)
            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")
        except Exception as e:
            return ActionResult(success=False, message=f"Event sourcing error: {str(e)}")
    
    def _register_aggregate(self, params: Dict[str, Any]) -> ActionResult:
        """Register an aggregate type."""
        store = self._get_store(params)
        aggregate_type = params.get("aggregate_type", "")
        
        if not aggregate_type:
            return ActionResult(success=False, message="aggregate_type is required")
        
        config = AggregateConfig(
            aggregate_type=aggregate_type,
            snapshot_threshold=params.get("snapshot_threshold", 100),
            snapshot_enabled=params.get("snapshot_enabled", True),
            description=params.get("description", "")
        )
        store.register_aggregate(config)
        return ActionResult(
            success=True,
            message=f"Aggregate '{aggregate_type}' registered",
            data={"aggregate_type": aggregate_type}
        )
    
    def _append_event(self, params: Dict[str, Any]) -> ActionResult:
        """Append an event to an aggregate."""
        store = self._get_store(params)
        aggregate_id = params.get("aggregate_id", "")
        aggregate_type = params.get("aggregate_type", "")
        event_type = params.get("event_type", "")
        data = params.get("data", {})
        
        if not all([aggregate_id, aggregate_type, event_type]):
            return ActionResult(success=False, message="aggregate_id, aggregate_type, and event_type are required")
        
        event = store.append_event(
            aggregate_id=aggregate_id,
            aggregate_type=aggregate_type,
            event_type=event_type,
            data=data,
            metadata=params.get("metadata"),
            causation_id=params.get("causation_id"),
            correlation_id=params.get("correlation_id"),
            user_id=params.get("user_id"),
            expected_version=params.get("expected_version")
        )
        return ActionResult(
            success=True,
            message=f"Event appended: {event.event_type_short} v{event.version}",
            data={
                "event_id": event.event_id,
                "aggregate_id": event.aggregate_id,
                "version": event.version,
                "timestamp": event.timestamp
            }
        )
    
    def _get_events(self, params: Dict[str, Any]) -> ActionResult:
        """Get events for an aggregate."""
        store = self._get_store(params)
        aggregate_id = params.get("aggregate_id", "")
        
        if not aggregate_id:
            return ActionResult(success=False, message="aggregate_id is required")
        
        events = store.get_events(
            aggregate_id,
            from_version=params.get("from_version", 0),
            to_version=params.get("to_version"),
            event_types=params.get("event_types")
        )
        return ActionResult(
            success=True,
            message=f"Found {len(events)} events",
            data={
                "events": [
                    {"event_id": e.event_id, "event_type": e.event_type_short,
                     "version": e.version, "timestamp": e.timestamp, "data": e.data}
                    for e in events
                ]
            }
        )
    
    def _reconstruct_state(self, params: Dict[str, Any]) -> ActionResult:
        """Reconstruct aggregate state by replaying events."""
        store = self._get_store(params)
        aggregate_id = params.get("aggregate_id", "")
        
        if not aggregate_id:
            return ActionResult(success=False, message="aggregate_id is required")
        
        state = store.reconstruct_state(
            aggregate_id,
            from_version=params.get("from_version", 0)
        )
        version = store.get_aggregate_version(aggregate_id)
        return ActionResult(
            success=True,
            message=f"State reconstructed at version {version}",
            data={"state": state, "version": version}
        )
    
    def _reconstruct_from_snapshot(self, params: Dict[str, Any]) -> ActionResult:
        """Reconstruct state using snapshot + events."""
        store = self._get_store(params)
        aggregate_id = params.get("aggregate_id", "")
        
        if not aggregate_id:
            return ActionResult(success=False, message="aggregate_id is required")
        
        state, version = store.reconstruct_from_snapshot(aggregate_id)
        snapshot = store.get_snapshot(aggregate_id)
        return ActionResult(
            success=True,
            message=f"State reconstructed from snapshot at version {version}",
            data={
                "state": state,
                "version": version,
                "snapshot_version": snapshot.version if snapshot else None
            }
        )
    
    def _get_snapshot(self, params: Dict[str, Any]) -> ActionResult:
        """Get snapshot for an aggregate."""
        store = self._get_store(params)
        aggregate_id = params.get("aggregate_id", "")
        
        if not aggregate_id:
            return ActionResult(success=False, message="aggregate_id is required")
        
        snapshot = store.get_snapshot(aggregate_id, params.get("version"))
        if not snapshot:
            return ActionResult(success=False, message="No snapshot found")
        
        return ActionResult(
            success=True,
            message=f"Snapshot at version {snapshot.version}",
            data={
                "snapshot_id": snapshot.snapshot_id,
                "version": snapshot.version,
                "timestamp": snapshot.timestamp,
                "state": snapshot.state
            }
        )
    
    def _get_version(self, params: Dict[str, Any]) -> ActionResult:
        """Get current version of an aggregate."""
        store = self._get_store(params)
        aggregate_id = params.get("aggregate_id", "")
        
        if not aggregate_id:
            return ActionResult(success=False, message="aggregate_id is required")
        
        version = store.get_aggregate_version(aggregate_id)
        return ActionResult(
            success=True,
            message=f"Aggregate version: {version}",
            data={"aggregate_id": aggregate_id, "version": version}
        )
    
    def _list_aggregates(self, params: Dict[str, Any]) -> ActionResult:
        """List aggregate IDs."""
        store = self._get_store(params)
        aggregate_type = params.get("aggregate_type")
        
        aggregates = store.list_aggregates(aggregate_type)
        return ActionResult(
            success=True,
            message=f"Found {len(aggregates)} aggregates",
            data={"aggregates": aggregates}
        )
    
    def _get_stream_info(self, params: Dict[str, Any]) -> ActionResult:
        """Get information about an aggregate's event stream."""
        store = self._get_store(params)
        aggregate_id = params.get("aggregate_id", "")
        
        if not aggregate_id:
            return ActionResult(success=False, message="aggregate_id is required")
        
        info = store.get_event_stream_info(aggregate_id)
        return ActionResult(
            success=True,
            message="Stream info retrieved",
            data=info
        )
