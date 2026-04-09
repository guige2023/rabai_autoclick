"""
Event sourcing action for immutable event storage.

Provides event store, replay, and projection capabilities.
"""

from typing import Any, Callable, Dict, List, Optional
import time
import json
import uuid


class EventSourcingAction:
    """Event sourcing for immutable event storage and replay."""

    def __init__(
        self,
        max_events: int = 100000,
        snapshot_interval: int = 100,
    ) -> None:
        """
        Initialize event sourcing.

        Args:
            max_events: Maximum events to store
            snapshot_interval: Events between snapshots
        """
        self.max_events = max_events
        self.snapshot_interval = snapshot_interval

        self._events: List[Dict[str, Any]] = []
        self._snapshots: Dict[str, Dict[str, Any]] = {}
        self._projections: Dict[str, Callable] = {}
        self._event_handlers: Dict[str, List[Callable]] = {}

    def execute(self, params: dict[str, Any]) -> dict[str, Any]:
        """
        Execute event sourcing operation.

        Args:
            params: Dictionary containing:
                - operation: 'append', 'replay', 'project', 'snapshot', 'handlers'
                - aggregate_id: Aggregate identifier
                - event_type: Type of event
                - event_data: Event payload
                - from_version: Starting version for replay

        Returns:
            Dictionary with operation result
        """
        operation = params.get("operation", "append")

        if operation == "append":
            return self._append_event(params)
        elif operation == "replay":
            return self._replay_events(params)
        elif operation == "project":
            return self._project_events(params)
        elif operation == "snapshot":
            return self._create_snapshot(params)
        elif operation == "handlers":
            return self._register_handlers(params)
        else:
            return {"success": False, "error": f"Unknown operation: {operation}"}

    def _append_event(self, params: dict[str, Any]) -> dict[str, Any]:
        """Append new event to store."""
        aggregate_id = params.get("aggregate_id", "")
        event_type = params.get("event_type", "")
        event_data = params.get("event_data", {})
        metadata = params.get("metadata", {})

        if not aggregate_id or not event_type:
            return {"success": False, "error": "aggregate_id and event_type are required"}

        event_id = str(uuid.uuid4())
        version = len(self._events) + 1

        event = {
            "id": event_id,
            "aggregate_id": aggregate_id,
            "event_type": event_type,
            "data": event_data,
            "metadata": metadata,
            "version": version,
            "timestamp": time.time(),
        }

        self._events.append(event)

        if len(self._events) > self.max_events:
            self._events = self._events[-self.max_events:]

        if event_type in self._event_handlers:
            for handler in self._event_handlers[event_type]:
                try:
                    handler(event)
                except Exception:
                    pass

        return {
            "success": True,
            "event_id": event_id,
            "version": version,
            "timestamp": event["timestamp"],
        }

    def _replay_events(self, params: dict[str, Any]) -> dict[str, Any]:
        """Replay events for an aggregate."""
        aggregate_id = params.get("aggregate_id", "")
        from_version = params.get("from_version", 1)
        to_version = params.get("to_version")
        event_types = params.get("event_types", [])

        if not aggregate_id:
            return {"success": False, "error": "aggregate_id is required"}

        events = [
            e for e in self._events
            if e["aggregate_id"] == aggregate_id
            and e["version"] >= from_version
            and (not to_version or e["version"] <= to_version)
            and (not event_types or e["event_type"] in event_types)
        ]

        return {
            "success": True,
            "aggregate_id": aggregate_id,
            "events": events,
            "event_count": len(events),
            "from_version": from_version,
            "to_version": events[-1]["version"] if events else None,
        }

    def _project_events(self, params: dict[str, Any]) -> dict[str, Any]:
        """Project events into read model."""
        projection_name = params.get("projection_name", "")
        aggregate_id = params.get("aggregate_id")
        initial_state = params.get("initial_state", {})

        if projection_name not in self._projections:
            return {"success": False, "error": f"Projection '{projection_name}' not found"}

        projection_fn = self._projections[projection_name]

        events_query = self._events
        if aggregate_id:
            events_query = [e for e in events_query if e["aggregate_id"] == aggregate_id]

        state = initial_state
        for event in events_query:
            try:
                state = projection_fn(state, event)
            except Exception as e:
                return {"success": False, "error": f"Projection failed: {e}"}

        return {
            "success": True,
            "projection": projection_name,
            "state": state,
            "events_processed": len(events_query),
        }

    def _create_snapshot(self, params: dict[str, Any]) -> dict[str, Any]:
        """Create snapshot for an aggregate."""
        aggregate_id = params.get("aggregate_id", "")
        state = params.get("state", {})

        if not aggregate_id:
            return {"success": False, "error": "aggregate_id is required"}

        version = len([e for e in self._events if e["aggregate_id"] == aggregate_id])

        self._snapshots[aggregate_id] = {
            "aggregate_id": aggregate_id,
            "state": state,
            "version": version,
            "created_at": time.time(),
        }

        return {
            "success": True,
            "aggregate_id": aggregate_id,
            "snapshot_version": version,
        }

    def _register_handlers(self, params: dict[str, Any]) -> dict[str, Any]:
        """Register event handlers."""
        event_type = params.get("event_type", "")
        handler = params.get("handler")

        if not event_type or not callable(handler):
            return {"success": False, "error": "event_type and handler are required"}

        if event_type not in self._event_handlers:
            self._event_handlers[event_type] = []

        self._event_handlers[event_type].append(handler)

        return {"success": True, "event_type": event_type, "handler_count": len(self._event_handlers[event_type])}

    def register_projection(self, name: str, projection_fn: Callable) -> None:
        """Register a named projection function."""
        self._projections[name] = projection_fn

    def get_event_store_stats(self) -> Dict[str, Any]:
        """Get event store statistics."""
        return {
            "total_events": len(self._events),
            "max_events": self.max_events,
            "snapshots": len(self._snapshots),
            "projections": list(self._projections.keys()),
            "event_types": list(self._event_handlers.keys()),
        }
