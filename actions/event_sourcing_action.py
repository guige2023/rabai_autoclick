"""Event sourcing action module for RabAI AutoClick.

Provides event sourcing patterns: event store, aggregate rebuild,
snapshots, and event replay capabilities.
"""

from __future__ import annotations

import sys
import os
import json
import hashlib
import uuid
from typing import Any, Dict, List, Optional, Callable
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


@dataclass
class StoredEvent:
    """A stored domain event."""
    event_id: str
    aggregate_id: str
    aggregate_type: str
    event_type: str
    version: int
    payload: Dict[str, Any]
    metadata: Dict[str, Any]
    timestamp: str
    checksum: str

    @classmethod
    def create(
        cls,
        aggregate_id: str,
        aggregate_type: str,
        event_type: str,
        version: int,
        payload: Dict[str, Any],
        metadata: Optional[Dict[str, Any]] = None
    ) -> "StoredEvent":
        ts = datetime.now(timezone.utc).isoformat()
        meta = metadata or {}
        raw = f"{aggregate_id}{aggregate_type}{event_type}{version}{json.dumps(payload, sort_keys=True)}{ts}"
        checksum = hashlib.sha256(raw.encode()).hexdigest()[:16]
        return cls(
            event_id=str(uuid.uuid4()),
            aggregate_id=aggregate_id,
            aggregate_type=aggregate_type,
            event_type=event_type,
            version=version,
            payload=payload,
            metadata=meta,
            timestamp=ts,
            checksum=checksum
        )

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "StoredEvent":
        return cls(**d)

    def verify(self) -> bool:
        raw = f"{self.aggregate_id}{self.aggregate_type}{self.event_type}" \
               f"{self.version}{json.dumps(self.payload, sort_keys=True)}{self.timestamp}"
        expected = hashlib.sha256(raw.encode()).hexdigest()[:16]
        return self.checksum == expected


class InMemoryEventStore:
    """In-memory event store for testing and small-scale usage."""

    def __init__(self):
        self._events: List[StoredEvent] = []
        self._snapshots: Dict[str, List[Dict[str, Any]]] = defaultdict(list)

    def append(self, event: StoredEvent) -> None:
        self._events.append(event)

    def get_for_aggregate(
        self,
        aggregate_id: str,
        from_version: int = 0
    ) -> List[StoredEvent]:
        return [e for e in self._events
                if e.aggregate_id == aggregate_id and e.version > from_version]

    def get_all(self, aggregate_type: Optional[str] = None,
                event_type: Optional[str] = None,
                limit: int = 1000) -> List[StoredEvent]:
        result = self._events
        if aggregate_type:
            result = [e for e in result if e.aggregate_type == aggregate_type]
        if event_type:
            result = [e for e in result if e.event_type == event_type]
        return result[-limit:]

    def save_snapshot(self, aggregate_id: str, version: int,
                      state: Dict[str, Any]) -> None:
        self._snapshots[aggregate_id].append({
            "version": version, "state": state, "timestamp": datetime.now(timezone.utc).isoformat()
        })

    def get_latest_snapshot(self, aggregate_id: str) -> Optional[Dict[str, Any]]:
        snaps = self._snapshots.get(aggregate_id, [])
        return snaps[-1] if snaps else None

    def clear(self) -> None:
        self._events.clear()
        self._snapshots.clear()


class EventStoreAction(BaseAction):
    """Append and query events from an event store.
    
    Supports append, get-by-aggregate, get-by-type,
    snapshot management, and checksum verification.
    
    Args:
        store: InMemoryEventStore instance (or subclass)
    """

    def __init__(self, store: Optional[InMemoryEventStore] = None):
        super().__init__()
        self._store = store or InMemoryEventStore()

    def execute(
        self,
        action: str,
        aggregate_id: Optional[str] = None,
        aggregate_type: Optional[str] = None,
        event_type: Optional[str] = None,
        version: Optional[int] = None,
        payload: Optional[Dict[str, Any]] = None,
        metadata: Optional[Dict[str, Any]] = None,
        from_version: int = 0,
        limit: int = 1000
    ) -> ActionResult:
        try:
            if action == "append":
                if not aggregate_id or not aggregate_type or not event_type or \
                   version is None or payload is None:
                    return ActionResult(success=False,
                                        error="aggregate_id, aggregate_type, event_type, version, payload required")
                event = StoredEvent.create(
                    aggregate_id, aggregate_type, event_type, version, payload, metadata
                )
                self._store.append(event)
                return ActionResult(success=True, data={"event_id": event.event_id, "checksum": event.checksum})

            elif action == "get_for_aggregate":
                if not aggregate_id:
                    return ActionResult(success=False, error="aggregate_id required")
                events = self._store.get_for_aggregate(aggregate_id, from_version)
                return ActionResult(success=True, data={
                    "count": len(events),
                    "events": [e.to_dict() for e in events]
                })

            elif action == "get_all":
                events = self._store.get_all(aggregate_type, event_type, limit)
                return ActionResult(success=True, data={
                    "count": len(events),
                    "events": [e.to_dict() for e in events]
                })

            elif action == "verify":
                if not aggregate_id:
                    return ActionResult(success=False, error="aggregate_id required")
                events = self._store.get_for_aggregate(aggregate_id)
                results = [{"event_id": e.event_id, "valid": e.verify()} for e in events]
                return ActionResult(success=True, data={"verifications": results})

            elif action == "stats":
                all_events = self._store.get_all()
                return ActionResult(success=True, data={
                    "total_events": len(all_events),
                    "aggregate_types": list(set(e.aggregate_type for e in all_events)),
                    "event_types": list(set(e.event_type for e in all_events)),
                })
            else:
                return ActionResult(success=False, error=f"Unknown action: {action}")
        except Exception as e:
            return ActionResult(success=False, error=str(e))


class EventReplayAction(BaseAction):
    """Replay events to rebuild aggregate state.
    
    Applies events sequentially to a reducer function to
    reconstruct the state at any version.
    
    Args:
        store: InMemoryEventStore instance
        reducer: Callable[[state, event] -> new_state]
    """

    def __init__(self, store: InMemoryEventStore,
                 reducer: Optional[Callable[[Dict[str, Any], StoredEvent], Dict[str, Any]]] = None):
        super().__init__()
        self._store = store
        self._reducer = reducer

    def execute(
        self,
        aggregate_id: str,
        reducer_type: str = "default",
        from_version: int = 0,
        use_snapshot: bool = True
    ) -> ActionResult:
        try:
            state: Dict[str, Any] = {}

            if use_snapshot:
                snap = self._store.get_latest_snapshot(aggregate_id)
                if snap and snap["version"] >= from_version:
                    state = snap["state"]
                    from_version = snap["version"]

            events = self._store.get_for_aggregate(aggregate_id, from_version)

            if not events:
                return ActionResult(success=True, data={"state": state, "events_applied": 0})

            for event in events:
                if self._reducer:
                    state = self._reducer(state, event)
                elif reducer_type == "default":
                    state = self._default_reducer(state, event)
                elif reducer_type == "counter":
                    state = self._counter_reducer(state, event)
                elif reducer_type == "append_only":
                    state = self._append_only_reducer(state, event)
                else:
                    return ActionResult(success=False, error=f"Unknown reducer_type: {reducer_type}")

            return ActionResult(success=True, data={
                "state": state,
                "events_applied": len(events),
                "final_version": events[-1].version if events else from_version
            })
        except Exception as e:
            return ActionResult(success=False, error=str(e))

    def _default_reducer(self, state: Dict[str, Any], event: StoredEvent) -> Dict[str, Any]:
        new_state = state.copy()
        new_state[event.event_type] = event.payload
        new_state["_version"] = event.version
        new_state["_last_event"] = event.event_type
        return new_state

    def _counter_reducer(self, state: Dict[str, Any], event: StoredEvent) -> Dict[str, Any]:
        new_state = state.copy()
        key = event.event_type
        new_state[key] = new_state.get(key, 0) + 1
        new_state["_version"] = event.version
        return new_state

    def _append_only_reducer(self, state: Dict[str, Any], event: StoredEvent) -> Dict[str, Any]:
        new_state = state.copy()
        if "events" not in new_state:
            new_state["events"] = []
        new_state["events"].append(event.to_dict())
        new_state["_version"] = event.version
        return new_state


class SnapshotAction(BaseAction):
    """Manage snapshots for event-sourced aggregates.
    
    Snapshots reduce replay time by storing periodic state.
    Supports save, get-latest, get-at-version, and prune.
    
    Args:
        store: InMemoryEventStore instance
        snapshot_interval: Create snapshot every N versions
    """

    def __init__(self, store: InMemoryEventStore, snapshot_interval: int = 10):
        super().__init__()
        self._store = store
        self._interval = snapshot_interval

    def execute(
        self,
        action: str,
        aggregate_id: str,
        state: Optional[Dict[str, Any]] = None,
        version: Optional[int] = None
    ) -> ActionResult:
        try:
            if action == "save":
                if state is None or version is None:
                    return ActionResult(success=False, error="state and version required")
                self._store.save_snapshot(aggregate_id, version, state)
                return ActionResult(success=True, data={
                    "aggregate_id": aggregate_id,
                    "version": version
                })

            elif action == "get_latest":
                snap = self._store.get_latest_snapshot(aggregate_id)
                if not snap:
                    return ActionResult(success=False, error="No snapshot found")
                return ActionResult(success=True, data={
                    "version": snap["version"],
                    "state": snap["state"],
                    "timestamp": snap["timestamp"]
                })

            elif action == "should_snapshot":
                events = self._store.get_for_aggregate(aggregate_id)
                if not events:
                    return ActionResult(success=False, error="No events found")
                latest_version = events[-1].version
                should = latest_version % self._interval == 0
                return ActionResult(success=True, data={
                    "latest_version": latest_version,
                    "should_snapshot": should
                })

            else:
                return ActionResult(success=False, error=f"Unknown action: {action}")
        except Exception as e:
            return ActionResult(success=False, error=str(e))
