"""Event Sourcing Action Module.

Provides event sourcing pattern for
state storage as event sequences.
"""

import time
from typing import Any, Dict, List, Optional, Callable
from dataclasses import dataclass, field
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


@dataclass
class DomainEvent:
    """Domain event."""
    event_id: str
    aggregate_id: str
    event_type: str
    payload: Dict
    timestamp: float = field(default_factory=time.time)
    version: int = 0


class EventStore:
    """Event store for event sourcing."""

    def __init__(self):
        self._events: Dict[str, List[DomainEvent]] = {}
        self._snapshots: Dict[str, Dict] = {}

    def append_event(
        self,
        aggregate_id: str,
        event_type: str,
        payload: Dict
    ) -> str:
        """Append an event."""
        event_id = f"evt_{int(time.time() * 1000)}"

        if aggregate_id not in self._events:
            self._events[aggregate_id] = []

        version = len(self._events[aggregate_id])

        event = DomainEvent(
            event_id=event_id,
            aggregate_id=aggregate_id,
            event_type=event_type,
            payload=payload,
            version=version
        )

        self._events[aggregate_id].append(event)
        return event_id

    def get_events(
        self,
        aggregate_id: str,
        from_version: int = 0
    ) -> List[DomainEvent]:
        """Get events for aggregate."""
        events = self._events.get(aggregate_id, [])
        return [e for e in events if e.version >= from_version]

    def reconstruct_state(
        self,
        aggregate_id: str,
        apply_func: Callable
    ) -> Optional[Dict]:
        """Reconstruct current state."""
        events = self._events.get(aggregate_id, [])
        if not events:
            return None

        state = self._snapshots.get(aggregate_id, {})

        for event in events:
            state = apply_func(state, event) or state

        return state


class EventSourcingAction(BaseAction):
    """Action for event sourcing operations."""

    def __init__(self):
        super().__init__("event_sourcing")
        self._store = EventStore()

    def execute(self, params: Dict) -> ActionResult:
        """Execute event sourcing action."""
        try:
            operation = params.get("operation", "append")

            if operation == "append":
                return self._append(params)
            elif operation == "get_events":
                return self._get_events(params)
            elif operation == "reconstruct":
                return self._reconstruct(params)
            else:
                return ActionResult(success=False, message=f"Unknown: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=str(e))

    def _append(self, params: Dict) -> ActionResult:
        """Append event."""
        event_id = self._store.append_event(
            aggregate_id=params.get("aggregate_id", ""),
            event_type=params.get("event_type", ""),
            payload=params.get("payload", {})
        )
        return ActionResult(success=True, data={"event_id": event_id})

    def _get_events(self, params: Dict) -> ActionResult:
        """Get events."""
        events = self._store.get_events(
            aggregate_id=params.get("aggregate_id", ""),
            from_version=params.get("from_version", 0)
        )
        return ActionResult(success=True, data={
            "events": [
                {
                    "event_id": e.event_id,
                    "event_type": e.event_type,
                    "version": e.version,
                    "timestamp": e.timestamp
                }
                for e in events
            ]
        })

    def _reconstruct(self, params: Dict) -> ActionResult:
        """Reconstruct state."""
        def default_apply(state, event):
            return state

        state = self._store.reconstruct_state(
            aggregate_id=params.get("aggregate_id", ""),
            apply_func=params.get("apply_func") or default_apply
        )
        if state is None:
            return ActionResult(success=False, message="No events found")
        return ActionResult(success=True, data={"state": state})
