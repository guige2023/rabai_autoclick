"""Observer Pattern Action Module.

Provides implementation of observer pattern for event-driven
architectures with support for async notifications.
"""
from __future__ import annotations

import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)


class ObserverEvent(Enum):
    """Event types for observers."""
    CREATED = "created"
    UPDATED = "updated"
    DELETED = "deleted"
    STATE_CHANGED = "state_changed"
    ERROR = "error"


@dataclass
class Observer:
    """Observer subscription."""
    id: str
    name: str
    event_types: List[ObserverEvent]
    callback: Any
    enabled: bool = True
    created_at: float = field(default_factory=time.time)


@dataclass
class ObservableState:
    """Observable state."""
    subject: str
    state: Dict[str, Any]
    last_updated: float = field(default_factory=time.time)


@dataclass
class EventNotification:
    """Event notification."""
    id: str
    event_type: ObserverEvent
    subject: str
    data: Any
    timestamp: float = field(default_factory=time.time)
    delivered: bool = False


class ObserverStore:
    """In-memory observer store."""

    def __init__(self):
        self._observers: Dict[str, List[Observer]] = {}
        self._state: Dict[str, ObservableState] = {}
        self._notifications: List[EventNotification] = []

    def register(self, subject: str, observer: Observer) -> None:
        """Register observer for subject."""
        if subject not in self._observers:
            self._observers[subject] = []
        self._observers[subject].append(observer)

    def unregister(self, subject: str, observer_id: str) -> bool:
        """Unregister observer."""
        if subject in self._observers:
            for i, obs in enumerate(self._observers[subject]):
                if obs.id == observer_id:
                    self._observers[subject].pop(i)
                    return True
        return False

    def get_observers(self, subject: str) -> List[Observer]:
        """Get all observers for subject."""
        return [o for o in self._observers.get(subject, []) if o.enabled]

    def notify(self, subject: str, event_type: ObserverEvent,
               data: Any) -> List[EventNotification]:
        """Notify observers of event."""
        observers = self.get_observers(subject)
        notifications = []

        for obs in observers:
            if event_type in obs.event_types:
                notification = EventNotification(
                    id=uuid.uuid4().hex,
                    event_type=event_type,
                    subject=subject,
                    data=data
                )
                try:
                    if callable(obs.callback):
                        obs.callback(notification)
                    notification.delivered = True
                except Exception:
                    pass
                notifications.append(notification)
                self._notifications.append(notification)

        return notifications

    def set_state(self, subject: str, state: Dict[str, Any]) -> None:
        """Set observable state."""
        self._state[subject] = ObservableState(
            subject=subject,
            state=state
        )

    def get_state(self, subject: str) -> Optional[ObservableState]:
        """Get observable state."""
        return self._state.get(subject)


_global_store = ObserverStore()


class ObserverPatternAction:
    """Observer pattern action.

    Example:
        action = ObserverPatternAction()

        action.subscribe("user", "UserObserver", ["created", "updated"], my_callback)
        action.emit("user", "created", {"user_id": "123"})
    """

    def __init__(self, store: Optional[ObserverStore] = None):
        self._store = store or _global_store
        self._callbacks: Dict[str, Callable] = {}

    def subscribe(self, subject: str, name: str,
                 event_types: List[str],
                 callback_id: Optional[str] = None) -> Dict[str, Any]:
        """Subscribe to subject events."""
        try:
            events = [ObserverEvent(e) for e in event_types]
        except ValueError:
            return {"success": False, "message": f"Invalid event type"}

        def default_callback(notification):
            print(f"[Observer] {notification.event_type.value}: {notification.subject}")

        callback = self._callbacks.get(callback_id) if callback_id else default_callback

        observer = Observer(
            id=uuid.uuid4().hex,
            name=name,
            event_types=events,
            callback=callback
        )

        self._store.register(subject, observer)

        return {
            "success": True,
            "observer_id": observer.id,
            "subject": subject,
            "name": name,
            "event_types": event_types,
            "message": f"Subscribed {name} to {subject}"
        }

    def unsubscribe(self, subject: str, observer_id: str) -> Dict[str, Any]:
        """Unsubscribe from subject."""
        if self._store.unregister(subject, observer_id):
            return {"success": True, "message": "Unsubscribed"}
        return {"success": False, "message": "Observer not found"}

    def emit(self, subject: str, event_type: str,
            data: Any = None) -> Dict[str, Any]:
        """Emit event to observers."""
        try:
            event = ObserverEvent(event_type)
        except ValueError:
            return {"success": False, "message": f"Invalid event type"}

        notifications = self._store.notify(subject, event, data)

        return {
            "success": True,
            "subject": subject,
            "event_type": event.value,
            "notifications_sent": len(notifications),
            "message": f"Emitted {event.value} to {len(notifications)} observers"
        }

    def set_state(self, subject: str, state: Dict[str, Any]) -> Dict[str, Any]:
        """Set observable state."""
        self._store.set_state(subject, state)
        return {
            "success": True,
            "subject": subject,
            "message": f"State set for {subject}"
        }

    def get_state(self, subject: str) -> Dict[str, Any]:
        """Get observable state."""
        state = self._store.get_state(subject)
        if state:
            return {
                "success": True,
                "subject": state.subject,
                "state": state.state,
                "last_updated": state.last_updated
            }
        return {"success": False, "message": "State not found"}

    def get_subscribers(self, subject: str) -> Dict[str, Any]:
        """Get subscribers for subject."""
        observers = self._store.get_observers(subject)
        return {
            "success": True,
            "subject": subject,
            "subscribers": [
                {
                    "id": o.id,
                    "name": o.name,
                    "event_types": [e.value for e in o.event_types],
                    "enabled": o.enabled
                }
                for o in observers
            ],
            "count": len(observers)
        }

    def get_notifications(self, subject: Optional[str] = None,
                         limit: int = 100) -> Dict[str, Any]:
        """Get notification history."""
        notifications = self._store._notifications
        if subject:
            notifications = [n for n in notifications if n.subject == subject]

        notifications = notifications[-limit:]

        return {
            "success": True,
            "notifications": [
                {
                    "id": n.id,
                    "event_type": n.event_type.value,
                    "subject": n.subject,
                    "timestamp": n.timestamp,
                    "delivered": n.delivered
                }
                for n in notifications
            ],
            "count": len(notifications)
        }


def execute(context: Any, params: Dict[str, Any]) -> Dict[str, Any]:
    """Execute observer pattern action."""
    operation = params.get("operation", "")
    action = ObserverPatternAction()

    try:
        if operation == "subscribe":
            subject = params.get("subject", "")
            name = params.get("name", "")
            event_types = params.get("event_types", [])
            if not subject or not name or not event_types:
                return {"success": False, "message": "subject, name, and event_types required"}
            return action.subscribe(
                subject=subject,
                name=name,
                event_types=event_types,
                callback_id=params.get("callback_id")
            )

        elif operation == "unsubscribe":
            subject = params.get("subject", "")
            observer_id = params.get("observer_id", "")
            if not subject or not observer_id:
                return {"success": False, "message": "subject and observer_id required"}
            return action.unsubscribe(subject, observer_id)

        elif operation == "emit":
            subject = params.get("subject", "")
            event_type = params.get("event_type", "")
            data = params.get("data")
            if not subject or not event_type:
                return {"success": False, "message": "subject and event_type required"}
            return action.emit(subject, event_type, data)

        elif operation == "set_state":
            subject = params.get("subject", "")
            state = params.get("state", {})
            if not subject:
                return {"success": False, "message": "subject required"}
            return action.set_state(subject, state)

        elif operation == "get_state":
            subject = params.get("subject", "")
            if not subject:
                return {"success": False, "message": "subject required"}
            return action.get_state(subject)

        elif operation == "get_subscribers":
            subject = params.get("subject", "")
            if not subject:
                return {"success": False, "message": "subject required"}
            return action.get_subscribers(subject)

        elif operation == "get_notifications":
            return action.get_notifications(
                subject=params.get("subject"),
                limit=params.get("limit", 100)
            )

        else:
            return {"success": False, "message": f"Unknown operation: {operation}"}

    except Exception as e:
        return {"success": False, "message": f"Observer pattern error: {str(e)}"}
