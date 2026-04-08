"""Observer Pattern Action Module.

Provides observer pattern implementation for
event notification systems.
"""

import time
import threading
from typing import Any, Dict, List, Optional, Callable
from dataclasses import dataclass, field
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


@dataclass
class Observer:
    """Observer definition."""
    observer_id: str
    name: str
    callback: Callable
    event_types: List[str]
    enabled: bool = True


@dataclass
class Event:
    """Event notification."""
    event_id: str
    event_type: str
    data: Any
    timestamp: float = field(default_factory=time.time)


class ObserverPatternManager:
    """Manages observer pattern."""

    def __init__(self):
        self._observers: Dict[str, Observer] = {}
        self._event_history: List[Event] = {}
        self._lock = threading.RLock()
        self._max_history = 1000

    def register_observer(
        self,
        name: str,
        callback: Callable,
        event_types: List[str]
    ) -> str:
        """Register observer."""
        observer_id = f"obs_{int(time.time() * 1000)}"

        observer = Observer(
            observer_id=observer_id,
            name=name,
            callback=callback,
            event_types=event_types
        )

        with self._lock:
            self._observers[observer_id] = observer

        return observer_id

    def unregister_observer(self, observer_id: str) -> bool:
        """Unregister observer."""
        with self._lock:
            if observer_id in self._observers:
                del self._observers[observer_id]
                return True
        return False

    def notify(self, event_type: str, data: Any) -> int:
        """Notify observers of event."""
        event = Event(
            event_id=f"evt_{int(time.time() * 1000)}",
            event_type=event_type,
            data=data
        )

        notified = 0

        with self._lock:
            if event_type not in self._event_history:
                self._event_history[event_type] = []

            self._event_history[event_type].append(event)

            if len(self._event_history[event_type]) > self._max_history:
                self._event_history[event_type] = \
                    self._event_history[event_type][-self._max_history // 2:]

        for observer in self._observers.values():
            if not observer.enabled:
                continue

            if event_type in observer.event_types or "*" in observer.event_types:
                try:
                    observer.callback(event)
                    notified += 1
                except Exception:
                    pass

        return notified

    def get_observers(self, event_type: Optional[str] = None) -> List[Dict]:
        """Get observers."""
        with self._lock:
            observers = list(self._observers.values())

            if event_type:
                observers = [
                    o for o in observers
                    if event_type in o.event_types or "*" in o.event_types
                ]

            return [
                {
                    "observer_id": o.observer_id,
                    "name": o.name,
                    "event_types": o.event_types,
                    "enabled": o.enabled
                }
                for o in observers
            ]

    def get_event_history(
        self,
        event_type: str,
        limit: int = 100
    ) -> List[Dict]:
        """Get event history."""
        with self._lock:
            events = self._event_history.get(event_type, [])
            events = events[-limit:]

            return [
                {
                    "event_id": e.event_id,
                    "event_type": e.event_type,
                    "timestamp": e.timestamp
                }
                for e in events
            ]


class ObserverPatternAction(BaseAction):
    """Action for observer pattern operations."""

    def __init__(self):
        super().__init__("observer_pattern")
        self._manager = ObserverPatternManager()

    def execute(self, params: Dict) -> ActionResult:
        """Execute observer action."""
        try:
            operation = params.get("operation", "register")

            if operation == "register":
                return self._register(params)
            elif operation == "unregister":
                return self._unregister(params)
            elif operation == "notify":
                return self._notify(params)
            elif operation == "observers":
                return self._observers(params)
            elif operation == "history":
                return self._history(params)
            else:
                return ActionResult(success=False, message=f"Unknown: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=str(e))

    def _register(self, params: Dict) -> ActionResult:
        """Register observer."""
        def callback(event):
            pass

        observer_id = self._manager.register_observer(
            name=params.get("name", ""),
            callback=params.get("callback") or callback,
            event_types=params.get("event_types", [])
        )
        return ActionResult(success=True, data={"observer_id": observer_id})

    def _unregister(self, params: Dict) -> ActionResult:
        """Unregister observer."""
        success = self._manager.unregister_observer(params.get("observer_id", ""))
        return ActionResult(success=success)

    def _notify(self, params: Dict) -> ActionResult:
        """Notify observers."""
        count = self._manager.notify(
            params.get("event_type", ""),
            params.get("data")
        )
        return ActionResult(success=True, data={"notified": count})

    def _observers(self, params: Dict) -> ActionResult:
        """Get observers."""
        observers = self._manager.get_observers(params.get("event_type"))
        return ActionResult(success=True, data={"observers": observers})

    def _history(self, params: Dict) -> ActionResult:
        """Get event history."""
        history = self._manager.get_event_history(
            params.get("event_type", ""),
            params.get("limit", 100)
        )
        return ActionResult(success=True, data={"history": history})
