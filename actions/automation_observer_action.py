"""Automation Observer Pattern.

This module provides observer pattern implementation for automation:
- Event subscription
- Observer groups
- Conditional notifications
- Observer lifecycle management

Example:
    >>> from actions.automation_observer_action import AutomationObserver, Subject
    >>> observer = AutomationObserver()
    >>> subject.attach(observer, event="task_complete")
"""

from __future__ import annotations

import time
import logging
import threading
from typing import Any, Callable, Optional
from dataclasses import dataclass, field
from enum import Enum

logger = logging.getLogger(__name__)


class EventType(Enum):
    """Automation event types."""
    TASK_STARTED = "task_started"
    TASK_COMPLETED = "task_completed"
    TASK_FAILED = "task_failed"
    TASK_CANCELLED = "task_cancelled"
    STATE_CHANGED = "state_changed"
    CONDITION_MET = "condition_met"
    TRIGGER_FIRED = "trigger_fired"


@dataclass
class Event:
    """An automation event."""
    event_type: str
    timestamp: float
    source: str
    data: dict[str, Any]
    subject_id: str = ""


@dataclass
class Observer:
    """An observer for automation events."""
    name: str
    callback: Callable[[Event], None]
    event_types: list[str] = field(default_factory=list)
    filter_func: Optional[Callable[[Event], bool]] = None
    enabled: bool = True
    received_count: int = 0
    last_event: Optional[Event] = None


class Subject:
    """Subject that notifies observers of events."""

    def __init__(self, subject_id: str = "") -> None:
        """Initialize the subject.

        Args:
            subject_id: Unique subject identifier.
        """
        self._subject_id = subject_id
        self._observers: dict[str, list[Observer]] = {}
        self._lock = threading.RLock()
        self._stats = {"events_published": 0, "notifications_sent": 0}

    def attach(
        self,
        observer: Observer,
        event_type: Optional[str] = None,
    ) -> None:
        """Attach an observer.

        Args:
            observer: Observer to attach.
            event_type: Specific event type. None = all.
        """
        with self._lock:
            if event_type:
                observer.event_types = [event_type]

            if event_type not in self._observers:
                self._observers[event_type] = []
            self._observers[event_type].append(observer)
            logger.info("Observer %s attached to %s for event %s", observer.name, self._subject_id, event_type or "all")

    def detach(
        self,
        observer_name: str,
        event_type: Optional[str] = None,
    ) -> bool:
        """Detach an observer.

        Args:
            observer_name: Name of observer to detach.
            event_type: Specific event type. None = all.

        Returns:
            True if detached.
        """
        with self._lock:
            if event_type:
                observers = self._observers.get(event_type, [])
                for i, o in enumerate(observers):
                    if o.name == observer_name:
                        observers.pop(i)
                        return True
            else:
                for et, observers in self._observers.items():
                    for i, o in enumerate(observers):
                        if o.name == observer_name:
                            observers.pop(i)
                            return True
        return False

    def notify(
        self,
        event_type: str,
        source: str,
        data: Optional[dict[str, Any]] = None,
    ) -> int:
        """Notify observers of an event.

        Args:
            event_type: Type of event.
            source: Event source.
            data: Event data.

        Returns:
            Number of notifications sent.
        """
        event = Event(
            event_type=event_type,
            timestamp=time.time(),
            source=source,
            data=data or {},
            subject_id=self._subject_id,
        )

        with self._lock:
            self._stats["events_published"] += 1

        notifications_sent = 0

        with self._lock:
            specific_observers = self._observers.get(event_type, [])
            wildcard_observers = self._observers.get(None, [])
            all_observers = specific_observers + wildcard_observers

        for observer in all_observers:
            if not observer.enabled:
                continue

            if observer.filter_func and not observer.filter_func(event):
                continue

            try:
                observer.callback(event)
                observer.received_count += 1
                observer.last_event = event
                notifications_sent += 1
                self._stats["notifications_sent"] += 1
            except Exception as e:
                logger.error("Observer %s callback failed: %s", observer.name, e)

        return notifications_sent

    def create_observer(
        self,
        name: str,
        callback: Callable[[Event], None],
        event_types: Optional[list[str]] = None,
        filter_func: Optional[Callable[[Event], bool]] = None,
    ) -> Observer:
        """Create and attach an observer.

        Args:
            name: Observer name.
            callback: Callback function.
            event_types: Event types to subscribe to.
            filter_func: Optional event filter.

        Returns:
            Created Observer.
        """
        observer = Observer(
            name=name,
            callback=callback,
            event_types=event_types or [],
            filter_func=filter_func,
        )
        for et in (event_types or [None]):
            self.attach(observer, event_type=et)
        return observer

    def get_stats(self) -> dict[str, Any]:
        """Get observer statistics."""
        with self._lock:
            total_observers = sum(len(obs) for obs in self._observers.values())
            return {
                **self._stats,
                "total_observers": total_observers,
                "subject_id": self._subject_id,
            }


class AutomationObserver:
    """Manages subjects and their observers."""

    def __init__(self) -> None:
        """Initialize the automation observer."""
        self._subjects: dict[str, Subject] = {}
        self._lock = threading.RLock()

    def create_subject(self, subject_id: str) -> Subject:
        """Create a subject.

        Args:
            subject_id: Subject identifier.

        Returns:
            Subject instance.
        """
        with self._lock:
            if subject_id in self._subjects:
                return self._subjects[subject_id]
            subject = Subject(subject_id=subject_id)
            self._subjects[subject_id] = subject
            return subject

    def get_subject(self, subject_id: str) -> Optional[Subject]:
        """Get a subject by ID."""
        with self._lock:
            return self._subjects.get(subject_id)

    def list_subjects(self) -> list[str]:
        """List all subject IDs."""
        with self._lock:
            return list(self._subjects.keys())
