"""
Automation Observer Action Module.

Provides Observer pattern implementation for event-driven
automation workflows with subject/observer lifecycle management.
"""

from typing import Callable, Dict, List, Any, Optional, Set
from dataclasses import dataclass, field
from enum import Enum
import threading
import asyncio
import time
from collections import defaultdict


class EventType(Enum):
    """Common event types for automation."""
    CREATED = "created"
    UPDATED = "updated"
    DELETED = "deleted"
    STARTED = "started"
    COMPLETED = "completed"
    FAILED = "failed"
    TIMEOUT = "timeout"
    RETRY = "retry"
    CANCELLED = "cancelled"


@dataclass
class Event:
    """Event object passed to observers."""
    type: EventType
    subject_id: str
    data: Dict[str, Any] = field(default_factory=dict)
    timestamp: float = field(default_factory=time.time)
    source: Optional[str] = None

    def __post_init__(self):
        if isinstance(self.type, str):
            self.type = EventType(self.type)


@dataclass
class ObserverConfig:
    """Configuration for observer behavior."""
    async_mode: bool = False
    buffer_size: int = 1000
    max_history: int = 100
    propagate_errors: bool = False
    error_handler: Optional[Callable[[Exception, Event], None]] = None


class Observer:
    """Base observer class."""

    def __init__(
        self,
        observer_id: str,
        event_types: Optional[List[EventType]] = None,
        filter_func: Optional[Callable[[Event], bool]] = None,
    ):
        self.observer_id = observer_id
        self.event_types: Set[EventType] = set(event_types or [])
        self.filter_func = filter_func
        self.event_count = 0
        self.last_event: Optional[Event] = None
        self.received_events: List[Event] = []

    def can_handle(self, event: Event) -> bool:
        """Check if observer can handle this event type."""
        if not self.event_types:
            return True
        return event.type in self.event_types

    def should_process(self, event: Event) -> bool:
        """Check if observer's filter accepts this event."""
        if self.filter_func is None:
            return True
        try:
            return self.filter_func(event)
        except Exception:
            return False

    def on_event(self, event: Event) -> None:
        """Handle event. Override in subclass."""
        self.event_count += 1
        self.last_event = event
        self.received_events.append(event)


class AsyncObserver(Observer):
    """Async observer for async event handling."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._queue: asyncio.Queue = None

    async def on_event_async(self, event: Event) -> None:
        """Handle event asynchronously."""
        self.event_count += 1
        self.last_event = event
        self.received_events.append(event)


class CallableObserver(Observer):
    """Observer that wraps a callable."""

    def __init__(
        self,
        observer_id: str,
        handler: Callable[[Event], Any],
        event_types: Optional[List[EventType]] = None,
        filter_func: Optional[Callable[[Event], bool]] = None,
    ):
        super().__init__(observer_id, event_types, filter_func)
        self.handler = handler

    def on_event(self, event: Event) -> None:
        super().on_event(event)
        self.handler(event)


class AutomationObserverAction:
    """
    Observer pattern implementation for event-driven automation.

    Manages subjects and observers with support for both sync and async
    event handling, filtering, and error recovery.
    """

    def __init__(self, config: Optional[ObserverConfig] = None):
        self.config = config or ObserverConfig()
        self._observers: Dict[str, Observer] = {}
        self._subject_events: Dict[str, List[Event]] = defaultdict(list)
        self._lock = threading.RLock()
        self._event_history: List[Event] = []
        self._async_mode = config.async_mode if config else False

    def subscribe(
        self,
        observer_id: str,
        event_types: Optional[List[EventType]] = None,
        filter_func: Optional[Callable[[Event], bool]] = None,
    ) -> Callable[[Event], None]:
        """
        Subscribe an observer to events.

        Args:
            observer_id: Unique identifier for observer
            event_types: List of event types to subscribe to
            filter_func: Optional filter function

        Returns:
            Callable to handle events
        """
        with self._lock:
            if observer_id in self._observers:
                raise ValueError(f"Observer {observer_id} already exists")

            observer = CallableObserver(
                observer_id=observer_id,
                handler=self._create_handler(observer_id),
                event_types=event_types,
                filter_func=filter_func,
            )
            self._observers[observer_id] = observer
            return observer.on_event

    def subscribe_async(
        self,
        observer_id: str,
        event_types: Optional[List[EventType]] = None,
        filter_func: Optional[Callable[[Event], bool]] = None,
    ) -> Callable[[Event], None]:
        """Subscribe an async observer."""
        with self._lock:
            if observer_id in self._observers:
                raise ValueError(f"Observer {observer_id} already exists")

            observer = AsyncObserver(
                observer_id=observer_id,
                event_types=event_types,
                filter_func=filter_func,
            )
            self._observers[observer_id] = observer
            return observer.on_event

    def unsubscribe(self, observer_id: str) -> None:
        """Unsubscribe an observer."""
        with self._lock:
            if observer_id in self._observers:
                del self._observers[observer_id]

    def _create_handler(self, observer_id: str) -> Callable[[Event], None]:
        """Create event handler for observer."""
        def handler(event: Event):
            observer = self._observers.get(observer_id)
            if observer and observer.can_handle(event) and observer.should_process(event):
                try:
                    observer.on_event(event)
                except Exception as e:
                    if self.config.error_handler:
                        self.config.error_handler(e, event)
                    elif self.config.propagate_errors:
                        raise
        return handler

    async def notify_async(self, event: Event) -> None:
        """Notify all matching observers (async version)."""
        for observer in list(self._observers.values()):
            if observer.can_handle(event) and observer.should_process(event):
                try:
                    if asyncio.iscoroutinefunction(getattr(observer, "on_event_async", None)):
                        await observer.on_event_async(event)
                    elif hasattr(observer, "on_event"):
                        observer.on_event(event)
                except Exception as e:
                    if self.config.error_handler:
                        self.config.error_handler(e, event)
                    elif self.config.propagate_errors:
                        raise

        # Record event
        with self._lock:
            self._event_history.append(event)
            if len(self._event_history) > self.config.max_history:
                self._event_history = self._event_history[-self.config.max_history:]

    def notify(self, event: Event) -> None:
        """Notify all matching observers (sync version)."""
        if asyncio.iscoroutinefunction(self.notify_async):
            asyncio.run(self.notify_async(event))
        else:
            for observer in list(self._observers.values()):
                if observer.can_handle(event) and observer.should_process(event):
                    try:
                        observer.on_event(event)
                    except Exception as e:
                        if self.config.error_handler:
                            self.config.error_handler(e, event)
                        elif self.config.propagate_errors:
                            raise

            # Record event
            with self._lock:
                self._event_history.append(event)
                if len(self._event_history) > self.config.max_history:
                    self._event_history = self._event_history[-self.config.max_history:]

    def emit(
        self,
        subject_id: str,
        event_type: EventType,
        data: Optional[Dict[str, Any]] = None,
        source: Optional[str] = None,
    ) -> None:
        """Emit an event."""
        event = Event(
            type=event_type,
            subject_id=subject_id,
            data=data or {},
            timestamp=time.time(),
            source=source,
        )

        # Record in subject history
        with self._lock:
            self._subject_events[subject_id].append(event)
            if len(self._subject_events[subject_id]) > self.config.buffer_size:
                self._subject_events[subject_id] = self._subject_events[subject_id][-self.config.buffer_size:]

        self.notify(event)

    def get_observer(self, observer_id: str) -> Optional[Observer]:
        """Get observer by ID."""
        return self._observers.get(observer_id)

    def get_observers(self, event_type: Optional[EventType] = None) -> List[Observer]:
        """Get all observers, optionally filtered by event type."""
        observers = list(self._observers.values())
        if event_type:
            observers = [o for o in observers if o.can_handle(Event(type=event_type, subject_id="", data={}))]
        return observers

    def get_event_history(
        self,
        subject_id: Optional[str] = None,
        event_type: Optional[EventType] = None,
        limit: int = 100,
    ) -> List[Event]:
        """Get event history."""
        events = self._event_history
        if subject_id:
            events = self._subject_events.get(subject_id, [])
        if event_type:
            events = [e for e in events if e.type == event_type]
        return events[-limit:]

    def get_stats(self) -> Dict[str, Any]:
        """Get observer statistics."""
        return {
            "total_observers": len(self._observers),
            "observers_by_type": {
                et.value: len([o for o in self._observers.values() if et in o.event_types])
                for et in EventType
            },
            "total_events": len(self._event_history),
            "events_by_type": {
                et.value: len([e for e in self._event_history if e.type == et])
                for et in EventType
            },
            "observer_counts": {
                oid: o.event_count for oid, o in self._observers.items()
            },
        }


class Subject:
    """Subject that can emit events."""

    def __init__(
        self,
        subject_id: str,
        observer_action: AutomationObserverAction,
    ):
        self.subject_id = subject_id
        self.observer_action = observer_action

    def emit(
        self,
        event_type: EventType,
        data: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Emit event from this subject."""
        self.observer_action.emit(
            subject_id=self.subject_id,
            event_type=event_type,
            data=data,
            source=self.subject_id,
        )
