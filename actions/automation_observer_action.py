"""Automation Observer Action Module.

Provides observer pattern implementation for automation
workflows with event tracking and notification.
"""

from typing import Any, Dict, List, Optional, Callable, Set, Type
from dataclasses import dataclass, field
from enum import Enum
from datetime import datetime
import asyncio
import json
import uuid


class EventType(Enum):
    """Types of observable events."""
    WORKFLOW_START = "workflow_start"
    WORKFLOW_END = "workflow_end"
    WORKFLOW_ERROR = "workflow_error"
    STEP_START = "step_start"
    STEP_END = "step_end"
    STEP_ERROR = "step_error"
    CONDITION_MET = "condition_met"
    CONDITION_FAILED = "condition_failed"
    STATE_CHANGE = "state_change"
    TIMEOUT = "timeout"
    RETRY = "retry"
    CUSTOM = "custom"


@dataclass
class Event:
    """Represents an observable event."""
    id: str
    event_type: EventType
    source: str
    timestamp: datetime = field(default_factory=datetime.now)
    data: Dict[str, Any] = field(default_factory=dict)
    metadata: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        if not self.id:
            self.id = str(uuid.uuid4())

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "id": self.id,
            "event_type": self.event_type.value,
            "source": self.source,
            "timestamp": self.timestamp.isoformat(),
            "data": self.data,
            "metadata": self.metadata,
        }


@dataclass
class Observer:
    """Observer that can receive events."""
    id: str
    name: str
    handler: Callable[[Event], None]
    event_types: Set[EventType] = field(default_factory=set)
    filter_fn: Optional[Callable[[Event], bool]] = None
    async_handler: bool = False
    metadata: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        if not self.id:
            self.id = str(uuid.uuid4())


@dataclass
class Subscription:
    """Event subscription configuration."""
    observer_id: str
    event_types: Set[EventType]
    source_filter: Optional[str] = None
    async_notification: bool = True


class EventBus:
    """Central event bus for observers."""

    def __init__(self):
        self._observers: Dict[str, Observer] = {}
        self._subscriptions: Dict[EventType, Set[str]] = {
            event_type: set() for event_type in EventType
        }
        self._event_history: List[Event] = []
        self._max_history: int = 10000
        self._handlers: Dict[str, asyncio.Task] = {}

    def register_observer(
        self,
        observer: Observer,
    ) -> str:
        """Register an observer."""
        self._observers[observer.id] = observer

        for event_type in observer.event_types:
            self._subscriptions[event_type].add(observer.id)

        return observer.id

    def unregister_observer(self, observer_id: str) -> bool:
        """Unregister an observer."""
        if observer_id not in self._observers:
            return False

        observer = self._observers[observer_id]
        for event_type in observer.event_types:
            self._subscriptions[event_type].discard(observer_id)

        del self._observers[observer_id]
        return True

    def get_observer(self, observer_id: str) -> Optional[Observer]:
        """Get observer by ID."""
        return self._observers.get(observer_id)

    def list_observers(
        self,
        event_type: Optional[EventType] = None,
    ) -> List[Observer]:
        """List observers, optionally filtered by event type."""
        observers = list(self._observers.values())
        if event_type:
            observer_ids = self._subscriptions.get(event_type, set())
            observers = [o for o in observers if o.id in observer_ids]
        return observers

    def subscribe(
        self,
        observer_id: str,
        event_types: Set[EventType],
    ) -> bool:
        """Subscribe observer to event types."""
        if observer_id not in self._observers:
            return False

        observer = self._observers[observer_id]
        for event_type in event_types:
            self._subscriptions[event_type].add(observer_id)
            observer.event_types.add(event_type)

        return True

    def unsubscribe(
        self,
        observer_id: str,
        event_types: Set[EventType],
    ) -> bool:
        """Unsubscribe observer from event types."""
        if observer_id not in self._observers:
            return False

        observer = self._observers[observer_id]
        for event_type in event_types:
            self._subscriptions[event_type].discard(observer_id)
            observer.event_types.discard(event_type)

        return True

    async def emit(self, event: Event) -> List[str]:
        """Emit an event to all matching observers."""
        self._event_history.append(event)
        if len(self._event_history) > self._max_history:
            self._event_history = self._event_history[-self._max_history:]

        observer_ids = self._subscriptions.get(event.event_type, set())
        notified = []

        for observer_id in observer_ids:
            observer = self._observers.get(observer_id)
            if not observer:
                continue

            if observer.filter_fn and not observer.filter_fn(event):
                continue

            if observer.source_filter and event.source != observer.source_filter:
                continue

            try:
                if observer.async_handler:
                    task = asyncio.create_task(self._notify_async(observer, event))
                    self._handlers[f"{observer_id}:{event.id}"] = task
                else:
                    await self._notify_sync(observer, event)
                notified.append(observer_id)
            except Exception:
                pass

        return notified

    async def _notify_async(self, observer: Observer, event: Event):
        """Notify observer asynchronously."""
        try:
            if asyncio.iscoroutinefunction(observer.handler):
                await observer.handler(event)
            else:
                observer.handler(event)
        finally:
            key = f"{observer.id}:{event.id}"
            if key in self._handlers:
                del self._handlers[key]

    async def _notify_sync(self, observer: Observer, event: Event):
        """Notify observer synchronously."""
        if asyncio.iscoroutinefunction(observer.handler):
            await observer.handler(event)
        else:
            observer.handler(event)

    def get_event_history(
        self,
        event_type: Optional[EventType] = None,
        source: Optional[str] = None,
        limit: int = 100,
    ) -> List[Event]:
        """Get event history with optional filters."""
        events = self._event_history

        if event_type:
            events = [e for e in events if e.event_type == event_type]

        if source:
            events = [e for e in events if e.source == source]

        return events[-limit:]

    def clear_history(self):
        """Clear event history."""
        self._event_history.clear()


class Subject:
    """Subject that can be observed."""

    def __init__(
        self,
        subject_id: str,
        event_bus: Optional[EventBus] = None,
    ):
        self.subject_id = subject_id
        self._event_bus = event_bus or EventBus()
        self._state: Dict[str, Any] = {}

    @property
    def event_bus(self) -> EventBus:
        """Get the event bus."""
        return self._event_bus

    async def emit_event(
        self,
        event_type: EventType,
        data: Optional[Dict[str, Any]] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> List[str]:
        """Emit an event."""
        event = Event(
            id=str(uuid.uuid4()),
            event_type=event_type,
            source=self.subject_id,
            data=data or {},
            metadata=metadata or {},
        )
        return await self._event_bus.emit(event)

    def set_state(self, key: str, value: Any):
        """Set state and emit state change event."""
        old_value = self._state.get(key)
        self._state[key] = value
        asyncio.create_task(self.emit_event(
            EventType.STATE_CHANGE,
            data={
                "key": key,
                "old_value": old_value,
                "new_value": value,
            },
        ))

    def get_state(self, key: str, default: Any = None) -> Any:
        """Get state value."""
        return self._state.get(key, default)

    async def notify_workflow_start(
        self,
        workflow_id: str,
        metadata: Optional[Dict[str, Any]] = None,
    ):
        """Notify workflow started."""
        return await self.emit_event(
            EventType.WORKFLOW_START,
            data={"workflow_id": workflow_id},
            metadata=metadata,
        )

    async def notify_workflow_end(
        self,
        workflow_id: str,
        status: str,
        metadata: Optional[Dict[str, Any]] = None,
    ):
        """Notify workflow ended."""
        return await self.emit_event(
            EventType.WORKFLOW_END,
            data={"workflow_id": workflow_id, "status": status},
            metadata=metadata,
        )

    async def notify_step_start(
        self,
        workflow_id: str,
        step_id: str,
        step_name: str,
    ):
        """Notify step started."""
        return await self.emit_event(
            EventType.STEP_START,
            data={
                "workflow_id": workflow_id,
                "step_id": step_id,
                "step_name": step_name,
            },
        )

    async def notify_step_end(
        self,
        workflow_id: str,
        step_id: str,
        step_name: str,
        status: str,
        result: Optional[Any] = None,
    ):
        """Notify step ended."""
        return await self.emit_event(
            EventType.STEP_END,
            data={
                "workflow_id": workflow_id,
                "step_id": step_id,
                "step_name": step_name,
                "status": status,
                "result": result,
            },
        )

    async def notify_error(
        self,
        workflow_id: str,
        step_id: Optional[str],
        error: Exception,
    ):
        """Notify error occurred."""
        return await self.emit_event(
            EventType.WORKFLOW_ERROR,
            data={
                "workflow_id": workflow_id,
                "step_id": step_id,
                "error": str(error),
                "error_type": type(error).__name__,
            },
        )


class NotificationChannel:
    """Channel for delivering notifications."""

    def __init__(self, channel_id: str):
        self.channel_id = channel_id
        self._enabled = True

    def enable(self):
        """Enable the channel."""
        self._enabled = True

    def disable(self):
        """Disable the channel."""
        self._enabled = False

    async def send(self, event: Event) -> bool:
        """Send notification (override in subclasses)."""
        raise NotImplementedError


class LogNotificationChannel(NotificationChannel):
    """Logs notifications."""

    def __init__(self):
        super().__init__("log")

    async def send(self, event: Event) -> bool:
        """Log the event."""
        if not self._enabled:
            return False
        print(f"[{event.timestamp.isoformat()}] {event.event_type.value}: {event.source}")
        return True


class WebhookNotificationChannel(NotificationChannel):
    """Sends notifications to webhooks."""

    def __init__(self, webhook_url: str):
        super().__init__("webhook")
        self.webhook_url = webhook_url

    async def send(self, event: Event) -> bool:
        """Send event to webhook."""
        if not self._enabled:
            return False
        return True


class NotificationManager:
    """Manages notification channels and dispatch."""

    def __init__(self):
        self._channels: Dict[str, NotificationChannel] = {}

    def add_channel(self, channel: NotificationChannel):
        """Add a notification channel."""
        self._channels[channel.channel_id] = channel

    def remove_channel(self, channel_id: str) -> bool:
        """Remove a notification channel."""
        if channel_id in self._channels:
            del self._channels[channel_id]
            return True
        return False

    def get_channel(self, channel_id: str) -> Optional[NotificationChannel]:
        """Get a notification channel."""
        return self._channels.get(channel_id)

    async def notify(self, event: Event) -> Dict[str, bool]:
        """Send notification to all channels."""
        results = {}
        for channel_id, channel in self._channels.items():
            try:
                results[channel_id] = await channel.send(event)
            except Exception:
                results[channel_id] = False
        return results


class AutomationObserverAction:
    """High-level automation observer action."""

    def __init__(
        self,
        event_bus: Optional[EventBus] = None,
        notification_manager: Optional[NotificationManager] = None,
    ):
        self.event_bus = event_bus or EventBus()
        self.notification_manager = notification_manager or NotificationManager()
        self._subjects: Dict[str, Subject] = {}

    def create_subject(
        self,
        subject_id: str,
    ) -> Subject:
        """Create an observable subject."""
        subject = Subject(subject_id, self.event_bus)
        self._subjects[subject_id] = subject
        return subject

    def get_subject(self, subject_id: str) -> Optional[Subject]:
        """Get a subject by ID."""
        return self._subjects.get(subject_id)

    def add_observer(
        self,
        name: str,
        handler: Callable,
        event_types: List[str],
        async_handler: bool = True,
    ) -> str:
        """Add an observer."""
        event_type_enums = [EventType(et) for et in event_types]
        observer = Observer(
            id=str(uuid.uuid4()),
            name=name,
            handler=handler,
            event_types=set(event_type_enums),
            async_handler=async_handler,
        )
        return self.event_bus.register_observer(observer)

    def remove_observer(self, observer_id: str) -> bool:
        """Remove an observer."""
        return self.event_bus.unregister_observer(observer_id)

    async def emit(
        self,
        event_type: str,
        source: str,
        data: Optional[Dict[str, Any]] = None,
    ) -> List[str]:
        """Emit an event."""
        event = Event(
            id=str(uuid.uuid4()),
            event_type=EventType(event_type),
            source=source,
            data=data or {},
        )
        return await self.event_bus.emit(event)

    def get_history(
        self,
        event_type: Optional[str] = None,
        limit: int = 100,
    ) -> List[Dict[str, Any]]:
        """Get event history."""
        et = EventType(event_type) if event_type else None
        events = self.event_bus.get_event_history(event_type=et, limit=limit)
        return [e.to_dict() for e in events]


# Module exports
__all__ = [
    "AutomationObserverAction",
    "EventBus",
    "Subject",
    "Observer",
    "Event",
    "EventType",
    "Subscription",
    "NotificationChannel",
    "LogNotificationChannel",
    "WebhookNotificationChannel",
    "NotificationManager",
]
