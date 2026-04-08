"""Observer action module for RabAI AutoClick.

Provides observer pattern implementation:
- Observer: Abstract observer interface
- Subject: Observable subject
- EventObserver: Event-based observer
- NotificationCenter: Central notification system
"""

from typing import Any, Callable, Dict, List, Optional, Set
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
import uuid
import threading


import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


@dataclass
class Event:
    """Event data."""
    event_type: str
    data: Any = None
    source_id: str = ""
    timestamp: float = field(default_factory=lambda: __import__("time").time())
    metadata: Dict[str, Any] = field(default_factory=dict)


class Observer(ABC):
    """Abstract observer interface."""

    @abstractmethod
    def update(self, event: Event) -> None:
        """Handle event notification."""
        pass


class ConcreteObserver(Observer):
    """Concrete observer implementation."""

    def __init__(self, observer_id: str, handler: Callable[[Event], None]):
        self.observer_id = observer_id
        self._handler = handler
        self._received_events: List[Event] = []

    def update(self, event: Event) -> None:
        """Handle event."""
        self._received_events.append(event)
        if self._handler:
            self._handler(event)

    def get_events(self) -> List[Event]:
        """Get received events."""
        return self._received_events.copy()

    def clear_events(self) -> None:
        """Clear received events."""
        self._received_events.clear()


class Subject:
    """Observable subject."""

    def __init__(self, subject_id: str):
        self.subject_id = subject_id
        self._observers: Dict[str, Observer] = {}
        self._lock = threading.RLock()

    def attach(self, observer: Observer) -> str:
        """Attach an observer."""
        with self._lock:
            observer_id = observer.observer_id if hasattr(observer, "observer_id") else str(uuid.uuid4())
            self._observers[observer_id] = observer
            return observer_id

    def detach(self, observer_id: str) -> bool:
        """Detach an observer."""
        with self._lock:
            if observer_id in self._observers:
                del self._observers[observer_id]
                return True
            return False

    def notify(self, event: Event) -> None:
        """Notify all observers."""
        with self._lock:
            for observer in self._observers.values():
                try:
                    observer.update(event)
                except Exception:
                    pass

    def get_observer_count(self) -> int:
        """Get number of observers."""
        with self._lock:
            return len(self._observers)


class EventChannel:
    """Event channel for pub/sub."""

    def __init__(self, channel_name: str):
        self.channel_name = channel_name
        self._observers: Dict[str, Observer] = {}
        self._lock = threading.RLock()
        self._history: List[Event] = []
        self._max_history = 1000

    def subscribe(self, observer: Observer) -> str:
        """Subscribe to channel."""
        with self._lock:
            observer_id = observer.observer_id if hasattr(observer, "observer_id") else str(uuid.uuid4())
            self._observers[observer_id] = observer
            return observer_id

    def unsubscribe(self, observer_id: str) -> bool:
        """Unsubscribe from channel."""
        with self._lock:
            if observer_id in self._observers:
                del self._observers[observer_id]
                return True
            return False

    def publish(self, event: Event) -> None:
        """Publish event to channel."""
        with self._lock:
            self._history.append(event)
            if len(self._history) > self._max_history:
                self._history.pop(0)

        for observer in list(self._observers.values()):
            try:
                observer.update(event)
            except Exception:
                pass

    def get_history(self, limit: int = 100) -> List[Event]:
        """Get event history."""
        return self._history[-limit:]


class NotificationCenter:
    """Central notification system."""

    def __init__(self):
        self._channels: Dict[str, EventChannel] = {}
        self._global_channel: EventChannel = EventChannel("_global")
        self._lock = threading.RLock()

    def create_channel(self, channel_name: str) -> EventChannel:
        """Create a channel."""
        with self._lock:
            if channel_name not in self._channels:
                self._channels[channel_name] = EventChannel(channel_name)
            return self._channels[channel_name]

    def get_channel(self, channel_name: str) -> Optional[EventChannel]:
        """Get a channel."""
        with self._lock:
            if channel_name == "_global":
                return self._global_channel
            return self._channels.get(channel_name)

    def post(self, channel_name: str, event: Event) -> None:
        """Post event to channel."""
        channel = self.get_channel(channel_name)
        if channel:
            channel.publish(event)
        self._global_channel.publish(event)

    def broadcast(self, event: Event) -> None:
        """Broadcast to all channels."""
        with self._lock:
            for channel in self._channels.values():
                channel.publish(event)
            self._global_channel.publish(event)

    def list_channels(self) -> List[str]:
        """List all channels."""
        with self._lock:
            return ["_global"] + list(self._channels.keys())


class ObserverAction(BaseAction):
    """Observer pattern action."""
    action_type = "observer"
    display_name = "观察者模式"
    description = "事件订阅通知"

    def __init__(self):
        super().__init__()
        self._center = NotificationCenter()
        self._subjects: Dict[str, Subject] = {}
        self._observers: Dict[str, ConcreteObserver] = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "observe")

            if operation == "observe":
                return self._observe(params)
            elif operation == "notify":
                return self._notify(params)
            elif operation == "subscribe":
                return self._subscribe(params)
            elif operation == "unsubscribe":
                return self._unsubscribe(params)
            elif operation == "post":
                return self._post(params)
            elif operation == "history":
                return self._get_history(params)
            elif operation == "channels":
                return self._list_channels()
            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Observer error: {str(e)}")

    def _observe(self, params: Dict[str, Any]) -> ActionResult:
        """Set up observer for subject."""
        subject_id = params.get("subject_id", str(uuid.uuid4()))
        observer_id = params.get("observer_id", str(uuid.uuid4()))
        handler = params.get("handler")

        if subject_id not in self._subjects:
            self._subjects[subject_id] = Subject(subject_id)

        subject = self._subjects[subject_id]

        if observer_id not in self._observers:
            self._observers[observer_id] = ConcreteObserver(observer_id, handler)

        subject.attach(self._observers[observer_id])

        return ActionResult(success=True, message=f"Observer attached: {observer_id}", data={"observer_id": observer_id})

    def _notify(self, params: Dict[str, Any]) -> ActionResult:
        """Notify observers of subject."""
        subject_id = params.get("subject_id")
        event_type = params.get("event_type", "notification")
        data = params.get("data")

        if not subject_id:
            return ActionResult(success=False, message="subject_id is required")

        subject = self._subjects.get(subject_id)
        if not subject:
            return ActionResult(success=False, message=f"Subject not found: {subject_id}")

        event = Event(event_type=event_type, data=data, source_id=subject_id)
        subject.notify(event)

        return ActionResult(success=True, message="Observers notified", data={"observer_count": subject.get_observer_count()})

    def _subscribe(self, params: Dict[str, Any]) -> ActionResult:
        """Subscribe to a channel."""
        channel_name = params.get("channel", "default")
        observer_id = params.get("observer_id", str(uuid.uuid4()))
        handler = params.get("handler")

        channel = self._center.create_channel(channel_name)

        observer = ConcreteObserver(observer_id, handler)
        self._observers[observer_id] = observer

        channel.subscribe(observer)

        return ActionResult(success=True, message=f"Subscribed to {channel_name}", data={"observer_id": observer_id, "channel": channel_name})

    def _unsubscribe(self, params: Dict[str, Any]) -> ActionResult:
        """Unsubscribe from channel."""
        channel_name = params.get("channel", "default")
        observer_id = params.get("observer_id")

        if not observer_id:
            return ActionResult(success=False, message="observer_id is required")

        channel = self._center.get_channel(channel_name)
        if not channel:
            return ActionResult(success=False, message=f"Channel not found: {channel_name}")

        success = channel.unsubscribe(observer_id)
        if observer_id in self._observers:
            del self._observers[observer_id]

        return ActionResult(success=success, message="Unsubscribed" if success else "Subscription not found")

    def _post(self, params: Dict[str, Any]) -> ActionResult:
        """Post event to channel."""
        channel_name = params.get("channel", "default")
        event_type = params.get("event_type", "notification")
        data = params.get("data")

        event = Event(event_type=event_type, data=data)
        self._center.post(channel_name, event)

        return ActionResult(success=True, message=f"Posted to {channel_name}")

    def _get_history(self, params: Dict[str, Any]) -> ActionResult:
        """Get channel event history."""
        channel_name = params.get("channel", "default")
        limit = params.get("limit", 100)

        channel = self._center.get_channel(channel_name)
        if not channel:
            return ActionResult(success=False, message=f"Channel not found: {channel_name}")

        history = channel.get_history(limit)

        return ActionResult(success=True, message=f"{len(history)} events", data={"history": [{"type": e.event_type, "data": e.data, "timestamp": e.timestamp} for e in history]})

    def _list_channels(self) -> ActionResult:
        """List all channels."""
        channels = self._center.list_channels()
        return ActionResult(success=True, message=f"{len(channels)} channels", data={"channels": channels})
