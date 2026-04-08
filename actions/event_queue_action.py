"""Event queue action module for RabAI AutoClick.

Provides event queue mechanisms:
- EventQueue: Thread-safe event queue
- PriorityQueue: Priority-based event queue
- EventBus: Pub/sub event bus
- EventHandler: Event handler registration
- DelayedEvent: Delayed event execution
- EventFilter: Event filtering and transformation
"""

import time
import threading
import queue
from typing import Any, Callable, Dict, List, Optional, Set
from dataclasses import dataclass, field
from enum import Enum
from datetime import datetime

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class EventPriority(Enum):
    """Event priority levels."""
    LOW = 0
    NORMAL = 1
    HIGH = 2
    CRITICAL = 3


@dataclass
class Event:
    """Represents an event."""
    id: str
    type: str
    data: Any
    priority: EventPriority = EventPriority.NORMAL
    timestamp: float = field(default_factory=time.time)
    metadata: Dict[str, Any] = field(default_factory=dict)
    source: Optional[str] = None


@dataclass
class EventSubscription:
    """Event subscription."""
    id: str
    event_type: str
    handler: Callable[[Event], None]
    filter_fn: Optional[Callable[[Event], bool]] = None
    async_handler: bool = False


class EventQueue:
    """Thread-safe event queue."""

    def __init__(self, max_size: int = 0):
        self._queue: queue.Queue = queue.Queue(maxsize=max_size)
        self._lock = threading.Lock()
        self._closed = False

    def put(self, event: Event, block: bool = True, timeout: Optional[float] = None) -> bool:
        """Add event to queue."""
        if self._closed:
            return False
        try:
            self._queue.put(event, block=block, timeout=timeout)
            return True
        except queue.Full:
            return False

    def get(self, block: bool = True, timeout: Optional[float] = None) -> Optional[Event]:
        """Get event from queue."""
        if self._closed:
            return None
        try:
            return self._queue.get(block=block, timeout=timeout)
        except queue.Empty:
            return None

    def get_nowait(self) -> Optional[Event]:
        """Get event without blocking."""
        return self.get(block=False)

    def size(self) -> int:
        """Get queue size."""
        return self._queue.qsize()

    def close(self):
        """Close the queue."""
        self._closed = True

    def is_closed(self) -> bool:
        """Check if queue is closed."""
        return self._closed


class PriorityEventQueue:
    """Priority-based event queue."""

    def __init__(self, max_size: int = 0):
        self._queues: Dict[EventPriority, queue.PriorityQueue] = {
            priority: queue.PriorityQueue(maxsize=max_size)
            for priority in EventPriority
        }
        self._total_size = 0
        self._lock = threading.Lock()
        self._closed = False
        self._counter = 0

    def put(self, event: Event, block: bool = True, timeout: Optional[float] = None) -> bool:
        """Add event to priority queue."""
        if self._closed:
            return False

        priority_val = (256 - event.priority.value, self._counter, event)
        self._counter += 1

        try:
            self._queues[event.priority].put(priority_val, block=block, timeout=timeout)
            with self._lock:
                self._total_size += 1
            return True
        except queue.Full:
            return False

    def get(self, block: bool = True, timeout: Optional[float] = None) -> Optional[Event]:
        """Get highest priority event."""
        if self._closed:
            return None

        for priority in EventPriority:
            try:
                item = self._queues[priority].get(block=False)
                with self._lock:
                    self._total_size -= 1
                return item[2]
            except queue.Empty:
                continue

        if block:
            time.sleep(0.01)
        return None

    def size(self) -> int:
        """Get total queue size."""
        with self._lock:
            return self._total_size

    def close(self):
        """Close the queue."""
        self._closed = True


class EventBus:
    """Publish/subscribe event bus."""

    def __init__(self):
        self._subscriptions: Dict[str, List[EventSubscription]] = {}
        self._global_handlers: List[EventSubscription] = []
        self._lock = threading.RLock()
        self._event_history: List[Event] = []
        self._history_size = 1000

    def subscribe(
        self,
        event_type: str,
        handler: Callable[[Event], None],
        filter_fn: Optional[Callable[[Event], bool]] = None,
        subscriber_id: Optional[str] = None,
    ) -> str:
        """Subscribe to event type."""
        with self._lock:
            sub_id = subscriber_id or f"sub_{len(self._subscriptions)}_{int(time.time() * 1000)}"
            subscription = EventSubscription(
                id=sub_id,
                event_type=event_type,
                handler=handler,
                filter_fn=filter_fn,
            )

            if event_type == "*":
                self._global_handlers.append(subscription)
            else:
                if event_type not in self._subscriptions:
                    self._subscriptions[event_type] = []
                self._subscriptions[event_type].append(subscription)

            return sub_id

    def unsubscribe(self, subscription_id: str) -> bool:
        """Unsubscribe from events."""
        with self._lock:
            for subs in self._subscriptions.values():
                for i, sub in enumerate(subs):
                    if sub.id == subscription_id:
                        subs.pop(i)
                        return True

            for i, sub in enumerate(self._global_handlers):
                if sub.id == subscription_id:
                    self._global_handlers.pop(i)
                    return True

            return False

    def publish(self, event: Event) -> int:
        """Publish event to subscribers."""
        delivered = 0

        with self._lock:
            self._event_history.append(event)
            if len(self._event_history) > self._history_size:
                self._event_history.pop(0)

            handlers = []
            if event.type in self._subscriptions:
                handlers.extend(self._subscriptions[event.type])
            handlers.extend(self._global_handlers)

        for subscription in handlers:
            try:
                if subscription.filter_fn is None or subscription.filter_fn(event):
                    subscription.handler(event)
                    delivered += 1
            except Exception:
                pass

        return delivered

    def get_history(self, event_type: Optional[str] = None, limit: int = 100) -> List[Event]:
        """Get event history."""
        with self._lock:
            if event_type:
                return [e for e in self._event_history if e.type == event_type][-limit:]
            return self._event_history[-limit:]

    def clear_history(self):
        """Clear event history."""
        with self._lock:
            self._event_history.clear()


class DelayedEvent:
    """Delayed event executor."""

    def __init__(self, event_bus: EventBus):
        self.event_bus = event_bus
        self._scheduled: Dict[str, Dict] = {}
        self._lock = threading.Lock()
        self._running = False
        self._thread: Optional[threading.Thread] = None

    def schedule(
        self,
        event: Event,
        delay: float,
        subscription_id: Optional[str] = None,
    ) -> str:
        """Schedule event for future execution."""
        with self._lock:
            event_id = f"delayed_{int(time.time() * 1000)}"
            self._scheduled[event_id] = {
                "event": event,
                "execute_at": time.time() + delay,
                "subscription_id": subscription_id,
            }
            return event_id

    def schedule_recurring(
        self,
        event: Event,
        interval: float,
        subscription_id: Optional[str] = None,
    ) -> str:
        """Schedule recurring event."""
        with self._lock:
            event_id = f"recurring_{int(time.time() * 1000)}"
            self._scheduled[event_id] = {
                "event": event,
                "interval": interval,
                "execute_at": time.time() + interval,
                "subscription_id": subscription_id,
                "recurring": True,
            }
            return event_id

    def cancel(self, event_id: str) -> bool:
        """Cancel scheduled event."""
        with self._lock:
            if event_id in self._scheduled:
                del self._scheduled[event_id]
                return True
            return False

    def start(self):
        """Start the delayed event processor."""
        if self._running:
            return
        self._running = True
        self._thread = threading.Thread(target=self._process_loop, daemon=True)
        self._thread.start()

    def stop(self):
        """Stop the delayed event processor."""
        self._running = False
        if self._thread:
            self._thread.join(timeout=1.0)

    def _process_loop(self):
        """Process scheduled events."""
        while self._running:
            now = time.time()
            to_execute = []

            with self._lock:
                for event_id, schedule_info in list(self._scheduled.items()):
                    if schedule_info["execute_at"] <= now:
                        to_execute.append((event_id, schedule_info))

            for event_id, schedule_info in to_execute:
                try:
                    self.event_bus.publish(schedule_info["event"])

                    if schedule_info.get("recurring"):
                        schedule_info["execute_at"] = now + schedule_info["interval"]
                    else:
                        with self._lock:
                            self._scheduled.pop(event_id, None)
                except Exception:
                    pass

            time.sleep(0.01)


class EventQueueAction(BaseAction):
    """Event queue action for automation."""
    action_type = "event_queue"
    display_name = "事件队列"
    description = "事件队列和发布订阅"

    def __init__(self):
        super().__init__()
        self._queues: Dict[str, EventQueue] = {}
        self._priority_queues: Dict[str, PriorityEventQueue] = {}
        self._event_buses: Dict[str, EventBus] = {}
        self._delayed_events: Dict[str, DelayedEvent] = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "publish")
            queue_name = params.get("queue", "default")
            event_type = params.get("type", "")

            if operation == "create_queue":
                return self._create_queue(queue_name, params)
            elif operation == "create_priority_queue":
                return self._create_priority_queue(queue_name, params)
            elif operation == "publish":
                return self._publish_event(queue_name, event_type, params)
            elif operation == "subscribe":
                return self._subscribe(queue_name, event_type, params)
            elif operation == "unsubscribe":
                return self._unsubscribe(queue_name, params)
            elif operation == "get":
                return self._get_event(queue_name, params)
            elif operation == "schedule":
                return self._schedule_event(queue_name, event_type, params)
            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Event queue error: {str(e)}")

    def _create_queue(self, name: str, params: Dict) -> ActionResult:
        """Create a new event queue."""
        max_size = params.get("max_size", 0)
        self._queues[name] = EventQueue(max_size=max_size)
        return ActionResult(success=True, message=f"Queue '{name}' created")

    def _create_priority_queue(self, name: str, params: Dict) -> ActionResult:
        """Create a priority queue."""
        max_size = params.get("max_size", 0)
        self._priority_queues[name] = PriorityEventQueue(max_size=max_size)
        return ActionResult(success=True, message=f"Priority queue '{name}' created")

    def _publish_event(self, queue_name: str, event_type: str, params: Dict) -> ActionResult:
        """Publish event to queue or event bus."""
        data = params.get("data")
        priority = params.get("priority", "NORMAL")
        priority_enum = EventPriority[priority.upper()]

        event = Event(
            id=f"evt_{int(time.time() * 1000)}",
            type=event_type,
            data=data,
            priority=priority_enum,
            source=params.get("source"),
        )

        if queue_name in self._queues:
            self._queues[queue_name].put(event)
            return ActionResult(success=True, message=f"Event added to queue '{queue_name}'")
        elif queue_name in self._event_buses:
            delivered = self._event_buses[queue_name].publish(event)
            return ActionResult(success=True, message=f"Event delivered to {delivered} subscribers")
        else:
            if queue_name not in self._event_buses:
                self._event_buses[queue_name] = EventBus()
            self._event_buses[queue_name].publish(event)
            return ActionResult(success=True, message=f"Event published to '{queue_name}'")

    def _subscribe(self, bus_name: str, event_type: str, params: Dict) -> ActionResult:
        """Subscribe to event type."""
        if bus_name not in self._event_buses:
            self._event_buses[bus_name] = EventBus()

        handler = params.get("handler")
        if not handler:
            return ActionResult(success=False, message="Handler is required")

        sub_id = self._event_buses[bus_name].subscribe(event_type, handler)
        return ActionResult(success=True, message=f"Subscribed with ID '{sub_id}'")

    def _unsubscribe(self, bus_name: str, params: Dict) -> ActionResult:
        """Unsubscribe from events."""
        sub_id = params.get("subscription_id")
        if not sub_id:
            return ActionResult(success=False, message="subscription_id is required")

        if bus_name not in self._event_buses:
            return ActionResult(success=False, message=f"Event bus '{bus_name}' not found")

        removed = self._event_buses[bus_name].unsubscribe(sub_id)
        return ActionResult(success=removed, message="Unsubscribed" if removed else "Subscription not found")

    def _get_event(self, queue_name: str, params: Dict) -> ActionResult:
        """Get event from queue."""
        block = params.get("block", True)
        timeout = params.get("timeout")

        if queue_name in self._queues:
            event = self._queues[queue_name].get(block=block, timeout=timeout)
        elif queue_name in self._priority_queues:
            event = self._priority_queues[queue_name].get(block=block, timeout=timeout)
        else:
            return ActionResult(success=False, message=f"Queue '{queue_name}' not found")

        if event:
            return ActionResult(success=True, message="Event retrieved", data={"event": event})
        return ActionResult(success=False, message="No event available")

    def _schedule_event(self, bus_name: str, event_type: str, params: Dict) -> ActionResult:
        """Schedule delayed event."""
        delay = params.get("delay", 1.0)

        if bus_name not in self._event_buses:
            self._event_buses[bus_name] = EventBus()

        if bus_name not in self._delayed_events:
            self._delayed_events[bus_name] = DelayedEvent(self._event_buses[bus_name])
            self._delayed_events[bus_name].start()

        event = Event(
            id=f"evt_{int(time.time() * 1000)}",
            type=event_type,
            data=params.get("data"),
            source=params.get("source"),
        )

        event_id = self._delayed_events[bus_name].schedule(event, delay)
        return ActionResult(success=True, message=f"Event scheduled with ID '{event_id}'")
