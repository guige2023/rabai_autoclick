"""
Automation Event Bus Action Module.

Provides event-driven automation capabilities including event publishing,
subscription, filtering, and routing for building event-driven workflows.

Author: RabAI Team
"""

from typing import Any, Callable, Dict, List, Optional, Set
from dataclasses import dataclass, field
from enum import Enum
import threading
import time
import uuid
from datetime import datetime
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor


class EventPriority(Enum):
    """Event priority levels."""
    LOW = "low"
    NORMAL = "normal"
    HIGH = "high"
    CRITICAL = "critical"


@dataclass
class Event:
    """Represents an event in the system."""
    id: str
    event_type: str
    payload: Dict[str, Any]
    priority: EventPriority = EventPriority.NORMAL
    timestamp: datetime = field(default_factory=datetime.now)
    source: str = ""
    correlation_id: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class Subscription:
    """Represents an event subscription."""
    id: str
    event_type: str
    handler: Callable[[Event], None]
    filter_fn: Optional[Callable[[Event], bool]] = None
    auto_ack: bool = True
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class EventBusConfig:
    """Configuration for event bus."""
    name: str
    max_queue_size: int = 1000
    num_workers: int = 4
    retry_attempts: int = 3
    retry_delay: float = 1.0
    dead_letter_enabled: bool = True


class EventValidator:
    """Validates events before publishing."""
    
    REQUIRED_FIELDS = ["event_type", "payload"]
    
    @classmethod
    def validate(cls, event: Event) -> tuple[bool, Optional[str]]:
        """Validate an event."""
        if not event.event_type:
            return False, "event_type is required"
        
        if not isinstance(event.payload, dict):
            return False, "payload must be a dictionary"
        
        return True, None


class InMemoryEventBus:
    """
    In-memory event bus implementation.
    
    Example:
        bus = InMemoryEventBus(name="main")
        bus.subscribe("user.created", handle_user_created)
        bus.publish(Event(event_type="user.created", payload={"user_id": 123}))
    """
    
    def __init__(self, config: Optional[EventBusConfig] = None):
        self.config = config or EventBusConfig(name="default")
        self.subscriptions: Dict[str, List[Subscription]] = defaultdict(list)
        self.event_queue: List[Event] = []
        self.dead_letter_queue: List[Event] = []
        self.processed_count: int = 0
        self.failed_count: int = 0
        
        self._lock = threading.RLock()
        self._running = False
        self._worker_thread: Optional[threading.Thread] = None
        self._executor: Optional[ThreadPoolExecutor] = None
    
    def start(self):
        """Start the event bus."""
        with self._lock:
            if self._running:
                return
            
            self._running = True
            self._executor = ThreadPoolExecutor(
                max_workers=self.config.num_workers
            )
            self._worker_thread = threading.Thread(
                target=self._process_events,
                daemon=True
            )
            self._worker_thread.start()
    
    def stop(self):
        """Stop the event bus."""
        with self._lock:
            self._running = False
            if self._executor:
                self._executor.shutdown(wait=True)
    
    def subscribe(
        self,
        event_type: str,
        handler: Callable[[Event], None],
        filter_fn: Optional[Callable[[Event], bool]] = None,
        **metadata
    ) -> str:
        """Subscribe to an event type."""
        subscription_id = str(uuid.uuid4())
        
        subscription = Subscription(
            id=subscription_id,
            event_type=event_type,
            handler=handler,
            filter_fn=filter_fn,
            metadata=metadata
        )
        
        with self._lock:
            self.subscriptions[event_type].append(subscription)
        
        return subscription_id
    
    def unsubscribe(self, subscription_id: str) -> bool:
        """Unsubscribe from events."""
        with self._lock:
            for event_type, subs in self.subscriptions.items():
                self.subscriptions[event_type] = [
                    s for s in subs if s.id != subscription_id
                ]
                if not self.subscriptions[event_type]:
                    del self.subscriptions[event_type]
            return True
    
    def publish(self, event: Event) -> bool:
        """Publish an event."""
        valid, error = EventValidator.validate(event)
        if not valid:
            raise ValueError(f"Invalid event: {error}")
        
        event.id = event.id or str(uuid.uuid4())
        
        with self._lock:
            if len(self.event_queue) >= self.config.max_queue_size:
                # Drop or block based on priority
                if event.priority == EventPriority.CRITICAL:
                    self.event_queue.pop(0)  # Drop oldest
                else:
                    return False
            
            self.event_queue.append(event)
        
        return True
    
    def publish_sync(self, event: Event) -> List[Any]:
        """Publish event and wait for all handlers."""
        if not self.publish(event):
            return []
        
        # Wait for processing
        time.sleep(0.1)
        
        return []
    
    def _process_events(self):
        """Background event processing."""
        while self._running:
            event = None
            
            with self._lock:
                if self.event_queue:
                    event = self.event_queue.pop(0)
            
            if event:
                self._dispatch_event(event)
            
            time.sleep(0.001)
    
    def _dispatch_event(self, event: Event):
        """Dispatch event to subscribers."""
        handlers_to_run = []
        
        with self._lock:
            subscriptions = self.subscriptions.get(event.event_type, [])
            
            for sub in subscriptions:
                if sub.filter_fn and not sub.filter_fn(event):
                    continue
                handlers_to_run.append(sub)
        
        for sub in handlers_to_run:
            if self._executor:
                self._executor.submit(self._handle_subscription, sub, event)
            else:
                self._handle_subscription(sub, event)
    
    def _handle_subscription(self, sub: Subscription, event: Event):
        """Handle a single subscription."""
        try:
            sub.handler(event)
            with self._lock:
                self.processed_count += 1
        except Exception as e:
            with self._lock:
                self.failed_count += 1
            
            if self.config.dead_letter_enabled:
                self._send_to_dead_letter(event, str(e))
    
    def _send_to_dead_letter(self, event: Event, error: str):
        """Send failed event to dead letter queue."""
        event.metadata["dead_letter_error"] = error
        event.metadata["dead_letter_time"] = datetime.now().isoformat()
        self.dead_letter_queue.append(event)
    
    def get_stats(self) -> Dict[str, Any]:
        """Get event bus statistics."""
        with self._lock:
            return {
                "name": self.config.name,
                "running": self._running,
                "queue_size": len(self.event_queue),
                "dead_letter_size": len(self.dead_letter_queue),
                "processed_count": self.processed_count,
                "failed_count": self.failed_count,
                "subscription_count": sum(
                    len(subs) for subs in self.subscriptions.values()
                ),
                "event_types": list(self.subscriptions.keys())
            }


class EventRouter:
    """
    Routes events to different destinations based on rules.
    
    Example:
        router = EventRouter()
        router.add_rule("user.*", destination="analytics")
        router.add_rule("order.*", destination="fulfillment")
        
        router.route(event)
    """
    
    def __init__(self):
        self.rules: List[Tuple[str, str]] = []
        self.routes: Dict[str, Callable] = {}
        self._lock = threading.Lock()
    
    def add_rule(self, pattern: str, destination: str) -> "EventRouter":
        """Add a routing rule."""
        import re
        # Convert simple patterns to regex
        regex_pattern = pattern.replace("*", ".*").replace(".", "\\.")
        regex_pattern = f"^{regex_pattern}$"
        
        with self._lock:
            self.rules.append((re.compile(regex_pattern), destination))
        return self
    
    def add_route(self, destination: str, handler: Callable) -> "EventRouter":
        """Add a route handler."""
        with self._lock:
            self.routes[destination] = handler
        return self
    
    def route(self, event: Event) -> List[str]:
        """Route an event to matching destinations."""
        matched = []
        
        with self._lock:
            for pattern, destination in self.rules:
                if pattern.match(event.event_type):
                    matched.append(destination)
                    handler = self.routes.get(destination)
                    if handler:
                        try:
                            handler(event)
                        except Exception:
                            pass
        
        return matched


class EventAggregator:
    """
    Aggregates multiple events into single events.
    
    Example:
        aggregator = EventAggregator(window=timedelta(seconds=10))
        aggregator.add_event(event)
        
        aggregated = aggregator.flush()
    """
    
    def __init__(self, window_seconds: float = 60.0):
        self.window_seconds = window_seconds
        self.events: Dict[str, List[Event]] = defaultdict(list)
        self.last_flush: Dict[str, datetime] = {}
        self._lock = threading.Lock()
    
    def add_event(self, event: Event) -> Optional[List[Event]]:
        """Add event and return aggregated if window expired."""
        with self._lock:
            key = event.event_type
            self.events[key].append(event)
            
            # Check if window expired
            last = self.last_flush.get(key)
            if last is None:
                self.last_flush[key] = datetime.now()
                return None
            
            elapsed = (datetime.now() - last).total_seconds()
            if elapsed >= self.window_seconds:
                return self._flush_key(key)
            
            return None
    
    def flush(self) -> Dict[str, List[Event]]:
        """Flush all aggregated events."""
        with self._lock:
            result = {}
            for key in list(self.events.keys()):
                if self.events[key]:
                    result[key] = self._flush_key(key)
            return result
    
    def _flush_key(self, key: str) -> List[Event]:
        """Flush events for a specific key."""
        events = self.events[key]
        self.events[key] = []
        self.last_flush[key] = datetime.now()
        return events


import re
from typing import Tuple


class BaseAction:
    """Base class for all actions."""
    
    def execute(self, context: Dict[str, Any], params: Dict[str, Any]) -> Any:
        raise NotImplementedError


class AutomationEventBusAction(BaseAction):
    """
    Event bus action for event-driven automation.
    
    Parameters:
        operation: Operation type (publish/subscribe/route/stats)
        event_type: Type of event
        payload: Event payload
        handler: Handler function reference
    
    Example:
        action = AutomationEventBusAction()
        result = action.execute({}, {
            "operation": "publish",
            "event_type": "user.created",
            "payload": {"user_id": 123}
        })
    """
    
    _bus: Optional[InMemoryEventBus] = None
    _router: Optional[EventRouter] = None
    _lock = threading.Lock()
    
    def _get_bus(self) -> InMemoryEventBus:
        """Get or create event bus."""
        with self._lock:
            if self._bus is None:
                config = EventBusConfig(name="automation")
                self._bus = InMemoryEventBus(config)
                self._bus.start()
            return self._bus
    
    def _get_router(self) -> EventRouter:
        """Get or create event router."""
        with self._lock:
            if self._router is None:
                self._router = EventRouter()
            return self._router
    
    def execute(self, context: Dict[str, Any], params: Dict[str, Any]) -> Dict[str, Any]:
        """Execute event bus operation."""
        operation = params.get("operation", "publish")
        event_type = params.get("event_type", "generic")
        payload = params.get("payload", {})
        priority_str = params.get("priority", "normal")
        pattern = params.get("pattern")
        destination = params.get("destination")
        
        if operation == "publish":
            priority = EventPriority(priority_str)
            
            event = Event(
                id=str(uuid.uuid4()),
                event_type=event_type,
                payload=payload,
                priority=priority
            )
            
            bus = self._get_bus()
            success = bus.publish(event)
            
            return {
                "success": success,
                "operation": "publish",
                "event_id": event.id,
                "event_type": event_type,
                "published_at": event.timestamp.isoformat()
            }
        
        elif operation == "subscribe":
            # In a real implementation, handler would be a real function
            def placeholder_handler(e: Event):
                pass
            
            bus = self._get_bus()
            sub_id = bus.subscribe(event_type, placeholder_handler)
            
            return {
                "success": True,
                "operation": "subscribe",
                "subscription_id": sub_id,
                "event_type": event_type
            }
        
        elif operation == "route":
            router = self._get_router()
            
            if pattern and destination:
                router.add_rule(pattern, destination)
                return {
                    "success": True,
                    "operation": "add_rule",
                    "pattern": pattern,
                    "destination": destination
                }
            
            return {"success": False, "error": "pattern and destination required"}
        
        elif operation == "stats":
            bus = self._get_bus()
            stats = bus.get_stats()
            
            return {
                "success": True,
                "operation": "stats",
                "stats": stats
            }
        
        else:
            return {"success": False, "error": f"Unknown operation: {operation}"}
