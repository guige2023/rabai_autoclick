"""Event bus action module for RabAI AutoClick.

Provides event-driven automation operations:
- EventBusPublishAction: Publish events to an event bus
- EventBusSubscribeAction: Subscribe to event bus topics
- EventBusEmitAction: Emit events with routing keys
- EventBusDeadLetterAction: Handle dead letter queue events
"""

import json
import threading
import time
import uuid
from typing import Any, Callable, Dict, List, Optional, Set
from dataclasses import dataclass, field
from datetime import datetime


import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


@dataclass
class Event:
    """Represents an event in the bus."""
    event_type: str
    payload: Dict[str, Any]
    source: str = "unknown"
    event_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    timestamp: str = field(default_factory=lambda: datetime.utcnow().isoformat())
    headers: Dict[str, str] = field(default_factory=dict)
    routing_key: str = ""


class InMemoryEventBus:
    """In-memory event bus implementation."""
    
    def __init__(self):
        self._subscribers: Dict[str, List[Callable]] = {}
        self._wildcard_subscribers: List[tuple] = []
        self._lock = threading.RLock()
        self._event_history: List[Event] = []
        self._max_history = 1000

    def publish(self, event: Event) -> None:
        """Publish an event to all matching subscribers."""
        with self._lock:
            self._event_history.append(event)
            if len(self._event_history) > self._max_history:
                self._event_history = self._event_history[-self._max_history:]

        handlers = []
        with self._lock:
            if event.event_type in self._subscribers:
                handlers.extend(self._subscribers[event.event_type])
            for pattern, handler in self._wildcard_subscribers:
                if self._match_pattern(event.event_type, pattern):
                    handlers.append(handler)

        for handler in handlers:
            try:
                handler(event)
            except Exception:
                pass

    def subscribe(self, event_type: str, handler: Callable[[Event], None]) -> str:
        """Subscribe to an event type."""
        with self._lock:
            if event_type not in self._subscribers:
                self._subscribers[event_type] = []
            self._subscribers[event_type].append(handler)
        return event_type

    def subscribe_wildcard(self, pattern: str, handler: Callable[[Event], None]) -> str:
        """Subscribe with wildcard pattern."""
        sub_id = str(uuid.uuid4())
        with self._lock:
            self._wildcard_subscribers.append((pattern, handler))
        return sub_id

    def unsubscribe(self, event_type: str, handler: Callable) -> bool:
        """Unsubscribe a handler from an event type."""
        with self._lock:
            if event_type in self._subscribers:
                try:
                    self._subscribers[event_type].remove(handler)
                    return True
                except ValueError:
                    return False
        return False

    def get_history(self, event_type: Optional[str] = None, limit: int = 100) -> List[Event]:
        """Get event history."""
        with self._lock:
            history = self._event_history
            if event_type:
                history = [e for e in history if e.event_type == event_type]
            return history[-limit:]

    @staticmethod
    def _match_pattern(event_type: str, pattern: str) -> bool:
        """Match event type against wildcard pattern."""
        if pattern == "*":
            return True
        if pattern.endswith(".*"):
            prefix = pattern[:-2]
            return event_type.startswith(prefix)
        if pattern.startswith("*."):
            suffix = pattern[2:]
            return event_type.endswith(suffix)
        return event_type == pattern


_bus = InMemoryEventBus()


class EventBusPublishAction(BaseAction):
    """Publish an event to the event bus."""
    action_type = "event_bus_publish"
    display_name = "事件总线发布"
    description = "向事件总线发布事件"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            event_type = params.get("event_type", "")
            payload = params.get("payload", {})
            source = params.get("source", "rabai_autoclick")
            routing_key = params.get("routing_key", "")
            headers = params.get("headers", {})

            if not event_type:
                return ActionResult(success=False, message="event_type is required")

            event = Event(
                event_type=event_type,
                payload=payload,
                source=source,
                routing_key=routing_key,
                headers=headers
            )

            _bus.publish(event)

            return ActionResult(
                success=True,
                message=f"Event published: {event_type}",
                data={"event_id": event.event_id, "event_type": event_type}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Event publish failed: {str(e)}")


class EventBusSubscribeAction(BaseAction):
    """Subscribe to event bus topics."""
    action_type = "event_bus_subscribe"
    display_name = "事件总线订阅"
    description = "订阅事件总线主题"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            event_type = params.get("event_type", "")
            pattern = params.get("pattern", None)
            handler_ref = params.get("handler_ref", None)

            if not event_type and not pattern:
                return ActionResult(success=False, message="event_type or pattern is required")

            if pattern:
                sub_id = _bus.subscribe_wildcard(pattern, lambda e: None)
                return ActionResult(
                    success=True,
                    message=f"Subscribed to pattern: {pattern}",
                    data={"subscription_id": sub_id}
                )
            else:
                sub_id = _bus.subscribe(event_type, lambda e: None)
                return ActionResult(
                    success=True,
                    message=f"Subscribed to event type: {event_type}",
                    data={"subscription_id": sub_id}
                )

        except Exception as e:
            return ActionResult(success=False, message=f"Event subscribe failed: {str(e)}")


class EventBusEmitAction(BaseAction):
    """Emit events with routing key support."""
    action_type = "event_bus_emit"
    display_name = "事件总线发送"
    description = "使用路由键发送事件"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            routing_key = params.get("routing_key", "")
            payload = params.get("payload", {})
            exchange = params.get("exchange", "default")
            headers = params.get("headers", {})

            event_type = routing_key.split(".")[-1] if routing_key else "unknown"

            event = Event(
                event_type=event_type,
                payload=payload,
                routing_key=routing_key,
                headers={**headers, "exchange": exchange}
            )

            _bus.publish(event)

            return ActionResult(
                success=True,
                message=f"Event emitted with routing key: {routing_key}",
                data={"event_id": event.event_id, "routing_key": routing_key}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Event emit failed: {str(e)}")


class EventBusDeadLetterAction(BaseAction):
    """Handle dead letter queue events."""
    action_type = "event_bus_dead_letter"
    display_name = "死信处理"
    description = "处理事件总线死信队列"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            max_retries = params.get("max_retries", 3)
            dead_letter_threshold = params.get("dead_letter_threshold", 5)
            action = params.get("action", "log")
            source = params.get("source", "")

            history = _bus.get_history(limit=1000)
            failed_events = []

            for event in history:
                retry_count = event.headers.get("retry_count", 0)
                if retry_count >= dead_letter_threshold:
                    failed_events.append({
                        "event_id": event.event_id,
                        "event_type": event.event_type,
                        "payload": event.payload,
                        "retry_count": retry_count,
                        "timestamp": event.timestamp
                    })

            if action == "log":
                return ActionResult(
                    success=True,
                    message=f"Found {len(failed_events)} dead letter events",
                    data={"dead_letters": failed_events}
                )
            elif action == "retry":
                for dl_event in failed_events[:max_retries]:
                    new_event = Event(
                        event_type=dl_event["event_type"],
                        payload=dl_event["payload"],
                        headers={**dl_event.get("headers", {}), "retry_count": 0}
                    )
                    _bus.publish(new_event)
                return ActionResult(
                    success=True,
                    message=f"Retried {min(len(failed_events), max_retries)} dead letter events",
                    data={"retried": min(len(failed_events), max_retries)}
                )
            elif action == "discard":
                return ActionResult(
                    success=True,
                    message=f"Discarded {len(failed_events)} dead letter events",
                    data={"discarded": len(failed_events)}
                )
            else:
                return ActionResult(success=False, message=f"Unknown action: {action}")

        except Exception as e:
            return ActionResult(success=False, message=f"Dead letter handling failed: {str(e)}")
