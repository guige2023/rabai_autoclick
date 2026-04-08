"""Automation Trigger Action Module.

Provides event-driven automation triggers with filtering,
conditional logic, and action dispatching.
"""

from typing import Any, Dict, List, Optional, Callable
from dataclasses import dataclass, field
from enum import Enum
from datetime import datetime
import asyncio
import json
import hashlib


class TriggerType(Enum):
    """Types of automation triggers."""
    EVENT = "event"
    SCHEDULE = "schedule"
    CONDITION = "condition"
    WEBHOOK = "webhook"
    DATA_CHANGE = "data_change"
    MANUAL = "manual"


class TriggerStatus(Enum):
    """Trigger execution status."""
    ACTIVE = "active"
    PAUSED = "paused"
    DISABLED = "disabled"
    ERROR = "error"


@dataclass
class TriggerEvent:
    """Represents an event that can trigger automation."""
    type: str
    source: str
    data: Dict[str, Any]
    timestamp: datetime = field(default_factory=datetime.now)
    correlation_id: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class TriggerCondition:
    """Condition for conditional trigger evaluation."""
    field: str
    operator: str
    value: Any
    logical_op: Optional[str] = None

    def evaluate(self, data: Dict[str, Any]) -> bool:
        """Evaluate condition against data."""
        field_value = self._get_nested_field(data, self.field)
        return self._compare(field_value, self.operator, self.value)

    def _get_nested_field(self, data: Dict[str, Any], field_path: str) -> Any:
        """Get nested field using dot notation."""
        parts = field_path.split(".")
        value = data
        for part in parts:
            if isinstance(value, dict):
                value = value.get(part)
            else:
                return None
        return value

    def _compare(self, left: Any, op: str, right: Any) -> bool:
        """Compare values using operator."""
        if op == "==":
            return left == right
        elif op == "!=":
            return left != right
        elif op == ">":
            return left > right
        elif op == ">=":
            return left >= right
        elif op == "<":
            return left < right
        elif op == "<=":
            return left <= right
        elif op == "in":
            return left in right if isinstance(right, (list, tuple)) else False
        elif op == "not in":
            return left not in right if isinstance(right, (list, tuple)) else True
        elif op == "contains":
            return str(right) in str(left)
        elif op == "startswith":
            return str(left).startswith(str(right))
        elif op == "endswith":
            return str(left).endswith(str(right))
        elif op == "exists":
            return left is not None
        elif op == "empty":
            return left is None or left == "" or left == []
        return False


@dataclass
class Trigger:
    """Defines an automation trigger."""
    id: str
    name: str
    trigger_type: TriggerType
    handler: Callable
    conditions: List[TriggerCondition] = field(default_factory=list)
    filters: Optional[Callable[[TriggerEvent], bool]] = None
    throttle_seconds: int = 0
    max_executions: int = 0
    status: TriggerStatus = TriggerStatus.ACTIVE
    metadata: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        if not self.id:
            self.id = hashlib.md5(
                f"{self.name}:{self.trigger_type.value}:{datetime.now().isoformat()}".encode()
            ).hexdigest()[:12]

    def should_fire(self, event: TriggerEvent) -> bool:
        """Check if trigger should fire for event."""
        if self.status != TriggerStatus.ACTIVE:
            return False
        if self.filters and not self.filters(event):
            return False
        for condition in self.conditions:
            if not condition.evaluate(event.data):
                return False
        return True


@dataclass
class TriggerExecution:
    """Records a trigger execution."""
    trigger_id: str
    event: TriggerEvent
    status: str
    started_at: datetime = field(default_factory=datetime.now)
    completed_at: Optional[datetime] = None
    result: Optional[Any] = None
    error: Optional[str] = None


class TriggerRegistry:
    """Registry for automation triggers."""

    def __init__(self):
        self._triggers: Dict[str, Trigger] = {}
        self._executions: Dict[str, List[TriggerExecution]] = {}
        self._last_execution: Dict[str, datetime] = {}
        self._execution_counts: Dict[str, int] = {}

    def register(self, trigger: Trigger) -> str:
        """Register a new trigger."""
        self._triggers[trigger.id] = trigger
        self._executions[trigger.id] = []
        return trigger.id

    def unregister(self, trigger_id: str) -> bool:
        """Unregister a trigger."""
        if trigger_id in self._triggers:
            del self._triggers[trigger_id]
            return True
        return False

    def get(self, trigger_id: str) -> Optional[Trigger]:
        """Get trigger by ID."""
        return self._triggers.get(trigger_id)

    def get_by_name(self, name: str) -> Optional[Trigger]:
        """Get trigger by name."""
        for trigger in self._triggers.values():
            if trigger.name == name:
                return trigger
        return None

    def list_triggers(
        self,
        trigger_type: Optional[TriggerType] = None,
        status: Optional[TriggerStatus] = None,
    ) -> List[Trigger]:
        """List triggers with optional filtering."""
        triggers = list(self._triggers.values())
        if trigger_type:
            triggers = [t for t in triggers if t.trigger_type == trigger_type]
        if status:
            triggers = [t for t in triggers if t.status == status]
        return triggers

    def record_execution(self, execution: TriggerExecution):
        """Record trigger execution."""
        if execution.trigger_id not in self._executions:
            self._executions[execution.trigger_id] = []
        self._executions[execution.trigger_id].append(execution)
        self._last_execution[execution.trigger_id] = execution.started_at

        if execution.trigger_id not in self._execution_counts:
            self._execution_counts[execution.trigger_id] = 0
        self._execution_counts[execution.trigger_id] += 1

    def get_execution_history(
        self,
        trigger_id: str,
        limit: int = 100,
    ) -> List[TriggerExecution]:
        """Get execution history for trigger."""
        return (
            self._executions.get(trigger_id, [])[-limit:]
        )

    def can_execute(self, trigger: Trigger) -> bool:
        """Check if trigger can be executed based on throttling."""
        if trigger.status != TriggerStatus.ACTIVE:
            return False

        if trigger.max_executions > 0:
            count = self._execution_counts.get(trigger.id, 0)
            if count >= trigger.max_executions:
                return False

        if trigger.throttle_seconds > 0:
            last_exec = self._last_execution.get(trigger.id)
            if last_exec:
                elapsed = (datetime.now() - last_exec).total_seconds()
                if elapsed < trigger.throttle_seconds:
                    return False

        return True


class EventBus:
    """Simple event bus for trigger dispatch."""

    def __init__(self, registry: Optional[TriggerRegistry] = None):
        self._registry = registry or TriggerRegistry()
        self._subscribers: Dict[str, List[Callable]] = {}
        self._event_history: List[TriggerEvent] = []
        self._max_history: int = 1000

    def subscribe(self, event_type: str, handler: Callable):
        """Subscribe to an event type."""
        if event_type not in self._subscribers:
            self._subscribers[event_type] = []
        self._subscribers[event_type].append(handler)

    def unsubscribe(self, event_type: str, handler: Callable):
        """Unsubscribe from event type."""
        if event_type in self._subscribers:
            self._subscribers[event_type] = [
                h for h in self._subscribers[event_type] if h != handler
            ]

    def publish(self, event: TriggerEvent):
        """Publish an event to all subscribers."""
        self._event_history.append(event)
        if len(self._event_history) > self._max_history:
            self._event_history = self._event_history[-self._max_history:]

        handlers = self._subscribers.get(event.type, [])
        for handler in handlers:
            try:
                if asyncio.iscoroutinefunction(handler):
                    asyncio.create_task(handler(event))
                else:
                    handler(event)
            except Exception:
                pass

    def get_event_history(
        self,
        event_type: Optional[str] = None,
        limit: int = 100,
    ) -> List[TriggerEvent]:
        """Get recent events."""
        events = self._event_history
        if event_type:
            events = [e for e in events if e.type == event_type]
        return events[-limit:]


class AutomationTriggerAction:
    """High-level automation trigger action."""

    def __init__(
        self,
        registry: Optional[TriggerRegistry] = None,
        event_bus: Optional[EventBus] = None,
    ):
        self._registry = registry or TriggerRegistry()
        self._event_bus = event_bus or EventBus(self._registry)
        self._running_tasks: Dict[str, asyncio.Task] = {}

    def create_trigger(
        self,
        name: str,
        trigger_type: TriggerType,
        handler: Callable,
        conditions: Optional[List[Dict[str, Any]]] = None,
        filters: Optional[Callable] = None,
        throttle_seconds: int = 0,
    ) -> Trigger:
        """Create a new trigger."""
        trigger_conditions = []
        if conditions:
            for cond in conditions:
                trigger_conditions.append(TriggerCondition(
                    field=cond.get("field", ""),
                    operator=cond.get("operator", "=="),
                    value=cond.get("value"),
                    logical_op=cond.get("logical_op"),
                ))

        trigger = Trigger(
            id="",
            name=name,
            trigger_type=trigger_type,
            handler=handler,
            conditions=trigger_conditions,
            filters=filters,
            throttle_seconds=throttle_seconds,
        )
        self._registry.register(trigger)
        return trigger

    async def fire_trigger(
        self,
        trigger_id: str,
        event_data: Dict[str, Any],
    ) -> TriggerExecution:
        """Fire a trigger with event data."""
        trigger = self._registry.get(trigger_id)
        if not trigger:
            raise ValueError(f"Trigger {trigger_id} not found")

        event = TriggerEvent(
            type=trigger.trigger_type.value,
            source=trigger.name,
            data=event_data,
        )

        execution = TriggerExecution(
            trigger_id=trigger_id,
            event=event,
            status="pending",
        )

        if not self._registry.can_execute(trigger):
            execution.status = "throttled"
            self._registry.record_execution(execution)
            return execution

        if not trigger.should_fire(event):
            execution.status = "skipped"
            self._registry.record_execution(execution)
            return execution

        execution.status = "running"
        self._registry.record_execution(execution)

        try:
            result = trigger.handler(event)
            if asyncio.iscoroutine(result):
                result = await result
            execution.result = result
            execution.status = "completed"
        except Exception as e:
            execution.error = str(e)
            execution.status = "failed"

        execution.completed_at = datetime.now()
        return execution

    async def handle_event(self, event: TriggerEvent):
        """Handle incoming event and fire matching triggers."""
        matching_triggers = [
            t for t in self._registry.list_triggers(status=TriggerStatus.ACTIVE)
            if t.trigger_type.value == event.type
        ]

        for trigger in matching_triggers:
            if trigger.should_fire(event):
                asyncio.create_task(
                    self.fire_trigger(trigger.id, event.data)
                )

    def publish_event(
        self,
        event_type: str,
        data: Dict[str, Any],
        source: str = "manual",
    ) -> TriggerEvent:
        """Publish an event to the event bus."""
        event = TriggerEvent(
            type=event_type,
            source=source,
            data=data,
        )
        self._event_bus.publish(event)
        asyncio.create_task(self.handle_event(event))
        return event

    def get_trigger_status(self) -> Dict[str, Any]:
        """Get status of all triggers."""
        triggers = self._registry.list_triggers()
        return {
            "total": len(triggers),
            "active": len([t for t in triggers if t.status == TriggerStatus.ACTIVE]),
            "paused": len([t for t in triggers if t.status == TriggerStatus.PAUSED]),
            "disabled": len([t for t in triggers if t.status == TriggerStatus.DISABLED]),
            "by_type": {
                t.value: len([tr for tr in triggers if tr.trigger_type == t])
                for t in TriggerType
            },
        }


# Module exports
__all__ = [
    "AutomationTriggerAction",
    "TriggerRegistry",
    "EventBus",
    "Trigger",
    "TriggerEvent",
    "TriggerCondition",
    "TriggerExecution",
    "TriggerType",
    "TriggerStatus",
]
