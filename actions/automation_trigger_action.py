"""Automation trigger action module.

Provides trigger-based automation:
- TriggerManager: Manage automation triggers
- CronTrigger: Cron-based trigger
- EventTrigger: Event-based trigger
- IntervalTrigger: Interval-based trigger
- WebhookTrigger: Webhook-based trigger
"""

from __future__ import annotations

import time
import logging
from typing import Any, Callable, Dict, List, Optional
from dataclasses import dataclass, field
from enum import Enum
import threading
import re

logger = logging.getLogger(__name__)


class TriggerType(Enum):
    """Type of automation trigger."""
    CRON = "cron"
    INTERVAL = "interval"
    EVENT = "event"
    WEBHOOK = "webhook"
    MANUAL = "manual"


class TriggerState(Enum):
    """State of a trigger."""
    INACTIVE = "inactive"
    ACTIVE = "active"
    PAUSED = "paused"
    ERROR = "error"


@dataclass
class TriggerEvent:
    """An event that can trigger automation."""
    trigger_id: str
    trigger_type: TriggerType
    payload: Dict[str, Any]
    timestamp: float = field(default_factory=time.time)
    source: Optional[str] = None


@dataclass
class TriggerConfig:
    """Configuration for a trigger."""
    id: str
    name: str
    trigger_type: TriggerType
    enabled: bool = True
    max_concurrent: int = 1
    cooldown_seconds: float = 0.0
    metadata: Dict[str, Any] = field(default_factory=dict)


class CronTrigger:
    """Cron-based trigger."""

    CRON_PATTERN = re.compile(
        r"^(\S+)\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S+)$"
    )

    def __init__(
        self,
        cron_expression: str,
        timezone: str = "UTC",
    ):
        self.cron_expression = cron_expression
        self.timezone = timezone
        self._parse_cron()

    def _parse_cron(self) -> None:
        """Parse cron expression."""
        match = self.CRON_PATTERN.match(self.cron_expression)
        if match:
            self.minute, self.hour, self.day, self.month, self.weekday = match.groups()
        else:
            raise ValueError(f"Invalid cron expression: {self.cron_expression}")

    def should_fire(self, check_time: Optional[time.StructTime] = None) -> bool:
        """Check if trigger should fire at given time."""
        if check_time is None:
            check_time = time.localtime()
        return True

    def get_next_fire_time(
        self,
        after: Optional[float] = None,
    ) -> Optional[float]:
        """Get next fire time after given timestamp."""
        return after or time.time() + 60.0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": "cron",
            "expression": self.cron_expression,
            "timezone": self.timezone,
        }


class IntervalTrigger:
    """Interval-based trigger."""

    def __init__(
        self,
        interval_seconds: float,
        offset: float = 0.0,
    ):
        if interval_seconds <= 0:
            raise ValueError("Interval must be positive")
        self.interval_seconds = interval_seconds
        self.offset = offset
        self._last_fire_time: Optional[float] = None

    def should_fire(self, check_time: Optional[float] = None) -> bool:
        """Check if trigger should fire."""
        if check_time is None:
            check_time = time.time()

        if self._last_fire_time is None:
            return True

        elapsed = check_time - self._last_fire_time
        return elapsed >= self.interval_seconds

    def mark_fired(self, fire_time: Optional[float] = None) -> None:
        """Mark that trigger has fired."""
        self._last_fire_time = fire_time or time.time()

    def get_next_fire_time(self, after: Optional[float] = None) -> float:
        """Get next fire time."""
        current = after or time.time()
        if self._last_fire_time is None:
            return current + self.offset
        return self._last_fire_time + self.interval_seconds

    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": "interval",
            "interval_seconds": self.interval_seconds,
            "offset": self.offset,
        }


class EventTrigger:
    """Event-based trigger."""

    def __init__(self, event_type: str, filter_fn: Optional[Callable[[Dict[str, Any]], bool]] = None):
        self.event_type = event_type
        self.filter_fn = filter_fn
        self._event_history: List[TriggerEvent] = []

    def should_fire(self, event: TriggerEvent) -> bool:
        """Check if event should trigger automation."""
        if event.trigger_type != TriggerType.EVENT:
            return False
        if event.payload.get("type") != self.event_type:
            return False
        if self.filter_fn:
            return self.filter_fn(event.payload)
        return True

    def record_event(self, event: TriggerEvent) -> None:
        """Record an event in history."""
        self._event_history.append(event)
        if len(self._event_history) > 1000:
            self._event_history = self._event_history[-500:]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": "event",
            "event_type": self.event_type,
        }


class WebhookTrigger:
    """Webhook-based trigger."""

    def __init__(self, path: str, secret: Optional[str] = None):
        self.path = path
        self.secret = secret
        self._call_count = 0

    def should_fire(self, request: Dict[str, Any]) -> bool:
        """Check if webhook request should trigger."""
        self._call_count += 1
        return True

    def verify_signature(
        self,
        payload: bytes,
        signature: str,
    ) -> bool:
        """Verify webhook signature."""
        if not self.secret:
            return True
        import hmac
        import hashlib
        expected = hmac.new(
            self.secret.encode(),
            payload,
            hashlib.sha256,
        ).hexdigest()
        return hmac.compare_digest(f"sha256={expected}", signature)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": "webhook",
            "path": self.path,
            "call_count": self._call_count,
        }


class TriggerManager:
    """Manage automation triggers."""

    def __init__(self):
        self._triggers: Dict[str, TriggerConfig] = {}
        self._trigger_instances: Dict[str, Any] = {}
        self._handlers: Dict[str, List[Callable]] = {}
        self._lock = threading.Lock()
        self._running = False
        self._poll_thread: Optional[threading.Thread] = None

    def register_trigger(
        self,
        config: TriggerConfig,
        instance: Any,
    ) -> None:
        """Register a trigger."""
        with self._lock:
            self._triggers[config.id] = config
            self._trigger_instances[config.id] = instance
            self._handlers[config.id] = []
            logger.info(f"Registered trigger: {config.id} ({config.trigger_type.value})")

    def add_handler(
        self,
        trigger_id: str,
        handler: Callable[[TriggerEvent], None],
    ) -> None:
        """Add a handler for a trigger."""
        with self._lock:
            if trigger_id not in self._handlers:
                self._handlers[trigger_id] = []
            self._handlers[trigger_id].append(handler)

    def fire_trigger(
        self,
        trigger_id: str,
        payload: Optional[Dict[str, Any]] = None,
    ) -> int:
        """Fire a trigger manually."""
        with self._lock:
            config = self._triggers.get(trigger_id)
            if not config or not config.enabled:
                return 0

            event = TriggerEvent(
                trigger_id=trigger_id,
                trigger_type=config.trigger_type,
                payload=payload or {},
                source="manual",
            )

            handlers = self._handlers.get(trigger_id, [])
            for handler in handlers:
                try:
                    handler(event)
                except Exception as e:
                    logger.error(f"Handler error for trigger {trigger_id}: {e}")

            return len(handlers)

    def start(self) -> None:
        """Start the trigger manager."""
        self._running = True
        logger.info("Trigger manager started")

    def stop(self) -> None:
        """Stop the trigger manager."""
        self._running = False
        logger.info("Trigger manager stopped")

    def get_trigger_status(self) -> Dict[str, Any]:
        """Get status of all triggers."""
        return {
            tid: {
                "name": cfg.name,
                "type": cfg.trigger_type.value,
                "enabled": cfg.enabled,
                "state": TriggerState.ACTIVE.value,
            }
            for tid, cfg in self._triggers.items()
        }


def create_cron_trigger(expression: str, timezone: str = "UTC") -> CronTrigger:
    """Create a cron trigger."""
    return CronTrigger(expression, timezone)


def create_interval_trigger(seconds: float, offset: float = 0.0) -> IntervalTrigger:
    """Create an interval trigger."""
    return IntervalTrigger(seconds, offset)


def create_webhook_trigger(path: str, secret: Optional[str] = None) -> WebhookTrigger:
    """Create a webhook trigger."""
    return WebhookTrigger(path, secret)
