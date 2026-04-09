"""Automation Trigger System.

This module provides event-based trigger automation:
- Cron-style scheduling
- Event-driven triggers
- Condition evaluation
- Trigger chain support

Example:
    >>> from actions.automation_trigger_action import Trigger, TriggerManager
    >>> manager = TriggerManager()
    >>> manager.add_trigger(Trigger(name="daily_backup", cron="0 2 * * *"))
"""

from __future__ import annotations

import time
import logging
import threading
import croniter
from dataclasses import dataclass, field
from typing import Any, Callable, Optional
from collections import defaultdict

logger = logging.getLogger(__name__)


@dataclass
class Trigger:
    """An automation trigger definition."""
    name: str
    enabled: bool = True
    cron_expr: Optional[str] = None
    interval_seconds: Optional[float] = None
    event_type: Optional[str] = None
    condition_func: Optional[str] = None
    action_func: Optional[str] = None
    last_triggered: Optional[float] = None
    trigger_count: int = 0
    metadata: dict[str, Any] = field(default_factory=dict)


class TriggerManager:
    """Manages automation triggers."""

    def __init__(self) -> None:
        """Initialize the trigger manager."""
        self._triggers: dict[str, Trigger] = {}
        self._lock = threading.RLock()
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._actions: dict[str, Callable] = {}
        self._conditions: dict[str, Callable] = {}
        self._event_listeners: dict[str, list[str]] = defaultdict(list)
        self._stats: dict[str, int] = defaultdict(int)

    def register_action(self, name: str, action: Callable) -> None:
        """Register an action function.

        Args:
            name: Action name.
            action: Callable to execute when trigger fires.
        """
        with self._lock:
            self._actions[name] = action
            logger.info("Registered action: %s", name)

    def register_condition(self, name: str, condition: Callable[[], bool]) -> None:
        """Register a condition function.

        Args:
            name: Condition name.
            condition: Callable that returns True/False.
        """
        with self._lock:
            self._conditions[name] = condition
            logger.info("Registered condition: %s", name)

    def add_trigger(
        self,
        name: str,
        cron_expr: Optional[str] = None,
        interval_seconds: Optional[float] = None,
        event_type: Optional[str] = None,
        action_name: Optional[str] = None,
        condition_name: Optional[str] = None,
        enabled: bool = True,
    ) -> Trigger:
        """Add a new trigger.

        Args:
            name: Unique trigger name.
            cron_expr: Cron expression (e.g., "0 2 * * *").
            interval_seconds: Interval between triggers.
            event_type: Event type to listen for.
            action_name: Name of action to execute.
            condition_name: Name of condition to evaluate.
            enabled: Whether trigger starts enabled.

        Returns:
            The created Trigger.
        """
        if not cron_expr and not interval_seconds and not event_type:
            raise ValueError("Must specify cron_expr, interval_seconds, or event_type")

        if cron_expr:
            croniter.CronTrigger(cron_expr)

        trigger = Trigger(
            name=name,
            cron_expr=cron_expr,
            interval_seconds=interval_seconds,
            event_type=event_type,
            action_func=action_name,
            condition_func=condition_name,
            enabled=enabled,
        )

        with self._lock:
            self._triggers[name] = trigger
            if event_type:
                self._event_listeners[event_type].append(name)
            logger.info("Added trigger: %s", name)

        return trigger

    def remove_trigger(self, name: str) -> bool:
        """Remove a trigger.

        Args:
            name: Trigger name.

        Returns:
            True if removed, False if not found.
        """
        with self._lock:
            if name not in self._triggers:
                return False
            trigger = self._triggers.pop(name)
            if trigger.event_type:
                self._event_listeners[trigger.event_type].remove(name)
            logger.info("Removed trigger: %s", name)
            return True

    def enable_trigger(self, name: str) -> bool:
        """Enable a trigger."""
        with self._lock:
            trigger = self._triggers.get(name)
            if trigger:
                trigger.enabled = True
                return True
            return False

    def disable_trigger(self, name: str) -> bool:
        """Disable a trigger."""
        with self._lock:
            trigger = self._triggers.get(name)
            if trigger:
                trigger.enabled = False
                return True
            return False

    def fire_event(self, event_type: str, data: Optional[dict[str, Any]] = None) -> int:
        """Fire an event, triggering all matching triggers.

        Args:
            event_type: The event type.
            data: Event data payload.

        Returns:
            Number of triggers fired.
        """
        with self._lock:
            listener_names = list(self._event_listeners.get(event_type, []))
            trigger_names = [
                name for name in listener_names
                if self._triggers[name].enabled
            ]

        fired = 0
        for name in trigger_names:
            if self._evaluate_trigger(name, data):
                fired += 1

        return fired

    def _evaluate_trigger(self, name: str, event_data: Optional[dict[str, Any]]) -> bool:
        """Evaluate and fire a single trigger."""
        with self._lock:
            trigger = self._triggers.get(name)
            if not trigger or not trigger.enabled:
                return False

            if trigger.condition_func:
                cond = self._conditions.get(trigger.condition_func)
                if cond and not cond():
                    return False

        if trigger.action_func:
            action = self._actions.get(trigger.action_func)
            if action:
                try:
                    action(event_data or {})
                    trigger.last_triggered = time.time()
                    trigger.trigger_count += 1
                    self._stats["triggered"] += 1
                    logger.info("Trigger %s fired", name)
                    return True
                except Exception as e:
                    logger.error("Trigger %s action failed: %s", name, e)
                    self._stats["errors"] += 1
                    return False

        return False

    def start(self) -> None:
        """Start the trigger scheduler thread."""
        with self._lock:
            if self._running:
                return
            self._running = True
            self._thread = threading.Thread(target=self._scheduler_loop, daemon=True)
            self._thread.start()
            logger.info("Trigger manager started")

    def stop(self) -> None:
        """Stop the trigger scheduler thread."""
        with self._lock:
            self._running = False
        if self._thread:
            self._thread.join(timeout=5.0)
        logger.info("Trigger manager stopped")

    def _scheduler_loop(self) -> None:
        """Main scheduler loop for time-based triggers."""
        while self._running:
            now = time.time()
            with self._lock:
                for trigger in self._triggers.values():
                    if not trigger.enabled:
                        continue

                    if trigger.cron_expr:
                        self._check_cron_trigger(trigger, now)
                    elif trigger.interval_seconds:
                        self._check_interval_trigger(trigger, now)

            time.sleep(1.0)

    def _check_cron_trigger(self, trigger: Trigger, now: float) -> None:
        """Check if a cron trigger should fire."""
        if not trigger.cron_expr:
            return

        try:
            cron = croniter.Croniter(trigger.cron_expr, now)
            prev = cron.get_prev(time.time)
            next_time = cron.get_next(time.time)

            if prev and (now - prev) < 2.0:
                if trigger.last_triggered is None or prev > trigger.last_triggered:
                    self._evaluate_trigger(trigger.name, None)
        except Exception as e:
            logger.error("Cron trigger error for %s: %s", trigger.name, e)

    def _check_interval_trigger(self, trigger: Trigger, now: float) -> None:
        """Check if an interval trigger should fire."""
        if not trigger.interval_seconds:
            return

        if trigger.last_triggered is None:
            self._evaluate_trigger(trigger.name, None)
        elif (now - trigger.last_triggered) >= trigger.interval_seconds:
            self._evaluate_trigger(trigger.name, None)

    def get_trigger(self, name: str) -> Optional[Trigger]:
        """Get a trigger by name."""
        with self._lock:
            return self._triggers.get(name)

    def list_triggers(self) -> list[Trigger]:
        """List all triggers."""
        with self._lock:
            return list(self._triggers.values())

    def get_stats(self) -> dict[str, int]:
        """Get trigger statistics."""
        with self._lock:
            return {
                **self._stats,
                "total_triggers": len(self._triggers),
                "enabled_triggers": sum(1 for t in self._triggers.values() if t.enabled),
            }
