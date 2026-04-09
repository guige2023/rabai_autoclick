"""Scheduled trigger action module for RabAI AutoClick.

Provides scheduled trigger operations:
- CronTriggerAction: Cron-based scheduled triggers
- IntervalTriggerAction: Interval-based scheduled triggers
- CalendarTriggerAction: Calendar-based scheduled triggers
- OneTimeTriggerAction: One-time scheduled triggers
- TriggerCoordinatorAction: Coordinate multiple trigger sources
"""

from __future__ import annotations

import json
import re
import time
import uuid
from croniter import croniter
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Set

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class ScheduledTrigger:
    """Represents a scheduled trigger."""

    def __init__(
        self,
        trigger_id: str,
        name: str,
        trigger_type: str,
        schedule: Dict[str, Any],
        target_action: str,
        enabled: bool = True,
    ) -> None:
        self.id = trigger_id
        self.name = name
        self.trigger_type = trigger_type
        self.schedule = schedule
        self.target_action = target_action
        self.enabled = enabled
        self.last_fired: Optional[datetime] = None
        self.next_fire: Optional[datetime] = None
        self.fire_count = 0

    def should_fire(self, now: datetime) -> bool:
        """Check if trigger should fire at given time."""
        if not self.enabled or self.next_fire is None:
            return False
        return now >= self.next_fire

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "name": self.name,
            "trigger_type": self.trigger_type,
            "schedule": self.schedule,
            "target_action": self.target_action,
            "enabled": self.enabled,
            "last_fired": self.last_fired.isoformat() if self.last_fired else None,
            "next_fire": self.next_fire.isoformat() if self.next_fire else None,
            "fire_count": self.fire_count,
        }


class CronTriggerAction(BaseAction):
    """Cron-based scheduled triggers."""
    action_type = "cron_trigger"
    display_name = "Cron触发器"
    description = "基于Cron表达式的定时触发"

    def __init__(self) -> None:
        super().__init__()
        self._triggers: Dict[str, ScheduledTrigger] = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            action = params.get("action", "")
            if action == "create":
                return self._create_cron_trigger(params)
            elif action == "list":
                return self._list_triggers()
            elif action == "check":
                return self._check_triggers(params)
            elif action == "toggle":
                return self._toggle_trigger(params)
            else:
                return ActionResult(success=False, message=f"Unknown action: {action}")
        except Exception as e:
            return ActionResult(success=False, message=f"Cron trigger failed: {e}")

    def _create_cron_trigger(self, params: Dict[str, Any]) -> ActionResult:
        name = params.get("name", "")
        cron_expr = params.get("cron", "")
        target = params.get("target_action", "")
        timezone_str = params.get("timezone", "UTC")
        if not name or not cron_expr or not target:
            return ActionResult(success=False, message="name, cron, and target_action are required")

        try:
            tz = timezone(timedelta(hours=float(timezone_str.replace("UTC", "").replace("+", "") or 0)))
            base_time = datetime.now(tz)
            cron = croniter(cron_expr, base_time)
            next_fire = cron.get_next(datetime)
        except Exception as e:
            return ActionResult(success=False, message=f"Invalid cron expression: {e}")

        trigger_id = str(uuid.uuid4())
        trigger = ScheduledTrigger(
            trigger_id=trigger_id,
            name=name,
            trigger_type="cron",
            schedule={"cron": cron_expr, "timezone": timezone_str},
            target_action=target,
        )
        trigger.next_fire = next_fire
        self._triggers[trigger_id] = trigger
        return ActionResult(
            success=True,
            message=f"Cron trigger '{name}' created, next fire: {next_fire.isoformat()}",
            data=trigger.to_dict(),
        )

    def _list_triggers(self) -> ActionResult:
        triggers = [t.to_dict() for t in self._triggers.values()]
        return ActionResult(success=True, message=f"{len(triggers)} cron triggers", data={"triggers": triggers})

    def _check_triggers(self, params: Dict[str, Any]) -> ActionResult:
        now = datetime.now(timezone.utc)
        fired = []
        for trigger in self._triggers.values():
            if trigger.should_fire(now):
                trigger.last_fired = now
                trigger.fire_count += 1
                try:
                    cron = croniter(trigger.schedule["cron"], now)
                    trigger.next_fire = cron.get_next(datetime)
                except Exception:
                    pass
                fired.append(trigger.to_dict())
        return ActionResult(success=True, message=f"{len(fired)} triggers fired", data={"fired": fired})

    def _toggle_trigger(self, params: Dict[str, Any]) -> ActionResult:
        trigger_id = params.get("trigger_id", "")
        enabled = params.get("enabled", True)
        if trigger_id not in self._triggers:
            return ActionResult(success=False, message="Trigger not found")
        self._triggers[trigger_id].enabled = enabled
        return ActionResult(success=True, message=f"Trigger {'enabled' if enabled else 'disabled'}")


class IntervalTriggerAction(BaseAction):
    """Interval-based scheduled triggers."""
    action_type = "interval_trigger"
    display_name = "间隔触发器"
    description = "基于固定间隔的定时触发"

    def __init__(self) -> None:
        super().__init__()
        self._triggers: Dict[str, ScheduledTrigger] = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            action = params.get("action", "")
            if action == "create":
                return self._create_interval_trigger(params)
            elif action == "list":
                return self._list_triggers()
            else:
                return ActionResult(success=False, message=f"Unknown action: {action}")
        except Exception as e:
            return ActionResult(success=False, message=f"Interval trigger failed: {e}")

    def _create_interval_trigger(self, params: Dict[str, Any]) -> ActionResult:
        name = params.get("name", "")
        interval_seconds = params.get("interval_seconds", 60)
        target = params.get("target_action", "")
        if not name or interval_seconds <= 0:
            return ActionResult(success=False, message="name and positive interval_seconds are required")

        trigger_id = str(uuid.uuid4())
        trigger = ScheduledTrigger(
            trigger_id=trigger_id,
            name=name,
            trigger_type="interval",
            schedule={"interval_seconds": interval_seconds},
            target_action=target,
        )
        trigger.next_fire = datetime.now(timezone.utc) + timedelta(seconds=interval_seconds)
        self._triggers[trigger_id] = trigger
        return ActionResult(success=True, message=f"Interval trigger '{name}' created", data=trigger.to_dict())

    def _list_triggers(self) -> ActionResult:
        triggers = [t.to_dict() for t in self._triggers.values()]
        return ActionResult(success=True, message=f"{len(triggers)} interval triggers", data={"triggers": triggers})


class CalendarTriggerAction(BaseAction):
    """Calendar-based scheduled triggers."""
    action_type = "calendar_trigger"
    display_name = "日历触发器"
    description = "基于日历日期的定时触发"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            name = params.get("name", "")
            dates = params.get("dates", [])
            target = params.get("target_action", "")
            if not name or not dates:
                return ActionResult(success=False, message="name and dates are required")

            trigger = {
                "id": str(uuid.uuid4()),
                "name": name,
                "type": "calendar",
                "dates": dates,
                "target_action": target,
                "created_at": datetime.now(timezone.utc).isoformat(),
            }
            return ActionResult(success=True, message=f"Calendar trigger '{name}' created with {len(dates)} dates", data=trigger)
        except Exception as e:
            return ActionResult(success=False, message=f"Calendar trigger failed: {e}")


class OneTimeTriggerAction(BaseAction):
    """One-time scheduled triggers."""
    action_type = "one_time_trigger"
    display_name = "一次性触发器"
    description = "单次定时触发"

    def __init__(self) -> None:
        super().__init__()
        self._triggers: Dict[str, Dict[str, Any]] = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            action = params.get("action", "")
            if action == "create":
                return self._create_one_time_trigger(params)
            elif action == "cancel":
                return self._cancel_trigger(params)
            elif action == "list":
                return self._list_triggers()
            else:
                return ActionResult(success=False, message=f"Unknown action: {action}")
        except Exception as e:
            return ActionResult(success=False, message=f"One-time trigger failed: {e}")

    def _create_one_time_trigger(self, params: Dict[str, Any]) -> ActionResult:
        name = params.get("name", "")
        run_at = params.get("run_at", "")
        target = params.get("target_action", "")
        payload = params.get("payload", {})
        if not name or not run_at:
            return ActionResult(success=False, message="name and run_at are required")

        trigger_id = str(uuid.uuid4())
        self._triggers[trigger_id] = {
            "id": trigger_id,
            "name": name,
            "run_at": run_at,
            "target_action": target,
            "payload": payload,
            "fired": False,
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
        return ActionResult(success=True, message=f"One-time trigger '{name}' scheduled for {run_at}", data=self._triggers[trigger_id])

    def _cancel_trigger(self, params: Dict[str, Any]) -> ActionResult:
        trigger_id = params.get("trigger_id", "")
        if trigger_id in self._triggers:
            del self._triggers[trigger_id]
            return ActionResult(success=True, message="Trigger cancelled")
        return ActionResult(success=False, message="Trigger not found")

    def _list_triggers(self) -> ActionResult:
        return ActionResult(success=True, message=f"{len(self._triggers)} one-time triggers", data={"triggers": self._triggers})


class TriggerCoordinatorAction(BaseAction):
    """Coordinate multiple trigger sources."""
    action_type = "trigger_coordinator"
    display_name = "触发器协调器"
    description = "协调多个触发源"

    def __init__(self) -> None:
        super().__init__()
        self._sources: Dict[str, Dict[str, Any]] = {}
        self._active_triggers: List[Dict[str, Any]] = []

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            action = params.get("action", "")
            if action == "register":
                return self._register_source(params)
            elif action == "fire":
                return self._fire_trigger(params)
            elif action == "status":
                return self._get_status()
            else:
                return ActionResult(success=False, message=f"Unknown action: {action}")
        except Exception as e:
            return ActionResult(success=False, message=f"Trigger coordinator failed: {e}")

    def _register_source(self, params: Dict[str, Any]) -> ActionResult:
        source_name = params.get("source_name", "")
        source_type = params.get("source_type", "")
        config = params.get("config", {})
        if not source_name:
            return ActionResult(success=False, message="source_name is required")
        self._sources[source_name] = {
            "name": source_name,
            "type": source_type,
            "config": config,
            "registered_at": datetime.now(timezone.utc).isoformat(),
        }
        return ActionResult(success=True, message=f"Source '{source_name}' registered")

    def _fire_trigger(self, params: Dict[str, Any]) -> ActionResult:
        trigger_name = params.get("trigger_name", "")
        payload = params.get("payload", {})
        if not trigger_name:
            return ActionResult(success=False, message="trigger_name is required")
        self._active_triggers.append({
            "name": trigger_name,
            "payload": payload,
            "fired_at": datetime.now(timezone.utc).isoformat(),
        })
        return ActionResult(success=True, message=f"Trigger '{trigger_name}' fired", data={"active_count": len(self._active_triggers)})

    def _get_status(self) -> ActionResult:
        return ActionResult(
            success=True,
            message="Trigger coordinator status",
            data={"sources": len(self._sources), "active_triggers": len(self._active_triggers)},
        )
