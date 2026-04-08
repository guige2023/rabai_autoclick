"""Automation trigger action module for RabAI AutoClick.

Provides trigger and event-driven automation:
- EventTriggerAction: Event-based trigger handling
- CronTriggerAction: Cron-based scheduling
- WebhookTriggerAction: Webhook-triggered automation
- ConditionalTriggerAction: Condition-based trigger
"""

import re
import time
from datetime import datetime, timedelta
from typing import Any, Callable, Dict, List, Optional, Set
from threading import Lock

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class EventTriggerState:
    """Shared state for event triggers."""

    def __init__(self):
        self._lock = Lock()
        self._listeners: Dict[str, List[Callable]] = {}
        self._event_history: List[Dict] = []

    def add_listener(self, event_type: str, callback: Callable):
        with self._lock:
            if event_type not in self._listeners:
                self._listeners[event_type] = []
            self._listeners[event_type].append(callback)

    def trigger_event(self, event_type: str, payload: Any) -> List[Any]:
        results = []
        with self._lock:
            self._event_history.append({"type": event_type, "payload": payload, "timestamp": datetime.now().isoformat()})
            callbacks = self._listeners.get(event_type, []) + self._listeners.get("*", [])

        for cb in callbacks:
            try:
                results.append(cb(payload))
            except Exception:
                pass
        return results

    def get_history(self, limit: int = 100) -> List[Dict]:
        with self._lock:
            return self._event_history[-limit:]


_event_state = EventTriggerState()


class EventTriggerAction(BaseAction):
    """Event-based trigger handling."""
    action_type = "event_trigger"
    display_name = "事件触发器"
    description = "基于事件的触发处理"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            action = params.get("action", "emit")
            event_type = params.get("event_type", "")
            payload = params.get("payload", {})
            filter_event_type = params.get("filter_event_type", None)

            if not event_type and action == "emit":
                return ActionResult(success=False, message="event_type is required")

            if action == "emit":
                results = _event_state.trigger_event(event_type, payload)
                return ActionResult(
                    success=True,
                    message=f"Event '{event_type}' emitted, {len(results)} handlers triggered",
                    data={"event_type": event_type, "handlers_triggered": len(results)},
                )

            elif action == "subscribe":
                if not filter_event_type:
                    return ActionResult(success=False, message="filter_event_type is required")

                def callback(payload: Any) -> Dict:
                    return {"processed": True, "payload": payload}

                _event_state.add_listener(filter_event_type, callback)
                return ActionResult(
                    success=True,
                    message=f"Subscribed to event '{filter_event_type}'",
                    data={"event_type": filter_event_type},
                )

            elif action == "history":
                limit = params.get("limit", 100)
                history = _event_state.get_history(limit)
                return ActionResult(
                    success=True,
                    message=f"{len(history)} events in history",
                    data={"history": history, "count": len(history)},
                )

            elif action == "clear":
                count = len(_event_state._event_history)
                _event_state._event_history = []
                return ActionResult(success=True, message=f"Cleared {count} events")

            return ActionResult(success=False, message=f"Unknown action: {action}")
        except Exception as e:
            return ActionResult(success=False, message=f"EventTrigger error: {e}")


class CronTriggerAction(BaseAction):
    """Cron-based scheduling."""
    action_type = "cron_trigger"
    display_name = "Cron触发器"
    description = "基于Cron表达式的定时触发"

    def __init__(self):
        super().__init__()
        self._schedules: Dict[str, Dict] = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            action = params.get("action", "schedule")
            schedule_id = params.get("schedule_id", "")
            cron_expr = params.get("cron", "")
            max_fires = params.get("max_fires", None)

            if action == "schedule":
                if not schedule_id or not cron_expr:
                    return ActionResult(success=False, message="schedule_id and cron are required")

                parts = cron_expr.split()
                if len(parts) not in (5, 6):
                    return ActionResult(success=False, message="Invalid cron expression")

                self._schedules[schedule_id] = {
                    "cron": cron_expr,
                    "parts": parts,
                    "max_fires": max_fires,
                    "fire_count": 0,
                    "last_fire": None,
                    "next_fire": self._compute_next_fire(parts),
                }

                return ActionResult(
                    success=True,
                    message=f"Cron schedule '{schedule_id}' set: {cron_expr}",
                    data={"schedule_id": schedule_id, "cron": cron_expr, "next_fire": self._schedules[schedule_id]["next_fire"]},
                )

            elif action == "check":
                now = datetime.now()
                due = []
                for sid, sched in self._schedules.items():
                    next_fire = sched.get("next_fire")
                    if next_fire and now >= datetime.fromisoformat(next_fire):
                        if sched.get("max_fires") and sched["fire_count"] >= sched["max_fires"]:
                            continue
                        due.append(sid)
                return ActionResult(
                    success=True,
                    message=f"{len(due)} schedules due",
                    data={"due_schedules": due, "count": len(due)},
                )

            elif action == "fire":
                if schedule_id not in self._schedules:
                    return ActionResult(success=False, message=f"Schedule '{schedule_id}' not found")

                sched = self._schedules[schedule_id]
                sched["fire_count"] += 1
                sched["last_fire"] = datetime.now().isoformat()
                sched["next_fire"] = self._compute_next_fire(sched["parts"])

                return ActionResult(
                    success=True,
                    message=f"Schedule '{schedule_id}' fired (#{sched['fire_count']})",
                    data={"schedule_id": schedule_id, "fire_count": sched["fire_count"], "next_fire": sched["next_fire"]},
                )

            elif action == "list":
                schedules = [
                    {
                        "id": sid,
                        "cron": s["cron"],
                        "fire_count": s["fire_count"],
                        "next_fire": s["next_fire"],
                        "last_fire": s["last_fire"],
                    }
                    for sid, s in self._schedules.items()
                ]
                return ActionResult(success=True, message=f"{len(schedules)} schedules", data={"schedules": schedules})

            elif action == "cancel":
                if schedule_id in self._schedules:
                    del self._schedules[schedule_id]
                return ActionResult(success=True, message=f"Schedule '{schedule_id}' cancelled")

            return ActionResult(success=False, message=f"Unknown action: {action}")
        except Exception as e:
            return ActionResult(success=False, message=f"CronTrigger error: {e}")

    def _compute_next_fire(self, parts: List[str]) -> str:
        now = datetime.now()
        minute_str = parts[0]
        hour_str = parts[1] if len(parts) >= 5 else None
        day_str = parts[2] if len(parts) >= 5 else None
        month_str = parts[3] if len(parts) >= 5 else None
        dow_str = parts[4] if len(parts) >= 5 else (parts[2] if len(parts) == 3 else None)

        next_fire = now + timedelta(minutes=1)
        next_fire = next_fire.replace(second=0, microsecond=0)

        return next_fire.isoformat()


class WebhookTriggerAction(BaseAction):
    """Webhook-triggered automation."""
    action_type = "webhook_trigger"
    display_name = "Webhook触发器"
    description = "Webhook触发的自动化"

    def __init__(self):
        super().__init__()
        self._endpoints: Dict[str, Dict] = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            action = params.get("action", "register")
            endpoint = params.get("endpoint", "")
            secret = params.get("secret", None)
            workflow_id = params.get("workflow_id", None)

            if action == "register":
                if not endpoint:
                    return ActionResult(success=False, message="endpoint is required")

                import hashlib
                import secrets
                webhook_id = hashlib.sha256((endpoint + str(time.time())).encode()).hexdigest()[:16]

                self._endpoints[webhook_id] = {
                    "endpoint": endpoint,
                    "secret": secret,
                    "workflow_id": workflow_id,
                    "created_at": time.time(),
                    "trigger_count": 0,
                }

                full_url = f"https://auto.rabai.com/webhook/{webhook_id}"
                return ActionResult(
                    success=True,
                    message=f"Webhook registered: {full_url}",
                    data={"webhook_id": webhook_id, "url": full_url, "endpoint": endpoint},
                )

            elif action == "receive":
                webhook_id = params.get("webhook_id", "")
                payload = params.get("payload", {})
                signature = params.get("signature", None)

                endpoint_info = self._endpoints.get(webhook_id)
                if not endpoint_info:
                    return ActionResult(success=False, message=f"Webhook '{webhook_id}' not found")

                if endpoint_info.get("secret") and signature:
                    import hmac
                    import hashlib
                    expected = hmac.new(endpoint_info["secret"].encode(), str(payload).encode(), hashlib.sha256).hexdigest()
                    if signature != expected:
                        return ActionResult(success=False, message="Invalid signature")

                endpoint_info["trigger_count"] += 1

                return ActionResult(
                    success=True,
                    message=f"Webhook '{webhook_id}' triggered",
                    data={"webhook_id": webhook_id, "workflow_id": endpoint_info["workflow_id"], "trigger_count": endpoint_info["trigger_count"]},
                )

            elif action == "list":
                return ActionResult(
                    success=True,
                    message=f"{len(self._endpoints)} webhooks",
                    data={"webhooks": [{"id": k, "endpoint": v["endpoint"], "trigger_count": v["trigger_count"]} for k, v in self._endpoints.items()]},
                )

            elif action == "delete":
                if endpoint in self._endpoints:
                    del self._endpoints[endpoint]
                return ActionResult(success=True, message=f"Webhook '{endpoint}' deleted")

            return ActionResult(success=False, message=f"Unknown action: {action}")
        except Exception as e:
            return ActionResult(success=False, message=f"WebhookTrigger error: {e}")


class ConditionalTriggerAction(BaseAction):
    """Condition-based trigger."""
    action_type = "conditional_trigger"
    display_name = "条件触发器"
    description = "基于条件的触发器"

    def __init__(self):
        super().__init__()
        self._conditions: Dict[str, Dict] = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            action = params.get("action", "add")
            condition_id = params.get("condition_id", "")
            condition = params.get("condition", {})
            threshold = params.get("threshold", 1)
            cooldown = params.get("cooldown", 60)

            if action == "add":
                if not condition_id:
                    return ActionResult(success=False, message="condition_id is required")

                self._conditions[condition_id] = {
                    "condition": condition,
                    "threshold": threshold,
                    "cooldown": cooldown,
                    "trigger_count": 0,
                    "last_triggered": None,
                }

                return ActionResult(
                    success=True,
                    message=f"Condition '{condition_id}' added",
                    data={"condition_id": condition_id, "threshold": threshold},
                )

            elif action == "evaluate":
                data = params.get("data", {})

                triggered = []
                for cid, cond_info in self._conditions.items():
                    last_triggered = cond_info.get("last_triggered")
                    if last_triggered and cond_info["cooldown"] > 0:
                        elapsed = time.time() - last_triggered
                        if elapsed < cond_info["cooldown"]:
                            continue

                    cond = cond_info["condition"]
                    field = cond.get("field")
                    operator = cond.get("operator", "eq")
                    value = cond.get("value")

                    ctx_value = data.get(field) if field else data

                    matched = False
                    if operator == "eq":
                        matched = ctx_value == value
                    elif operator == "ne":
                        matched = ctx_value != value
                    elif operator == "gt":
                        matched = ctx_value is not None and ctx_value > value
                    elif operator == "lt":
                        matched = ctx_value is not None and ctx_value < value
                    elif operator == "ge":
                        matched = ctx_value is not None and ctx_value >= value
                    elif operator == "le":
                        matched = ctx_value is not None and ctx_value <= value
                    elif operator == "contains":
                        matched = value in ctx_value if ctx_value else False
                    elif operator == "exists":
                        matched = ctx_value is not None

                    if matched:
                        cond_info["trigger_count"] += 1
                        cond_info["last_triggered"] = time.time()
                        triggered.append(cid)

                return ActionResult(
                    success=True,
                    message=f"{len(triggered)} conditions triggered",
                    data={"triggered": triggered, "count": len(triggered)},
                )

            elif action == "list":
                return ActionResult(
                    success=True,
                    message=f"{len(self._conditions)} conditions",
                    data={"conditions": [{"id": k, "trigger_count": v["trigger_count"]} for k, v in self._conditions.items()]},
                )

            return ActionResult(success=False, message=f"Unknown action: {action}")
        except Exception as e:
            return ActionResult(success=False, message=f"ConditionalTrigger error: {e}")
