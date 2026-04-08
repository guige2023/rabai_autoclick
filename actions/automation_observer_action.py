"""Automation observer action module for RabAI AutoClick.

Provides observer pattern for automation:
- EventObserverAction: Observe and react to events
- NotificationObserverAction: Send notifications on events
- MetricsObserverAction: Track and observe metrics
- StateObserverAction: Observe state changes
"""

import time
from typing import Any, Dict, List, Optional, Callable, Union
from datetime import datetime

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class EventObserverAction(BaseAction):
    """Observe and react to events."""
    action_type = "automation_event_observer"
    display_name = "事件观察者"
    description = "观察并响应事件"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            action = params.get("action", "observe")
            event_type = params.get("event_type", "")
            event_data = params.get("event_data", {})
            handlers = params.get("handlers", [])

            if not hasattr(context, "_event_observers"):
                context._event_observers = {}
                context._event_history = []

            if action == "subscribe":
                if event_type not in context._event_observers:
                    context._event_observers[event_type] = []
                context._event_observers[event_type].extend(handlers)

                return ActionResult(
                    success=True,
                    data={
                        "subscribed": True,
                        "event_type": event_type,
                        "handlers_count": len(context._event_observers[event_type])
                    },
                    message=f"Subscribed {len(handlers)} handlers to '{event_type}'"
                )

            elif action == "observe":
                if not event_type:
                    return ActionResult(success=False, message="event_type is required")

                event = {
                    "type": event_type,
                    "data": event_data,
                    "timestamp": datetime.now().isoformat(),
                    "handlers_triggered": 0
                }

                handlers_to_fire = context._event_observers.get(event_type, [])
                results = []

                for handler in handlers_to_fire:
                    handler_result = handler.get("result", True)
                    results.append(handler_result)
                    event["handlers_triggered"] += 1

                context._event_history.append(event)
                if len(context._event_history) > 100:
                    context._event_history = context._event_history[-100:]

                return ActionResult(
                    success=True,
                    data={
                        "event": event,
                        "handlers_fired": len(results),
                        "results": results
                    },
                    message=f"Event '{event_type}' observed, {len(results)} handlers triggered"
                )

            elif action == "unsubscribe":
                if event_type in context._event_observers:
                    count = len(context._event_observers[event_type])
                    context._event_observers[event_type] = []
                    return ActionResult(
                        success=True,
                        data={"unsubscribed": True, "handlers_removed": count},
                        message=f"Unsubscribed {count} handlers from '{event_type}'"
                    )
                return ActionResult(success=True, data={"unsubscribed": True, "handlers_removed": 0}, message=f"No handlers for '{event_type}'")

            elif action == "history":
                limit = params.get("limit", 10)
                history = context._event_history[-limit:]
                return ActionResult(
                    success=True,
                    data={
                        "history": history,
                        "total_events": len(context._event_history)
                    },
                    message=f"Retrieved {len(history)} events from history"
                )

            return ActionResult(success=False, message=f"Unknown action: {action}")

        except Exception as e:
            return ActionResult(success=False, message=f"Event observer error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ["action"]

    def get_optional_params(self) -> Dict[str, Any]:
        return {"event_type": "", "event_data": {}, "handlers": [], "limit": 10}


class NotificationObserverAction(BaseAction):
    """Send notifications on events."""
    action_type = "automation_notification_observer"
    display_name = "通知观察者"
    description = "在事件发生时发送通知"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            action = params.get("action", "notify")
            channel = params.get("channel", "log")
            message = params.get("message", "")
            recipients = params.get("recipients", [])
            priority = params.get("priority", "normal")
            metadata = params.get("metadata", {})

            if action == "notify":
                if not message:
                    return ActionResult(success=False, message="message is required")

                notification = {
                    "message": message,
                    "channel": channel,
                    "priority": priority,
                    "recipients": recipients,
                    "metadata": metadata,
                    "sent_at": datetime.now().isoformat(),
                    "notification_id": f"notif_{int(time.time())}"
                }

                channel_results = {}
                if channel in ("log", "all"):
                    channel_results["log"] = True
                if channel in ("email", "all") and recipients:
                    channel_results["email"] = {"sent_to": len(recipients)}
                if channel in ("webhook", "all"):
                    channel_results["webhook"] = True

                return ActionResult(
                    success=True,
                    data={
                        "notification": notification,
                        "channels_used": list(channel_results.keys()),
                        "channel_results": channel_results
                    },
                    message=f"Notification sent via {channel}: {message[:50]}..."
                )

            elif action == "configure":
                config = {
                    "default_channel": channel,
                    "default_recipients": recipients,
                    "priority_levels": ["low", "normal", "high", "urgent"]
                }
                return ActionResult(
                    success=True,
                    data={"config": config},
                    message=f"Notification configured: channel={channel}"
                )

            elif action == "template":
                template = params.get("template", {})
                variables = params.get("variables", {})

                rendered = template.get("message", "")
                for var, value in variables.items():
                    rendered = rendered.replace(f"{{{var}}}", str(value))

                return ActionResult(
                    success=True,
                    data={
                        "rendered": rendered,
                        "template": template,
                        "variables_used": list(variables.keys())
                    },
                    message=f"Notification template rendered"
                )

            return ActionResult(success=False, message=f"Unknown action: {action}")

        except Exception as e:
            return ActionResult(success=False, message=f"Notification observer error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ["action"]

    def get_optional_params(self) -> Dict[str, Any]:
        return {"channel": "log", "message": "", "recipients": [], "priority": "normal", "metadata": {}, "template": {}, "variables": {}}


class MetricsObserverAction(BaseAction):
    """Track and observe metrics."""
    action_type = "automation_metrics_observer"
    display_name = "指标观察者"
    description = "跟踪和观察指标"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            action = params.get("action", "record")
            metric_name = params.get("metric_name", "")
            metric_value = params.get("metric_value")
            metric_type = params.get("metric_type", "gauge")
            tags = params.get("tags", {})

            if not hasattr(context, "_metrics"):
                context._metrics = {}
                context._metrics_history = []

            if action == "record":
                if not metric_name:
                    return ActionResult(success=False, message="metric_name is required")

                if metric_name not in context._metrics:
                    context._metrics[metric_name] = {"values": [], "count": 0, "sum": 0}

                metric = context._metrics[metric_name]
                metric["values"].append(metric_value)
                metric["count"] += 1
                metric["sum"] += float(metric_value) if metric_value is not None else 0
                metric["last_value"] = metric_value
                metric["last_updated"] = datetime.now().isoformat()
                metric["tags"] = tags

                if len(metric["values"]) > 1000:
                    metric["values"] = metric["values"][-1000:]

                return ActionResult(
                    success=True,
                    data={
                        "metric_name": metric_name,
                        "value_recorded": metric_value,
                        "metric_type": metric_type,
                        "total_count": metric["count"]
                    },
                    message=f"Recorded metric '{metric_name}': {metric_value}"
                )

            elif action == "get":
                if not metric_name:
                    all_metrics = {k: {"count": v["count"], "last_value": v.get("last_value")} for k, v in context._metrics.items()}
                    return ActionResult(
                        success=True,
                        data={"metrics": all_metrics, "count": len(all_metrics)},
                        message=f"Retrieved {len(all_metrics)} metrics"
                    )

                if metric_name not in context._metrics:
                    return ActionResult(success=False, message=f"Metric '{metric_name}' not found")

                metric = context._metrics[metric_name]
                values = metric["values"]
                avg = metric["sum"] / len(values) if values else 0

                return ActionResult(
                    success=True,
                    data={
                        "metric_name": metric_name,
                        "count": metric["count"],
                        "last_value": metric.get("last_value"),
                        "avg": avg,
                        "min": min(values) if values else None,
                        "max": max(values) if values else None,
                        "tags": metric.get("tags", {})
                    },
                    message=f"Metric '{metric_name}': count={metric['count']}, avg={avg:.2f}"
                )

            elif action == "reset":
                if metric_name:
                    if metric_name in context._metrics:
                        del context._metrics[metric_name]
                    return ActionResult(success=True, data={"reset": metric_name}, message=f"Reset metric '{metric_name}'")
                context._metrics = {}
                return ActionResult(success=True, data={"reset": "all"}, message="Reset all metrics")

            return ActionResult(success=False, message=f"Unknown action: {action}")

        except Exception as e:
            return ActionResult(success=False, message=f"Metrics observer error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ["action"]

    def get_optional_params(self) -> Dict[str, Any]:
        return {"metric_name": "", "metric_value": None, "metric_type": "gauge", "tags": {}}


class StateObserverAction(BaseAction):
    """Observe state changes."""
    action_type = "automation_state_observer"
    display_name = "状态观察者"
    description = "观察状态变化"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            action = params.get("action", "watch")
            state_key = params.get("state_key", "default")
            old_state = params.get("old_state", {})
            new_state = params.get("new_state", {})
            watchers = params.get("watchers", [])

            if not hasattr(context, "_state_observers"):
                context._state_observers = {}
                context._state_changes = []

            if action == "watch":
                if state_key not in context._state_observers:
                    context._state_observers[state_key] = []
                context._state_observers[state_key].extend(watchers)

                return ActionResult(
                    success=True,
                    data={
                        "watching": True,
                        "state_key": state_key,
                        "watchers_count": len(context._state_observers[state_key])
                    },
                    message=f"Watching state '{state_key}': {len(watchers)} watchers"
                )

            elif action == "notify_change":
                if not hasattr(context, "_last_states"):
                    context._last_states = {}

                last_state = context._last_states.get(state_key, {})

                change_detected = False
                changes = {}

                for key in set(list(old_state.keys()) + list(new_state.keys())):
                    old_val = old_state.get(key)
                    new_val = new_state.get(key)
                    if old_val != new_val:
                        change_detected = True
                        changes[key] = {"from": old_val, "to": new_val}

                context._last_states[state_key] = new_state

                change_record = {
                    "state_key": state_key,
                    "changes": changes,
                    "change_detected": change_detected,
                    "timestamp": datetime.now().isoformat()
                }

                context._state_changes.append(change_record)
                if len(context._state_changes) > 100:
                    context._state_changes = context._state_changes[-100:]

                return ActionResult(
                    success=True,
                    data={
                        "change_record": change_record,
                        "change_detected": change_detected,
                        "fields_changed": len(changes)
                    },
                    message=f"State change for '{state_key}': {'yes' if change_detected else 'no'}, {len(changes)} fields"
                )

            elif action == "get_changes":
                limit = params.get("limit", 10)
                changes = context._state_changes[-limit:]

                return ActionResult(
                    success=True,
                    data={
                        "changes": changes,
                        "total_changes": len(context._state_changes)
                    },
                    message=f"Retrieved {len(changes)} state changes"
                )

            elif action == "unwatch":
                if state_key in context._state_observers:
                    count = len(context._state_observers[state_key])
                    del context._state_observers[state_key]
                    return ActionResult(success=True, data={"unwatched": state_key, "watchers_removed": count}, message=f"Unwatched '{state_key}'")

                return ActionResult(success=True, data={"unwatched": state_key, "watchers_removed": 0}, message=f"No watchers for '{state_key}'")

            return ActionResult(success=False, message=f"Unknown action: {action}")

        except Exception as e:
            return ActionResult(success=False, message=f"State observer error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ["action"]

    def get_optional_params(self) -> Dict[str, Any]:
        return {"state_key": "default", "old_state": {}, "new_state": {}, "watchers": [], "limit": 10}
