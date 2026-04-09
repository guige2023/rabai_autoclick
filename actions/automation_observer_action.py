"""Automation observer action module for RabAI AutoClick.

Provides observer pattern for automation:
- AutomationObserverAction: Observer pattern for events
- AutomationObservableAction: Observable/Subject pattern
- AutomationEventBusAction: Event bus for automation
- AutomationEventFilterAction: Filter automation events
"""

from typing import Any, Dict, List, Optional, Callable
from datetime import datetime
from collections import defaultdict

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class AutomationObserverAction(BaseAction):
    """Observer pattern for automation events."""
    action_type = "automation_observer"
    display_name = "自动化观察者"
    description = "观察者模式事件处理"

    def __init__(self):
        super().__init__()
        self._observers: Dict[str, List[Callable]] = defaultdict(list)

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "notify")
            event_name = params.get("event_name")
            observer = params.get("observer")
            event_data = params.get("event_data")

            if operation == "subscribe":
                if not event_name or not callable(observer):
                    return ActionResult(success=False, message="event_name and callable observer required")
                self._observers[event_name].append(observer)
                return ActionResult(success=True, message=f"Subscribed to '{event_name}'", data={"observer_count": len(self._observers[event_name])})

            elif operation == "unsubscribe":
                if not event_name or not callable(observer):
                    return ActionResult(success=False, message="event_name and observer required")
                if observer in self._observers[event_name]:
                    self._observers[event_name].remove(observer)
                return ActionResult(success=True, message=f"Unsubscribed from '{event_name}'")

            elif operation == "notify":
                if not event_name:
                    return ActionResult(success=False, message="event_name required")
                if event_name not in self._observers:
                    return ActionResult(success=True, message=f"No observers for '{event_name}'", data={"notified": 0})

                notified = 0
                errors = []
                for obs in self._observers[event_name]:
                    try:
                        obs(event_data)
                        notified += 1
                    except Exception as e:
                        errors.append({"observer": str(obs), "error": str(e)})

                return ActionResult(
                    success=len(errors) == 0,
                    message=f"Notified {notified}/{len(self._observers[event_name])} observers",
                    data={"notified": notified, "errors": errors}
                )

            elif operation == "list":
                return ActionResult(success=True, message="Observer list", data={"events": {k: len(v) for k, v in self._observers.items()}})

            elif operation == "clear":
                count = sum(len(v) for v in self._observers.values())
                self._observers.clear()
                return ActionResult(success=True, message=f"Cleared {count} observers")

            return ActionResult(success=False, message=f"Unknown operation: {operation}")
        except Exception as e:
            return ActionResult(success=False, message=f"Observer error: {e}")


class AutomationObservableAction(BaseAction):
    """Observable/Subject for automation."""
    action_type = "automation_observable"
    display_name = "自动化可观察对象"
    description = "可观察对象模式"

    def __init__(self):
        super().__init__()
        self._state = {}
        self._observers: List[Callable] = []

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "set_state")
            observer = params.get("observer")
            key = params.get("key")
            value = params.get("value")

            if operation == "subscribe":
                if not callable(observer):
                    return ActionResult(success=False, message="callable observer required")
                self._observers.append(observer)
                return ActionResult(success=True, message="Observer subscribed", data={"observer_count": len(self._observers)})

            elif operation == "set_state":
                if not key:
                    return ActionResult(success=False, message="key required")
                old_value = self._state.get(key)
                self._state[key] = value
                self._notify_observers({"type": "state_change", "key": key, "old": old_value, "new": value})
                return ActionResult(success=True, message=f"State updated: {key}", data={"state": self._state})

            elif operation == "get_state":
                if key:
                    return ActionResult(success=True, message=f"State of '{key}'", data={"value": self._state.get(key)})
                return ActionResult(success=True, message="Full state", data={"state": self._state})

            elif operation == "clear":
                self._state.clear()
                self._notify_observers({"type": "state_cleared"})
                return ActionResult(success=True, message="State cleared")

            return ActionResult(success=False, message=f"Unknown operation: {operation}")
        except Exception as e:
            return ActionResult(success=False, message=f"Observable error: {e}")

    def _notify_observers(self, data: Dict[str, Any]) -> None:
        for obs in self._observers:
            try:
                obs(data)
            except Exception:
                pass


class AutomationEventBusAction(BaseAction):
    """Event bus for automation communication."""
    action_type = "automation_event_bus"
    display_name = "自动化事件总线"
    description = "自动化事件总线"

    def __init__(self):
        super().__init__()
        self._handlers: Dict[str, List[Callable]] = defaultdict(list)
        self._event_history: List[Dict[str, Any]] = []
        self._max_history = 100

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "publish")
            channel = params.get("channel")
            handler = params.get("handler")
            event = params.get("event", {})
            max_history = params.get("max_history", self._max_history)

            if operation == "subscribe":
                if not channel or not callable(handler):
                    return ActionResult(success=False, message="channel and callable handler required")
                self._handlers[channel].append(handler)
                return ActionResult(success=True, message=f"Subscribed to '{channel}'")

            elif operation == "publish":
                if not channel:
                    return ActionResult(success=False, message="channel required")

                event_record = {
                    "channel": channel,
                    "event": event,
                    "timestamp": datetime.now().isoformat(),
                }
                self._event_history.append(event_record)
                if len(self._event_history) > max_history:
                    self._event_history = self._event_history[-max_history:]

                handled = 0
                for h in self._handlers.get(channel, []):
                    try:
                        h(event)
                        handled += 1
                    except Exception:
                        pass

                return ActionResult(success=True, message=f"Published to '{channel}', {handled} handled", data={"handled": handled})

            elif operation == "history":
                if channel:
                    filtered = [e for e in self._event_history if e["channel"] == channel]
                    return ActionResult(success=True, message=f"{len(filtered)} events on '{channel}'", data={"events": filtered})
                return ActionResult(success=True, message=f"{len(self._event_history)} total events", data={"events": self._event_history})

            elif operation == "clear":
                if channel:
                    self._handlers[channel].clear()
                return ActionResult(success=True, message=f"Cleared handlers for '{channel}'")

            return ActionResult(success=False, message=f"Unknown operation: {operation}")
        except Exception as e:
            return ActionResult(success=False, message=f"Event bus error: {e}")


class AutomationEventFilterAction(BaseAction):
    """Filter automation events."""
    action_type = "automation_event_filter"
    display_name = "自动化事件过滤"
    description = "过滤自动化事件"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            events = params.get("events", [])
            event_type = params.get("event_type")
            source = params.get("source")
            time_range = params.get("time_range")
            filter_fn = params.get("filter_fn")

            if not events:
                return ActionResult(success=False, message="events list required")

            filtered = list(events)

            if event_type:
                filtered = [e for e in filtered if e.get("type") == event_type or e.get("event_type") == event_type]

            if source:
                filtered = [e for e in filtered if e.get("source") == source]

            if time_range:
                start = time_range.get("start")
                end = time_range.get("end")
                if start:
                    filtered = [e for e in filtered if e.get("timestamp", "") >= start]
                if end:
                    filtered = [e for e in filtered if e.get("timestamp", "") <= end]

            if filter_fn and callable(filter_fn):
                filtered = [e for e in filtered if filter_fn(e)]

            return ActionResult(
                success=True,
                message=f"Filtered {len(events)} → {len(filtered)} events",
                data={"filtered": filtered, "original_count": len(events), "filtered_count": len(filtered)}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Event filter error: {e}")
