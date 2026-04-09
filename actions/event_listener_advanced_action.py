"""Event listener advanced action module for RabAI AutoClick.

Provides advanced event listening:
- AdvancedEventListenerAction: Multi-type event listener with filtering
- EventAggregatorAction: Aggregate multiple events into one
- EventSplitterAction: Split one event into multiple
- EventTransformerAction: Transform event payload
- EventCorrelationAction: Correlate related events across streams
"""

from __future__ import annotations

import json
import re
import time
import uuid
from datetime import datetime, timezone
from enum import Enum, auto
from typing import Any, Callable, Dict, List, Optional, Pattern, Set

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class EventPriority(Enum):
    """Event priority levels."""
    LOW = 1
    NORMAL = 5
    HIGH = 10
    CRITICAL = 100


class EventSubscription:
    """Represents an event subscription."""

    def __init__(
        self,
        sub_id: str,
        event_type: str,
        callback: Callable,
        filter_fn: Optional[Callable[[Dict[str, Any]], bool]] = None,
        priority: EventPriority = EventPriority.NORMAL,
    ) -> None:
        self.id = sub_id
        self.event_type = event_type
        self.callback = callback
        self.filter_fn = filter_fn
        self.priority = priority
        self.active = True
        self.received_count = 0
        self.last_event: Optional[Dict[str, Any]] = None


class AdvancedEventListenerAction(BaseAction):
    """Multi-type event listener with filtering and priority."""
    action_type = "advanced_event_listener"
    display_name = "高级事件监听器"
    description = "多类型事件监听器，支持过滤和优先级"

    def __init__(self) -> None:
        super().__init__()
        self._subscriptions: Dict[str, EventSubscription] = {}
        self._event_history: List[Dict[str, Any]] = []
        self._max_history = 1000

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            action = params.get("action", "")
            if action == "subscribe":
                return self._subscribe(params)
            elif action == "unsubscribe":
                return self._unsubscribe(params)
            elif action == "emit":
                return self._emit_event(params)
            elif action == "list":
                return self._list_subscriptions()
            elif action == "history":
                return self._get_history(params)
            else:
                return ActionResult(success=False, message=f"Unknown action: {action}")
        except Exception as e:
            return ActionResult(success=False, message=f"Event listener failed: {e}")

    def _subscribe(self, params: Dict[str, Any]) -> ActionResult:
        event_type = params.get("event_type", "")
        filter_pattern = params.get("filter_pattern", "")
        priority_str = params.get("priority", "NORMAL")
        if not event_type:
            return ActionResult(success=False, message="event_type is required")

        try:
            priority = EventPriority[priority_str.upper()]
        except KeyError:
            priority = EventPriority.NORMAL

        filter_fn: Optional[Callable[[Dict[str, Any]], bool]] = None
        if filter_pattern:
            pattern = re.compile(filter_pattern)
            filter_fn = lambda e, p=pattern: bool(p.search(json.dumps(e)))

        sub_id = str(uuid.uuid4())
        self._subscriptions[sub_id] = EventSubscription(
            sub_id=sub_id,
            event_type=event_type,
            callback=lambda e, sid=sub_id: self._on_event(sid, e),
            filter_fn=filter_fn,
            priority=priority,
        )
        return ActionResult(
            success=True,
            message=f"Subscribed to '{event_type}' with priority {priority.name}",
            data={"subscription_id": sub_id},
        )

    def _on_event(self, sub_id: str, event: Dict[str, Any]) -> None:
        sub = self._subscriptions.get(sub_id)
        if sub:
            sub.received_count += 1
            sub.last_event = event

    def _unsubscribe(self, params: Dict[str, Any]) -> ActionResult:
        sub_id = params.get("subscription_id", "")
        if sub_id in self._subscriptions:
            del self._subscriptions[sub_id]
            return ActionResult(success=True, message="Unsubscribed")
        return ActionResult(success=False, message="Subscription not found")

    def _emit_event(self, params: Dict[str, Any]) -> ActionResult:
        event_type = params.get("event_type", "")
        payload = params.get("payload", {})
        source = params.get("source", "")
        if not event_type:
            return ActionResult(success=False, message="event_type is required")

        event = {
            "id": str(uuid.uuid4()),
            "type": event_type,
            "payload": payload,
            "source": source,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "received_by": [],
        }

        for sub_id, sub in self._subscriptions.items():
            if sub.event_type == event_type and sub.active:
                if sub.filter_fn is None or sub.filter_fn(payload):
                    event["received_by"].append(sub_id)
                    try:
                        sub.callback(payload)
                    except Exception:
                        pass

        self._event_history.append(event)
        if len(self._event_history) > self._max_history:
            self._event_history = self._event_history[-self._max_history :]

        return ActionResult(
            success=True,
            message=f"Event '{event_type}' delivered to {len(event['received_by'])} subscribers",
            data={"event_id": event["id"], "delivered_count": len(event["received_by"])},
        )

    def _list_subscriptions(self) -> ActionResult:
        subs = [
            {
                "id": s.id,
                "event_type": s.event_type,
                "priority": s.priority.name,
                "active": s.active,
                "received_count": s.received_count,
            }
            for s in self._subscriptions.values()
        ]
        return ActionResult(success=True, message=f"{len(subs)} subscriptions", data={"subscriptions": subs})

    def _get_history(self, params: Dict[str, Any]) -> ActionResult:
        event_type = params.get("event_type", "")
        limit = params.get("limit", 100)
        history = self._event_history
        if event_type:
            history = [e for e in history if e["type"] == event_type]
        return ActionResult(success=True, message=f"{len(history)} events", data={"events": history[-limit:]})


class EventAggregatorAction(BaseAction):
    """Aggregate multiple events into one."""
    action_type = "event_aggregator"
    display_name = "事件聚合器"
    description = "将多个事件聚合为一个"

    def __init__(self) -> None:
        super().__init__()
        self._aggregation_windows: Dict[str, Dict[str, Any]] = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            action = params.get("action", "")
            if action == "open_window":
                return self._open_window(params)
            elif action == "add_event":
                return self._add_event(params)
            elif action == "close_window":
                return self._close_window(params)
            elif action == "flush":
                return self._flush(params)
            else:
                return ActionResult(success=False, message=f"Unknown action: {action}")
        except Exception as e:
            return ActionResult(success=False, message=f"Event aggregation failed: {e}")

    def _open_window(self, params: Dict[str, Any]) -> ActionResult:
        window_id = params.get("window_id", str(uuid.uuid4()))
        window_type = params.get("window_type", "time")
        max_size = params.get("max_size", 100)
        timeout_seconds = params.get("timeout_seconds", 60)
        self._aggregation_windows[window_id] = {
            "id": window_id,
            "type": window_type,
            "max_size": max_size,
            "timeout_seconds": timeout_seconds,
            "events": [],
            "opened_at": datetime.now(timezone.utc).isoformat(),
            "closed": False,
        }
        return ActionResult(success=True, message=f"Aggregation window '{window_id[:8]}' opened", data={"window_id": window_id})

    def _add_event(self, params: Dict[str, Any]) -> ActionResult:
        window_id = params.get("window_id", "")
        event = params.get("event", {})
        if window_id not in self._aggregation_windows:
            return ActionResult(success=False, message=f"Window not found: {window_id}")
        window = self._aggregation_windows[window_id]
        if window["closed"]:
            return ActionResult(success=False, message="Window is closed")
        window["events"].append(event)
        return ActionResult(
            success=True,
            message=f"Event added to window (size: {len(window['events'])}/{window['max_size']})",
            data={"window_id": window_id, "size": len(window["events"])},
        )

    def _close_window(self, params: Dict[str, Any]) -> ActionResult:
        window_id = params.get("window_id", "")
        if window_id not in self._aggregation_windows:
            return ActionResult(success=False, message=f"Window not found: {window_id}")
        window = self._aggregation_windows[window_id]
        window["closed"] = True
        window["closed_at"] = datetime.now(timezone.utc).isoformat()
        aggregated_event = {
            "type": "aggregated",
            "window_id": window_id,
            "event_count": len(window["events"]),
            "events": window["events"],
            "aggregated_at": datetime.now(timezone.utc).isoformat(),
        }
        return ActionResult(success=True, message=f"Window closed: {len(window['events'])} events aggregated", data=aggregated_event)

    def _flush(self, params: Dict[str, Any]) -> ActionResult:
        window_id = params.get("window_id", "")
        if window_id in self._aggregation_windows:
            self._aggregation_windows[window_id]["events"] = []
            return ActionResult(success=True, message="Window flushed")
        return ActionResult(success=False, message="Window not found")


class EventSplitterAction(BaseAction):
    """Split one event into multiple."""
    action_type = "event_splitter"
    display_name = "事件分割器"
    description = "将一个事件分割为多个"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            event = params.get("event", {})
            split_key = params.get("split_key", "")
            split_type = params.get("split_type", "array")
            if not event:
                return ActionResult(success=False, message="event is required")

            if split_type == "array" and split_key:
                items = event.get("payload", {}).get(split_key, [])
                if not isinstance(items, list):
                    return ActionResult(success=False, message=f"split_key '{split_key}' is not an array")
                split_events = [
                    {**event, "payload": {**event.get("payload", {}), split_key: item}, "split_item": item}
                    for item in items
                ]
            elif split_type == "fields":
                fields = params.get("fields", [])
                split_events = [{**event, "payload": {**event.get("payload", {}), f: event.get("payload", {}).get(f)}} for f in fields]
            else:
                return ActionResult(success=False, message=f"Unknown split_type: {split_type}")

            return ActionResult(success=True, message=f"Split into {len(split_events)} events", data={"events": split_events})
        except Exception as e:
            return ActionResult(success=False, message=f"Event splitting failed: {e}")


class EventTransformerAction(BaseAction):
    """Transform event payload."""
    action_type = "event_transformer"
    display_name = "事件转换器"
    description = "转换事件负载"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            event = params.get("event", {})
            transforms = params.get("transforms", [])
            if not event:
                return ActionResult(success=False, message="event is required")

            transformed = event.copy()
            for t in transforms:
                t_type = t.get("type", "")
                if t_type == "rename_field":
                    old_name = t.get("old_name", "")
                    new_name = t.get("new_name", "")
                    if old_name in transformed.get("payload", {}):
                        transformed["payload"][new_name] = transformed["payload"].pop(old_name)
                elif t_type == "add_field":
                    field_name = t.get("field_name", "")
                    field_value = t.get("field_value", None)
                    transformed["payload"][field_name] = field_value
                elif t_type == "remove_field":
                    field_name = t.get("field_name", "")
                    transformed["payload"].pop(field_name, None)
                elif t_type == "map_values":
                    mapping = t.get("mapping", {})
                    for old_val, new_val in mapping.items():
                        for k, v in transformed.get("payload", {}).items():
                            if v == old_val:
                                transformed["payload"][k] = new_val

            transformed["transformed"] = True
            transformed["transformed_at"] = datetime.now(timezone.utc).isoformat()
            return ActionResult(success=True, message=f"Event transformed with {len(transforms)} transforms", data=transformed)
        except Exception as e:
            return ActionResult(success=False, message=f"Event transformation failed: {e}")


class EventCorrelationAction(BaseAction):
    """Correlate related events across streams."""
    action_type = "event_correlation"
    display_name = "事件关联"
    description = "跨事件流关联相关事件"

    def __init__(self) -> None:
        super().__init__()
        self._correlation_windows: Dict[str, List[Dict[str, Any]]] = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            action = params.get("action", "")
            if action == "add":
                return self._add_to_correlation(params)
            elif action == == "correlate":
                return self._correlate(params)
            elif action == "clear":
                return self._clear(params)
            else:
                return ActionResult(success=False, message=f"Unknown action: {action}")
        except Exception as e:
            return ActionResult(success=False, message=f"Event correlation failed: {e}")

    def _add_to_correlation(self, params: Dict[str, Any]) -> ActionResult:
        correlation_id = params.get("correlation_id", "")
        event = params.get("event", {})
        window_seconds = params.get("window_seconds", 300)
        if not correlation_id or not event:
            return ActionResult(success=False, message="correlation_id and event are required")
        self._correlation_windows.setdefault(correlation_id, []).append({
            **event,
            "added_at": datetime.now(timezone.utc).isoformat(),
        })
        return ActionResult(
            success=True,
            message=f"Event added to correlation '{correlation_id[:8]}'",
            data={"correlation_id": correlation_id, "count": len(self._correlation_windows[correlation_id])},
        )

    def _correlate(self, params: Dict[str, Any]) -> ActionResult:
        correlation_id = params.get("correlation_id", "")
        match_keys = params.get("match_keys", [])
        if not correlation_id:
            return ActionResult(success=False, message="correlation_id is required")
        events = self._correlation_windows.get(correlation_id, [])
        if not match_keys:
            return ActionResult(success=True, message="No match keys, returning all", data={"events": events})
        groups: Dict[str, List[Dict[str, Any]]] = {}
        for event in events:
            key_values = tuple(event.get("payload", {}).get(k, "") for k in match_keys)
            key_str = "|".join(str(v) for v in key_values)
            groups.setdefault(key_str, []).append(event)
        return ActionResult(success=True, message=f"Found {len(groups)} correlated groups", data={"groups": groups})

    def _clear(self, params: Dict[str, Any]) -> ActionResult:
        correlation_id = params.get("correlation_id", "")
        if correlation_id:
            self._correlation_windows.pop(correlation_id, None)
            return ActionResult(success=True, message="Correlation window cleared")
        self._correlation_windows.clear()
        return ActionResult(success=True, message="All correlation windows cleared")
