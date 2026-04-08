"""Automation event bus action module for RabAI AutoClick.

Provides event bus operations:
- EventBusPublishAction: Publish event
- EventBusSubscribeAction: Subscribe to events
- EventBusUnsubscribeAction: Unsubscribe
- EventBusListAction: List event types
- EventBusClearAction: Clear event bus
"""

import time
import uuid
from typing import Any, Dict, List, Optional

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class EventBusPublishAction(BaseAction):
    """Publish an event to the bus."""
    action_type = "eventbus_publish"
    display_name = "事件发布"
    description = "向事件总线发布事件"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            event_type = params.get("event_type", "")
            payload = params.get("payload", {})
            source = params.get("source", "default")
            priority = params.get("priority", "normal")

            if not event_type:
                return ActionResult(success=False, message="event_type is required")

            event_id = str(uuid.uuid4())[:8]

            if not hasattr(context, "eventbus"):
                context.eventbus = {"events": [], "subscriptions": {}}
            context.eventbus["events"].append({
                "event_id": event_id,
                "event_type": event_type,
                "payload": payload,
                "source": source,
                "priority": priority,
                "timestamp": time.time(),
            })

            return ActionResult(
                success=True,
                data={"event_id": event_id, "event_type": event_type, "source": source},
                message=f"Event {event_id} published: {event_type}",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"EventBus publish failed: {e}")


class EventBusSubscribeAction(BaseAction):
    """Subscribe to event types."""
    action_type = "eventbus_subscribe"
    display_name = "事件订阅"
    description = "订阅事件类型"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            event_type = params.get("event_type", "")
            subscriber_id = params.get("subscriber_id", str(uuid.uuid4())[:8])
            callback = params.get("callback", "")

            if not event_type:
                return ActionResult(success=False, message="event_type is required")

            if not hasattr(context, "eventbus"):
                context.eventbus = {"events": [], "subscriptions": {}}
            if event_type not in context.eventbus["subscriptions"]:
                context.eventbus["subscriptions"][event_type] = []
            context.eventbus["subscriptions"][event_type].append({
                "subscriber_id": subscriber_id,
                "callback": callback,
                "subscribed_at": time.time(),
            })

            return ActionResult(
                success=True,
                data={"subscriber_id": subscriber_id, "event_type": event_type},
                message=f"Subscribed {subscriber_id} to {event_type}",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"EventBus subscribe failed: {e}")


class EventBusUnsubscribeAction(BaseAction):
    """Unsubscribe from events."""
    action_type = "eventbus_unsubscribe"
    display_name = "取消订阅"
    description = "取消事件订阅"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            event_type = params.get("event_type", "")
            subscriber_id = params.get("subscriber_id", "")

            if not event_type:
                return ActionResult(success=False, message="event_type is required")

            bus = getattr(context, "eventbus", {"subscriptions": {}})
            if event_type in bus["subscriptions"]:
                bus["subscriptions"][event_type] = [
                    s for s in bus["subscriptions"][event_type] if s["subscriber_id"] != subscriber_id
                ]

            return ActionResult(
                success=True,
                data={"event_type": event_type, "subscriber_id": subscriber_id},
                message=f"Unsubscribed {subscriber_id} from {event_type}",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"EventBus unsubscribe failed: {e}")


class EventBusListAction(BaseAction):
    """List registered event types."""
    action_type = "eventbus_list"
    display_name = "事件列表"
    description = "列出注册的事件类型"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            bus = getattr(context, "eventbus", {"subscriptions": {}})
            event_types = list(bus["subscriptions"].keys())
            subscribers = {et: len(subs) for et, subs in bus["subscriptions"].items()}

            return ActionResult(
                success=True,
                data={"event_types": event_types, "subscriber_counts": subscribers},
                message=f"EventBus: {len(event_types)} event types registered",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"EventBus list failed: {e}")


class EventBusClearAction(BaseAction):
    """Clear event bus."""
    action_type = "eventbus_clear"
    display_name = "清空事件总线"
    description = "清空事件总线"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            clear_events = params.get("clear_events", True)
            clear_subscriptions = params.get("clear_subscriptions", False)

            bus = getattr(context, "eventbus", {"events": [], "subscriptions": {}})
            event_count = len(bus["events"]) if clear_events else 0
            sub_count = len(bus["subscriptions"]) if clear_subscriptions else 0

            if clear_events:
                bus["events"] = []
            if clear_subscriptions:
                bus["subscriptions"] = {}

            return ActionResult(
                success=True,
                data={"cleared_events": event_count, "cleared_subscriptions": sub_count},
                message=f"EventBus cleared: {event_count} events, {sub_count} subscriptions",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"EventBus clear failed: {e}")
