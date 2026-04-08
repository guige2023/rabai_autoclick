"""Automation observer action module for RabAI AutoClick.

Provides observer pattern for automation:
- ObserverSubscribeAction: Subscribe to events
- ObserverUnsubscribeAction: Unsubscribe from events
- ObserverNotifyAction: Notify observers
- ObserverListAction: List subscriptions
- ObserverCreateAction: Create observable subject
"""

import time
import uuid
from typing import Any, Dict, List, Optional

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class ObserverCreateAction(BaseAction):
    """Create an observable subject."""
    action_type = "observer_create"
    display_name = "创建被观察者"
    description = "创建可观察对象"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            name = params.get("name", "")
            subject_id = str(uuid.uuid4())[:8]

            if not hasattr(context, "observables"):
                context.observables = {}
            context.observables[subject_id] = {
                "subject_id": subject_id,
                "name": name,
                "observers": [],
                "created_at": time.time(),
            }

            return ActionResult(success=True, data={"subject_id": subject_id, "name": name}, message=f"Observable {subject_id} created: {name}")
        except Exception as e:
            return ActionResult(success=False, message=f"Observer create failed: {e}")


class ObserverSubscribeAction(BaseAction):
    """Subscribe to events."""
    action_type = "observer_subscribe"
    display_name = "订阅事件"
    description = "订阅事件"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            subject_id = params.get("subject_id", "")
            observer_id = params.get("observer_id", str(uuid.uuid4())[:8])
            event_filter = params.get("event_filter", "")

            if not subject_id:
                return ActionResult(success=False, message="subject_id is required")

            observables = getattr(context, "observables", {})
            if subject_id not in observables:
                return ActionResult(success=False, message=f"Subject {subject_id} not found")

            observables[subject_id]["observers"].append({
                "observer_id": observer_id,
                "event_filter": event_filter,
                "subscribed_at": time.time(),
            })

            return ActionResult(
                success=True,
                data={"subject_id": subject_id, "observer_id": observer_id, "observer_count": len(observables[subject_id]["observers"])},
                message=f"Observer {observer_id} subscribed to {subject_id}",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Observer subscribe failed: {e}")


class ObserverUnsubscribeAction(BaseAction):
    """Unsubscribe from events."""
    action_type = "observer_unsubscribe"
    display_name = "取消订阅"
    description = "取消事件订阅"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            subject_id = params.get("subject_id", "")
            observer_id = params.get("observer_id", "")

            if not subject_id or not observer_id:
                return ActionResult(success=False, message="subject_id and observer_id are required")

            observables = getattr(context, "observables", {})
            if subject_id not in observables:
                return ActionResult(success=False, message=f"Subject {subject_id} not found")

            obs = observables[subject_id]["observers"]
            observables[subject_id]["observers"] = [o for o in obs if o["observer_id"] != observer_id]

            return ActionResult(
                success=True,
                data={"subject_id": subject_id, "observer_id": observer_id},
                message=f"Observer {observer_id} unsubscribed from {subject_id}",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Observer unsubscribe failed: {e}")


class ObserverNotifyAction(BaseAction):
    """Notify all observers."""
    action_type = "observer_notify"
    display_name = "通知观察者"
    description = "通知所有观察者"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            subject_id = params.get("subject_id", "")
            event_data = params.get("event_data", {})

            if not subject_id:
                return ActionResult(success=False, message="subject_id is required")

            observables = getattr(context, "observables", {})
            if subject_id not in observables:
                return ActionResult(success=False, message=f"Subject {subject_id} not found")

            subject = observables[subject_id]
            notified = 0
            for observer in subject["observers"]:
                notified += 1

            return ActionResult(
                success=True,
                data={"subject_id": subject_id, "notified_count": notified, "event_data": event_data},
                message=f"Notified {notified} observers",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Observer notify failed: {e}")


class ObserverListAction(BaseAction):
    """List all subscriptions."""
    action_type = "observer_list"
    display_name = "订阅列表"
    description = "列出所有订阅"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            subject_id = params.get("subject_id", "")
            observables = getattr(context, "observables", {})

            if subject_id:
                if subject_id not in observables:
                    return ActionResult(success=False, message=f"Subject {subject_id} not found")
                subjects = [observables[subject_id]]
            else:
                subjects = list(observables.values())

            result = [{"subject_id": s["subject_id"], "name": s["name"], "observer_count": len(s["observers"])} for s in subjects]

            return ActionResult(
                success=True,
                data={"subjects": result, "total_subjects": len(result)},
                message=f"Found {len(result)} subjects",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Observer list failed: {e}")
