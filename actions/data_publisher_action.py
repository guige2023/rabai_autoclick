"""Data publisher action module for RabAI AutoClick.

Provides data publishing:
- DataPublisherAction: Publish data to topics
- TopicManagerAction: Manage topics
- SubscriptionManagerAction: Manage subscriptions
"""

from typing import Any, Dict, List, Optional, Callable
from datetime import datetime
from collections import defaultdict

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class DataPublisherAction(BaseAction):
    """Publish data to topics."""
    action_type = "data_publisher"
    display_name = "数据发布"
    description = "发布数据到主题"

    def __init__(self):
        super().__init__()
        self._topics = defaultdict(list)
        self._subscribers = defaultdict(list)

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "publish")
            topic = params.get("topic", "default")
            message = params.get("message", {})
            subscriber_id = params.get("subscriber_id", None)

            if operation == "publish":
                self._topics[topic].append({
                    "message": message,
                    "timestamp": datetime.now().isoformat()
                })

                delivered = 0
                for callback in self._subscribers[topic]:
                    delivered += 1

                return ActionResult(
                    success=True,
                    data={
                        "topic": topic,
                        "message_id": len(self._topics[topic]) - 1,
                        "subscribers_notified": delivered
                    },
                    message=f"Published to '{topic}': {delivered} subscribers notified"
                )

            elif operation == "subscribe":
                self._subscribers[topic].append(subscriber_id)
                return ActionResult(
                    success=True,
                    data={
                        "topic": topic,
                        "subscriber_id": subscriber_id,
                        "subscribers_count": len(self._subscribers[topic])
                    },
                    message=f"Subscribed '{subscriber_id}' to '{topic}'"
                )

            elif operation == "unsubscribe":
                if subscriber_id in self._subscribers[topic]:
                    self._subscribers[topic].remove(subscriber_id)
                return ActionResult(
                    success=True,
                    data={"topic": topic, "unsubscribed": subscriber_id},
                    message=f"Unsubscribed '{subscriber_id}' from '{topic}'"
                )

            elif operation == "messages":
                messages = self._topics.get(topic, [])
                return ActionResult(
                    success=True,
                    data={
                        "topic": topic,
                        "messages": messages,
                        "count": len(messages)
                    },
                    message=f"Topic '{topic}': {len(messages)} messages"
                )

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Data publisher error: {str(e)}")


class TopicManagerAction(BaseAction):
    """Manage topics."""
    action_type = "topic_manager"
    display_name = "主题管理"
    description = "管理发布主题"

    def __init__(self):
        super().__init__()
        self._topic_metadata = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "create")
            topic = params.get("topic", "default")

            if operation == "create":
                metadata = params.get("metadata", {})
                self._topic_metadata[topic] = {
                    "created_at": datetime.now().isoformat(),
                    "metadata": metadata
                }
                return ActionResult(
                    success=True,
                    data={"topic": topic, "created": True},
                    message=f"Topic '{topic}' created"
                )

            elif operation == "delete":
                if topic in self._topic_metadata:
                    del self._topic_metadata[topic]
                return ActionResult(
                    success=True,
                    data={"topic": topic, "deleted": True},
                    message=f"Topic '{topic}' deleted"
                )

            elif operation == "list":
                return ActionResult(
                    success=True,
                    data={
                        "topics": list(self._topic_metadata.keys()),
                        "count": len(self._topic_metadata)
                    },
                    message=f"Topics: {len(self._topic_metadata)}"
                )

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Topic manager error: {str(e)}")


class SubscriptionManagerAction(BaseAction):
    """Manage subscriptions."""
    action_type = "subscription_manager"
    display_name = "订阅管理"
    description = "管理订阅"

    def __init__(self):
        super().__init__()
        self._subscriptions = defaultdict(list)

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "subscribe")
            subscriber_id = params.get("subscriber_id", "default")
            topics = params.get("topics", [])

            if operation == "subscribe":
                for topic in topics:
                    if subscriber_id not in self._subscriptions[topic]:
                        self._subscriptions[topic].append(subscriber_id)
                return ActionResult(
                    success=True,
                    data={
                        "subscriber_id": subscriber_id,
                        "subscribed_topics": topics
                    },
                    message=f"Subscribed '{subscriber_id}' to {len(topics)} topics"
                )

            elif operation == "unsubscribe":
                for topic in topics:
                    if subscriber_id in self._subscriptions[topic]:
                        self._subscriptions[topic].remove(subscriber_id)
                return ActionResult(
                    success=True,
                    data={
                        "subscriber_id": subscriber_id,
                        "unsubscribed_from": topics
                    },
                    message=f"Unsubscribed '{subscriber_id}' from {len(topics)} topics"
                )

            elif operation == "list":
                all_subscriptions = {topic: list(subs) for topic, subs in self._subscriptions.items()}
                return ActionResult(
                    success=True,
                    data={
                        "subscriptions": all_subscriptions,
                        "topics_count": len(self._subscriptions)
                    },
                    message=f"Subscriptions: {len(self._subscriptions)} topics"
                )

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Subscription manager error: {str(e)}")
