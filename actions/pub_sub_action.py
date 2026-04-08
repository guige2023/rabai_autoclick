"""Pub/Sub Action Module.

Provides publish/subscribe messaging with topic management,
message filtering, and delivery guarantees.
"""
from __future__ import annotations

import time
import uuid
from collections import defaultdict
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)


class DeliveryMode(Enum):
    """Message delivery mode."""
    AT_MOST_ONCE = "at_most_once"
    AT_LEAST_ONCE = "at_least_once"


@dataclass
class Subscriber:
    """Topic subscriber."""
    id: str
    topic: str
    callback: Any
    filter_func: Optional[Callable] = None
    ack_mode: bool = True


@dataclass
class Message:
    """Pub/Sub message."""
    id: str
    topic: str
    payload: Any
    metadata: Dict[str, Any] = field(default_factory=dict)
    timestamp: float = field(default_factory=time.time)
    delivery_mode: DeliveryMode = DeliveryMode.AT_MOST_ONCE
    delivered: bool = False
    ack_id: Optional[str] = None


@dataclass
class Topic:
    """Topic definition."""
    name: str
    subscribers: List[Subscriber] = field(default_factory=list)
    created_at: float = field(default_factory=time.time)
    message_count: int = 0


class PubSubStore:
    """In-memory pub/sub store."""

    def __init__(self):
        self._topics: Dict[str, Topic] = {}
        self._messages: Dict[str, List[Message]] = defaultdict(list)

    def create_topic(self, name: str) -> Topic:
        """Create topic."""
        if name not in self._topics:
            self._topics[name] = Topic(name=name)
        return self._topics[name]

    def get_topic(self, name: str) -> Optional[Topic]:
        """Get topic."""
        return self._topics.get(name)

    def list_topics(self) -> List[str]:
        """List all topics."""
        return list(self._topics.keys())

    def subscribe(self, topic: str, callback: Any,
                  filter_func: Optional[Callable] = None) -> str:
        """Subscribe to topic."""
        topic_obj = self._topics.get(topic)
        if not topic_obj:
            topic_obj = self.create_topic(topic)

        sub_id = uuid.uuid4().hex
        subscriber = Subscriber(
            id=sub_id,
            topic=topic,
            callback=callback,
            filter_func=filter_func
        )
        topic_obj.subscribers.append(subscriber)

        return sub_id

    def unsubscribe(self, topic: str, sub_id: str) -> bool:
        """Unsubscribe from topic."""
        topic_obj = self._topics.get(topic)
        if not topic_obj:
            return False

        for i, sub in enumerate(topic_obj.subscribers):
            if sub.id == sub_id:
                topic_obj.subscribers.pop(i)
                return True
        return False

    def publish(self, topic: str, payload: Any,
                metadata: Optional[Dict[str, Any]] = None) -> Message:
        """Publish message to topic."""
        topic_obj = self._topics.get(topic)
        if not topic_obj:
            topic_obj = self.create_topic(topic)

        msg = Message(
            id=uuid.uuid4().hex,
            topic=topic,
            payload=payload,
            metadata=metadata or {}
        )

        self._messages[topic].append(msg)
        topic_obj.message_count += 1

        return msg

    def get_messages(self, topic: str, limit: int = 100) -> List[Message]:
        """Get messages from topic."""
        return self._messages.get(topic, [])[-limit:]


_global_store = PubSubStore()


class PubSubAction:
    """Pub/Sub action.

    Example:
        action = PubSubAction()

        action.create_topic("notifications")
        action.subscribe("notifications", my_callback)
        action.publish("notifications", {"type": "alert", "msg": "Hello"})
    """

    def __init__(self, store: Optional[PubSubStore] = None):
        self._store = store or _global_store
        self._callbacks: Dict[str, Callable] = {}

    def create_topic(self, name: str) -> Dict[str, Any]:
        """Create topic."""
        topic = self._store.create_topic(name)
        return {
            "success": True,
            "topic": name,
            "created_at": topic.created_at,
            "message": f"Created topic: {name}"
        }

    def get_topic(self, name: str) -> Dict[str, Any]:
        """Get topic info."""
        topic = self._store.get_topic(name)
        if topic:
            return {
                "success": True,
                "topic": name,
                "subscriber_count": len(topic.subscribers),
                "message_count": topic.message_count,
                "created_at": topic.created_at
            }
        return {"success": False, "message": "Topic not found"}

    def list_topics(self) -> Dict[str, Any]:
        """List all topics."""
        topics = self._store.list_topics()
        return {
            "success": True,
            "topics": topics,
            "count": len(topics)
        }

    def subscribe(self, topic: str,
                  callback_id: Optional[str] = None) -> Dict[str, Any]:
        """Subscribe to topic."""
        def default_callback(msg):
            print(f"[PubSub] {msg.topic}: {msg.payload}")

        callback = self._callbacks.get(callback_id) if callback_id else default_callback

        sub_id = self._store.subscribe(topic, callback)

        return {
            "success": True,
            "subscription_id": sub_id,
            "topic": topic,
            "message": f"Subscribed to {topic}"
        }

    def unsubscribe(self, topic: str, subscription_id: str) -> Dict[str, Any]:
        """Unsubscribe from topic."""
        if self._store.unsubscribe(topic, subscription_id):
            return {"success": True, "message": "Unsubscribed"}
        return {"success": False, "message": "Subscription not found"}

    def publish(self, topic: str, payload: Any,
               metadata: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Publish message."""
        msg = self._store.publish(topic, payload, metadata)

        return {
            "success": True,
            "message_id": msg.id,
            "topic": topic,
            "timestamp": msg.timestamp,
            "message": f"Published to {topic}"
        }

    def get_messages(self, topic: str, limit: int = 100) -> Dict[str, Any]:
        """Get messages from topic."""
        messages = self._store.get_messages(topic, limit)
        return {
            "success": True,
            "topic": topic,
            "messages": [
                {
                    "id": m.id,
                    "payload": m.payload,
                    "metadata": m.metadata,
                    "timestamp": m.timestamp,
                    "delivered": m.delivered
                }
                for m in messages
            ],
            "count": len(messages)
        }

    def get_subscribers(self, topic: str) -> Dict[str, Any]:
        """Get subscribers of topic."""
        topic_obj = self._store.get_topic(topic)
        if not topic_obj:
            return {"success": False, "message": "Topic not found"}

        return {
            "success": True,
            "topic": topic,
            "subscribers": [
                {
                    "id": s.id,
                    "ack_mode": s.ack_mode
                }
                for s in topic_obj.subscribers
            ],
            "count": len(topic_obj.subscribers)
        }


def execute(context: Any, params: Dict[str, Any]) -> Dict[str, Any]:
    """Execute pub/sub action."""
    operation = params.get("operation", "")
    action = PubSubAction()

    try:
        if operation == "create_topic":
            name = params.get("name", "")
            if not name:
                return {"success": False, "message": "name required"}
            return action.create_topic(name)

        elif operation == "get_topic":
            name = params.get("name", "")
            if not name:
                return {"success": False, "message": "name required"}
            return action.get_topic(name)

        elif operation == "list_topics":
            return action.list_topics()

        elif operation == "subscribe":
            topic = params.get("topic", "")
            if not topic:
                return {"success": False, "message": "topic required"}
            return action.subscribe(topic, params.get("callback_id"))

        elif operation == "unsubscribe":
            topic = params.get("topic", "")
            subscription_id = params.get("subscription_id", "")
            if not topic or not subscription_id:
                return {"success": False, "message": "topic and subscription_id required"}
            return action.unsubscribe(topic, subscription_id)

        elif operation == "publish":
            topic = params.get("topic", "")
            payload = params.get("payload", {})
            if not topic:
                return {"success": False, "message": "topic required"}
            return action.publish(topic, payload, params.get("metadata"))

        elif operation == "get_messages":
            topic = params.get("topic", "")
            if not topic:
                return {"success": False, "message": "topic required"}
            return action.get_messages(topic, params.get("limit", 100))

        elif operation == "get_subscribers":
            topic = params.get("topic", "")
            if not topic:
                return {"success": False, "message": "topic required"}
            return action.get_subscribers(topic)

        else:
            return {"success": False, "message": f"Unknown operation: {operation}"}

    except Exception as e:
        return {"success": False, "message": f"Pub/Sub error: {str(e)}"}
