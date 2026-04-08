"""Publisher Subscriber Action Module.

Provides pub/sub pattern for event
distribution.
"""

import time
import threading
from typing import Any, Dict, List, Optional, Callable
from dataclasses import dataclass, field
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


@dataclass
class Subscription:
    """Subscription entry."""
    subscription_id: str
    topic: str
    subscriber_id: str
    callback: Callable
    created_at: float = field(default_factory=time.time)


@dataclass
class Message:
    """Pub/sub message."""
    message_id: str
    topic: str
    payload: Any
    timestamp: float = field(default_factory=time.time)


class PublisherSubscriber:
    """Pub/sub implementation."""

    def __init__(self):
        self._subscriptions: Dict[str, List[Subscription]] = {}
        self._dead_letter_queue: List[Dict] = []
        self._lock = threading.RLock()

    def subscribe(
        self,
        topic: str,
        subscriber_id: str,
        callback: Callable
    ) -> str:
        """Subscribe to topic."""
        subscription_id = f"sub_{int(time.time() * 1000)}"

        subscription = Subscription(
            subscription_id=subscription_id,
            topic=topic,
            subscriber_id=subscriber_id,
            callback=callback
        )

        with self._lock:
            if topic not in self._subscriptions:
                self._subscriptions[topic] = []
            self._subscriptions[topic].append(subscription)

        return subscription_id

    def unsubscribe(self, subscription_id: str) -> bool:
        """Unsubscribe."""
        with self._lock:
            for topic_subs in self._subscriptions.values():
                for i, sub in enumerate(topic_subs):
                    if sub.subscription_id == subscription_id:
                        topic_subs.pop(i)
                        return True
        return False

    def publish(self, topic: str, payload: Any) -> int:
        """Publish message to topic."""
        message = Message(
            message_id=f"msg_{int(time.time() * 1000)}",
            topic=topic,
            payload=payload
        )

        delivered = 0

        with self._lock:
            subscriptions = self._subscriptions.get(topic, [])

        for subscription in subscriptions:
            try:
                subscription.callback(message)
                delivered += 1
            except Exception:
                self._dead_letter_queue.append({
                    "subscription_id": subscription.subscription_id,
                    "message": message.__dict__
                })

        return delivered

    def get_topics(self) -> List[str]:
        """Get all topics."""
        with self._lock:
            return list(self._subscriptions.keys())

    def get_dead_letter_queue(self) -> List[Dict]:
        """Get DLQ."""
        with self._lock:
            return self._dead_letter_queue.copy()


class PublisherSubscriberAction(BaseAction):
    """Action for pub/sub operations."""

    def __init__(self):
        super().__init__("publisher_subscriber")
        self._manager = PublisherSubscriber()

    def execute(self, params: Dict) -> ActionResult:
        """Execute pub/sub action."""
        try:
            operation = params.get("operation", "subscribe")

            if operation == "subscribe":
                return self._subscribe(params)
            elif operation == "unsubscribe":
                return self._unsubscribe(params)
            elif operation == "publish":
                return self._publish(params)
            elif operation == "topics":
                return self._topics(params)
            elif operation == "dead_letter":
                return self._dead_letter(params)
            else:
                return ActionResult(success=False, message=f"Unknown: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=str(e))

    def _subscribe(self, params: Dict) -> ActionResult:
        """Subscribe."""
        subscription_id = self._manager.subscribe(
            topic=params.get("topic", ""),
            subscriber_id=params.get("subscriber_id", ""),
            callback=params.get("callback") or (lambda m: None)
        )
        return ActionResult(success=True, data={"subscription_id": subscription_id})

    def _unsubscribe(self, params: Dict) -> ActionResult:
        """Unsubscribe."""
        success = self._manager.unsubscribe(params.get("subscription_id", ""))
        return ActionResult(success=success)

    def _publish(self, params: Dict) -> ActionResult:
        """Publish."""
        delivered = self._manager.publish(
            params.get("topic", ""),
            params.get("payload")
        )
        return ActionResult(success=True, data={"delivered": delivered})

    def _topics(self, params: Dict) -> ActionResult:
        """Get topics."""
        return ActionResult(success=True, data={"topics": self._manager.get_topics()})

    def _dead_letter(self, params: Dict) -> ActionResult:
        """Get dead letter queue."""
        return ActionResult(success=True, data={
            "dead_letter": self._manager.get_dead_letter_queue()
        })
