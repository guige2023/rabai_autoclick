"""Notification action module for RabAI AutoClick.

Provides notification utilities:
- NotificationCenter: Send notifications
- NotificationQueue: Queue notifications
- NotificationFormatter: Format notifications
"""

from typing import Any, Callable, Dict, List, Optional
import threading
import time
import uuid

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class Notification:
    """Notification object."""

    def __init__(
        self,
        title: str,
        body: str,
        notification_id: str = "",
        priority: int = 0,
        category: str = "general",
        metadata: Optional[Dict[str, Any]] = None,
    ):
        self.notification_id = notification_id or str(uuid.uuid4())
        self.title = title
        self.body = body
        self.priority = priority
        self.category = category
        self.metadata = metadata or {}
        self.created_at = time.time()
        self.delivered = False


class NotificationCenter:
    """Thread-safe notification center."""

    def __init__(self):
        self._handlers: Dict[str, List[Callable]] = {}
        self._history: List[Notification] = []
        self._lock = threading.RLock()
        self._max_history = 1000

    def subscribe(self, category: str, handler: Callable) -> str:
        """Subscribe to notifications."""
        subscription_id = str(uuid.uuid4())
        if category not in self._handlers:
            self._handlers[category] = []
        self._handlers[category].append(handler)
        return subscription_id

    def unsubscribe(self, category: str, handler: Callable) -> bool:
        """Unsubscribe from notifications."""
        if category not in self._handlers:
            return False
        try:
            self._handlers[category].remove(handler)
            return True
        except ValueError:
            return False

    def send(self, notification: Notification) -> str:
        """Send a notification."""
        with self._lock:
            handlers = self._handlers.get(notification.category, []) + self._handlers.get("*", [])

            for handler in handlers:
                try:
                    handler(notification)
                    notification.delivered = True
                except Exception:
                    pass

            self._history.append(notification)
            if len(self._history) > self._max_history:
                self._history = self._history[-self._max_history:]

            return notification.notification_id

    def get_history(self, category: Optional[str] = None, limit: int = 100) -> List[Notification]:
        """Get notification history."""
        with self._lock:
            history = self._history
            if category:
                history = [n for n in history if n.category == category]
            return history[-limit:]


class NotificationQueue:
    """Queue notifications for later delivery."""

    def __init__(self, center: Optional[NotificationCenter] = None):
        self.center = center or NotificationCenter()
        self._queue: List[Notification] = []
        self._lock = threading.RLock()

    def enqueue(self, notification: Notification) -> None:
        """Add to queue."""
        with self._lock:
            self._queue.append(notification)

    def dequeue(self) -> Optional[Notification]:
        """Remove and return next notification."""
        with self._lock:
            if not self._queue:
                return None
            return self._queue.pop(0)

    def flush(self) -> int:
        """Send all queued notifications."""
        count = 0
        while True:
            notification = self.dequeue()
            if not notification:
                break
            self.center.send(notification)
            count += 1
        return count

    def size(self) -> int:
        """Get queue size."""
        with self._lock:
            return len(self._queue)


class NotificationFormatter:
    """Format notifications."""

    @staticmethod
    def to_text(notification: Notification) -> str:
        """Format as text."""
        return f"[{notification.category.upper()}] {notification.title}: {notification.body}"

    @staticmethod
    def to_dict(notification: Notification) -> Dict[str, Any]:
        """Format as dict."""
        return {
            "id": notification.notification_id,
            "title": notification.title,
            "body": notification.body,
            "category": notification.category,
            "priority": notification.priority,
            "created_at": notification.created_at,
            "delivered": notification.delivered,
            "metadata": notification.metadata,
        }


class NotificationAction(BaseAction):
    """Notification management action."""
    action_type = "notification"
    display_name = "通知管理"
    description = "发送通知"

    def __init__(self):
        super().__init__()
        self._center = NotificationCenter()
        self._queue = NotificationQueue(self._center)

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "send")

            if operation == "send":
                return self._send(params)
            elif operation == "subscribe":
                return self._subscribe(params)
            elif operation == "history":
                return self._history(params)
            elif operation == "enqueue":
                return self._enqueue(params)
            elif operation == "flush":
                return self._flush(params)
            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Notification error: {str(e)}")

    def _send(self, params: Dict[str, Any]) -> ActionResult:
        """Send a notification."""
        title = params.get("title")
        body = params.get("body")
        category = params.get("category", "general")
        priority = params.get("priority", 0)
        metadata = params.get("metadata")

        if not title or not body:
            return ActionResult(success=False, message="title and body are required")

        notification = Notification(title, body, priority=priority, category=category, metadata=metadata)
        notification_id = self._center.send(notification)

        return ActionResult(success=True, message=f"Sent: {notification_id}", data={"notification_id": notification_id})

    def _subscribe(self, params: Dict[str, Any]) -> ActionResult:
        """Subscribe to notifications."""
        category = params.get("category", "*")

        def handler(n):
            pass

        subscription_id = self._center.subscribe(category, handler)

        return ActionResult(success=True, message=f"Subscribed: {subscription_id}", data={"subscription_id": subscription_id})

    def _history(self, params: Dict[str, Any]) -> ActionResult:
        """Get notification history."""
        category = params.get("category")
        limit = params.get("limit", 100)

        history = self._center.get_history(category, limit)
        formatted = [NotificationFormatter.to_dict(n) for n in history]

        return ActionResult(success=True, message=f"{len(formatted)} notifications", data={"notifications": formatted})

    def _enqueue(self, params: Dict[str, Any]) -> ActionResult:
        """Enqueue a notification."""
        title = params.get("title")
        body = params.get("body")
        category = params.get("category", "general")
        priority = params.get("priority", 0)

        if not title or not body:
            return ActionResult(success=False, message="title and body are required")

        notification = Notification(title, body, priority=priority, category=category)
        self._queue.enqueue(notification)

        return ActionResult(success=True, message=f"Enqueued: {notification.notification_id}")

    def _flush(self, params: Dict[str, Any]) -> ActionResult:
        """Flush queued notifications."""
        count = self._queue.flush()
        return ActionResult(success=True, message=f"Flushed {count} notifications", data={"count": count})
