"""
Notification Manager Action Module.

Manages system notifications for automation events,
including queuing, throttling, and notification actions.
"""

import time
from collections import deque
from dataclasses import dataclass, field
from typing import Callable, Optional


@dataclass
class Notification:
    """A notification message."""
    title: str
    body: str
    timestamp: float
    priority: int = 0
    actions: list[str] = field(default_factory=list)
    metadata: dict = field(default_factory=dict)


class NotificationManager:
    """Manages system notifications for automation."""

    def __init__(
        self,
        max_queue: int = 100,
        throttle_seconds: float = 1.0,
    ):
        """
        Initialize notification manager.

        Args:
            max_queue: Maximum queued notifications.
            throttle_seconds: Minimum interval between notifications.
        """
        self.max_queue = max_queue
        self.throttle_seconds = throttle_seconds
        self._queue: deque[Notification] = deque(maxlen=max_queue)
        self._last_sent: float = 0
        self._handlers: list[Callable[[Notification], None]] = []

    def send(
        self,
        title: str,
        body: str,
        priority: int = 0,
        actions: Optional[list[str]] = None,
        **metadata,
    ) -> bool:
        """
        Send a notification.

        Args:
            title: Notification title.
            body: Notification body text.
            priority: Priority level (higher = more urgent).
            actions: Optional action buttons.
            **metadata: Additional metadata.

        Returns:
            True if sent immediately, False if queued.
        """
        notification = Notification(
            title=title,
            body=body,
            timestamp=time.time(),
            priority=priority,
            actions=actions or [],
            metadata=metadata,
        )

        time_since_last = time.time() - self._last_sent

        if time_since_last >= self.throttle_seconds:
            return self._dispatch(notification)
        else:
            self._queue.append(notification)
            return False

    def _dispatch(self, notification: Notification) -> bool:
        """Dispatch a notification to all handlers."""
        self._last_sent = time.time()

        for handler in self._handlers:
            try:
                handler(notification)
            except Exception:
                pass

        return True

    def process_queue(self) -> int:
        """
        Process queued notifications.

        Returns:
            Number of notifications sent.
        """
        sent = 0
        now = time.time()

        while self._queue:
            notification = self._queue[0]
            if now - self._last_sent >= self.throttle_seconds:
                self._queue.popleft()
                self._dispatch(notification)
                sent += 1
            else:
                break

        return sent

    def register_handler(
        self,
        handler: Callable[[Notification], None],
    ) -> None:
        """
        Register a notification handler.

        Args:
            handler: Function to handle notifications.
        """
        self._handlers.append(handler)

    def clear_queue(self) -> None:
        """Clear the notification queue."""
        self._queue.clear()

    def get_queue_size(self) -> int:
        """Get current queue size."""
        return len(self._queue)

    def get_pending(self) -> list[Notification]:
        """Get all pending notifications."""
        return list(self._queue)
