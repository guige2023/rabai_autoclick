"""
Notification Action Module

Manages system and in-app notifications for automation workflows,
including toast messages, system alerts, and notification queues.

MIT License - Copyright (c) 2025 RabAi Research
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional

logger = logging.getLogger(__name__)


class NotificationLevel(Enum):
    """Notification severity levels."""

    DEBUG = "debug"
    INFO = "info"
    SUCCESS = "success"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


class NotificationType(Enum):
    """Notification types."""

    TOAST = "toast"
    SYSTEM = "system"
    EMAIL = "email"
    SMS = "sms"
    PUSH = "push"
    IN_APP = "in_app"


@dataclass
class Notification:
    """Represents a notification."""

    id: str
    title: str
    message: str
    level: NotificationLevel = NotificationLevel.INFO
    notification_type: NotificationType = NotificationType.TOAST
    timestamp: float = field(default_factory=time.time)
    duration: float = 3.0
    actions: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)
    read: bool = False
    dismissed: bool = False


@dataclass
class NotificationConfig:
    """Configuration for notifications."""

    default_duration: float = 3.0
    max_queue_size: int = 100
    enable_sound: bool = True
    enable_logging: bool = True
    sound_path: Optional[str] = None


class NotificationManager:
    """
    Manages notifications for automation workflows.

    Supports queuing, batching, sound alerts,
    and notification action handling.
    """

    def __init__(
        self,
        config: Optional[NotificationConfig] = None,
        sender: Optional[Callable[[Notification], None]] = None,
    ):
        self.config = config or NotificationConfig()
        self.sender = sender or self._default_sender
        self._notifications: Dict[str, Notification] = {}
        self._queue: List[str] = []
        self._subscribers: List[Callable[[Notification], None]] = []

    def _default_sender(self, notification: Notification) -> None:
        """Default notification sender."""
        logger.info(f"[{notification.level.value.upper()}] {notification.title}: {notification.message}")

    def send(
        self,
        title: str,
        message: str,
        level: NotificationLevel = NotificationLevel.INFO,
        notification_type: NotificationType = NotificationType.TOAST,
        duration: Optional[float] = None,
        actions: Optional[List[str]] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> str:
        """
        Send a notification.

        Args:
            title: Notification title
            message: Notification message
            level: Severity level
            notification_type: Type of notification
            duration: Display duration in seconds
            actions: Available action names
            metadata: Additional metadata

        Returns:
            Notification ID
        """
        notification_id = f"notif_{time.time()}_{len(self._notifications)}"

        notification = Notification(
            id=notification_id,
            title=title,
            message=message,
            level=level,
            notification_type=notification_type,
            duration=duration or self.config.default_duration,
            actions=actions or [],
            metadata=metadata or {},
        )

        self._notifications[notification_id] = notification
        self._queue.append(notification_id)

        if self.config.enable_logging:
            self._default_sender(notification)

        self.sender(notification)

        for subscriber in self._subscribers:
            try:
                subscriber(notification)
            except Exception as e:
                logger.error(f"Notification subscriber failed: {e}")

        return notification_id

    def send_batch(
        self,
        notifications: List[Dict[str, Any]],
    ) -> List[str]:
        """
        Send multiple notifications.

        Args:
            notifications: List of notification data dicts

        Returns:
            List of notification IDs
        """
        ids = []
        for notif_data in notifications:
            notif_id = self.send(**notif_data)
            ids.append(notif_id)
        return ids

    def dismiss(self, notification_id: str) -> bool:
        """Dismiss a notification."""
        if notification_id in self._notifications:
            self._notifications[notification_id].dismissed = True
            return True
        return False

    def mark_read(self, notification_id: str) -> bool:
        """Mark notification as read."""
        if notification_id in self._notifications:
            self._notifications[notification_id].read = True
            return True
        return False

    def get_notification(self, notification_id: str) -> Optional[Notification]:
        """Get a notification by ID."""
        return self._notifications.get(notification_id)

    def get_unread(self) -> List[Notification]:
        """Get all unread notifications."""
        return [n for n in self._notifications.values() if not n.read and not n.dismissed]

    def get_recent(self, limit: int = 10) -> List[Notification]:
        """Get recent notifications."""
        sorted_notifs = sorted(
            self._notifications.values(),
            key=lambda n: n.timestamp,
            reverse=True,
        )
        return sorted_notifs[:limit]

    def subscribe(self, callback: Callable[[Notification], None]) -> None:
        """Subscribe to notifications."""
        self._subscribers.append(callback)

    def unsubscribe(self, callback: Callable[[Notification], None]) -> None:
        """Unsubscribe from notifications."""
        if callback in self._subscribers:
            self._subscribers.remove(callback)

    def clear_all(self) -> None:
        """Clear all notifications."""
        self._notifications.clear()
        self._queue.clear()

    def clear_read(self) -> int:
        """Clear all read notifications."""
        to_remove = [nid for nid, n in self._notifications.items() if n.read]
        for nid in to_remove:
            del self._notifications[nid]
            if nid in self._queue:
                self._queue.remove(nid)
        return len(to_remove)


def create_notification_manager(
    config: Optional[NotificationConfig] = None,
) -> NotificationManager:
    """Factory function to create NotificationManager."""
    return NotificationManager(config=config)
