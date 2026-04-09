"""Notification action module for sending system notifications.

Handles cross-platform notification delivery with action buttons,
urgency levels, and notification history tracking.
"""

import logging
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Optional

logger = logging.getLogger(__name__)


class UrgencyLevel(Enum):
    LOW = "low"
    NORMAL = "normal"
    CRITICAL = "critical"


@dataclass
class Notification:
    title: str
    body: str
    urgency: UrgencyLevel = UrgencyLevel.NORMAL
    timeout: float = 5.0
    actions: list[str] = field(default_factory=list)
    notification_id: Optional[str] = None


class NotificationAction:
    """Send system notifications with action support.

    Args:
        max_history: Maximum notification history size.
        default_urgency: Default urgency level for notifications.
    """

    def __init__(
        self,
        max_history: int = 100,
        default_urgency: UrgencyLevel = UrgencyLevel.NORMAL,
    ) -> None:
        self._history: list[Notification] = []
        self._max_history = max_history
        self._default_urgency = default_urgency
        self._action_handlers: dict[str, Callable[[], None]] = {}
        self._active_notifications: dict[str, Notification] = {}

    def send(
        self,
        title: str,
        body: str,
        urgency: Optional[UrgencyLevel] = None,
        timeout: float = 5.0,
        actions: Optional[list[str]] = None,
    ) -> str:
        """Send a notification.

        Args:
            title: Notification title.
            body: Notification body text.
            urgency: Urgency level (uses default if not specified).
            timeout: Display timeout in seconds.
            actions: Optional list of action button labels.

        Returns:
            Notification ID.
        """
        notification_id = f"notif_{int(time.time() * 1000)}"
        notification = Notification(
            title=title,
            body=body,
            urgency=urgency or self._default_urgency,
            timeout=timeout,
            actions=actions or [],
            notification_id=notification_id,
        )
        self._history.append(notification)
        if len(self._history) > self._max_history:
            self._history.pop(0)
        self._active_notifications[notification_id] = notification
        logger.info(
            f"Sent notification [{notification_id}]: {title} ({urgency.value})"
        )
        return notification_id

    def dismiss(self, notification_id: str) -> bool:
        """Dismiss an active notification.

        Args:
            notification_id: ID of notification to dismiss.

        Returns:
            True if notification was found and dismissed.
        """
        if notification_id in self._active_notifications:
            del self._active_notifications[notification_id]
            logger.debug(f"Dismissed notification {notification_id}")
            return True
        return False

    def handle_action(
        self,
        notification_id: str,
        action: str,
    ) -> bool:
        """Handle a notification action callback.

        Args:
            notification_id: ID of the notification.
            action: Action identifier that was triggered.

        Returns:
            True if action handler was found and executed.
        """
        handler = self._action_handlers.get(action)
        if handler:
            handler()
            return True
        logger.warning(f"No handler for action '{action}'")
        return False

    def register_action_handler(
        self,
        action: str,
        handler: Callable[[], None],
    ) -> None:
        """Register a handler for a notification action.

        Args:
            action: Action identifier.
            handler: Callback function to execute.
        """
        self._action_handlers[action] = handler

    def get_history(
        self,
        limit: int = 20,
        urgency_filter: Optional[UrgencyLevel] = None,
    ) -> list[Notification]:
        """Get notification history.

        Args:
            limit: Maximum number of notifications to return.
            urgency_filter: Filter by urgency level if specified.

        Returns:
            List of notifications in chronological order (newest first).
        """
        history = self._history
        if urgency_filter:
            history = [n for n in history if n.urgency == urgency_filter]
        return history[-limit:][::-1]

    def clear_history(self) -> int:
        """Clear notification history.

        Returns:
            Number of notifications cleared.
        """
        count = len(self._history)
        self._history.clear()
        self._active_notifications.clear()
        return count

    def get_stats(self) -> dict[str, Any]:
        """Get notification statistics.

        Returns:
            Dictionary with notification stats.
        """
        total = len(self._history)
        by_urgency = {
            "low": sum(1 for n in self._history if n.urgency == UrgencyLevel.LOW),
            "normal": sum(
                1 for n in self._history if n.urgency == UrgencyLevel.NORMAL
            ),
            "critical": sum(
                1 for n in self._history if n.urgency == UrgencyLevel.CRITICAL
            ),
        }
        return {
            "total": total,
            "active": len(self._active_notifications),
            "by_urgency": by_urgency,
            "max_history": self._max_history,
        }
