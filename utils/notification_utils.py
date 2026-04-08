"""
Notification Utilities

Provides utilities for sending notifications
in automation workflows.

Author: Agent3
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any
from enum import Enum, auto


class NotificationLevel(Enum):
    """Notification levels."""
    INFO = auto()
    WARNING = auto()
    ERROR = auto()
    SUCCESS = auto()


@dataclass
class Notification:
    """Represents a notification."""
    title: str
    message: str
    level: NotificationLevel
    data: dict[str, Any] | None = None


class NotificationService:
    """
    Service for sending notifications.
    
    Supports multiple notification channels
    and priority levels.
    """

    def __init__(self) -> None:
        self._handlers: dict[NotificationLevel, list[callable]] = {}

    def send(
        self,
        title: str,
        message: str,
        level: NotificationLevel = NotificationLevel.INFO,
        **kwargs: Any,
    ) -> Notification:
        """
        Send a notification.
        
        Args:
            title: Notification title.
            message: Notification message.
            level: Notification level.
            
        Returns:
            Created Notification.
        """
        notification = Notification(
            title=title,
            message=message,
            level=level,
            data=kwargs if kwargs else None,
        )
        self._dispatch(notification)
        return notification

    def register_handler(
        self,
        level: NotificationLevel,
        handler: callable,
    ) -> None:
        """Register a handler for a notification level."""
        if level not in self._handlers:
            self._handlers[level] = []
        self._handlers[level].append(handler)

    def _dispatch(self, notification: Notification) -> None:
        """Dispatch notification to handlers."""
        if notification.level in self._handlers:
            for handler in self._handlers[notification.level]:
                handler(notification)
