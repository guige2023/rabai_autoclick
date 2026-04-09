"""
Notification Utilities for UI Automation.

This module provides utilities for sending notifications and alerts
during automation workflows, supporting multiple notification channels.

Author: AI Assistant
License: MIT
"""

from __future__ import annotations

import time
import uuid
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Callable, Optional


class NotificationPriority(Enum):
    """Notification priority levels."""
    LOW = 0
    NORMAL = 1
    HIGH = 2
    URGENT = 3


class NotificationStatus(Enum):
    """Notification delivery status."""
    PENDING = auto()
    SENT = auto()
    DELIVERED = auto()
    FAILED = auto()
    CANCELLED = auto()


@dataclass
class Notification:
    """
    Represents a notification.
    
    Attributes:
        notification_id: Unique identifier
        title: Notification title
        message: Notification message body
        priority: Notification priority
        status: Current delivery status
        created_at: Creation timestamp
        sent_at: When notification was sent
        metadata: Additional notification data
    """
    notification_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    title: str = ""
    message: str = ""
    priority: NotificationPriority = NotificationPriority.NORMAL
    status: NotificationStatus = NotificationStatus.PENDING
    created_at: float = field(default_factory=time.time)
    sent_at: Optional[float] = None
    delivered_at: Optional[float] = None
    error: Optional[str] = None
    metadata: dict[str, Any] = field(default_factory=dict)


class NotificationChannel:
    """Base class for notification channels."""
    
    def __init__(self, name: str):
        self.name = name
        self._enabled = True
    
    def enable(self) -> None:
        """Enable this channel."""
        self._enabled = True
    
    def disable(self) -> None:
        """Disable this channel."""
        self._enabled = False
    
    @property
    def is_enabled(self) -> bool:
        """Check if channel is enabled."""
        return self._enabled
    
    def send(self, notification: Notification) -> bool:
        """
        Send a notification through this channel.
        
        Args:
            notification: Notification to send
            
        Returns:
            True if sent successfully, False otherwise
        """
        raise NotImplementedError


class ConsoleChannel(NotificationChannel):
    """Console output notification channel."""
    
    def __init__(self):
        super().__init__("console")
    
    def send(self, notification: Notification) -> bool:
        """Send notification to console."""
        try:
            prefix = self._get_prefix(notification.priority)
            print(f"{prefix} [{notification.title}] {notification.message}")
            return True
        except Exception as e:
            notification.error = str(e)
            return False
    
    def _get_prefix(self, priority: NotificationPriority) -> str:
        """Get console prefix for priority."""
        prefixes = {
            NotificationPriority.LOW: "[INFO]",
            NotificationPriority.NORMAL: "[INFO]",
            NotificationPriority.HIGH: "[WARN]",
            NotificationPriority.URGENT: "[ERROR]"
        }
        return prefixes.get(priority, "[INFO]")


class LogChannel(NotificationChannel):
    """Logging notification channel."""
    
    def __init__(self, log_file: Optional[str] = None):
        super().__init__("log")
        self.log_file = log_file
    
    def send(self, notification: Notification) -> bool:
        """Log notification to file or logger."""
        try:
            timestamp = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            log_entry = (
                f"[{timestamp}] [{notification.priority.name}] "
                f"{notification.title}: {notification.message}\n"
            )
            
            if self.log_file:
                with open(self.log_file, "a") as f:
                    f.write(log_entry)
            else:
                # Use Python's logging module
                import logging
                logger = logging.getLogger("automation")
                logger.info(log_entry)
            
            return True
        except Exception as e:
            notification.error = str(e)
            return False


class CallbackChannel(NotificationChannel):
    """Callback-based notification channel."""
    
    def __init__(self, callback: Callable[[Notification], None]):
        super().__init__("callback")
        self.callback = callback
    
    def send(self, notification: Notification) -> bool:
        """Execute callback with notification."""
        try:
            self.callback(notification)
            return True
        except Exception as e:
            notification.error = str(e)
            return False


class NotificationManager:
    """
    Manages notification dispatch to multiple channels.
    
    Example:
        manager = NotificationManager()
        manager.add_channel(ConsoleChannel())
        manager.notify(Notification(title="Done", message="Task completed"))
    """
    
    def __init__(self):
        self._channels: list[NotificationChannel] = []
        self._history: list[Notification] = []
        self._max_history: int = 100
    
    def add_channel(self, channel: NotificationChannel) -> None:
        """Add a notification channel."""
        self._channels.append(channel)
    
    def remove_channel(self, name: str) -> bool:
        """Remove a channel by name."""
        for i, channel in enumerate(self._channels):
            if channel.name == name:
                del self._channels[i]
                return True
        return False
    
    def notify(
        self,
        title: str,
        message: str,
        priority: NotificationPriority = NotificationPriority.NORMAL,
        metadata: Optional[dict[str, Any]] = None
    ) -> Notification:
        """
        Send a notification to all enabled channels.
        
        Args:
            title: Notification title
            message: Notification message
            priority: Notification priority
            metadata: Additional metadata
            
        Returns:
            The created Notification object
        """
        notification = Notification(
            title=title,
            message=message,
            priority=priority,
            metadata=metadata or {}
        )
        
        self._send_to_channels(notification)
        self._add_to_history(notification)
        
        return notification
    
    def _send_to_channels(self, notification: Notification) -> None:
        """Send notification to all enabled channels."""
        notification.status = NotificationStatus.PENDING
        
        success_count = 0
        for channel in self._channels:
            if channel.is_enabled:
                try:
                    if channel.send(notification):
                        success_count += 1
                except Exception as e:
                    notification.error = str(e)
        
        if success_count > 0:
            notification.status = NotificationStatus.SENT
            notification.sent_at = time.time()
        else:
            notification.status = NotificationStatus.FAILED
    
    def _add_to_history(self, notification: Notification) -> None:
        """Add notification to history."""
        self._history.append(notification)
        if len(self._history) > self._max_history:
            self._history.pop(0)
    
    def get_history(
        self, 
        limit: int = 50,
        priority_filter: Optional[NotificationPriority] = None
    ) -> list[Notification]:
        """
        Get notification history.
        
        Args:
            limit: Maximum number of notifications to return
            priority_filter: Filter by priority level
            
        Returns:
            List of notifications
        """
        history = self._history
        
        if priority_filter is not None:
            history = [n for n in history if n.priority == priority_filter]
        
        return history[-limit:]
    
    def clear_history(self) -> None:
        """Clear notification history."""
        self._history.clear()
    
    def get_pending_count(self) -> int:
        """Get count of pending notifications."""
        return sum(
            1 for n in self._history 
            if n.status == NotificationStatus.PENDING
        )


class AlertNotification(Notification):
    """Extended notification for alerts with additional alert-specific fields."""
    
    def __init__(
        self,
        title: str,
        message: str,
        alert_id: Optional[str] = None,
        source: Optional[str] = None,
        actions: Optional[list[str]] = None,
        **kwargs
    ):
        super().__init__(title=title, message=message, **kwargs)
        self.alert_id = alert_id or str(uuid.uuid4())
        self.source = source
        self.actions = actions or []
        self.action_taken: Optional[str] = None
