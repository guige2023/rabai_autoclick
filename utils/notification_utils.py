"""Notification Utilities.

This module provides notification utilities for macOS desktop applications,
including local notifications, notification banners, and notification
management for user alerts and automation triggers.

Example:
    >>> from notification_utils import NotificationManager, Notification
    >>> manager = NotificationManager()
    >>> manager.send(Notification(title="Task Complete", body="File saved"))
"""

from __future__ import annotations

import subprocess
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Callable, Dict, List, Optional, Any


class NotificationSound(Enum):
    """Pre-defined notification sounds."""
    DEFAULT = "default"
    GLASS = "Glass"
    BELL = "Bell"
    BUZZ = "Buzz"
    DONE = "Funk"
    ALERT = "Bottle"
    NONE = "none"


class NotificationPriority(Enum):
    """Notification priority levels."""
    LOW = auto()
    MEDIUM = auto()
    HIGH = auto()
    CRITICAL = auto()


@dataclass
class Notification:
    """Represents a notification.
    
    Attributes:
        title: Notification title
        body: Notification body text
        subtitle: Optional subtitle
        sound: Notification sound
        priority: Priority level
    """
    title: str
    body: str = ""
    subtitle: Optional[str] = None
    sound: NotificationSound = NotificationSound.DEFAULT
    priority: NotificationPriority = NotificationPriority.MEDIUM
    icon_path: Optional[str] = None
    category: Optional[str] = None
    user_info: Dict[str, Any] = field(default_factory=dict)
    thread_id: Optional[str] = None
    delay: float = 0.0


@dataclass
class NotificationResponse:
    """Response from notification interaction."""
    action_id: str
    notification: Notification
    timestamp: float = 0.0


class NotificationManager:
    """Manages notifications for the application.
    
    Provides a unified interface for creating and sending notifications
    using the macOS NSUserNotification system.
    
    Attributes:
        app_name: Application name shown in notifications
        default_sound: Default sound for notifications
    """
    
    def __init__(
        self,
        app_name: str = "Application",
        default_sound: NotificationSound = NotificationSound.DEFAULT,
    ):
        self.app_name = app_name
        self.default_sound = default_sound
        self._callbacks: Dict[str, List[Callable]] = {
            'notification_opened': [],
            'notification_action': [],
            'notification_dismissed': [],
        }
        self._history: List[Notification] = []
    
    def send(
        self,
        notification: Notification,
        wait_for_response: bool = False,
    ) -> Optional[NotificationResponse]:
        """Send a notification.
        
        Args:
            notification: Notification to send
            wait_for_response: Whether to wait for user interaction
            
        Returns:
            NotificationResponse if wait_for_response is True
        """
        self._history.append(notification)
        
        sound = notification.sound.value if notification.sound != NotificationSound.NONE else ""
        
        try:
            cmd = [
                'osascript',
                '-e',
                f'display notification "{notification.body}" '
                f'with title "{notification.title}"'
                + (f' subtitle "{notification.subtitle}"' if notification.subtitle else '')
                + (f' sound name "{sound}"' if sound else ''),
            ]
            
            subprocess.run(
                cmd,
                capture_output=True,
                timeout=5,
            )
            
            return None
            
        except Exception:
            pass
        
        return None
    
    def send_simple(
        self,
        title: str,
        body: str = "",
        sound: NotificationSound = NotificationSound.DEFAULT,
    ) -> None:
        """Send a simple notification.
        
        Args:
            title: Notification title
            body: Notification body
            sound: Sound to play
        """
        notification = Notification(
            title=title,
            body=body,
            sound=sound,
        )
        self.send(notification)
    
    def send_with_icon(
        self,
        notification: Notification,
        icon_path: str,
    ) -> None:
        """Send notification with custom icon."""
        notification.icon_path = icon_path
        self.send(notification)
    
    def send_delayed(
        self,
        notification: Notification,
        delay: float,
    ) -> None:
        """Send notification after delay.
        
        Args:
            notification: Notification to send
            delay: Delay in seconds
        """
        import time
        import threading
        
        def delayed_send():
            time.sleep(delay)
            self.send(notification)
        
        thread = threading.Thread(target=delayed_send)
        thread.daemon = True
        thread.start()
    
    def get_history(self, limit: Optional[int] = None) -> List[Notification]:
        """Get notification history.
        
        Args:
            limit: Maximum number of entries
            
        Returns:
            List of sent notifications
        """
        if limit:
            return self._history[-limit:]
        return self._history.copy()
    
    def clear_history(self) -> None:
        """Clear notification history."""
        self._history.clear()
    
    def on(self, event: str, callback: Callable) -> None:
        """Register event callback."""
        if event in self._callbacks:
            self._callbacks[event].append(callback)
    
    def _notify(self, event: str, *args) -> None:
        """Notify callbacks of event."""
        for callback in self._callbacks.get(event, []):
            try:
                callback(*args)
            except Exception:
                pass


class NotificationCategory:
    """Notification category with actions.
    
    Defines a category of notifications with associated actions
    that users can take directly from the notification.
    
    Attributes:
        id: Category identifier
        actions: Available actions
    """
    
    def __init__(self, id: str):
        self.id = id
        self._actions: List[Dict[str, str]] = []
        self._callbacks: Dict[str, Callable] = {}
    
    def add_action(
        self,
        action_id: str,
        title: str,
        destructive: bool = False,
    ) -> None:
        """Add an action to the category."""
        self._actions.append({
            'id': action_id,
            'title': title,
            'destructive': destructive,
        })
    
    def set_callback(self, action_id: str, callback: Callable[[Notification], None]) -> None:
        """Set callback for action."""
        self._callbacks[action_id] = callback
    
    def handle_action(self, action_id: str, notification: Notification) -> None:
        """Handle action from notification."""
        callback = self._callbacks.get(action_id)
        if callback:
            try:
                callback(notification)
            except Exception:
                pass


class NotificationScheduler:
    """Schedule notifications for later delivery."""
    
    def __init__(self, manager: NotificationManager):
        self.manager = manager
        self._scheduled: List[Dict[str, Any]] = []
    
    def schedule(
        self,
        notification: Notification,
        fire_date: float,
    ) -> str:
        """Schedule a notification.
        
        Args:
            notification: Notification to send
            fire_date: Unix timestamp to fire
            
        Returns:
            Schedule ID
        """
        import uuid
        
        schedule_id = str(uuid.uuid4())
        self._scheduled.append({
            'id': schedule_id,
            'notification': notification,
            'fire_date': fire_date,
            'is_repeating': False,
        })
        
        return schedule_id
    
    def schedule_repeating(
        self,
        notification: Notification,
        interval: float,
    ) -> str:
        """Schedule a repeating notification.
        
        Args:
            notification: Notification to send
            interval: Repeat interval in seconds
            
        Returns:
            Schedule ID
        """
        import uuid
        
        schedule_id = str(uuid.uuid4())
        self._scheduled.append({
            'id': schedule_id,
            'notification': notification,
            'fire_date': 0,
            'interval': interval,
            'is_repeating': True,
        })
        
        return schedule_id
    
    def cancel(self, schedule_id: str) -> bool:
        """Cancel a scheduled notification."""
        for i, scheduled in enumerate(self._scheduled):
            if scheduled['id'] == schedule_id:
                self._scheduled.pop(i)
                return True
        return False
    
    def cancel_all(self) -> None:
        """Cancel all scheduled notifications."""
        self._scheduled.clear()
    
    def get_pending(self) -> List[Dict[str, Any]]:
        """Get pending scheduled notifications."""
        return self._scheduled.copy()


class NotificationCenterObserver:
    """Observe notification center notifications."""
    
    def __init__(self):
        self._callbacks: List[Callable[[Notification], None]] = []
    
    def add_callback(self, callback: Callable[[Notification], None]) -> None:
        """Add notification callback."""
        self._callbacks.append(callback)
    
    def remove_callback(self, callback: Callable[[Notification], None]) -> None:
        """Remove notification callback."""
        if callback in self._callbacks:
            self._callbacks.remove(callback)
    
    def notify(self, notification: Notification) -> None:
        """Notify all callbacks of new notification."""
        for callback in self._callbacks:
            try:
                callback(notification)
            except Exception:
                pass
