"""Notification utilities for RabAI AutoClick.

Provides:
- System notifications
- Notification queue
- Toast notifications
"""

import threading
import time
from dataclasses import dataclass
from enum import Enum
from typing import Callable, List, Optional


class NotificationLevel(Enum):
    """Notification importance levels."""
    DEBUG = "debug"
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


@dataclass
class Notification:
    """Notification data."""
    title: str
    message: str
    level: NotificationLevel = NotificationLevel.INFO
    duration: float = 5.0
    icon: Optional[str] = None


class NotificationCenter:
    """Central notification management."""

    def __init__(self) -> None:
        """Initialize notification center."""
        self._handlers: dict = {}
        self._history: List[Notification] = []
        self._lock = threading.Lock()

    def add_handler(
        self,
        level: NotificationLevel,
        handler: Callable[[Notification], None],
    ) -> None:
        """Add notification handler.

        Args:
            level: Level to handle.
            handler: Handler function.
        """
        with self._lock:
            if level not in self._handlers:
                self._handlers[level] = []
            self._handlers[level].append(handler)

    def notify(self, notification: Notification) -> None:
        """Send notification.

        Args:
            notification: Notification to send.
        """
        with self._lock:
            self._history.append(notification)

        # Call handlers
        with self._lock:
            handlers = self._handlers.get(notification.level, [])

        for handler in handlers:
            try:
                handler(notification)
            except Exception:
                pass

    def get_history(self, limit: int = 50) -> List[Notification]:
        """Get notification history.

        Args:
            limit: Maximum entries.

        Returns:
            List of notifications.
        """
        with self._lock:
            return self._history[-limit:]

    def clear_history(self) -> None:
        """Clear notification history."""
        with self._lock:
            self._history.clear()


class SystemNotification:
    """Send system notifications."""

    @staticmethod
    def show(
        title: str,
        message: str,
        level: NotificationLevel = NotificationLevel.INFO,
    ) -> bool:
        """Show system notification.

        Args:
            title: Notification title.
            message: Notification message.
            level: Importance level.

        Returns:
            True if successful.
        """
        try:
            import win32gui
            import win32con
            from win32lib.pywin32 import win32gui as w32ui

            # Try using Windows toast notifications
            try:
                from win10toast import ToastNotifier
                toaster = ToastNotifier()
                toaster.show_toast(
                    title,
                    message,
                    duration=5,
                    threaded=False,
                )
                return True
            except ImportError:
                pass

            # Fallback to simple message box for errors
            if level in (NotificationLevel.ERROR, NotificationLevel.CRITICAL):
                win32gui.MessageBox(
                    0,
                    message,
                    title,
                    win32con.MB_OK | win32con.MB_ICONWARNING,
                )
                return True

            return False
        except Exception:
            return False

    @staticmethod
    def show_toast(
        title: str,
        message: str,
        icon_path: Optional[str] = None,
        duration: int = 5,
    ) -> bool:
        """Show toast notification.

        Args:
            title: Toast title.
            message: Toast message.
            icon_path: Optional icon path.
            duration: Duration in seconds.

        Returns:
            True if successful.
        """
        try:
            from win10toast import ToastNotifier
            toaster = ToastNotifier()
            toaster.show_toast(
                title,
                message,
                icon_path=icon_path,
                duration=duration,
                threaded=True,
            )
            return True
        except Exception:
            return False


class NotificationQueue:
    """Queue notifications for batch delivery."""

    def __init__(self, batch_interval: float = 1.0) -> None:
        """Initialize queue.

        Args:
            batch_interval: Seconds between batch sends.
        """
        self._queue: List[Notification] = []
        self._batch_interval = batch_interval
        self._lock = threading.Lock()
        self._running = False
        self._thread: Optional[threading.Thread] = None

    def enqueue(self, notification: Notification) -> None:
        """Add notification to queue.

        Args:
            notification: Notification to queue.
        """
        with self._lock:
            self._queue.append(notification)

    def start(self, center: NotificationCenter) -> None:
        """Start processing queue.

        Args:
            center: Notification center to dispatch to.
        """
        if self._running:
            return

        self._running = True
        self._thread = threading.Thread(
            target=self._process_loop,
            args=(center,),
            daemon=True,
        )
        self._thread.start()

    def stop(self) -> None:
        """Stop processing queue."""
        if not self._running:
            return

        self._running = False
        if self._thread:
            self._thread.join(timeout=2)

    def _process_loop(self, center: NotificationCenter) -> None:
        """Queue processing loop."""
        while self._running:
            time.sleep(self._batch_interval)

            with self._lock:
                if self._queue:
                    batch = self._queue.copy()
                    self._queue.clear()

            for notification in batch:
                center.notify(notification)

    @property
    def size(self) -> int:
        """Get queue size."""
        with self._lock:
            return len(self._queue)


class NotificationFilter:
    """Filter notifications by criteria."""

    def __init__(self) -> None:
        """Initialize filter."""
        self._level_filter: Optional[NotificationLevel] = None
        self._keyword_filter: Optional[str] = None

    def level(self, level: NotificationLevel) -> "NotificationFilter":
        """Filter by minimum level.

        Args:
            level: Minimum level.

        Returns:
            Self for chaining.
        """
        self._level_filter = level
        return self

    def keyword(self, keyword: str) -> "NotificationFilter":
        """Filter by keyword in title or message.

        Args:
            keyword: Keyword to search for.

        Returns:
            Self for chaining.
        """
        self._keyword_filter = keyword.lower()
        return self

    def matches(self, notification: Notification) -> bool:
        """Check if notification matches filter.

        Args:
            notification: Notification to check.

        Returns:
            True if matches.
        """
        if self._level_filter:
            if notification.level.value < self._level_filter.value:
                return False

        if self._keyword_filter:
            text = (notification.title + notification.message).lower()
            if self._keyword_filter not in text:
                return False

        return True
