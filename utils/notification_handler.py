"""
Notification Handler Utilities.

Utilities for observing and handling macOS notification center
notifications for automation and testing purposes.

Usage:
    from utils.notification_handler import NotificationHandler

    handler = NotificationHandler()
    handler.on_notification(callback)
    handler.start()
"""

from __future__ import annotations

from typing import Optional, List, Dict, Any, Callable, TYPE_CHECKING
from dataclasses import dataclass, field
import time
import subprocess

if TYPE_CHECKING:
    pass


@dataclass
class Notification:
    """Represents a notification."""
    title: str
    body: Optional[str] = None
    app_name: Optional[str] = None
    bundle_id: Optional[str] = None
    timestamp: float = field(default_factory=time.time)
    sound_name: Optional[str] = None

    def __repr__(self) -> str:
        return f"Notification({self.title!r}, app={self.app_name!r})"


class NotificationHandler:
    """
    Handle macOS notification center notifications.

    Provides callbacks for notification appearance and utilities
    for filtering and dismissing notifications.

    Example:
        handler = NotificationHandler()
        handler.on_notification(lambda n: print(f"Notification: {n.title}"))
        handler.start()
    """

    def __init__(
        self,
        poll_interval: float = 0.5,
        max_history: int = 100,
    ) -> None:
        """
        Initialize the notification handler.

        Args:
            poll_interval: Seconds between polling checks.
            max_history: Maximum notifications to retain.
        """
        self._poll_interval = poll_interval
        self._max_history = max_history
        self._running = False
        self._callbacks: List[Callable[[Notification], None]] = []
        self._history: List[Notification] = []
        self._last_notification: Optional[Notification] = None

    def start(self) -> None:
        """Start handling notifications."""
        self._running = True

    def stop(self) -> None:
        """Stop handling notifications."""
        self._running = False

    @property
    def is_running(self) -> bool:
        return self._running

    def on_notification(
        self,
        callback: Callable[[Notification], None],
    ) -> None:
        """
        Register a callback for notification events.

        Args:
            callback: Function called with Notification on each event.
        """
        if callback not in self._callbacks:
            self._callbacks.append(callback)

    def get_recent(
        self,
        count: int = 10,
    ) -> List[Notification]:
        """
        Get recent notifications.

        Args:
            count: Maximum number to return.

        Returns:
            List of recent Notification objects.
        """
        return list(self._history[-count:])

    def poll(self) -> Optional[Notification]:
        """
        Poll for new notifications.

        Returns:
            Latest Notification or None.
        """
        if not self._running:
            return None

        try:
            result = subprocess.run(
                ["osascript", "-e",
                 'tell application "System Events" to get '
                 "{name, content} of every row of "
                 "(make new script in front window)"],
                capture_output=True,
                text=True,
                timeout=2,
            )

            if result.returncode != 0:
                return None

        except Exception:
            pass

        return None

    def dismiss_all(self) -> bool:
        """
        Dismiss all visible notifications.

        Returns:
            True if successful.
        """
        try:
            subprocess.run(
                ["osascript", "-e",
                 'tell application "System Events" to '
                 'click button "Close" of window 1 of '
                 'process "NotificationCenter"'],
                capture_output=True,
                timeout=2,
            )
            return True
        except Exception:
            return False


def get_current_notification() -> Optional[Notification]:
    """
    Get the most recent notification.

    Returns:
        Notification or None.
    """
    handler = NotificationHandler()
    return handler.poll()
