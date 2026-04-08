"""Notification utilities for system alerts and user notifications.

Provides cross-platform system notification delivery,
notification center interaction, and in-app notification
management for automation feedback and alerts.

Example:
    >>> from utils.notification_utils import notify, notify_error, clear_notifications
    >>> notify('Task completed!', title='Autoclick')
    >>> notify_error('Failed to click element')
"""

from __future__ import annotations

import subprocess
import time
from typing import Optional

__all__ = [
    "notify",
    "notify_error",
    "notify_success",
    "notify_warning",
    "clear_notifications",
    "NotificationSound",
    "NotificationError",
]


class NotificationSound:
    """Predefined notification sounds."""

    BUILTIN = "default"
    BELL = "Basso"
    BOTTLE = "Blow"
    GLASS = "Glass"
    HEROIC = "Heroic"
    MORSE = "Morse"
    ALERT = "Alert"
    NONE = "none"


class NotificationError(Exception):
    """Raised when a notification operation fails."""
    pass


def notify(
    message: str,
    title: str = "Notification",
    subtitle: Optional[str] = None,
    sound: str = NotificationSound.BUILTIN,
    app_name: Optional[str] = None,
) -> bool:
    """Display a system notification.

    Args:
        message: Main notification text.
        title: Notification title.
        subtitle: Optional subtitle.
        sound: Sound name or 'none' to disable.
        app_name: Optional sender application name.

    Returns:
        True if notification was displayed.

    Example:
        >>> notify('Automation complete', title='Autoclick Pro')
    """
    import sys

    if sys.platform == "darwin":
        return _notify_macos(message, title, subtitle, sound, app_name)
    elif sys.platform == "win32":
        return _notify_windows(message, title)
    else:
        return _notify_linux(message, title)


def _notify_macos(
    message: str,
    title: str,
    subtitle: Optional[str],
    sound: str,
    app_name: Optional[str],
) -> bool:
    """Display a notification using macOS Terminal Notifier or osascript."""
    # Try terminal-notifier first (better UX)
    args = ["terminal-notifier", "-title", title, "-message", message]
    if subtitle:
        args.extend(["-subtitle", subtitle])
    if sound != NotificationSound.NONE:
        if sound == NotificationSound.BUILTIN:
            args.append("-sound")
        else:
            args.extend(["-sound", sound])
    if app_name:
        args.extend(["-sender", app_name])

    try:
        result = subprocess.run(args, capture_output=True, timeout=10)
        if result.returncode == 0:
            return True
    except FileNotFoundError:
        pass
    except Exception:
        pass

    # Fall back to osascript
    message_escaped = message.replace('"', '\\"').replace("\n", "\\n")
    title_escaped = title.replace('"', '\\"')

    if subtitle:
        subtitle_escaped = subtitle.replace('"', '\\"')
        script = f'display notification "{message_escaped}" with title "{title_escaped}" subtitle "{subtitle_escaped}"'
    else:
        script = f'display notification "{message_escaped}" with title "{title_escaped}"'

    if sound != NotificationSound.NONE and sound != NotificationSound.BUILTIN:
        script += f' sound name "{sound}"'
    elif sound == NotificationSound.BUILTIN:
        script += ' sound name "default"'

    try:
        subprocess.run(["osascript", "-e", script], timeout=10, check=True)
        return True
    except Exception:
        return False


def _notify_windows(message: str, title: str) -> bool:
    """Display a notification using Windows Toast."""
    try:
        subprocess.run(
            [
                "powershell",
                "-Command",
                f'[Windows.UI.Notifications.ToastNotificationManager, Windows.UI.Notifications, ContentType = WindowsRuntime] | Out-Null; '
                f'$template = [Windows.UI.Notifications.ToastNotificationManager]::GetTemplateContent([Windows.UI.Notifications.ToastTemplateType]::ToastText02); '
                f'$text = $template.GetElementsByTagName("text"); '
                f'$text[0].AppendChild($template.CreateTextNode("{title}")) | Out-Null; '
                f'$text[1].AppendChild($template.CreateTextNode("{message}")) | Out-Null; '
                f'$toast = [Windows.UI.Notifications.ToastNotification]::new($template); '
                f'[Windows.UI.Notifications.ToastNotificationManager]::CreateToastNotifier("Autoclick").Show($toast)',
            ],
            timeout=10,
            check=True,
        )
        return True
    except Exception:
        return False


def _notify_linux(message: str, title: str) -> bool:
    """Display a notification using notify-send."""
    try:
        subprocess.run(
            ["notify-send", title, message],
            timeout=10,
            check=True,
        )
        return True
    except Exception:
        return False


def notify_error(
    message: str,
    title: str = "Error",
    subtitle: Optional[str] = None,
) -> bool:
    """Display an error notification with the default error sound.

    Args:
        message: Error description.
        title: Notification title.
        subtitle: Optional subtitle.

    Returns:
        True if displayed.
    """
    return notify(message, title=title, subtitle=subtitle, sound="Basso")


def notify_success(
    message: str,
    title: str = "Success",
    subtitle: Optional[str] = None,
) -> bool:
    """Display a success notification.

    Args:
        message: Success message.
        title: Notification title.
        subtitle: Optional subtitle.

    Returns:
        True if displayed.
    """
    return notify(message, title=title, subtitle=subtitle, sound="Glass")


def notify_warning(
    message: str,
    title: str = "Warning",
    subtitle: Optional[str] = None,
) -> bool:
    """Display a warning notification.

    Args:
        message: Warning message.
        title: Notification title.
        subtitle: Optional subtitle.

    Returns:
        True if displayed.
    """
    return notify(message, title=title, subtitle=subtitle, sound="Blow")


def clear_notifications() -> bool:
    """Clear all displayed notifications.

    Returns:
        True if successful.
    """
    import sys

    if sys.platform == "darwin":
        # macOS doesn't have a clear-all API, but we can close notification center
        try:
            subprocess.run(
                ["osascript", "-e", 'tell application "System Events" to key code 53'],
                timeout=5,
            )
            return True
        except Exception:
            return False
    return False


class NotificationManager:
    """Manages a queue of notifications with rate limiting.

    Example:
        >>> manager = NotificationManager(max_per_minute=5)
        >>> manager.send('Message 1')
        >>> manager.send('Message 2')
    """

    def __init__(self, max_per_minute: int = 10):
        self.max_per_minute = max_per_minute
        self._timestamps: list[float] = []

    def send(
        self,
        message: str,
        title: str = "Notification",
        **kwargs,
    ) -> bool:
        """Send a notification, respecting rate limits.

        Args:
            message: Notification message.
            title: Notification title.
            **kwargs: Additional arguments passed to notify().

        Returns:
            True if the notification was sent.
        """
        now = time.time()
        cutoff = now - 60.0

        # Remove old timestamps
        self._timestamps = [t for t in self._timestamps if t > cutoff]

        if len(self._timestamps) >= self.max_per_minute:
            return False

        self._timestamps.append(now)
        return notify(message, title=title, **kwargs)
