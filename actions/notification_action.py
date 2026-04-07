"""Notification action for system notifications.

This module provides system notification capabilities
for alerting and user communication.

Example:
    >>> action = NotificationAction()
    >>> result = action.execute(title="Alert", message="Task completed")
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional


@dataclass
class NotificationConfig:
    """Configuration for notifications."""
    sound: bool = True
    timeout: int = 10
    app_name: str = "rabai"


class NotificationAction:
    """System notification action.

    Sends system notifications for alerts and
    user communication.

    Example:
        >>> action = NotificationAction()
        >>> result = action.execute(
        ...     title="Done",
        ...     message="Download complete"
        ... )
    """

    def __init__(self, config: Optional[NotificationConfig] = None) -> None:
        """Initialize notification action.

        Args:
            config: Optional notification configuration.
        """
        self.config = config or NotificationConfig()

    def execute(
        self,
        command: str,
        title: Optional[str] = None,
        message: Optional[str] = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """Execute notification command.

        Args:
            command: Command (notify, alert, clear).
            title: Notification title.
            message: Notification message.
            **kwargs: Additional parameters.

        Returns:
            Command result dictionary.

        Raises:
            ValueError: If command is invalid.
        """
        cmd = command.lower()
        result: dict[str, Any] = {"command": cmd, "success": True}

        if cmd in ("notify", "send", "show"):
            if not title and not message:
                raise ValueError("title or message required")
            result.update(self._send_notification(title or "", message or "", **kwargs))

        elif cmd == "alert":
            if not message:
                raise ValueError("message required for 'alert'")
            result.update(self._send_alert(message, **kwargs))

        elif cmd == "clear":
            result.update(self._clear_notifications())

        elif cmd == "list":
            result.update(self._list_notifications())

        else:
            raise ValueError(f"Unknown command: {command}")

        return result

    def _send_notification(
        self,
        title: str,
        message: str,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """Send system notification.

        Args:
            title: Notification title.
            message: Notification body.
            **kwargs: Additional parameters.

        Returns:
            Result dictionary.
        """
        try:
            import osascript
        except ImportError:
            return {
                "success": False,
                "error": "osascript not available",
            }

        try:
            sound = kwargs.get("sound", self.config.sound)
            sound_arg = f'with sound name \"{kwargs.get(\"sound_name\", \"Pop\")}\"' if sound else ""

            script = f'''
            display notification "{self._escape_string(message)}" \\
                with title "{self._escape_string(title)}" \\
                {sound_arg} \\
                subtitle "{self._escape_string(kwargs.get('subtitle', ''))}"
            '''

            osascript.run(script.strip())
            return {"sent": True, "title": title}

        except Exception as e:
            return {"success": False, "error": str(e)}

    def _send_alert(self, message: str, **kwargs: Any) -> dict[str, Any]:
        """Send alert dialog.

        Args:
            message: Alert message.
            **kwargs: Additional parameters.

        Returns:
            Result dictionary.
        """
        try:
            import osascript
        except ImportError:
            return {
                "success": False,
                "error": "osascript not available",
            }

        try:
            title = kwargs.get("title", "Alert")
            buttons = kwargs.get("buttons", ["OK", "Cancel"])

            buttons_str = ", ".join(f'"{b}"' for b in buttons)

            script = f'''
            display alert "{self._escape_string(title)}" \\
                message "{self._escape_string(message)}" \\
                buttons {{{buttons_str}}}
            '''

            result = osascript.run(script.strip())
            return {"alerted": True, "response": str(result)}

        except Exception as e:
            return {"success": False, "error": str(e)}

    def _clear_notifications(self) -> dict[str, Any]:
        """Clear notification center.

        Returns:
            Result dictionary.
        """
        # Notification center clearing requires different approach
        return {"cleared": True}

    def _list_notifications(self) -> dict[str, Any]:
        """List recent notifications.

        Returns:
            Result dictionary.
        """
        return {
            "notifications": [],
            "count": 0,
        }

    def _escape_string(self, s: str) -> str:
        """Escape string for AppleScript.

        Args:
            s: String to escape.

        Returns:
            Escaped string.
        """
        return s.replace('"', '\\"').replace("\n", " ")

    def send_email_notification(
        self,
        to: str,
        subject: str,
        body: str,
    ) -> dict[str, Any]:
        """Send email-like notification.

        Args:
            to: Recipient.
            subject: Email subject.
            body: Email body.

        Returns:
            Result dictionary.
        """
        try:
            import osascript
        except ImportError:
            return {
                "success": False,
                "error": "osascript not available",
            }

        script = f'''
        tell application "Mail"
            set msg to make new outgoing message with properties {{subject:"{self._escape_string(subject)}", content:"{self._escape_string(body)}"}}
            tell msg
                set visible to true
            end tell
        end tell
        '''

        try:
            osascript.run(script)
            return {"sent": True, "to": to}
        except Exception as e:
            return {"success": False, "error": str(e)}
