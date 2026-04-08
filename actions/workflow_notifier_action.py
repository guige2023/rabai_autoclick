"""
Workflow Notifier Action Module.

Sends notifications on workflow events: start, complete, failure,
with configurable channels and message templates.
"""
from typing import Any, Optional, Callable
from dataclasses import dataclass, field
from actions.base_action import BaseAction


@dataclass
class Notification:
    """A workflow notification."""
    id: str
    workflow_id: str
    event: str  # started, completed, failed, cancelled
    message: str
    timestamp: float
    channel: str
    recipient: str


@dataclass
class NotificationResult:
    """Result of notification sending."""
    sent: bool
    notification_id: str
    error: Optional[str] = None


class WorkflowNotifierAction(BaseAction):
    """Send workflow notifications."""

    def __init__(self) -> None:
        super().__init__("workflow_notifier")
        self._handlers: dict[str, Callable] = {}
        self._notifications: list[Notification] = []

    def execute(self, context: dict, params: dict) -> dict:
        """
        Send or configure notifications.

        Args:
            context: Execution context
            params: Parameters:
                - action: notify, configure, history
                - workflow_id: Workflow identifier
                - event: Event type (started, completed, failed, cancelled)
                - message: Notification message
                - channel: Notification channel (email, slack, webhook, sms)
                - recipient: Recipient address/ID

        Returns:
            NotificationResult or notification history
        """
        import time
        import hashlib

        action = params.get("action", "notify")
        workflow_id = params.get("workflow_id", "default")
        event = params.get("event", "")
        message = params.get("message", "")
        channel = params.get("channel", "log")
        recipient = params.get("recipient", "")

        if action == "notify":
            notification = Notification(
                id=hashlib.md5(f"{time.time()}{workflow_id}".encode()).hexdigest()[:12],
                workflow_id=workflow_id,
                event=event,
                message=message,
                timestamp=time.time(),
                channel=channel,
                recipient=recipient
            )

            handler = self._handlers.get(channel)
            if handler:
                try:
                    handler(notification)
                except Exception as e:
                    return NotificationResult(False, notification.id, str(e)).__dict__

            self._notifications.append(notification)
            return NotificationResult(True, notification.id).__dict__

        elif action == "configure":
            channel = params.get("channel", "log")
            handler = params.get("handler")
            self._handlers[channel] = handler
            return {"configured": True, "channel": channel}

        elif action == "history":
            limit = params.get("limit", 100)
            workflow_filter = params.get("workflow_id")
            results = self._notifications
            if workflow_filter:
                results = [n for n in results if n.workflow_id == workflow_filter]
            return {"count": len(results), "notifications": [vars(n) for n in results[-limit:]]}

        return {"error": f"Unknown action: {action}"}

    def register_handler(self, channel: str, handler: Callable) -> None:
        """Register a notification handler."""
        self._handlers[channel] = handler
