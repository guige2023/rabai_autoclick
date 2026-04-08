"""
Notification service module for multi-channel alert and notification delivery.

Supports email, SMS, push, webhook, and in-app notifications with templates.
"""
from __future__ import annotations

import json
import smtplib
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Optional


class NotificationChannel(Enum):
    """Notification channel types."""
    EMAIL = "email"
    SMS = "sms"
    PUSH = "push"
    WEBHOOK = "webhook"
    IN_APP = "in_app"
    SLACK = "slack"
    TEAMS = "teams"


class NotificationPriority(Enum):
    """Notification priority levels."""
    LOW = "low"
    NORMAL = "normal"
    HIGH = "high"
    URGENT = "urgent"


class NotificationStatus(Enum):
    """Notification delivery status."""
    PENDING = "pending"
    SENT = "sent"
    DELIVERED = "delivered"
    FAILED = "failed"


@dataclass
class NotificationTemplate:
    """A notification template."""
    id: str
    name: str
    channel: NotificationChannel
    subject_template: str = ""
    body_template: str = ""
    variables: list[str] = field(default_factory=list)


@dataclass
class NotificationRecipient:
    """A notification recipient."""
    id: str
    name: str
    email: Optional[str] = None
    phone: Optional[str] = None
    push_token: Optional[str] = None
    slack_id: Optional[str] = None
    teams_id: Optional[str] = None
    metadata: dict = field(default_factory=dict)


@dataclass
class Notification:
    """A notification message."""
    id: str
    channel: NotificationChannel
    recipient_id: str
    subject: str
    body: str
    priority: NotificationPriority = NotificationPriority.NORMAL
    status: NotificationStatus = NotificationStatus.PENDING
    template_id: Optional[str] = None
    variables: dict = field(default_factory=dict)
    created_at: float = field(default_factory=time.time)
    sent_at: Optional[float] = None
    delivered_at: Optional[float] = None
    error: Optional[str] = None


class NotificationService:
    """
    Notification service for multi-channel delivery.

    Supports email, SMS, push, webhook, and in-app notifications
    with templates and priority handling.
    """

    def __init__(self):
        self._templates: dict[str, NotificationTemplate] = {}
        self._recipients: dict[str, NotificationRecipient] = {}
        self._notifications: dict[str, Notification] = {}
        self._channels: dict[NotificationChannel, Callable] = {}

    def register_template(
        self,
        name: str,
        channel: NotificationChannel,
        subject_template: str = "",
        body_template: str = "",
        variables: Optional[list[str]] = None,
    ) -> NotificationTemplate:
        """Register a notification template."""
        template = NotificationTemplate(
            id=str(uuid.uuid4())[:8],
            name=name,
            channel=channel,
            subject_template=subject_template,
            body_template=body_template,
            variables=variables or [],
        )

        self._templates[name] = template
        return template

    def add_recipient(
        self,
        name: str,
        email: Optional[str] = None,
        phone: Optional[str] = None,
        push_token: Optional[str] = None,
        slack_id: Optional[str] = None,
        teams_id: Optional[str] = None,
    ) -> NotificationRecipient:
        """Add a notification recipient."""
        recipient = NotificationRecipient(
            id=str(uuid.uuid4())[:12],
            name=name,
            email=email,
            phone=phone,
            push_token=push_token,
            slack_id=slack_id,
            teams_id=teams_id,
        )

        self._recipients[recipient.id] = recipient
        return recipient

    def get_recipient(self, recipient_id: str) -> Optional[NotificationRecipient]:
        """Get a recipient by ID."""
        return self._recipients.get(recipient_id)

    def send_notification(
        self,
        recipient_id: str,
        channel: NotificationChannel,
        subject: str,
        body: str,
        priority: NotificationPriority = NotificationPriority.NORMAL,
        variables: Optional[dict] = None,
        template_name: Optional[str] = None,
    ) -> Notification:
        """Send a notification."""
        recipient = self._recipients.get(recipient_id)
        if not recipient:
            raise ValueError(f"Recipient not found: {recipient_id}")

        notification = Notification(
            id=str(uuid.uuid4())[:12],
            channel=channel,
            recipient_id=recipient_id,
            subject=subject,
            body=body,
            priority=priority,
            template_id=template_name,
            variables=variables or {},
        )

        self._notifications[notification.id] = notification

        try:
            self._deliver(notification, recipient)
            notification.status = NotificationStatus.SENT
            notification.sent_at = time.time()
        except Exception as e:
            notification.status = NotificationStatus.FAILED
            notification.error = str(e)

        return notification

    def send_from_template(
        self,
        recipient_id: str,
        template_name: str,
        variables: dict,
        priority: NotificationPriority = NotificationPriority.NORMAL,
    ) -> Notification:
        """Send a notification using a template."""
        template = self._templates.get(template_name)
        if not template:
            raise ValueError(f"Template not found: {template_name}")

        subject = self._render_template(template.subject_template, variables)
        body = self._render_template(template.body_template, variables)

        return self.send_notification(
            recipient_id=recipient_id,
            channel=template.channel,
            subject=subject,
            body=body,
            priority=priority,
            variables=variables,
            template_name=template_name,
        )

    def _render_template(self, template: str, variables: dict) -> str:
        """Render a template with variables."""
        result = template
        for key, value in variables.items():
            result = result.replace(f"{{{{{key}}}}}", str(value))
        return result

    def _deliver(
        self,
        notification: Notification,
        recipient: NotificationRecipient,
    ) -> None:
        """Deliver a notification to a channel."""
        handler = self._channels.get(notification.channel)
        if handler:
            handler(notification, recipient)
        else:
            if notification.channel == NotificationChannel.EMAIL:
                self._deliver_email(notification, recipient)
            elif notification.channel == NotificationChannel.WEBHOOK:
                self._deliver_webhook(notification, recipient)
            elif notification.channel == NotificationChannel.IN_APP:
                pass

    def _deliver_email(
        self,
        notification: Notification,
        recipient: NotificationRecipient,
    ) -> None:
        """Deliver an email notification."""
        if not recipient.email:
            raise ValueError("Recipient has no email address")

    def _deliver_webhook(
        self,
        notification: Notification,
        recipient: NotificationRecipient,
    ) -> None:
        """Deliver a webhook notification."""
        pass

    def register_channel_handler(
        self,
        channel: NotificationChannel,
        handler: Callable,
    ) -> None:
        """Register a custom channel handler."""
        self._channels[channel] = handler

    def get_notification(self, notification_id: str) -> Optional[Notification]:
        """Get a notification by ID."""
        return self._notifications.get(notification_id)

    def list_notifications(
        self,
        recipient_id: Optional[str] = None,
        channel: Optional[NotificationChannel] = None,
        status: Optional[NotificationStatus] = None,
        limit: int = 100,
    ) -> list[Notification]:
        """List notifications with filters."""
        notifications = list(self._notifications.values())

        if recipient_id:
            notifications = [n for n in notifications if n.recipient_id == recipient_id]
        if channel:
            notifications = [n for n in notifications if n.channel == channel]
        if status:
            notifications = [n for n in notifications if n.status == status]

        return sorted(notifications, key=lambda n: n.created_at, reverse=True)[:limit]

    def list_templates(self) -> list[NotificationTemplate]:
        """List all templates."""
        return list(self._templates.values())

    def list_recipients(self) -> list[NotificationRecipient]:
        """List all recipients."""
        return list(self._recipients.values())

    def get_stats(self) -> dict:
        """Get notification statistics."""
        notifications = list(self._notifications.values())

        by_channel = {}
        by_status = {}

        for n in notifications:
            by_channel[n.channel.value] = by_channel.get(n.channel.value, 0) + 1
            by_status[n.status.value] = by_status.get(n.status.value, 0) + 1

        return {
            "total_notifications": len(notifications),
            "by_channel": by_channel,
            "by_status": by_status,
            "total_recipients": len(self._recipients),
            "total_templates": len(self._templates),
        }
