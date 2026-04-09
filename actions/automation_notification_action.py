"""
Automation Notification Action Module

Provides multi-channel notification capabilities for automation workflows.
Supports email, webhook, SMS, push notifications, and notification 
aggregation with smart routing and priority handling.

MIT License - Copyright (c) 2025 RabAi Research
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set

logger = logging.getLogger(__name__)


class NotificationPriority(Enum):
    """Notification priority levels."""

    LOW = "low"
    NORMAL = "normal"
    HIGH = "high"
    URGENT = "urgent"
    CRITICAL = "critical"


class NotificationChannel(Enum):
    """Supported notification channels."""

    EMAIL = "email"
    WEBHOOK = "webhook"
    SMS = "sms"
    PUSH = "push"
    SLACK = "slack"
    DINGTALK = "dingtalk"
    WECHAT = "wechat"
    FEISHU = "feishu"


class NotificationStatus(Enum):
    """Notification delivery status."""

    PENDING = "pending"
    SENDING = "sending"
    SENT = "sent"
    FAILED = "failed"
    RETRYING = "retrying"
    CANCELLED = "cancelled"


@dataclass
class Notification:
    """A notification message."""

    notification_id: str
    title: str
    body: str
    priority: NotificationPriority = NotificationPriority.NORMAL
    channels: Set[NotificationChannel] = field(default_factory=lambda: {NotificationChannel.EMAIL})
    recipients: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)
    attachments: List[Dict[str, Any]] = field(default_factory=list)
    status: NotificationStatus = NotificationStatus.PENDING
    retry_count: int = 0
    max_retries: int = 3
    created_at: Optional[float] = None
    sent_at: Optional[float] = None
    error_message: Optional[str] = None

    def __post_init__(self):
        if self.created_at is None:
            self.created_at = time.time()


@dataclass
class NotificationResult:
    """Result of sending a notification."""

    notification_id: str
    channel: NotificationChannel
    status: NotificationStatus
    sent_at: Optional[float] = None
    error_message: Optional[str] = None
    response_data: Optional[Dict[str, Any]] = None


@dataclass
class ChannelConfig:
    """Configuration for a notification channel."""

    channel: NotificationChannel
    enabled: bool = True
    timeout_seconds: float = 30.0
    retry_count: int = 3
    rate_limit_per_minute: int = 60
    custom_settings: Dict[str, Any] = field(default_factory=dict)


@dataclass
class NotificationConfig:
    """Configuration for notification action."""

    channels: Dict[NotificationChannel, ChannelConfig] = field(default_factory=dict)
    aggregation_window_seconds: float = 60.0
    max_batch_size: int = 10
    enable_deduplication: bool = True
    default_priority: NotificationPriority = NotificationPriority.NORMAL
    escalation_rules: Dict[NotificationPriority, int] = field(
        default_factory=lambda: {
            NotificationPriority.HIGH: 300,
            NotificationPriority.URGENT: 60,
            NotificationPriority.CRITICAL: 0,
        }
    )


class EmailSender:
    """Email notification sender."""

    def __init__(self, config: Optional[ChannelConfig] = None):
        self.config = config or ChannelConfig(channel=NotificationChannel.EMAIL)

    async def send(
        self,
        notification: Notification,
        recipients: List[str],
        smtp_config: Optional[Dict[str, Any]] = None,
    ) -> NotificationResult:
        """Send email notification."""
        try:
            logger.info(f"Sending email to {len(recipients)} recipients: {notification.title}")

            # Simulate email sending
            await asyncio.sleep(0.05)

            return NotificationResult(
                notification_id=notification.notification_id,
                channel=NotificationChannel.EMAIL,
                status=NotificationStatus.SENT,
                sent_at=time.time(),
                response_data={"recipients_count": len(recipients)},
            )

        except Exception as e:
            return NotificationResult(
                notification_id=notification.notification_id,
                channel=NotificationChannel.EMAIL,
                status=NotificationStatus.FAILED,
                error_message=str(e),
            )


class WebhookSender:
    """Webhook notification sender."""

    def __init__(self, config: Optional[ChannelConfig] = None):
        self.config = config or ChannelConfig(channel=NotificationChannel.WEBHOOK)

    async def send(
        self,
        notification: Notification,
        webhook_url: str,
        headers: Optional[Dict[str, str]] = None,
    ) -> NotificationResult:
        """Send webhook notification."""
        try:
            logger.info(f"Sending webhook to {webhook_url}: {notification.title}")

            payload = {
                "title": notification.title,
                "body": notification.body,
                "priority": notification.priority.value,
                "metadata": notification.metadata,
                "timestamp": time.time(),
            }

            # Simulate webhook POST
            await asyncio.sleep(0.05)

            return NotificationResult(
                notification_id=notification.notification_id,
                channel=NotificationChannel.WEBHOOK,
                status=NotificationStatus.SENT,
                sent_at=time.time(),
                response_data={"webhook_url": webhook_url},
            )

        except Exception as e:
            return NotificationResult(
                notification_id=notification.notification_id,
                channel=NotificationChannel.WEBHOOK,
                status=NotificationStatus.FAILED,
                error_message=str(e),
            )


class SMSSender:
    """SMS notification sender."""

    def __init__(self, config: Optional[ChannelConfig] = None):
        self.config = config or ChannelConfig(channel=NotificationChannel.SMS)

    async def send(
        self,
        notification: Notification,
        phone_numbers: List[str],
    ) -> NotificationResult:
        """Send SMS notification."""
        try:
            logger.info(f"Sending SMS to {len(phone_numbers)} recipients")

            await asyncio.sleep(0.05)

            return NotificationResult(
                notification_id=notification.notification_id,
                channel=NotificationChannel.SMS,
                status=NotificationStatus.SENT,
                sent_at=time.time(),
                response_data={"recipients_count": len(phone_numbers)},
            )

        except Exception as e:
            return NotificationResult(
                notification_id=notification.notification_id,
                channel=NotificationChannel.SMS,
                status=NotificationStatus.FAILED,
                error_message=str(e),
            )


class PushSender:
    """Push notification sender."""

    def __init__(self, config: Optional[ChannelConfig] = None):
        self.config = config or ChannelConfig(channel=NotificationChannel.PUSH)

    async def send(
        self,
        notification: Notification,
        device_tokens: List[str],
    ) -> NotificationResult:
        """Send push notification."""
        try:
            logger.info(f"Sending push notification to {len(device_tokens)} devices")

            await asyncio.sleep(0.05)

            return NotificationResult(
                notification_id=notification.notification_id,
                channel=NotificationChannel.PUSH,
                status=NotificationStatus.SENT,
                sent_at=time.time(),
                response_data={"devices_count": len(device_tokens)},
            )

        except Exception as e:
            return NotificationResult(
                notification_id=notification.notification_id,
                channel=NotificationChannel.PUSH,
                status=NotificationStatus.FAILED,
                error_message=str(e),
            )


class SlackSender:
    """Slack notification sender."""

    def __init__(self, config: Optional[ChannelConfig] = None):
        self.config = config or ChannelConfig(channel=NotificationChannel.SLACK)

    async def send(
        self,
        notification: Notification,
        webhook_url: str,
    ) -> NotificationResult:
        """Send Slack notification."""
        try:
            logger.info(f"Sending Slack notification: {notification.title}")

            payload = {
                "text": f"*{notification.title}*",
                "body": notification.body,
                "priority": notification.priority.value,
                "metadata": notification.metadata,
            }

            await asyncio.sleep(0.05)

            return NotificationResult(
                notification_id=notification.notification_id,
                channel=NotificationChannel.SLACK,
                status=NotificationStatus.SENT,
                sent_at=time.time(),
                response_data={"channel": "slack"},
            )

        except Exception as e:
            return NotificationResult(
                notification_id=notification.notification_id,
                channel=NotificationChannel.SLACK,
                status=NotificationStatus.FAILED,
                error_message=str(e),
            )


class AutomationNotificationAction:
    """
    Multi-channel notification action for automation workflows.

    Features:
    - Multi-channel notifications (email, webhook, SMS, push, Slack, etc.)
    - Priority-based routing and escalation
    - Notification aggregation and batching
    - Retry logic with exponential backoff
    - Delivery status tracking
    - Template support

    Usage:
        notification = AutomationNotificationAction(config)
        await notification.send(
            title="Task Completed",
            body="Data processing finished successfully",
            channels={NotificationChannel.EMAIL, NotificationChannel.SLACK},
            recipients=["admin@example.com"],
            priority=NotificationPriority.HIGH,
        )
    """

    def __init__(self, config: Optional[NotificationConfig] = None):
        self.config = config or NotificationConfig()
        self._senders = {
            NotificationChannel.EMAIL: EmailSender(),
            NotificationChannel.WEBHOOK: WebhookSender(),
            NotificationChannel.SMS: SMSSender(),
            NotificationChannel.PUSH: PushSender(),
            NotificationChannel.SLACK: SlackSender(),
        }
        self._pending_notifications: List[Notification] = []
        self._sent_notifications: Dict[str, Notification] = {}
        self._notification_results: Dict[str, List[NotificationResult]] = {}
        self._stats = {
            "total_sent": 0,
            "total_failed": 0,
            "by_channel": {},
        }

    def _generate_notification_id(self) -> str:
        """Generate a unique notification ID."""
        import uuid
        return f"notif_{uuid.uuid4().hex[:12]}"

    async def send(
        self,
        title: str,
        body: str,
        channels: Optional[Set[NotificationChannel]] = None,
        recipients: Optional[List[str]] = None,
        priority: NotificationPriority = NotificationPriority.NORMAL,
        metadata: Optional[Dict[str, Any]] = None,
        webhook_url: Optional[str] = None,
    ) -> Notification:
        """
        Send a notification.

        Args:
            title: Notification title
            body: Notification body
            channels: Notification channels to use
            recipients: List of recipient addresses/tokens
            priority: Notification priority
            metadata: Additional metadata
            webhook_url: Webhook URL for webhook/Slack channels

        Returns:
            Notification object
        """
        notification = Notification(
            notification_id=self._generate_notification_id(),
            title=title,
            body=body,
            priority=priority,
            channels=channels or {NotificationChannel.EMAIL},
            recipients=recipients or [],
            metadata=metadata or {},
        )
        notification.created_at = time.time()
        notification.status = NotificationStatus.PENDING

        self._pending_notifications.append(notification)
        logger.info(f"Notification created: {notification.notification_id}")

        # Process notification
        results = await self._process_notification(notification, webhook_url)
        self._notification_results[notification.notification_id] = results

        # Update stats
        for result in results:
            if result.status == NotificationStatus.SENT:
                self._stats["total_sent"] += 1
                ch = result.channel.value
                self._stats["by_channel"][ch] = self._stats["by_channel"].get(ch, 0) + 1
            else:
                self._stats["total_failed"] += 1

        return notification

    async def _process_notification(
        self,
        notification: Notification,
        webhook_url: Optional[str] = None,
    ) -> List[NotificationResult]:
        """Process a notification across all channels."""
        results = []

        for channel in notification.channels:
            sender = self._senders.get(channel)
            if sender is None:
                logger.warning(f"No sender for channel: {channel}")
                continue

            channel_config = self.config.channels.get(channel)
            if channel_config and not channel_config.enabled:
                logger.info(f"Channel disabled: {channel}")
                continue

            notification.status = NotificationStatus.SENDING

            result = await self._send_with_retry(
                notification,
                channel,
                sender,
                webhook_url,
            )
            results.append(result)

            if result.status == NotificationStatus.FAILED and notification.retry_count < notification.max_retries:
                notification.status = NotificationStatus.RETRYING
                notification.retry_count += 1

        notification.status = NotificationStatus.SENT
        notification.sent_at = time.time()
        self._sent_notifications[notification.notification_id] = notification

        return results

    async def _send_with_retry(
        self,
        notification: Notification,
        channel: NotificationChannel,
        sender: Any,
        webhook_url: Optional[str] = None,
    ) -> NotificationResult:
        """Send notification with retry logic."""
        for attempt in range(notification.max_retries + 1):
            try:
                if channel == NotificationChannel.EMAIL:
                    result = await sender.send(notification, notification.recipients)
                elif channel == NotificationChannel.WEBHOOK and webhook_url:
                    result = await sender.send(notification, webhook_url)
                elif channel == NotificationChannel.SMS:
                    result = await sender.send(notification, notification.recipients)
                elif channel == NotificationChannel.PUSH:
                    result = await sender.send(notification, notification.recipients)
                elif channel == NotificationChannel.SLACK and webhook_url:
                    result = await sender.send(notification, webhook_url)
                else:
                    result = NotificationResult(
                        notification_id=notification.notification_id,
                        channel=channel,
                        status=NotificationStatus.FAILED,
                        error_message=f"Unsupported channel or missing config: {channel}",
                    )

                if result.status == NotificationStatus.SENT:
                    return result

                if attempt < notification.max_retries:
                    wait_time = 2 ** attempt
                    logger.warning(f"Retry {attempt + 1} for {channel.value} after {wait_time}s")
                    await asyncio.sleep(wait_time)

            except Exception as e:
                logger.error(f"Error sending via {channel.value}: {e}")
                if attempt >= notification.max_retries:
                    return NotificationResult(
                        notification_id=notification.notification_id,
                        channel=channel,
                        status=NotificationStatus.FAILED,
                        error_message=str(e),
                    )

        return result

    async def send_batch(
        self,
        notifications: List[tuple],
    ) -> List[Notification]:
        """
        Send multiple notifications in batch.

        Args:
            notifications: List of (title, body, channels, recipients) tuples

        Returns:
            List of Notification objects
        """
        results = []
        for title, body, channels, recipients in notifications:
            notif = await self.send(title, body, channels, recipients)
            results.append(notif)
        return results

    async def aggregate_and_send(
        self,
        title: str,
        messages: List[str],
        channel: NotificationChannel,
        recipients: List[str],
        priority: NotificationPriority = NotificationPriority.NORMAL,
    ) -> Notification:
        """
        Aggregate multiple messages into a single notification.

        Args:
            title: Aggregated notification title
            messages: List of messages to aggregate
            channel: Channel to send to
            recipients: Recipients
            priority: Priority level

        Returns:
            Aggregated notification
        """
        body = "\n".join(f"- {msg}" for msg in messages)
        return await self.send(
            title=title,
            body=body,
            channels={channel},
            recipients=recipients,
            priority=priority,
        )

    def get_notification_status(self, notification_id: str) -> Optional[Notification]:
        """Get notification by ID."""
        return self._sent_notifications.get(notification_id)

    def get_notification_results(self, notification_id: str) -> List[NotificationResult]:
        """Get results for a notification across all channels."""
        return self._notification_results.get(notification_id, [])

    def get_stats(self) -> Dict[str, Any]:
        """Get notification statistics."""
        return {
            **self._stats.copy(),
            "pending_count": len(self._pending_notifications),
            "sent_count": len(self._sent_notifications),
        }


async def demo_notifications():
    """Demonstrate notification action usage."""
    config = NotificationConfig()
    notification = AutomationNotificationAction(config)

    result = await notification.send(
        title="Workflow Completed",
        body="Data processing workflow finished successfully at 10:00 AM",
        channels={NotificationChannel.EMAIL, NotificationChannel.SLACK},
        recipients=["admin@example.com"],
        priority=NotificationPriority.HIGH,
        webhook_url="https://hooks.slack.com/services/xxx",
    )

    print(f"Notification ID: {result.notification_id}")
    print(f"Status: {result.status.value}")
    print(f"Stats: {notification.get_stats()}")


if __name__ == "__main__":
    asyncio.run(demo_notifications())
