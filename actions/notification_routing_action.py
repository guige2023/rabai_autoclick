"""
Notification Routing Action Module.

Intelligent notification routing with delivery channel management,
throttling, deduplication, personalization, and multi-channel fan-out.

Author: RabAi Team
"""

from __future__ import annotations

import json
import time
import uuid
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set


class NotificationChannel(Enum):
    """Supported notification channels."""
    EMAIL = "email"
    SMS = "sms"
    PUSH = "push"
    WEBHOOK = "webhook"
    SLACK = "slack"
    DISCORD = "discord"
    FEISHU = "feishu"
    DINGTALK = "dingtalk"
    WECHAT = "wechat"
    TELEGRAM = "telegram"


class NotificationPriority(Enum):
    """Notification priority levels."""
    URGENT = 1
    HIGH = 2
    NORMAL = 3
    LOW = 4


class NotificationStatus(Enum):
    """Delivery status."""
    PENDING = "pending"
    SENT = "sent"
    DELIVERED = "delivered"
    FAILED = "failed"
    RATE_LIMITED = "rate_limited"
    DEDUPLICATED = "deduplicated"


@dataclass
class Recipient:
    """Notification recipient with channel preferences."""
    id: str
    name: str
    email: Optional[str] = None
    phone: Optional[str] = None
    user_token: Optional[str] = None
    slack_id: Optional[str] = None
    telegram_id: Optional[str] = None
    feishu_id: Optional[str] = None
    dingtalk_id: Optional[str] = None
    channels: List[NotificationChannel] = field(default_factory=list)
    preferences: Dict[str, Any] = field(default_factory=dict)
    timezone: str = "Asia/Shanghai"
    tags: Set[str] = field(default_factory=set)

    def get_channel_addr(self, channel: NotificationChannel) -> Optional[str]:
        """Get address for a specific channel."""
        mapping = {
            NotificationChannel.EMAIL: self.email,
            NotificationChannel.SMS: self.phone,
            NotificationChannel.PUSH: self.user_token,
            NotificationChannel.SLACK: self.slack_id,
            NotificationChannel.TELEGRAM: self.telegram_id,
            NotificationChannel.FEISHU: self.feishu_id,
            NotificationChannel.DINGTALK: self.dingtalk_id,
        }
        return mapping.get(channel)


@dataclass
class Notification:
    """Notification message."""
    id: str
    title: str
    body: str
    priority: NotificationPriority = NotificationPriority.NORMAL
    channels: List[NotificationChannel] = field(default_factory=list)
    recipients: List[str] = field(default_factory=list)
    tags: Set[str] = field(default_factory=set)
    metadata: Dict[str, Any] = field(default_factory=dict)
    created_at: datetime = field(default_factory=datetime.now)
    scheduled_at: Optional[datetime] = None
    expires_at: Optional[datetime] = None
    deduplication_key: Optional[str] = None
    template_id: Optional[str] = None
    template_vars: Dict[str, str] = field(default_factory=dict)

    @classmethod
    def create(
        cls,
        title: str,
        body: str,
        priority: NotificationPriority = NotificationPriority.NORMAL,
        **kwargs,
    ) -> "Notification":
        return cls(
            id=str(uuid.uuid4()),
            title=title,
            body=body,
            priority=priority,
            **kwargs,
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "title": self.title,
            "body": self.body,
            "priority": self.priority.value,
            "channels": [c.value for c in self.channels],
            "recipients": self.recipients,
            "tags": list(self.tags),
            "metadata": self.metadata,
            "created_at": self.created_at.isoformat(),
            "scheduled_at": self.scheduled_at.isoformat() if self.scheduled_at else None,
            "expires_at": self.expires_at.isoformat() if self.expires_at else None,
            "deduplication_key": self.deduplication_key,
        }


@dataclass
class DeliveryRecord:
    """Record of notification delivery attempt."""
    notification_id: str
    recipient_id: str
    channel: NotificationChannel
    status: NotificationStatus
    sent_at: Optional[datetime] = None
    delivered_at: Optional[datetime] = None
    error: Optional[str] = None
    attempts: int = 0


@dataclass
class RateLimitConfig:
    """Rate limiting configuration per channel."""
    max_per_minute: int = 60
    max_per_hour: int = 1000
    max_per_day: int = 10000
    burst_size: int = 10


class ChannelAdapter(ABC):
    """Abstract base for channel delivery adapters."""

    @abstractmethod
    def send(
        self,
        notification: Notification,
        recipient: Recipient,
    ) -> bool:
        """Send notification to recipient via this channel."""
        pass

    @abstractmethod
    def get_name(self) -> NotificationChannel:
        """Get channel name."""
        pass


class EmailAdapter(ChannelAdapter):
    """Email channel adapter."""

    def get_name(self) -> NotificationChannel:
        return NotificationChannel.EMAIL

    def send(self, notification: Notification, recipient: Recipient) -> bool:
        addr = recipient.get_channel_addr(NotificationChannel.EMAIL)
        if not addr:
            return False
        # Placeholder - integrate with actual email service
        return True


class WebhookAdapter(ChannelAdapter):
    """Webhook channel adapter."""

    def get_name(self) -> NotificationChannel:
        return NotificationChannel.WEBHOOK

    def send(self, notification: Notification, recipient: Recipient) -> bool:
        addr = recipient.get_channel_addr(NotificationChannel.WEBHOOK)
        if not addr:
            return False
        # Placeholder - integrate with actual webhook service
        return True


class NotificationRouter:
    """
    Intelligent notification routing engine.

    Handles multi-channel delivery, rate limiting, deduplication,
    personalization, and delivery tracking.

    Example:
        >>> router = NotificationRouter()
        >>> router.register_adapter(EmailAdapter())
        >>> router.add_recipient(Recipient(id="1", name="User", email="u@example.com"))
        >>> router.send(Notification.create("Hello", "World"))
    """

    def __init__(self):
        self._adapters: Dict[NotificationChannel, ChannelAdapter] = {}
        self._recipients: Dict[str, Recipient] = {}
        self._rate_limits: Dict[NotificationChannel, RateLimitConfig] = {
            ch: RateLimitConfig() for ch in NotificationChannel
        }
        self._rate_counters: Dict[str, List[datetime]] = defaultdict(list)
        self._deduplication_cache: Dict[str, datetime] = {}
        self._delivery_records: Dict[str, DeliveryRecord] = {}
        self._routing_rules: List[Callable] = []
        self._templates: Dict[str, Dict[str, str]] = {}

    def register_adapter(self, adapter: ChannelAdapter) -> None:
        """Register a channel delivery adapter."""
        self._adapters[adapter.get_name()] = adapter

    def add_recipient(self, recipient: Recipient) -> None:
        """Add a recipient."""
        self._recipients[recipient.id] = recipient

    def remove_recipient(self, recipient_id: str) -> bool:
        """Remove a recipient."""
        if recipient_id in self._recipients:
            del self._recipients[recipient_id]
            return True
        return False

    def get_recipient(self, recipient_id: str) -> Optional[Recipient]:
        """Get a recipient by ID."""
        return self._recipients.get(recipient_id)

    def set_rate_limit(
        self,
        channel: NotificationChannel,
        max_per_minute: int,
        max_per_hour: int,
        max_per_day: int,
    ) -> None:
        """Configure rate limits for a channel."""
        self._rate_limits[channel] = RateLimitConfig(
            max_per_minute=max_per_minute,
            max_per_hour=max_per_hour,
            max_per_day=max_per_day,
        )

    def add_template(self, template_id: str, template: Dict[str, str]) -> None:
        """Add a notification template."""
        self._templates[template_id] = template

    def send(
        self,
        notification: Notification,
        recipient_ids: Optional[List[str]] = None,
    ) -> Dict[str, DeliveryRecord]:
        """
        Send notification to recipients.

        Returns:
            Dict mapping recipient_id:channel to DeliveryRecord
        """
        results = {}

        # Apply template if specified
        if notification.template_id and notification.template_id in self._templates:
            template = self._templates[notification.template_id]
            notification.title = template.get("title", notification.title)
            notification.body = template.get("body", notification.body)

        # Personalize
        notification.body = self._personalize(notification)

        target_recipients = recipient_ids or notification.recipients

        for recipient_id in target_recipients:
            recipient = self._recipients.get(recipient_id)
            if not recipient:
                continue

            for channel in notification.channels:
                if channel not in recipient.channels:
                    continue

                record_key = f"{notification.id}:{recipient_id}:{channel.value}"
                record = DeliveryRecord(
                    notification_id=notification.id,
                    recipient_id=recipient_id,
                    channel=channel,
                    status=NotificationStatus.PENDING,
                )

                # Check deduplication
                if notification.deduplication_key:
                    dedup_key = f"{notification.deduplication_key}:{recipient_id}"
                    if self._is_duplicate(dedup_key):
                        record.status = NotificationStatus.DEDUPLICATED
                        results[record_key] = record
                        continue

                # Check rate limit
                if not self._check_rate_limit(channel, recipient_id):
                    record.status = NotificationStatus.RATE_LIMITED
                    results[record_key] = record
                    continue

                # Send via adapter
                adapter = self._adapters.get(channel)
                if not adapter:
                    record.status = NotificationStatus.FAILED
                    record.error = f"No adapter for {channel.value}"
                    results[record_key] = record
                    continue

                try:
                    success = adapter.send(notification, recipient)
                    if success:
                        record.status = NotificationStatus.SENT
                        record.sent_at = datetime.now()
                        self._update_rate_counter(channel, recipient_id)
                    else:
                        record.status = NotificationStatus.FAILED
                        record.error = "Delivery failed"
                except Exception as e:
                    record.status = NotificationStatus.FAILED
                    record.error = str(e)
                    record.attempts += 1

                results[record_key] = record
                self._delivery_records[record_key] = record

        return results

    def send_to_tag(
        self,
        notification: Notification,
        tag: str,
    ) -> Dict[str, DeliveryRecord]:
        """Send notification to all recipients with a specific tag."""
        recipient_ids = [
            rid for rid, r in self._recipients.items()
            if tag in r.tags
        ]
        return self.send(notification, recipient_ids)

    def get_delivery_status(self, notification_id: str) -> Dict[str, Any]:
        """Get delivery status for all recipients of a notification."""
        records = {
            key: record.to_dict() if hasattr(record, 'to_dict') else record.__dict__
            for key, record in self._delivery_records.items()
            if record.notification_id == notification_id
        }
        return {
            "notification_id": notification_id,
            "records": records,
            "total": len(records),
            "sent": sum(1 for r in records.values() if r.get("status") == "sent"),
            "failed": sum(1 for r in records.values() if r.get("status") == "failed"),
        }

    def _personalize(self, notification: Notification) -> str:
        """Personalize notification body with template vars."""
        body = notification.body
        for key, value in notification.template_vars.items():
            body = body.replace(f"{{{key}}}", str(value))
        return body

    def _is_duplicate(self, key: str) -> bool:
        """Check if notification is duplicate within deduplication window."""
        now = datetime.now()
        cutoff = now - timedelta(hours=24)
        if key in self._deduplication_cache:
            if self._deduplication_cache[key] > cutoff:
                return True
        self._deduplication_cache[key] = now
        return False

    def _check_rate_limit(
        self,
        channel: NotificationChannel,
        recipient_id: str,
    ) -> bool:
        """Check if rate limit allows sending."""
        config = self._rate_limits[channel]
        counter_key = f"{channel.value}:{recipient_id}"
        now = datetime.now()
        minute_ago = now - timedelta(minutes=1)

        recent = [t for t in self._rate_counters[counter_key] if t > minute_ago]
        if len(recent) >= config.max_per_minute:
            return False

        self._rate_counters[counter_key] = recent
        return True

    def _update_rate_counter(
        self,
        channel: NotificationChannel,
        recipient_id: str,
    ) -> None:
        """Update rate limit counter."""
        counter_key = f"{channel.value}:{recipient_id}"
        self._rate_counters[counter_key].append(datetime.now())


def create_notification_router() -> NotificationRouter:
    """Factory to create a notification router."""
    router = NotificationRouter()
    router.register_adapter(EmailAdapter())
    router.register_adapter(WebhookAdapter())
    return router
