"""
Webhook handler module for receiving and processing webhooks.

Supports signature verification, retry handling, event queuing,
and multiple delivery strategies.
"""
from __future__ import annotations

import hashlib
import hmac
import json
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Optional


class DeliveryStatus(Enum):
    """Webhook delivery status."""
    PENDING = "pending"
    DELIVERED = "delivered"
    FAILED = "failed"
    RETRYING = "retrying"


@dataclass
class WebhookEvent:
    """A webhook event."""
    id: str
    webhook_id: str
    event_type: str
    payload: dict
    headers: dict = field(default_factory=dict)
    received_at: float = field(default_factory=time.time)
    delivery_status: DeliveryStatus = DeliveryStatus.PENDING
    delivery_attempts: int = 0
    last_delivery_at: Optional[float] = None
    response_status: Optional[int] = None
    response_body: Optional[str] = None


@dataclass
class Webhook:
    """A webhook subscription."""
    id: str
    url: str
    event_types: list[str]
    secret: str = ""
    enabled: bool = True
    filter_function: Optional[Callable] = None
    metadata: dict = field(default_factory=dict)
    created_at: float = field(default_factory=time.time)


@dataclass
class DeliveryAttempt:
    """A webhook delivery attempt."""
    event_id: str
    attempt_number: int
    status: DeliveryStatus
    response_status: Optional[int] = None
    response_body: Optional[str] = None
    error: Optional[str] = None
    timestamp: float = field(default_factory=time.time)
    duration_ms: float = 0.0


class WebhookHandler:
    """
    Webhook handler for receiving and processing webhooks.

    Supports signature verification, retry handling, event queuing,
    and multiple delivery strategies.
    """

    def __init__(self):
        self._webhooks: dict[str, Webhook] = {}
        self._events: dict[str, WebhookEvent] = {}
        self._delivery_history: list[DeliveryAttempt] = []
        self._handlers: dict[str, Callable] = {}

    def register_webhook(
        self,
        url: str,
        event_types: list[str],
        secret: str = "",
        webhook_id: Optional[str] = None,
        filter_function: Optional[Callable] = None,
        metadata: Optional[dict] = None,
    ) -> Webhook:
        """Register a webhook subscription."""
        webhook = Webhook(
            id=webhook_id or str(uuid.uuid4())[:12],
            url=url,
            event_types=event_types,
            secret=secret,
            filter_function=filter_function,
            metadata=metadata or {},
        )

        self._webhooks[webhook.id] = webhook
        return webhook

    def remove_webhook(self, webhook_id: str) -> bool:
        """Remove a webhook subscription."""
        if webhook_id in self._webhooks:
            del self._webhooks[webhook_id]
            return True
        return False

    def receive_event(
        self,
        webhook_id: str,
        event_type: str,
        payload: dict,
        headers: Optional[dict] = None,
        signature: Optional[str] = None,
    ) -> WebhookEvent:
        """Receive and process an incoming webhook event."""
        webhook = self._webhooks.get(webhook_id)
        if not webhook:
            raise ValueError(f"Webhook not found: {webhook_id}")

        if event_type not in webhook.event_types:
            raise ValueError(f"Event type not supported: {event_type}")

        if signature and webhook.secret:
            if not self._verify_signature(payload, signature, webhook.secret):
                raise ValueError("Invalid webhook signature")

        if webhook.filter_function:
            if not webhook.filter_function(payload):
                raise ValueError("Event filtered by webhook filter")

        event = WebhookEvent(
            id=str(uuid.uuid4())[:12],
            webhook_id=webhook_id,
            event_type=event_type,
            payload=payload,
            headers=headers or {},
        )

        self._events[event.id] = event
        return event

    def _verify_signature(self, payload: dict, signature: str, secret: str) -> bool:
        """Verify webhook signature."""
        payload_bytes = json.dumps(payload, sort_keys=True).encode("utf-8")

        expected = hmac.new(
            secret.encode("utf-8"),
            payload_bytes,
            hashlib.sha256,
        ).hexdigest()

        return hmac.compare_digest(f"sha256={expected}", signature)

    def process_event(
        self,
        event_id: str,
        handler: Optional[Callable] = None,
    ) -> DeliveryStatus:
        """Process a webhook event."""
        event = self._events.get(event_id)
        if not event:
            raise ValueError(f"Event not found: {event_id}")

        webhook = self._webhooks.get(event.webhook_id)
        if not webhook or not webhook.enabled:
            event.delivery_status = DeliveryStatus.FAILED
            return event.delivery_status

        handler = handler or self._handlers.get(event.event_type)
        if not handler:
            event.delivery_status = DeliveryStatus.FAILED
            return event.delivery_status

        try:
            result = handler(event.payload)
            event.delivery_status = DeliveryStatus.DELIVERED
            event.delivery_attempts += 1
            event.last_delivery_at = time.time()
            return event.delivery_status

        except Exception as e:
            event.delivery_status = DeliveryStatus.FAILED
            event.delivery_attempts += 1
            event.last_delivery_at = time.time()
            return event.delivery_status

    def retry_event(
        self,
        event_id: str,
        max_attempts: int = 3,
    ) -> DeliveryStatus:
        """Retry a failed webhook event."""
        event = self._events.get(event_id)
        if not event:
            raise ValueError(f"Event not found: {event_id}")

        if event.delivery_attempts >= max_attempts:
            event.delivery_status = DeliveryStatus.FAILED
            return event.delivery_status

        event.delivery_status = DeliveryStatus.RETRYING
        event.delivery_attempts += 1

        webhook = self._webhooks.get(event.webhook_id)
        handler = self._handlers.get(event.event_type)

        if webhook and webhook.enabled and handler:
            try:
                handler(event.payload)
                event.delivery_status = DeliveryStatus.DELIVERED
            except Exception:
                event.delivery_status = DeliveryStatus.FAILED
        else:
            event.delivery_status = DeliveryStatus.FAILED

        event.last_delivery_at = time.time()
        return event.delivery_status

    def register_handler(self, event_type: str, handler: Callable) -> None:
        """Register a handler for an event type."""
        self._handlers[event_type] = handler

    def get_event(self, event_id: str) -> Optional[WebhookEvent]:
        """Get an event by ID."""
        return self._events.get(event_id)

    def list_events(
        self,
        webhook_id: Optional[str] = None,
        event_type: Optional[str] = None,
        status: Optional[DeliveryStatus] = None,
        limit: int = 100,
    ) -> list[WebhookEvent]:
        """List webhook events."""
        events = list(self._events.values())

        if webhook_id:
            events = [e for e in events if e.webhook_id == webhook_id]
        if event_type:
            events = [e for e in events if e.event_type == event_type]
        if status:
            events = [e for e in events if e.delivery_status == status]

        return sorted(events, key=lambda e: e.received_at, reverse=True)[:limit]

    def list_webhooks(self, event_type: Optional[str] = None) -> list[Webhook]:
        """List webhook subscriptions."""
        webhooks = list(self._webhooks.values())

        if event_type:
            webhooks = [w for w in webhooks if event_type in w.event_types]

        return webhooks

    def enable_webhook(self, webhook_id: str) -> bool:
        """Enable a webhook."""
        webhook = self._webhooks.get(webhook_id)
        if webhook:
            webhook.enabled = True
            return True
        return False

    def disable_webhook(self, webhook_id: str) -> bool:
        """Disable a webhook."""
        webhook = self._webhooks.get(webhook_id)
        if webhook:
            webhook.enabled = False
            return True
        return False

    def get_webhook_stats(self, webhook_id: str) -> dict:
        """Get statistics for a webhook."""
        webhook = self._webhooks.get(webhook_id)
        if not webhook:
            return {}

        events = [e for e in self._events.values() if e.webhook_id == webhook_id]

        return {
            "webhook_id": webhook_id,
            "url": webhook.url,
            "enabled": webhook.enabled,
            "total_events": len(events),
            "pending_events": sum(1 for e in events if e.delivery_status == DeliveryStatus.PENDING),
            "delivered_events": sum(1 for e in events if e.delivery_status == DeliveryStatus.DELIVERED),
            "failed_events": sum(1 for e in events if e.delivery_status == DeliveryStatus.FAILED),
        }
