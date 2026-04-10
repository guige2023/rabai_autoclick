"""
Webhook management for external integrations.
"""

import hashlib
import hmac
import json
import logging
import secrets
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Callable, Optional

import httpx

logger = logging.getLogger(__name__)


class DeliveryStatus(Enum):
    PENDING = "pending"
    SUCCESS = "success"
    FAILED = "failed"
    RETRYING = "retrying"


class EventFilterMode(Enum):
    INCLUDE = "include"
    EXCLUDE = "exclude"


@dataclass
class WebhookPayloadTemplate:
    """Template for webhook payloads with variable substitution."""

    name: str
    template: dict[str, Any]
    default_headers: dict[str, str] = field(default_factory=dict)


@dataclass
class Webhook:
    """Represents a registered webhook."""

    id: str
    url: str
    events: list[str]
    secret: str
    template: Optional[WebhookPayloadTemplate] = None
    filter_mode: EventFilterMode = EventFilterMode.INCLUDE
    filter_patterns: list[str] = field(default_factory=list)
    batch_size: int = 1
    batch_timeout_seconds: float = 5.0
    rate_limit_rpm: int = 60
    is_active: bool = True
    created_at: datetime = field(default_factory=datetime.utcnow)
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class DeliveryRecord:
    """Record of a webhook delivery attempt."""

    id: str
    webhook_id: str
    event: str
    payload: dict[str, Any]
    status: DeliveryStatus
    attempts: int = 0
    max_attempts: int = 3
    response_status: Optional[int] = None
    response_body: Optional[str] = None
    error_message: Optional[str] = None
    created_at: datetime = field(default_factory=datetime.utcnow)
    delivered_at: Optional[datetime] = None


class RateLimiter:
    """Token bucket rate limiter for webhook deliveries."""

    def __init__(self, requests_per_minute: int):
        self.requests_per_minute = requests_per_minute
        self.tokens = requests_per_minute
        self.last_refill = time.monotonic()
        self._lock = None

    def _refill(self) -> None:
        """Refill tokens based on elapsed time."""
        now = time.monotonic()
        elapsed = now - self.last_refill
        refill_amount = elapsed * (self.requests_per_minute / 60.0)
        self.tokens = min(self.requests_per_minute, self.tokens + refill_amount)
        self.last_refill = now

    async def acquire(self) -> bool:
        """Acquire a token, waiting if necessary. Returns True when allowed."""
        while True:
            self._refill()
            if self.tokens >= 1:
                self.tokens -= 1
                return True
            wait_time = (1 - self.tokens) * (60.0 / self.requests_per_minute)
            time.sleep(min(wait_time, 0.1))


class BatchQueue:
    """Queues events for batch delivery."""

    def __init__(self, webhook_id: str, batch_size: int, timeout_seconds: float):
        self.webhook_id = webhook_id
        self.batch_size = batch_size
        self.timeout_seconds = timeout_seconds
        self.events: list[dict[str, Any]] = []
        self.first_event_time: Optional[float] = None
        self.payload_template: Optional[WebhookPayloadTemplate] = None

    def add(self, event: dict[str, Any], payload_template: Optional[WebhookPayloadTemplate] = None) -> bool:
        """Add event to batch. Returns True if batch is ready for delivery."""
        if self.first_event_time is None:
            self.first_event_time = time.monotonic()
            self.payload_template = payload_template

        self.events.append(event)

        if len(self.events) >= self.batch_size:
            return True

        elapsed = time.monotonic() - self.first_event_time
        if elapsed >= self.timeout_seconds and self.events:
            return True

        return False

    def clear(self) -> list[dict[str, Any]]:
        """Clear the queue and return all events."""
        events = self.events
        self.events = []
        self.first_event_time = None
        return events


class WebhookManager:
    """
    Manages webhooks for workflow events with registration, delivery, signing,
    filtering, batching, testing, tracking, templates, secret rotation, and rate limiting.
    """

    def __init__(self, http_client: Optional[httpx.AsyncClient] = None):
        self._webhooks: dict[str, Webhook] = {}
        self._delivery_records: dict[str, DeliveryRecord] = {}
        self._batch_queues: dict[str, BatchQueue] = {}
        self._http_client = http_client or httpx.AsyncClient(timeout=30.0)
        self._event_handlers: dict[str, list[Callable]] = {}
        self._secret_versions: dict[str, list[str]] = {}

    def register_webhook(
        self,
        url: str,
        events: list[str],
        secret: Optional[str] = None,
        template: Optional[WebhookPayloadTemplate] = None,
        filter_mode: EventFilterMode = EventFilterMode.INCLUDE,
        filter_patterns: Optional[list[str]] = None,
        batch_size: int = 1,
        batch_timeout_seconds: float = 5.0,
        rate_limit_rpm: int = 60,
        metadata: Optional[dict[str, Any]] = None,
    ) -> Webhook:
        """
        Register a new webhook for the specified events.

        Args:
            url: Target URL for webhook deliveries
            events: List of event types to subscribe to
            secret: Secret key for HMAC signing (auto-generated if not provided)
            template: Optional payload template
            filter_mode: Include or exclude mode for filtering
            filter_patterns: Patterns to match for filtering
            batch_size: Number of events to batch together
            batch_timeout_seconds: Max time to wait before delivering a batch
            rate_limit_rpm: Rate limit for this webhook's target server
            metadata: Additional metadata to store with the webhook

        Returns:
            The registered Webhook object
        """
        webhook_id = str(uuid.uuid4())
        webhook_secret = secret or secrets.token_hex(32)

        webhook = Webhook(
            id=webhook_id,
            url=url,
            events=events,
            secret=webhook_secret,
            template=template,
            filter_mode=filter_mode,
            filter_patterns=filter_patterns or [],
            batch_size=batch_size,
            batch_timeout_seconds=batch_timeout_seconds,
            rate_limit_rpm=rate_limit_rpm,
            metadata=metadata or {},
        )

        self._webhooks[webhook_id] = webhook
        self._secret_versions[webhook_id] = [webhook_secret]
        self._batch_queues[webhook_id] = BatchQueue(webhook_id, batch_size, batch_timeout_seconds)

        logger.info(f"Registered webhook {webhook_id} for events: {events}")
        return webhook

    def unregister_webhook(self, webhook_id: str) -> bool:
        """Unregister a webhook."""
        if webhook_id in self._webhooks:
            del self._webhooks[webhook_id]
            if webhook_id in self._batch_queues:
                del self._batch_queues[webhook_id]
            logger.info(f"Unregistered webhook {webhook_id}")
            return True
        return False

    def get_webhook(self, webhook_id: str) -> Optional[Webhook]:
        """Get a webhook by ID."""
        return self._webhooks.get(webhook_id)

    def list_webhooks(self, event_filter: Optional[str] = None) -> list[Webhook]:
        """List all webhooks, optionally filtered by event type."""
        webhooks = list(self._webhooks.values())
        if event_filter:
            webhooks = [w for w in webhooks if event_filter in w.events]
        return webhooks

    def sign_payload(self, payload: dict[str, Any], secret: str, timestamp: Optional[int] = None) -> dict[str, str]:
        """
        Generate HMAC signature for a webhook payload.

        Args:
            payload: The payload to sign
            secret: The secret key to use for signing
            timestamp: Optional Unix timestamp (defaults to current time)

        Returns:
            Dictionary with signature headers
        """
        if timestamp is None:
            timestamp = int(time.time())

        payload_bytes = json.dumps(payload, sort_keys=True).encode()
        timestamp_bytes = str(timestamp).encode()

        signed_payload = f"{timestamp}.{payload_bytes.decode()}".encode()
        signature = hmac.new(secret.encode(), signed_payload, hashlib.sha256).hexdigest()

        return {
            "X-Webhook-Signature": signature,
            "X-Webhook-Timestamp": str(timestamp),
            "X-Webhook-ID": str(uuid.uuid4()),
        }

    def verify_signature(
        self,
        payload: dict[str, Any],
        signature: str,
        secret: str,
        timestamp: int,
        tolerance_seconds: int = 300,
    ) -> bool:
        """
        Verify the HMAC signature of a webhook payload.

        Args:
            payload: The payload to verify
            signature: The signature to verify against
            secret: The secret key used for signing
            timestamp: Unix timestamp from the webhook
            tolerance_seconds: Max age of the webhook to accept

        Returns:
            True if signature is valid and timestamp is within tolerance
        """
        current_time = int(time.time())
        if abs(current_time - timestamp) > tolerance_seconds:
            logger.warning(f"Webhook timestamp outside tolerance: {timestamp}")
            return False

        payload_bytes = json.dumps(payload, sort_keys=True).encode()
        signed_payload = f"{timestamp}.{payload_bytes.decode()}".encode()
        expected_signature = hmac.new(secret.encode(), signed_payload, hashlib.sha256).hexdigest()

        return hmac.compare_digest(signature, expected_signature)

    def should_deliver_event(self, webhook: Webhook, event: str, event_data: dict[str, Any]) -> bool:
        """
        Determine if an event should trigger a webhook delivery based on filters.

        Args:
            webhook: The webhook to check
            event: The event type
            event_data: The event payload data

        Returns:
            True if the event should be delivered
        """
        if not webhook.is_active:
            return False

        if event not in webhook.events:
            return False

        if not webhook.filter_patterns:
            return True

        event_str = json.dumps(event_data, sort_keys=True)

        matches = any(pattern in event_str for pattern in webhook.filter_patterns)

        if webhook.filter_mode == EventFilterMode.INCLUDE:
            return matches
        else:
            return not matches

    def apply_template(self, template: WebhookPayloadTemplate, event: str, event_data: dict[str, Any]) -> dict[str, Any]:
        """
        Apply a payload template to an event, substituting variables.

        Args:
            template: The template to apply
            event: The event type
            event_data: The raw event data

        Returns:
            Templated payload
        """
        result = {"event": event, "timestamp": datetime.utcnow().isoformat(), "data": {}}

        for key, value in template.template.items():
            if isinstance(value, str):
                if value.startswith("${") and value.endswith("}"):
                    path = value[2:-1]
                    result["data"][key] = event_data.get(path, value)
                else:
                    result["data"][key] = value
            else:
                result["data"][key] = value

        return result

    def _create_delivery_record(self, webhook: Webhook, event: str, payload: dict[str, Any]) -> DeliveryRecord:
        """Create a delivery record for tracking."""
        record_id = str(uuid.uuid4())
        record = DeliveryRecord(
            id=record_id,
            webhook_id=webhook.id,
            event=event,
            payload=payload,
            status=DeliveryStatus.PENDING,
        )
        self._delivery_records[record_id] = record
        return record

    async def _deliver_webhook(
        self,
        webhook: Webhook,
        payload: dict[str, Any],
        record: DeliveryRecord,
        retry_count: int = 0,
    ) -> DeliveryRecord:
        """Deliver a webhook payload with retry logic."""
        headers = {"Content-Type": "application/json", "User-Agent": "Rabai-Webhook/1.0"}

        if webhook.template:
            headers.update(webhook.template.default_headers)

        signature_headers = self.sign_payload(payload, webhook.secret)
        headers.update(signature_headers)

        try:
            response = await self._http_client.post(webhook.url, json=payload, headers=headers)
            record.response_status = response.status_code
            record.response_body = response.text[:1000] if response.text else None
            record.attempts += 1

            if 200 <= response.status_code < 300:
                record.status = DeliveryStatus.SUCCESS
                record.delivered_at = datetime.utcnow()
                logger.info(f"Webhook {webhook.id} delivered successfully")
            else:
                record.status = DeliveryStatus.FAILED
                record.error_message = f"HTTP {response.status_code}"
                logger.warning(f"Webhook {webhook.id} failed with status {response.status_code}")

        except httpx.TimeoutException as e:
            record.status = DeliveryStatus.RETRYING
            record.error_message = f"Timeout: {e}"
            record.attempts += 1
            logger.warning(f"Webhook {webhook.id} timed out, attempt {record.attempts}")

        except httpx.RequestError as e:
            record.status = DeliveryStatus.RETRYING
            record.error_message = f"Request error: {e}"
            record.attempts += 1
            logger.warning(f"Webhook {webhook.id} request error: {e}")

        return record

    async def _retry_delivery(self, webhook: Webhook, record: DeliveryRecord) -> DeliveryRecord:
        """Retry delivery with exponential backoff."""
        if record.attempts >= record.max_attempts:
            record.status = DeliveryStatus.FAILED
            return record

        record.status = DeliveryStatus.RETRYING
        base_delay = 2 ** record.attempts
        await asyncio.sleep(min(base_delay, 30))

        return await self._deliver_webhook(webhook, record.payload, record, record.attempts)

    async def deliver_event(
        self,
        event: str,
        event_data: dict[str, Any],
        webhook_ids: Optional[list[str]] = None,
    ) -> list[DeliveryRecord]:
        """
        Deliver an event to matching webhooks.

        Args:
            event: The event type
            event_data: The event payload
            webhook_ids: Optional list of specific webhook IDs to deliver to

        Returns:
            List of delivery records
        """
        records = []

        webhooks = []
        if webhook_ids:
            for wid in webhook_ids:
                if wid in self._webhooks:
                    webhooks.append(self._webhooks[wid])
        else:
            webhooks = [w for w in self._webhooks.values() if w.is_active]

        for webhook in webhooks:
            if not self.should_deliver_event(webhook, event, event_data):
                continue

            payload = event_data
            if webhook.template:
                payload = self.apply_template(webhook.template, event, event_data)

            record = self._create_delivery_record(webhook, event, payload)

            rate_limiter = RateLimiter(webhook.rate_limit_rpm)
            await rate_limiter.acquire()

            record = await self._deliver_webhook(webhook, payload, record)

            if record.status != DeliveryStatus.SUCCESS:
                record = await self._retry_delivery(webhook, record)

            records.append(record)

        return records

    async def batch_deliver_events(
        self,
        events: list[tuple[str, dict[str, Any]]],
        webhook_id: str,
    ) -> Optional[DeliveryRecord]:
        """
        Batch multiple events into a single delivery.

        Args:
            events: List of (event_type, event_data) tuples
            webhook_id: The webhook to deliver to

        Returns:
            Delivery record or None if webhook not found
        """
        webhook = self._webhooks.get(webhook_id)
        if not webhook:
            return None

        batch_payload = {
            "batch": True,
            "count": len(events),
            "timestamp": datetime.utcnow().isoformat(),
            "events": [],
        }

        for event, event_data in events:
            event_payload = event_data
            if webhook.template:
                event_payload = self.apply_template(webhook.template, event, event_data)
            batch_payload["events"].append({"event": event, "data": event_payload})

        record = self._create_delivery_record(webhook, "batch", batch_payload)

        rate_limiter = RateLimiter(webhook.rate_limit_rpm)
        await rate_limiter.acquire()

        record = await self._deliver_webhook(webhook, batch_payload, record)

        if record.status != DeliveryStatus.SUCCESS:
            record = await self._retry_delivery(webhook, record)

        return record

    def queue_event_for_batching(self, webhook_id: str, event: str, event_data: dict[str, Any]) -> bool:
        """
        Queue an event for batch delivery. Returns True if batch is ready.

        Args:
            webhook_id: The webhook to queue for
            event: The event type
            event_data: The event data

        Returns:
            True if the batch is ready for delivery
        """
        if webhook_id not in self._batch_queues:
            webhook = self._webhooks.get(webhook_id)
            if not webhook:
                return False
            self._batch_queues[webhook_id] = BatchQueue(
                webhook_id, webhook.batch_size, webhook.batch_timeout_seconds
            )

        queue = self._batch_queues[webhook_id]
        webhook = self._webhooks[webhook_id]
        return queue.add({"event": event, "data": event_data}, webhook.template)

    def flush_batch_queue(self, webhook_id: str) -> list[dict[str, Any]]:
        """Flush and return all queued events for a webhook."""
        if webhook_id in self._batch_queues:
            return self._batch_queues[webhook_id].clear()
        return []

    async def test_webhook(
        self,
        webhook_id: str,
        event: Optional[str] = None,
        test_data: Optional[dict[str, Any]] = None,
    ) -> DeliveryRecord:
        """
        Test a webhook with a synthetic event.

        Args:
            webhook_id: The webhook to test
            event: Optional event type (defaults to "test")
            test_data: Optional test payload data

        Returns:
            Delivery record from the test delivery
        """
        webhook = self._webhooks.get(webhook_id)
        if not webhook:
            raise ValueError(f"Webhook {webhook_id} not found")

        test_event = event or "test"
        test_payload = test_data or {
            "test": True,
            "message": "This is a synthetic test event",
            "webhook_id": webhook_id,
            "timestamp": datetime.utcnow().isoformat(),
        }

        payload = test_payload
        if webhook.template:
            payload = self.apply_template(webhook.template, test_event, test_payload)

        record = self._create_delivery_record(webhook, test_event, payload)
        record.max_attempts = 1

        rate_limiter = RateLimiter(webhook.rate_limit_rpm)
        await rate_limiter.acquire()

        return await self._deliver_webhook(webhook, payload, record)

    def get_delivery_record(self, record_id: str) -> Optional[DeliveryRecord]:
        """Get a delivery record by ID."""
        return self._delivery_records.get(record_id)

    def get_webhook_delivery_history(
        self,
        webhook_id: str,
        limit: int = 100,
    ) -> list[DeliveryRecord]:
        """Get delivery history for a webhook."""
        records = [
            r for r in self._delivery_records.values()
            if r.webhook_id == webhook_id
        ]
        records.sort(key=lambda r: r.created_at, reverse=True)
        return records[:limit]

    def get_delivery_statistics(self, webhook_id: Optional[str] = None) -> dict[str, Any]:
        """
        Get delivery statistics.

        Args:
            webhook_id: Optional webhook ID to filter by

        Returns:
            Dictionary with success, failure, and pending counts
        """
        records = list(self._delivery_records.values())
        if webhook_id:
            records = [r for r in records if r.webhook_id == webhook_id]

        stats = {
            "total": len(records),
            "success": sum(1 for r in records if r.status == DeliveryStatus.SUCCESS),
            "failed": sum(1 for r in records if r.status == DeliveryStatus.FAILED),
            "pending": sum(1 for r in records if r.status == DeliveryStatus.PENDING),
            "retrying": sum(1 for r in records if r.status == DeliveryStatus.RETRYING),
        }
        stats["success_rate"] = (
            stats["success"] / stats["total"] if stats["total"] > 0 else 0.0
        )
        return stats

    def rotate_secret(self, webhook_id: str, keep_previous: bool = True) -> str:
        """
        Safely rotate a webhook's secret key.

        Args:
            webhook_id: The webhook to rotate
            keep_previous: If True, keep previous secret for verification

        Returns:
            The new secret
        """
        webhook = self._webhooks.get(webhook_id)
        if not webhook:
            raise ValueError(f"Webhook {webhook_id} not found")

        new_secret = secrets.token_hex(32)
        webhook.secret = new_secret

        if keep_previous and webhook_id in self._secret_versions:
            self._secret_versions[webhook_id].insert(0, new_secret)
            if len(self._secret_versions[webhook_id]) > 5:
                self._secret_versions[webhook_id] = self._secret_versions[webhook_id][:5]
        elif webhook_id in self._secret_versions:
            self._secret_versions[webhook_id] = [new_secret]

        logger.info(f"Rotated secret for webhook {webhook_id}")
        return new_secret

    def verify_with_any_secret(
        self,
        payload: dict[str, Any],
        signature: str,
        timestamp: int,
        webhook_id: str,
    ) -> bool:
        """
        Verify a signature using any of the known secret versions.

        Args:
            payload: The payload to verify
            signature: The signature to check
            timestamp: Unix timestamp from webhook
            webhook_id: The webhook ID

        Returns:
            True if signature matches any known secret version
        """
        secrets_list = self._secret_versions.get(webhook_id, [])
        if not secrets_list:
            return False

        for secret in secrets_list:
            if self.verify_signature(payload, signature, secret, timestamp):
                return True

        return False

    def register_template(
        self,
        name: str,
        template: dict[str, Any],
        default_headers: Optional[dict[str, str]] = None,
    ) -> WebhookPayloadTemplate:
        """Create and return a webhook payload template."""
        return WebhookPayloadTemplate(
            name=name,
            template=template,
            default_headers=default_headers or {},
        )

    def update_webhook(
        self,
        webhook_id: str,
        url: Optional[str] = None,
        events: Optional[list[str]] = None,
        is_active: Optional[bool] = None,
        batch_size: Optional[int] = None,
        rate_limit_rpm: Optional[int] = None,
    ) -> Optional[Webhook]:
        """Update an existing webhook's configuration."""
        webhook = self._webhooks.get(webhook_id)
        if not webhook:
            return None

        if url is not None:
            webhook.url = url
        if events is not None:
            webhook.events = events
        if is_active is not None:
            webhook.is_active = is_active
        if batch_size is not None:
            webhook.batch_size = batch_size
            self._batch_queues[webhook_id] = BatchQueue(
                webhook_id, batch_size, webhook.batch_timeout_seconds
            )
        if rate_limit_rpm is not None:
            webhook.rate_limit_rpm = rate_limit_rpm

        return webhook

    async def close(self) -> None:
        """Close the HTTP client and cleanup resources."""
        await self._http_client.aclose()


import asyncio
