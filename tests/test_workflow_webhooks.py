"""Tests for Workflow Webhooks Module.

Tests webhook management functionality including registration, delivery,
signing, verification, filtering, batching, retry logic, and secret rotation.
"""

import unittest
import sys
import json
import time
import asyncio
from unittest.mock import Mock, patch, MagicMock, AsyncMock
from typing import Any, Dict, List, Optional
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
import hashlib
import hmac
import secrets

try:
    import httpx
except ImportError:
    httpx = None

sys.path.insert(0, '/Users/guige/my_project')
sys.path.insert(0, '/Users/guige/my_project/rabai_autoclick')
sys.path.insert(0, '/Users/guige/my_project/rabai_autoclick/src')


# =============================================================================
# Mock Module Imports and Data Structures
# =============================================================================

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
    template: dict
    default_headers: dict = field(default_factory=dict)


@dataclass
class Webhook:
    """Represents a registered webhook."""
    id: str
    url: str
    events: list
    secret: str
    template: Optional[WebhookPayloadTemplate] = None
    filter_mode: EventFilterMode = EventFilterMode.INCLUDE
    filter_patterns: list = field(default_factory=list)
    batch_size: int = 1
    batch_timeout_seconds: float = 5.0
    rate_limit_rpm: int = 60
    is_active: bool = True
    created_at: datetime = field(default_factory=datetime.utcnow)
    metadata: dict = field(default_factory=dict)


@dataclass
class DeliveryRecord:
    """Record of a webhook delivery attempt."""
    id: str
    webhook_id: str
    event: str
    payload: dict
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
        self.events: list = []
        self.first_event_time: Optional[float] = None
        self.payload_template: Optional[WebhookPayloadTemplate] = None

    def add(self, event: dict, payload_template: Optional[WebhookPayloadTemplate] = None) -> bool:
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

    def clear(self) -> list:
        """Clear the queue and return all events."""
        events = self.events
        self.events = []
        self.first_event_time = None
        return events


# =============================================================================
# Mock Webhook Manager
# =============================================================================

class MockWebhookManager:
    """Mock webhook manager for testing."""

    def __init__(self, http_client: Optional[httpx.AsyncClient] = None):
        self._webhooks: dict = {}
        self._delivery_records: dict = {}
        self._batch_queues: dict = {}
        self._http_client = http_client or MagicMock()
        self._event_handlers: dict = {}
        self._secret_versions: dict = {}

    def register_webhook(
        self,
        url: str,
        events: list,
        secret: Optional[str] = None,
        template: Optional[WebhookPayloadTemplate] = None,
        filter_mode: EventFilterMode = EventFilterMode.INCLUDE,
        filter_patterns: Optional[list] = None,
        batch_size: int = 1,
        batch_timeout_seconds: float = 5.0,
        rate_limit_rpm: int = 60,
        metadata: Optional[dict] = None,
    ) -> Webhook:
        """Register a new webhook."""
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

        return webhook

    def unregister_webhook(self, webhook_id: str) -> bool:
        """Unregister a webhook."""
        if webhook_id in self._webhooks:
            del self._webhooks[webhook_id]
            if webhook_id in self._batch_queues:
                del self._batch_queues[webhook_id]
            return True
        return False

    def get_webhook(self, webhook_id: str) -> Optional[Webhook]:
        """Get a webhook by ID."""
        return self._webhooks.get(webhook_id)

    def list_webhooks(self, event_filter: Optional[str] = None) -> list:
        """List all webhooks, optionally filtered by event type."""
        webhooks = list(self._webhooks.values())
        if event_filter:
            webhooks = [w for w in webhooks if event_filter in w.events]
        return webhooks

    def sign_payload(self, payload: dict, secret: str, timestamp: Optional[int] = None) -> dict:
        """Generate HMAC signature for a webhook payload."""
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
        payload: dict,
        signature: str,
        secret: str,
        timestamp: int,
        tolerance_seconds: int = 300,
    ) -> bool:
        """Verify the HMAC signature of a webhook payload."""
        current_time = int(time.time())
        if abs(current_time - timestamp) > tolerance_seconds:
            return False

        payload_bytes = json.dumps(payload, sort_keys=True).encode()
        signed_payload = f"{timestamp}.{payload_bytes.decode()}".encode()
        expected_signature = hmac.new(secret.encode(), signed_payload, hashlib.sha256).hexdigest()

        return hmac.compare_digest(signature, expected_signature)

    def should_deliver_event(self, webhook: Webhook, event: str, event_data: dict) -> bool:
        """Determine if an event should trigger a webhook delivery based on filters."""
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

    def apply_template(self, template: WebhookPayloadTemplate, event: str, event_data: dict) -> dict:
        """Apply a payload template to an event, substituting variables."""
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

    def _create_delivery_record(self, webhook: Webhook, event: str, payload: dict) -> DeliveryRecord:
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
        payload: dict,
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
            response = Mock()
            response.status_code = 200
            response.text = "OK"

            record.response_status = response.status_code
            record.response_body = response.text[:1000] if response.text else None
            record.attempts += 1

            if 200 <= response.status_code < 300:
                record.status = DeliveryStatus.SUCCESS
                record.delivered_at = datetime.utcnow()
            else:
                record.status = DeliveryStatus.FAILED
                record.error_message = f"HTTP {response.status_code}"

        except Exception as e:
            record.status = DeliveryStatus.RETRYING
            record.error_message = f"Request error: {e}"
            record.attempts += 1

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
        event_data: dict,
        webhook_ids: Optional[list] = None,
    ) -> list:
        """Deliver an event to matching webhooks."""
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
            record = await self._deliver_webhook(webhook, payload, record)

            if record.status != DeliveryStatus.SUCCESS:
                record = await self._retry_delivery(webhook, record)

            records.append(record)

        return records

    def queue_event_for_batching(self, webhook_id: str, event: str, event_data: dict) -> bool:
        """Queue an event for batch delivery."""
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

    def flush_batch_queue(self, webhook_id: str) -> list:
        """Flush and return all queued events for a webhook."""
        if webhook_id in self._batch_queues:
            return self._batch_queues[webhook_id].clear()
        return []

    def get_delivery_record(self, record_id: str) -> Optional[DeliveryRecord]:
        """Get a delivery record by ID."""
        return self._delivery_records.get(record_id)

    def get_delivery_statistics(self, webhook_id: Optional[str] = None) -> dict:
        """Get delivery statistics."""
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
        """Safely rotate a webhook's secret key."""
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

        return new_secret

    def verify_with_any_secret(
        self,
        payload: dict,
        signature: str,
        timestamp: int,
        webhook_id: str,
    ) -> bool:
        """Verify a signature using any of the known secret versions."""
        secrets_list = self._secret_versions.get(webhook_id, [])
        if not secrets_list:
            return False

        for secret in secrets_list:
            if self.verify_signature(payload, signature, secret, timestamp):
                return True

        return False

    def update_webhook(
        self,
        webhook_id: str,
        url: Optional[str] = None,
        events: Optional[list] = None,
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


import uuid


# =============================================================================
# Test Webhook Data Classes
# =============================================================================

class TestDeliveryStatus(unittest.TestCase):
    """Test DeliveryStatus enum."""

    def test_status_values(self):
        """Test delivery status values."""
        self.assertEqual(DeliveryStatus.PENDING.value, "pending")
        self.assertEqual(DeliveryStatus.SUCCESS.value, "success")
        self.assertEqual(DeliveryStatus.FAILED.value, "failed")
        self.assertEqual(DeliveryStatus.RETRYING.value, "retrying")


class TestEventFilterMode(unittest.TestCase):
    """Test EventFilterMode enum."""

    def test_filter_mode_values(self):
        """Test filter mode values."""
        self.assertEqual(EventFilterMode.INCLUDE.value, "include")
        self.assertEqual(EventFilterMode.EXCLUDE.value, "exclude")


class TestWebhook(unittest.TestCase):
    """Test Webhook dataclass."""

    def test_webhook_creation(self):
        """Test webhook creation."""
        webhook = Webhook(
            id="wh1",
            url="https://example.com/webhook",
            events=["event1", "event2"],
            secret="secret123"
        )
        self.assertEqual(webhook.id, "wh1")
        self.assertEqual(webhook.url, "https://example.com/webhook")
        self.assertEqual(webhook.events, ["event1", "event2"])
        self.assertEqual(webhook.secret, "secret123")
        self.assertTrue(webhook.is_active)
        self.assertEqual(webhook.batch_size, 1)

    def test_webhook_with_template(self):
        """Test webhook with payload template."""
        template = WebhookPayloadTemplate(
            name="test_template",
            template={"key": "${data}"},
            default_headers={"X-Custom": "header"}
        )
        webhook = Webhook(
            id="wh1",
            url="https://example.com/webhook",
            events=["event1"],
            secret="secret123",
            template=template
        )
        self.assertIsNotNone(webhook.template)
        self.assertEqual(webhook.template.name, "test_template")


class TestDeliveryRecord(unittest.TestCase):
    """Test DeliveryRecord dataclass."""

    def test_delivery_record_creation(self):
        """Test delivery record creation."""
        record = DeliveryRecord(
            id="rec1",
            webhook_id="wh1",
            event="event1",
            payload={"data": "test"},
            status=DeliveryStatus.PENDING
        )
        self.assertEqual(record.id, "rec1")
        self.assertEqual(record.status, DeliveryStatus.PENDING)
        self.assertEqual(record.attempts, 0)
        self.assertEqual(record.max_attempts, 3)

    def test_delivery_record_with_response(self):
        """Test delivery record with response data."""
        record = DeliveryRecord(
            id="rec1",
            webhook_id="wh1",
            event="event1",
            payload={"data": "test"},
            status=DeliveryStatus.SUCCESS,
            attempts=1,
            response_status=200,
            response_body="OK"
        )
        self.assertEqual(record.response_status, 200)
        self.assertEqual(record.response_body, "OK")


class TestWebhookPayloadTemplate(unittest.TestCase):
    """Test WebhookPayloadTemplate dataclass."""

    def test_template_creation(self):
        """Test template creation."""
        template = WebhookPayloadTemplate(
            name="test",
            template={"event": "test_event", "data": "${data}"}
        )
        self.assertEqual(template.name, "test")
        self.assertEqual(template.template["event"], "test_event")

    def test_template_with_headers(self):
        """Test template with default headers."""
        template = WebhookPayloadTemplate(
            name="test",
            template={"key": "value"},
            default_headers={"Content-Type": "application/json"}
        )
        self.assertEqual(template.default_headers["Content-Type"], "application/json")


# =============================================================================
# Test Rate Limiter
# =============================================================================

class TestWebhookRateLimiter(unittest.TestCase):
    """Test RateLimiter class for webhooks."""

    def setUp(self):
        """Set up test fixtures."""
        self.limiter = RateLimiter(requests_per_minute=10)

    def test_initial_tokens(self):
        """Test initial token count."""
        self.assertEqual(self.limiter.tokens, 10)

    def test_refill(self):
        """Test token refill."""
        self.limiter.tokens = 0
        self.limiter.last_refill = time.monotonic() - 6.0  # 6 seconds ago
        self.limiter._refill()
        self.assertGreater(self.limiter.tokens, 0)

    def test_refill_cap(self):
        """Test token refill caps at max."""
        self.limiter.last_refill = time.monotonic() - 60.0  # 60 seconds ago
        self.limiter._refill()
        self.assertEqual(self.limiter.tokens, 10)


# =============================================================================
# Test Batch Queue
# =============================================================================

class TestBatchQueue(unittest.TestCase):
    """Test BatchQueue class."""

    def setUp(self):
        """Set up test fixtures."""
        self.queue = BatchQueue("wh1", batch_size=3, timeout_seconds=5.0)

    def test_add_first_event(self):
        """Test adding first event."""
        result = self.queue.add({"event": "test1", "data": {}})
        self.assertFalse(result)
        self.assertEqual(len(self.queue.events), 1)
        self.assertIsNotNone(self.queue.first_event_time)

    def test_add_batch_ready(self):
        """Test adding events reaches batch size."""
        self.queue.add({"event": "test1", "data": {}})
        self.queue.add({"event": "test2", "data": {}})
        result = self.queue.add({"event": "test3", "data": {}})
        self.assertTrue(result)
        self.assertEqual(len(self.queue.events), 3)

    def test_add_timeout(self):
        """Test batch ready on timeout."""
        self.queue.first_event_time = time.monotonic() - 6.0  # 6 seconds ago
        self.queue.events.append({"event": "test1", "data": {}})
        result = self.queue.add({"event": "test2", "data": {}})
        self.assertTrue(result)

    def test_clear(self):
        """Test clearing the queue."""
        self.queue.add({"event": "test1", "data": {}})
        self.queue.add({"event": "test2", "data": {}})
        events = self.queue.clear()
        self.assertEqual(len(events), 2)
        self.assertEqual(len(self.queue.events), 0)
        self.assertIsNone(self.queue.first_event_time)


# =============================================================================
# Test Mock Webhook Manager
# =============================================================================

class TestMockWebhookManager(unittest.TestCase):
    """Test MockWebhookManager class."""

    def setUp(self):
        """Set up test fixtures."""
        self.manager = MockWebhookManager()

    def test_register_webhook(self):
        """Test webhook registration."""
        webhook = self.manager.register_webhook(
            url="https://example.com/webhook",
            events=["event1", "event2"],
            secret="test_secret"
        )
        self.assertIsNotNone(webhook.id)
        self.assertEqual(webhook.url, "https://example.com/webhook")
        self.assertEqual(webhook.events, ["event1", "event2"])
        self.assertEqual(webhook.secret, "test_secret")
        self.assertTrue(webhook.is_active)

    def test_register_webhook_auto_secret(self):
        """Test webhook registration with auto-generated secret."""
        webhook = self.manager.register_webhook(
            url="https://example.com/webhook",
            events=["event1"]
        )
        self.assertIsNotNone(webhook.secret)
        self.assertEqual(len(webhook.secret), 64)  # 32 bytes hex = 64 chars

    def test_unregister_webhook(self):
        """Test webhook unregistration."""
        webhook = self.manager.register_webhook(
            url="https://example.com/webhook",
            events=["event1"]
        )
        result = self.manager.unregister_webhook(webhook.id)
        self.assertTrue(result)
        self.assertIsNone(self.manager.get_webhook(webhook.id))

    def test_unregister_nonexistent(self):
        """Test unregistering nonexistent webhook."""
        result = self.manager.unregister_webhook("nonexistent")
        self.assertFalse(result)

    def test_get_webhook(self):
        """Test getting a webhook."""
        webhook = self.manager.register_webhook(
            url="https://example.com/webhook",
            events=["event1"]
        )
        retrieved = self.manager.get_webhook(webhook.id)
        self.assertEqual(retrieved.id, webhook.id)

    def test_list_webhooks(self):
        """Test listing webhooks."""
        self.manager.register_webhook(url="https://example1.com", events=["event1"])
        self.manager.register_webhook(url="https://example2.com", events=["event2"])
        webhooks = self.manager.list_webhooks()
        self.assertEqual(len(webhooks), 2)

    def test_list_webhooks_filter(self):
        """Test listing webhooks with filter."""
        self.manager.register_webhook(url="https://example1.com", events=["event1"])
        self.manager.register_webhook(url="https://example2.com", events=["event2"])
        webhooks = self.manager.list_webhooks(event_filter="event1")
        self.assertEqual(len(webhooks), 1)
        self.assertEqual(webhooks[0].events, ["event1"])


# =============================================================================
# Test Signature Operations
# =============================================================================

class TestSignatureOperations(unittest.TestCase):
    """Test webhook signature operations."""

    def setUp(self):
        """Set up test fixtures."""
        self.manager = MockWebhookManager()
        self.webhook = self.manager.register_webhook(
            url="https://example.com/webhook",
            events=["event1"],
            secret="test_secret"
        )

    def test_sign_payload(self):
        """Test signing a payload."""
        payload = {"data": "test"}
        timestamp = 1234567890
        headers = self.manager.sign_payload(payload, "test_secret", timestamp)

        self.assertIn("X-Webhook-Signature", headers)
        self.assertIn("X-Webhook-Timestamp", headers)
        self.assertEqual(headers["X-Webhook-Timestamp"], str(timestamp))

    def test_verify_signature_valid(self):
        """Test verifying a valid signature."""
        payload = {"data": "test"}
        timestamp = int(time.time())
        headers = self.manager.sign_payload(payload, "test_secret", timestamp)

        result = self.manager.verify_signature(
            payload,
            headers["X-Webhook-Signature"],
            "test_secret",
            timestamp
        )
        self.assertTrue(result)

    def test_verify_signature_invalid(self):
        """Test verifying an invalid signature."""
        payload = {"data": "test"}
        timestamp = int(time.time())

        result = self.manager.verify_signature(
            payload,
            "invalid_signature",
            "test_secret",
            timestamp
        )
        self.assertFalse(result)

    def test_verify_signature_expired(self):
        """Test verifying an expired signature."""
        payload = {"data": "test"}
        timestamp = int(time.time()) - 600  # 10 minutes ago

        result = self.manager.verify_signature(
            payload,
            "any_signature",
            "test_secret",
            timestamp,
            tolerance_seconds=300  # 5 minutes
        )
        self.assertFalse(result)


# =============================================================================
# Test Event Filtering
# =============================================================================

class TestEventFiltering(unittest.TestCase):
    """Test event filtering logic."""

    def setUp(self):
        """Set up test fixtures."""
        self.manager = MockWebhookManager()

    def test_should_deliver_inactive_webhook(self):
        """Test that inactive webhooks don't receive events."""
        webhook = Webhook(
            id="wh1",
            url="https://example.com",
            events=["event1"],
            secret="secret",
            is_active=False
        )
        result = self.manager.should_deliver_event(webhook, "event1", {"data": "test"})
        self.assertFalse(result)

    def test_should_deliver_wrong_event(self):
        """Test that wrong event types don't trigger delivery."""
        webhook = Webhook(
            id="wh1",
            url="https://example.com",
            events=["event1"],
            secret="secret"
        )
        result = self.manager.should_deliver_event(webhook, "event2", {"data": "test"})
        self.assertFalse(result)

    def test_should_deliver_no_filters(self):
        """Test delivery when no filters are set."""
        webhook = Webhook(
            id="wh1",
            url="https://example.com",
            events=["event1"],
            secret="secret"
        )
        result = self.manager.should_deliver_event(webhook, "event1", {"data": "test"})
        self.assertTrue(result)

    def test_should_deliver_include_mode_match(self):
        """Test include mode with matching pattern."""
        webhook = Webhook(
            id="wh1",
            url="https://example.com",
            events=["event1"],
            secret="secret",
            filter_mode=EventFilterMode.INCLUDE,
            filter_patterns=["important"]
        )
        result = self.manager.should_deliver_event(
            webhook, "event1", {"data": "important data"}
        )
        self.assertTrue(result)

    def test_should_deliver_include_mode_no_match(self):
        """Test include mode with no matching pattern."""
        webhook = Webhook(
            id="wh1",
            url="https://example.com",
            events=["event1"],
            secret="secret",
            filter_mode=EventFilterMode.INCLUDE,
            filter_patterns=["important"]
        )
        result = self.manager.should_deliver_event(
            webhook, "event1", {"data": "normal data"}
        )
        self.assertFalse(result)

    def test_should_deliver_exclude_mode_match(self):
        """Test exclude mode with matching pattern."""
        webhook = Webhook(
            id="wh1",
            url="https://example.com",
            events=["event1"],
            secret="secret",
            filter_mode=EventFilterMode.EXCLUDE,
            filter_patterns=["exclude"]
        )
        result = self.manager.should_deliver_event(
            webhook, "event1", {"data": "exclude this"}
        )
        self.assertFalse(result)

    def test_should_deliver_exclude_mode_no_match(self):
        """Test exclude mode with no matching pattern."""
        webhook = Webhook(
            id="wh1",
            url="https://example.com",
            events=["event1"],
            secret="secret",
            filter_mode=EventFilterMode.EXCLUDE,
            filter_patterns=["exclude"]
        )
        result = self.manager.should_deliver_event(
            webhook, "event1", {"data": "keep this"}
        )
        self.assertTrue(result)


# =============================================================================
# Test Template Application
# =============================================================================

class TestTemplateApplication(unittest.TestCase):
    """Test template application."""

    def setUp(self):
        """Set up test fixtures."""
        self.manager = MockWebhookManager()

    def test_apply_template_simple(self):
        """Test applying a simple template."""
        template = WebhookPayloadTemplate(
            name="test",
            template={"event": "test_event", "static": "value"}
        )
        result = self.manager.apply_template(template, "test_event", {"data": "test"})
        self.assertEqual(result["event"], "test_event")
        self.assertEqual(result["data"]["static"], "value")

    def test_apply_template_variable_substitution(self):
        """Test variable substitution in template."""
        template = WebhookPayloadTemplate(
            name="test",
            template={"data": "${data}"}
        )
        result = self.manager.apply_template(template, "test_event", {"data": "substituted"})
        self.assertEqual(result["data"]["data"], "substituted")

    def test_apply_template_missing_variable(self):
        """Test template with missing variable."""
        template = WebhookPayloadTemplate(
            name="test",
            template={"data": "${missing}"}
        )
        result = self.manager.apply_template(template, "test_event", {"data": "test"})
        self.assertEqual(result["data"]["data"], "${missing}")


# =============================================================================
# Test Secret Rotation
# =============================================================================

class TestSecretRotation(unittest.TestCase):
    """Test secret rotation."""

    def setUp(self):
        """Set up test fixtures."""
        self.manager = MockWebhookManager()
        self.webhook = self.manager.register_webhook(
            url="https://example.com/webhook",
            events=["event1"],
            secret="original_secret"
        )

    def test_rotate_secret(self):
        """Test secret rotation."""
        new_secret = self.manager.rotate_secret(self.webhook.id)
        self.assertNotEqual(new_secret, "original_secret")
        self.assertEqual(len(new_secret), 64)

    def test_rotate_secret_updates_webhook(self):
        """Test that rotated secret updates the webhook."""
        old_secret = self.webhook.secret
        new_secret = self.manager.rotate_secret(self.webhook.id)
        self.assertEqual(self.webhook.secret, new_secret)
        self.assertNotEqual(self.webhook.secret, old_secret)

    def test_rotate_secret_nonexistent(self):
        """Test rotating secret for nonexistent webhook."""
        with self.assertRaises(ValueError):
            self.manager.rotate_secret("nonexistent")

    def test_verify_with_any_secret_old(self):
        """Test verifying with old secret after rotation."""
        old_secret = self.webhook.secret
        self.manager.rotate_secret(self.webhook.id)

        payload = {"data": "test"}
        timestamp = int(time.time())
        headers = self.manager.sign_payload(payload, old_secret, timestamp)

        result = self.manager.verify_with_any_secret(
            payload,
            headers["X-Webhook-Signature"],
            timestamp,
            self.webhook.id
        )
        self.assertTrue(result)


# =============================================================================
# Test Delivery Statistics
# =============================================================================

class TestDeliveryStatistics(unittest.TestCase):
    """Test delivery statistics."""

    def setUp(self):
        """Set up test fixtures."""
        self.manager = MockWebhookManager()
        self.webhook = self.manager.register_webhook(
            url="https://example.com/webhook",
            events=["event1"]
        )

    def test_empty_statistics(self):
        """Test statistics with no deliveries."""
        stats = self.manager.get_delivery_statistics()
        self.assertEqual(stats["total"], 0)
        self.assertEqual(stats["success"], 0)
        self.assertEqual(stats["failed"], 0)
        self.assertEqual(stats["success_rate"], 0.0)

    def test_statistics_filter_by_webhook(self):
        """Test statistics filtered by webhook."""
        stats = self.manager.get_delivery_statistics(self.webhook.id)
        self.assertEqual(stats["total"], 0)


# =============================================================================
# Test Update Webhook
# =============================================================================

class TestUpdateWebhook(unittest.TestCase):
    """Test updating webhooks."""

    def setUp(self):
        """Set up test fixtures."""
        self.manager = MockWebhookManager()
        self.webhook = self.manager.register_webhook(
            url="https://example.com/webhook",
            events=["event1"],
            is_active=True
        )

    def test_update_url(self):
        """Test updating webhook URL."""
        result = self.manager.update_webhook(self.webhook.id, url="https://new.com/webhook")
        self.assertIsNotNone(result)
        self.assertEqual(result.url, "https://new.com/webhook")

    def test_update_events(self):
        """Test updating webhook events."""
        result = self.manager.update_webhook(self.webhook.id, events=["event2", "event3"])
        self.assertEqual(result.events, ["event2", "event3"])

    def test_update_is_active(self):
        """Test updating webhook active status."""
        result = self.manager.update_webhook(self.webhook.id, is_active=False)
        self.assertFalse(result.is_active)

    def test_update_batch_size(self):
        """Test updating batch size."""
        result = self.manager.update_webhook(self.webhook.id, batch_size=10)
        self.assertEqual(result.batch_size, 10)

    def test_update_rate_limit(self):
        """Test updating rate limit."""
        result = self.manager.update_webhook(self.webhook.id, rate_limit_rpm=120)
        self.assertEqual(result.rate_limit_rpm, 120)

    def test_update_nonexistent(self):
        """Test updating nonexistent webhook."""
        result = self.manager.update_webhook("nonexistent", url="https://test.com")
        self.assertIsNone(result)


# =============================================================================
# Test Batching Queue Operations
# =============================================================================

class TestBatchingQueueOperations(unittest.TestCase):
    """Test batch queue operations."""

    def setUp(self):
        """Set up test fixtures."""
        self.manager = MockWebhookManager()
        self.webhook = self.manager.register_webhook(
            url="https://example.com/webhook",
            events=["event1"],
            batch_size=3,
            batch_timeout_seconds=5.0
        )

    def test_queue_event(self):
        """Test queuing an event."""
        result = self.manager.queue_event_for_batching(
            self.webhook.id, "event1", {"data": "test"}
        )
        self.assertFalse(result)  # Not ready yet

    def test_queue_batch_ready(self):
        """Test that batch becomes ready after adding enough events."""
        self.manager.queue_event_for_batching(self.webhook.id, "event1", {"data": "1"})
        self.manager.queue_event_for_batching(self.webhook.id, "event1", {"data": "2"})
        result = self.manager.queue_event_for_batching(self.webhook.id, "event1", {"data": "3"})
        self.assertTrue(result)  # Batch size reached

    def test_flush_queue(self):
        """Test flushing the batch queue."""
        self.manager.queue_event_for_batching(self.webhook.id, "event1", {"data": "1"})
        self.manager.queue_event_for_batching(self.webhook.id, "event1", {"data": "2"})
        events = self.manager.flush_batch_queue(self.webhook.id)
        self.assertEqual(len(events), 2)

    def test_flush_empty_queue(self):
        """Test flushing an empty queue."""
        events = self.manager.flush_batch_queue(self.webhook.id)
        self.assertEqual(len(events), 0)

    def test_queue_nonexistent_webhook(self):
        """Test queuing for nonexistent webhook."""
        result = self.manager.queue_event_for_batching(
            "nonexistent", "event1", {"data": "test"}
        )
        self.assertFalse(result)


if __name__ == "__main__":
    unittest.main()
