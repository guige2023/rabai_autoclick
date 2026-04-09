"""
API Webhook Action Module

Webhook delivery, retry logic, signature verification,
event filtering, and delivery status tracking.

MIT License - Copyright (c) 2025 RabAi Research
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, List, Optional

logger = logging.getLogger(__name__)


class DeliveryStatus(Enum):
    """Webhook delivery status."""

    PENDING = "pending"
    DELIVERED = "delivered"
    FAILED = "failed"
    RETRYING = "retrying"
    DROPPED = "dropped"


@dataclass
class WebhookEvent:
    """A webhook event to be delivered."""

    event_id: str
    event_type: str
    payload: Dict[str, Any]
    created_at: float = field(default_factory=time.time)
    headers: Dict[str, str] = field(default_factory=dict)
    retry_count: int = 0
    max_retries: int = 3


@dataclass
class DeliveryAttempt:
    """Record of a delivery attempt."""

    attempt_number: int
    timestamp: float
    status_code: Optional[int] = None
    response_body: Optional[str] = None
    error: Optional[str] = None
    duration_ms: float = 0.0


@dataclass
class DeliveryResult:
    """Result of webhook delivery."""

    event_id: str
    status: DeliveryStatus
    attempts: List[DeliveryAttempt] = field(default_factory=list)
    delivered_at: Optional[float] = None
    final_error: Optional[str] = None


class WebhookSignature:
    """
    Webhook signature generation and verification.

    Supports HMAC-SHA256 signatures commonly used by webhooks.
    """

    @staticmethod
    def generate_signature(
        payload: str,
        secret: str,
        algorithm: str = "sha256",
    ) -> str:
        """Generate a signature for a payload."""
        if algorithm == "sha256":
            mac = hmac.new(
                secret.encode("utf-8"),
                payload.encode("utf-8"),
                hashlib.sha256,
            )
        elif algorithm == "sha512":
            mac = hmac.new(
                secret.encode("utf-8"),
                payload.encode("utf-8"),
                hashlib.sha512,
            )
        else:
            raise ValueError(f"Unsupported algorithm: {algorithm}")

        return f"sha256={mac.hexdigest()}"

    @staticmethod
    def verify_signature(
        payload: str,
        signature: str,
        secret: str,
        algorithm: str = "sha256",
        tolerance_seconds: int = 300,
    ) -> bool:
        """Verify a webhook signature."""
        try:
            expected = WebhookSignature.generate_signature(payload, secret, algorithm)
            is_valid = hmac.compare_digest(signature, expected)

            if not is_valid:
                logger.warning("Webhook signature mismatch")

            return is_valid

        except Exception as e:
            logger.error(f"Signature verification error: {e}")
            return False


class WebhookDelivery:
    """
    Handles webhook delivery with retry logic.
    """

    def __init__(
        self,
        endpoint: str,
        secret: Optional[str] = None,
        max_retries: int = 3,
        timeout_seconds: float = 30.0,
    ):
        self.endpoint = endpoint
        self.secret = secret
        self.max_retries = max_retries
        self.timeout_seconds = timeout_seconds
        self._retry_delays = [1, 5, 30]  # seconds

    async def deliver(
        self,
        event: WebhookEvent,
    ) -> DeliveryResult:
        """Deliver a webhook event."""
        result = DeliveryResult(event_id=event.event_id, status=DeliveryStatus.PENDING)

        for attempt_num in range(self.max_retries + 1):
            attempt = DeliveryAttempt(attempt_number=attempt_num + 1)
            attempt.timestamp = time.time()

            try:
                status_code, response_body, duration = await self._send_webhook(
                    event,
                    attempt_num,
                )

                attempt.status_code = status_code
                attempt.response_body = response_body
                attempt.duration_ms = duration

                result.attempts.append(attempt)

                if 200 <= status_code < 300:
                    result.status = DeliveryStatus.DELIVERED
                    result.delivered_at = time.time()
                    logger.info(f"Webhook delivered: {event.event_id}")
                    return result

                logger.warning(
                    f"Webhook delivery failed with status {status_code}: {event.event_id}"
                )

            except Exception as e:
                attempt.error = f"{type(e).__name__}: {str(e)}"
                result.attempts.append(attempt)
                logger.error(f"Webhook delivery error: {e}")

            # Retry if allowed
            if attempt_num < self.max_retries:
                delay = self._retry_delays[attempt_num]
                logger.info(f"Retrying webhook in {delay}s: {event.event_id}")
                result.status = DeliveryStatus.RETRYING
                await asyncio.sleep(delay)

        result.status = DeliveryStatus.FAILED
        result.final_error = f"Failed after {len(result.attempts)} attempts"
        logger.error(f"Webhook delivery failed permanently: {event.event_id}")

        return result

    async def _send_webhook(
        self,
        event: WebhookEvent,
        attempt: int,
    ) -> tuple[int, str, float]:
        """Send the webhook request. Returns (status_code, response_body, duration_ms)."""
        import httpx

        headers = {
            "Content-Type": "application/json",
            "User-Agent": "RabAi-Webhook/1.0",
            "X-Webhook-Event": event.event_type,
            "X-Webhook-Event-ID": event.event_id,
        }

        # Add signature if secret is configured
        if self.secret:
            payload_str = json.dumps(event.payload)
            signature = WebhookSignature.generate_signature(payload_str, self.secret)
            headers["X-Webhook-Signature"] = signature

        payload_str = json.dumps(event.payload)

        start_time = time.time()

        async with httpx.AsyncClient() as client:
            response = await client.post(
                self.endpoint,
                content=payload_str,
                headers=headers,
                timeout=self.timeout_seconds,
            )

        duration_ms = (time.time() - start_time) * 1000
        return response.status_code, response.text[:500], duration_ms


class WebhookQueue:
    """
    Queue for managing webhook events with delivery guarantees.
    """

    def __init__(self, max_size: int = 10000):
        self.max_size = max_size
        self._queue: asyncio.Queue[WebhookEvent] = asyncio.Queue(maxsize=max_size)
        self._pending: Dict[str, WebhookEvent] = {}
        self._results: Dict[str, DeliveryResult] = {}

    async def enqueue(self, event: WebhookEvent) -> bool:
        """Add an event to the delivery queue."""
        try:
            self._queue.put_nowait(event)
            self._pending[event.event_id] = event
            return True
        except asyncio.QueueFull:
            logger.error(f"Webhook queue full, event dropped: {event.event_id}")
            return False

    async def dequeue(self, timeout: float = 1.0) -> Optional[WebhookEvent]:
        """Get an event from the queue."""
        try:
            event = await asyncio.wait_for(self._queue.get(), timeout=timeout)
            return event
        except asyncio.TimeoutError:
            return None

    def mark_delivered(self, event_id: str, result: DeliveryResult) -> None:
        """Mark an event as delivered."""
        self._results[event_id] = result
        if event_id in self._pending:
            del self._pending[event_id]

    def get_result(self, event_id: str) -> Optional[DeliveryResult]:
        """Get delivery result for an event."""
        return self._results.get(event_id)

    def get_pending_count(self) -> int:
        """Get number of pending events."""
        return len(self._pending)

    def get_queue_size(self) -> int:
        """Get current queue size."""
        return self._queue.qsize()


class WebhookProcessor:
    """
    Processes webhook events with filtering and transformation.
    """

    def __init__(self):
        self._filters: List[Callable[[WebhookEvent], bool]] = []
        self._transformers: List[Callable[[WebhookEvent], WebhookEvent]] = []

    def add_filter(
        self,
        filter_fn: Callable[[WebhookEvent], bool],
    ) -> "WebhookProcessor":
        """Add an event filter."""
        self._filters.append(filter_fn)
        return self

    def add_transformer(
        self,
        transformer: Callable[[WebhookEvent], WebhookEvent],
    ) -> "WebhookProcessor":
        """Add an event transformer."""
        self._transformers.append(transformer)
        return self

    def filter_event(self, event: WebhookEvent) -> bool:
        """Check if event passes all filters."""
        for filter_fn in self._filters:
            if not filter_fn(event):
                return False
        return True

    def transform_event(self, event: WebhookEvent) -> WebhookEvent:
        """Apply all transformers to event."""
        for transformer in self._transformers:
            event = transformer(event)
        return event


class APIWebhookAction:
    """
    Main action class for webhook management.

    Features:
    - Webhook delivery with retry logic
    - Signature generation and verification
    - Event filtering and transformation
    - Delivery queue management
    - Status tracking and reporting
    """

    def __init__(
        self,
        endpoint: Optional[str] = None,
        secret: Optional[str] = None,
    ):
        self.endpoint = endpoint
        self.secret = secret
        self._delivery = WebhookDelivery(endpoint, secret) if endpoint else None
        self._queue = WebhookQueue()
        self._processor = WebhookProcessor()
        self._stats = {
            "delivered": 0,
            "failed": 0,
            "pending": 0,
            "retried": 0,
        }

    def create_event(
        self,
        event_type: str,
        payload: Dict[str, Any],
        event_id: Optional[str] = None,
    ) -> WebhookEvent:
        """Create a webhook event."""
        import uuid

        return WebhookEvent(
            event_id=event_id or str(uuid.uuid4()),
            event_type=event_type,
            payload=payload,
        )

    async def send(
        self,
        event: WebhookEvent,
        endpoint: Optional[str] = None,
    ) -> DeliveryResult:
        """Send a webhook event."""
        if endpoint:
            delivery = WebhookDelivery(endpoint, self.secret)
        elif self._delivery:
            delivery = self._delivery
        else:
            raise ValueError("No endpoint configured")

        # Apply processors
        if not self._processor.filter_event(event):
            return DeliveryResult(
                event_id=event.event_id,
                status=DeliveryStatus.DROPPED,
            )

        event = self._processor.transform_event(event)

        # Queue for async delivery
        if await self._queue.enqueue(event):
            return DeliveryResult(
                event_id=event.event_id,
                status=DeliveryStatus.PENDING,
            )

        # Sync delivery
        result = await delivery.deliver(event)
        self._update_stats(result)
        return result

    def _update_stats(self, result: DeliveryResult) -> None:
        """Update delivery statistics."""
        if result.status == DeliveryStatus.DELIVERED:
            self._stats["delivered"] += 1
        elif result.status == DeliveryStatus.FAILED:
            self._stats["failed"] += 1
        elif result.status == DeliveryStatus.RETRYING:
            self._stats["retried"] += 1

    def verify_signature(
        self,
        payload: str,
        signature: str,
        algorithm: str = "sha256",
    ) -> bool:
        """Verify webhook signature."""
        if not self.secret:
            return True
        return WebhookSignature.verify_signature(
            payload, signature, self.secret, algorithm
        )

    def get_stats(self) -> Dict[str, Any]:
        """Get webhook statistics."""
        return {
            **self._stats,
            "queue_size": self._queue.get_queue_size(),
            "pending": self._queue.get_pending_count(),
        }


async def demo_webhook():
    """Demonstrate webhook usage."""
    action = APIWebhookAction(endpoint="https://example.com/webhook", secret="secret123")

    # Create and send event
    event = action.create_event("user.created", {"user_id": "123", "email": "test@example.com"})
    print(f"Created event: {event.event_id}")

    # Verify signature
    import json
    payload = json.dumps(event.payload)
    signature = WebhookSignature.generate_signature(payload, "secret123")
    is_valid = action.verify_signature(payload, signature)
    print(f"Signature valid: {is_valid}")


if __name__ == "__main__":
    asyncio.run(demo_webhook())
