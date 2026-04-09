"""Webhook delivery action module.

Handles webhook delivery with retry logic, signature verification,
and delivery status tracking.
"""

from __future__ import annotations

import hashlib
import hmac
import time
import json
from typing import Any, Optional
from dataclasses import dataclass, field
from enum import Enum
import logging

logger = logging.getLogger(__name__)


class DeliveryStatus(Enum):
    """Webhook delivery status."""
    PENDING = "pending"
    DELIVERED = "delivered"
    FAILED = "failed"
    RETRYING = "retrying"


@dataclass
class WebhookDelivery:
    """Represents a webhook delivery attempt."""
    id: str
    payload: dict[str, Any]
    headers: dict[str, str]
    status: DeliveryStatus = DeliveryStatus.PENDING
    attempts: int = 0
    max_attempts: int = 3
    created_at: float = field(default_factory=time.time)
    last_attempt_at: Optional[float] = None
    response_status: Optional[int] = None
    response_body: Optional[str] = None
    error: Optional[str] = None


class WebhookSignature:
    """Webhook signature generator and verifier."""

    @staticmethod
    def generate(secret: str, payload: str, timestamp: Optional[int] = None) -> str:
        """Generate webhook signature.

        Args:
            secret: Webhook secret key
            payload: JSON payload string
            timestamp: Unix timestamp (optional)

        Returns:
            Signature string
        """
        if timestamp is None:
            timestamp = int(time.time())

        signed_payload = f"{timestamp}.{payload}"
        signature = hmac.new(
            secret.encode(),
            signed_payload.encode(),
            hashlib.sha256
        ).hexdigest()
        return f"t={timestamp},v1={signature}"

    @staticmethod
    def verify(
        secret: str,
        payload: str,
        signature: str,
        tolerance: int = 300
    ) -> bool:
        """Verify webhook signature.

        Args:
            secret: Webhook secret key
            payload: JSON payload string
            signature: Signature to verify
            tolerance: Time tolerance in seconds

        Returns:
            True if signature is valid
        """
        try:
            parts = dict(p.split("=", 1) for p in signature.split(","))
            timestamp = int(parts.get("t", 0))
            sig_v1 = parts.get("v1", "")

            current_time = int(time.time())
            if abs(current_time - timestamp) > tolerance:
                logger.warning(f"Webhook timestamp outside tolerance: {timestamp}")
                return False

            expected_sig = hmac.new(
                secret.encode(),
                f"{timestamp}.{payload}".encode(),
                hashlib.sha256
            ).hexdigest()

            return hmac.compare_digest(sig_v1, expected_sig)
        except (ValueError, KeyError) as e:
            logger.error(f"Signature verification error: {e}")
            return False


class WebhookDeliveryClient:
    """Client for delivering webhooks with retry logic."""

    def __init__(
        self,
        endpoint: str,
        secret: Optional[str] = None,
        max_attempts: int = 3,
        retry_delay: float = 1.0,
        timeout: float = 10.0,
    ):
        """Initialize webhook delivery client.

        Args:
            endpoint: Webhook endpoint URL
            secret: Secret for signature generation
            max_attempts: Maximum delivery attempts
            retry_delay: Base delay between retries (seconds)
            timeout: Request timeout (seconds)
        """
        self.endpoint = endpoint
        self.secret = secret
        self.max_attempts = max_attempts
        self.retry_delay = retry_delay
        self.timeout = timeout
        self._deliveries: dict[str, WebhookDelivery] = {}

    def deliver(
        self,
        payload: dict[str, Any],
        event_type: Optional[str] = None,
        idempotency_key: Optional[str] = None,
    ) -> WebhookDelivery:
        """Deliver webhook payload.

        Args:
            payload: Webhook payload data
            event_type: Event type header
            idempotency_key: Unique key for deduplication

        Returns:
            WebhookDelivery object
        """
        import urllib.request
        import urllib.error

        delivery_id = idempotency_key or self._generate_id()
        body = json.dumps(payload)

        headers: dict[str, str] = {
            "Content-Type": "application/json",
            "User-Agent": "WebhookClient/1.0",
        }

        if event_type:
            headers["X-Webhook-Event"] = event_type
        if idempotency_key:
            headers["X-Idempotency-Key"] = idempotency_key

        if self.secret:
            signature = WebhookSignature.generate(self.secret, body)
            headers["X-Webhook-Signature"] = signature

        delivery = WebhookDelivery(
            id=delivery_id,
            payload=payload,
            headers=headers,
        )
        self._deliveries[delivery_id] = delivery

        while delivery.attempts < self.max_attempts:
            delivery.attempts += 1
            delivery.last_attempt_at = time.time()

            try:
                req = urllib.request.Request(
                    self.endpoint,
                    data=body.encode(),
                    headers=headers,
                    method="POST"
                )
                with urllib.request.urlopen(req, timeout=self.timeout) as response:
                    delivery.response_status = response.status
                    delivery.response_body = response.read().decode()
                    delivery.status = DeliveryStatus.DELIVERED
                    logger.info(f"Webhook delivered: {delivery_id}")
                    return delivery

            except urllib.error.HTTPError as e:
                delivery.response_status = e.code
                delivery.response_body = e.read().decode() if e.fp else None
                delivery.error = str(e)
                if e.code >= 400 and e.code < 500:
                    delivery.status = DeliveryStatus.FAILED
                    logger.error(f"Webhook HTTP client error: {e.code}")
                    return delivery

            except Exception as e:
                delivery.error = str(e)
                logger.warning(f"Webhook delivery attempt {delivery.attempts} failed: {e}")

            if delivery.attempts < self.max_attempts:
                delivery.status = DeliveryStatus.RETRYING
                delay = self.retry_delay * (2 ** (delivery.attempts - 1))
                time.sleep(delay)

        delivery.status = DeliveryStatus.FAILED
        logger.error(f"Webhook delivery failed after {delivery.attempts} attempts")
        return delivery

    def get_delivery(self, delivery_id: str) -> Optional[WebhookDelivery]:
        """Get delivery by ID."""
        return self._deliveries.get(delivery_id)

    def _generate_id(self) -> str:
        """Generate unique delivery ID."""
        import secrets
        return secrets.token_hex(16)


def create_webhook_client(
    endpoint: str,
    secret: Optional[str] = None,
    max_attempts: int = 3,
) -> WebhookDeliveryClient:
    """Create webhook delivery client.

    Args:
        endpoint: Webhook endpoint URL
        secret: Secret for signature
        max_attempts: Maximum delivery attempts

    Returns:
        WebhookDeliveryClient instance
    """
    return WebhookDeliveryClient(
        endpoint=endpoint,
        secret=secret,
        max_attempts=max_attempts,
    )
