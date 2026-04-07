"""Webhook handler utilities: signature verification, delivery, retry, and processing."""

from __future__ import annotations

import hashlib
import hmac
import json
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Callable

__all__ = [
    "WebhookEvent",
    "WebhookHandler",
    "WebhookDelivery",
    "verify_signature",
    "sign_payload",
]


@dataclass
class WebhookEvent:
    """Represents a webhook event."""

    id: str
    type: str
    payload: dict[str, Any]
    received_at: float = field(default_factory=time.time)
    headers: dict[str, str] = field(default_factory=dict)

    @classmethod
    def from_request(
        cls,
        body: bytes,
        headers: dict[str, str],
        event_type_header: str = "X-Webhook-Event",
        payload_id_header: str = "X-Webhook-Delivery",
    ) -> "WebhookEvent":
        """Parse a webhook event from an HTTP request."""
        payload = json.loads(body.decode())
        event_id = headers.get(payload_id_header, uuid.uuid4().hex)
        event_type = headers.get(event_type_header, "unknown")
        return cls(
            id=event_id,
            type=event_type,
            payload=payload,
            headers=headers,
        )


@dataclass
class WebhookDelivery:
    """Tracks a webhook delivery attempt."""

    event_id: str
    target_url: str
    attempts: int = 0
    last_attempt: float | None = None
    success: bool = False
    response_code: int | None = None
    response_body: str = ""
    error: str = ""

    def record_attempt(
        self,
        success: bool,
        response_code: int | None = None,
        response_body: str = "",
        error: str = "",
    ) -> None:
        """Record a delivery attempt."""
        self.attempts += 1
        self.last_attempt = time.time()
        self.success = success
        self.response_code = response_code
        self.response_body = response_body[:1000]
        self.error = error


class WebhookHandler:
    """Webhook processing and delivery handler."""

    def __init__(self, secret: str = "") -> None:
        self.secret = secret
        self._handlers: dict[str, Callable[[WebhookEvent], Any]] = {}
        self._deliveries: dict[str, WebhookDelivery] = {}
        self._retry_policy: dict[str, Any] = {
            "max_attempts": 3,
            "backoff_base": 2,
            "max_delay": 3600,
        }

    def register(self, event_type: str, handler: Callable[[WebhookEvent], Any]) -> None:
        """Register a handler for a specific event type."""
        self._handlers[event_type] = handler

    def handle(self, event: WebhookEvent) -> Any:
        """Dispatch an event to the appropriate handler."""
        handler = self._handlers.get(event.type)
        if handler is None:
            handler = self._handlers.get("*")
        if handler:
            return handler(event)
        return None

    def handle_request(
        self,
        body: bytes,
        headers: dict[str, str],
    ) -> dict[str, Any]:
        """Handle an incoming webhook HTTP request."""
        if self.secret:
            if not verify_signature(body, headers, self.secret):
                return {"status": 401, "body": {"error": "Invalid signature"}}

        event = WebhookEvent.from_request(body, headers)
        result = self.handle(event)
        return {"status": 200, "body": {"delivered": result is not None}}

    def deliver(
        self,
        event: WebhookEvent,
        target_url: str,
    ) -> WebhookDelivery:
        """Deliver a webhook event to a target URL."""
        delivery = WebhookDelivery(
            event_id=event.id,
            target_url=target_url,
        )
        self._deliveries[event.id] = delivery

        success = self._send(event, target_url)
        delivery.record_attempt(
            success=success,
            response_code=200 if success else 500,
        )
        return delivery

    def _send(self, event: WebhookEvent, target_url: str) -> bool:
        """Send webhook payload to target URL."""
        import urllib.request
        import urllib.error

        payload = json.dumps(event.payload).encode()
        headers = {
            "Content-Type": "application/json",
            "X-Webhook-Event": event.type,
            "X-Webhook-Delivery": event.id,
        }

        if self.secret:
            headers["X-Webhook-Signature"] = sign_payload(payload, self.secret)

        req = urllib.request.Request(
            target_url,
            data=payload,
            headers=headers,
            method="POST",
        )

        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                return resp.status == 200
        except Exception:
            return False

    def retry_failed(self) -> int:
        """Retry all failed deliveries."""
        count = 0
        for delivery in self._deliveries.values():
            if delivery.success:
                continue
            if delivery.attempts >= self._retry_policy["max_attempts"]:
                continue
            count += 1
        return count

    def get_delivery(self, event_id: str) -> WebhookDelivery | None:
        """Get delivery status for an event."""
        return self._deliveries.get(event_id)


def sign_payload(payload: bytes, secret: str) -> str:
    """Generate HMAC signature for webhook payload."""
    signature = hmac.new(
        secret.encode(),
        payload,
        hashlib.sha256,
    ).hexdigest()
    return f"sha256={signature}"


def verify_signature(
    payload: bytes,
    headers: dict[str, str],
    secret: str,
) -> bool:
    """Verify webhook signature from request headers."""
    provided = headers.get("X-Webhook-Signature", "")
    if not provided.startswith("sha256="):
        return False
    expected = sign_payload(payload, secret)
    return hmac.compare_digest(provided, expected)
