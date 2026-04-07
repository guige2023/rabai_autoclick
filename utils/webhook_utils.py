"""Webhook utilities for sending, receiving, verifying, and retrying webhooks."""

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
    "WebhookDelivery",
    "WebhookSender",
    "WebhookReceiver",
    "verify_webhook_signature",
]


@dataclass
class WebhookEvent:
    """A webhook event to be sent."""
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    type: str = ""
    data: dict[str, Any] = field(default_factory=dict)
    created_at: float = field(default_factory=time.time)
    secret: str = ""

    def to_json(self) -> str:
        payload = {
            "id": self.id,
            "type": self.type,
            "data": self.data,
            "created_at": self.created_at,
        }
        return json.dumps(payload, default=str)

    def signature(self, secret: str | None = None) -> str:
        sig_key = secret or self.secret
        return hashlib.sha256(self.to_json().encode() + sig_key.encode()).hexdigest()


@dataclass
class WebhookDelivery:
    """Record of a webhook delivery attempt."""
    event_id: str
    url: str
    payload: str
    headers: dict[str, str]
    response_status: int | None = None
    response_body: str | None = None
    attempts: int = 0
    success: bool = False
    delivered_at: float | None = None
    error: str | None = None


class WebhookSender:
    """Sends webhooks to configured endpoints with retry and signature verification."""

    def __init__(self, default_secret: str = "") -> None:
        self.default_secret = default_secret
        self._delivery_log: list[WebhookDelivery] = []

    def send(
        self,
        url: str,
        event: WebhookEvent,
        secret: str | None = None,
        headers: dict[str, str] | None = None,
        timeout: float = 10.0,
    ) -> WebhookDelivery:
        sig_key = secret or self.default_secret
        payload = event.to_json()
        timestamp = str(int(time.time()))

        delivery_headers = {
            "Content-Type": "application/json",
            "X-Webhook-ID": event.id,
            "X-Webhook-Type": event.type,
            "X-Webhook-Timestamp": timestamp,
            "X-Webhook-Signature": self._make_signature(payload, timestamp, sig_key),
            **(headers or {}),
        }

        delivery = WebhookDelivery(
            event_id=event.id,
            url=url,
            payload=payload,
            headers=delivery_headers,
        )

        try:
            import urllib.request
            req = urllib.request.Request(
                url,
                data=payload.encode("utf-8"),
                headers=delivery_headers,
                method="POST",
            )
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                delivery.response_status = resp.status
                delivery.response_body = resp.read().decode("utf-8", errors="replace")
                delivery.success = 200 <= resp.status < 300
        except urllib.error.HTTPError as e:
            delivery.response_status = e.code
            delivery.response_body = e.read().decode("utf-8", errors="replace") if hasattr(e, "read") else None
            delivery.error = str(e)
        except Exception as e:
            delivery.error = str(e)

        delivery.attempts = 1
        delivery.delivered_at = time.time()
        self._delivery_log.append(delivery)
        return delivery

    def send_with_retry(
        self,
        url: str,
        event: WebhookEvent,
        max_attempts: int = 3,
        backoff_base: float = 1.0,
        **kwargs: Any,
    ) -> WebhookDelivery:
        last_delivery: WebhookDelivery | None = None
        for attempt in range(max_attempts):
            delivery = self.send(url, event, **kwargs)
            last_delivery = delivery
            if delivery.success:
                return delivery
            if attempt < max_attempts - 1:
                delay = backoff_base * (2 ** attempt)
                time.sleep(delay)

        return last_delivery or WebhookDelivery(
            event_id=event.id, url=url, payload=event.to_json(), headers={}, error="Max attempts reached"
        )

    def _make_signature(self, payload: str, timestamp: str, secret: str) -> str:
        signed_payload = f"{timestamp}.{payload}"
        return hmac.new(
            secret.encode("utf-8"),
            signed_payload.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()

    def deliveries(self, event_id: str | None = None) -> list[WebhookDelivery]:
        if event_id:
            return [d for d in self._delivery_log if d.event_id == event_id]
        return list(self._delivery_log)


class WebhookReceiver:
    """Verifies and processes incoming webhooks."""

    def __init__(self, secret: str = "") -> None:
        self.secret = secret
        self._handlers: dict[str, Callable[[dict[str, Any]], None]] = {}

    def verify(
        self,
        payload: bytes | str,
        signature: str,
        timestamp: str,
    ) -> bool:
        if not self.secret:
            return True
        if isinstance(payload, bytes):
            payload = payload.decode("utf-8")
        signed_payload = f"{timestamp}.{payload}"
        expected = hmac.new(
            self.secret.encode("utf-8"),
            signed_payload.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()
        return hmac.compare_digest(expected, signature)

    def on(self, event_type: str, handler: Callable[[dict[str, Any]], None]) -> None:
        self._handlers[event_type] = handler

    def handle(
        self,
        payload: bytes | str,
        headers: dict[str, str],
    ) -> tuple[bool, str]:
        if isinstance(payload, bytes):
            payload = payload.decode("utf-8")

        signature = headers.get("X-Webhook-Signature", "")
        timestamp = headers.get("X-Webhook-Timestamp", "")

        if not self.verify(payload, signature, timestamp):
            return False, "Invalid signature"

        try:
            data = json.loads(payload)
            event_type = data.get("type", "")
            handler = self._handlers.get(event_type)
            if handler:
                handler(data.get("data", {}))
            return True, "OK"
        except json.JSONDecodeError as e:
            return False, f"Invalid JSON: {e}"


def verify_webhook_signature(
    payload: str | bytes,
    signature: str,
    timestamp: str,
    secret: str,
    tolerance_seconds: int = 300,
) -> bool:
    """Verify a webhook signature with timestamp tolerance."""
    try:
        ts = int(timestamp)
        if abs(time.time() - ts) > tolerance_seconds:
            return False
    except ValueError:
        return False

    if isinstance(payload, bytes):
        payload = payload.decode("utf-8")

    signed_payload = f"{timestamp}.{payload}"
    expected = hmac.new(
        secret.encode("utf-8"),
        signed_payload.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()
    return hmac.compare_digest(expected, signature)
