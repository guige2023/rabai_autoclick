"""Webhook utilities: delivery, retry, signature generation, and event routing."""

from __future__ import annotations

import hashlib
import hmac
import json
import time
import urllib.request
import urllib.error
import uuid
from dataclasses import dataclass, field
from typing import Any, Callable

__all__ = [
    "WebhookDelivery",
    "WebhookRouter",
    "deliver_webhook",
    "sign_webhook",
    "verify_webhook_signature",
]


@dataclass
class WebhookDelivery:
    """Tracks webhook delivery attempts."""

    id: str
    url: str
    payload: dict[str, Any]
    attempts: int = 0
    max_attempts: int = 3
    success: bool = False
    response_code: int | None = None
    response_body: str = ""

    def record_attempt(self, success: bool, code: int | None = None, body: str = "") -> None:
        self.attempts += 1
        self.success = success
        self.response_code = code
        self.response_body = body[:500]


class WebhookRouter:
    """Route webhook events to handlers based on event type."""

    def __init__(self, secret: str = "") -> None:
        self.secret = secret
        self._handlers: dict[str, Callable[[dict[str, Any]], Any]] = {}

    def on(self, event_type: str) -> Callable[[Callable[[dict[str, Any]], Any]], Callable[[dict[str, Any]], Any]]:
        """Decorator to register a handler for an event type."""
        def decorator(handler: Callable[[dict[str, Any]], Any]) -> Callable[[dict[str, Any]], Any]:
            self._handlers[event_type] = handler
            return handler
        return decorator

    def dispatch(self, event_type: str, payload: dict[str, Any]) -> Any:
        """Dispatch an event to the registered handler."""
        handler = self._handlers.get(event_type)
        if handler:
            return handler(payload)
        return None


def deliver_webhook(
    url: str,
    payload: dict[str, Any],
    secret: str = "",
    headers: dict[str, str] | None = None,
    timeout: float = 30.0,
) -> tuple[bool, int | None, str]:
    """Deliver a webhook to a URL."""
    body = json.dumps(payload).encode()

    request_headers = {
        "Content-Type": "application/json",
        "X-Webhook-ID": str(uuid.uuid4()),
        "X-Webhook-Timestamp": str(int(time.time())),
    }

    if secret:
        request_headers["X-Webhook-Signature"] = sign_webhook(body, secret)

    if headers:
        request_headers.update(headers)

    req = urllib.request.Request(
        url,
        data=body,
        headers=request_headers,
        method="POST",
    )

    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return True, resp.status, resp.read().decode()[:500]
    except urllib.error.HTTPError as e:
        return False, e.code, e.read().decode()[:500]
    except Exception as e:
        return False, None, str(e)


def sign_webhook(payload: bytes, secret: str) -> str:
    """Generate HMAC signature for webhook payload."""
    sig = hmac.new(secret.encode(), payload, hashlib.sha256).hexdigest()
    return f"sha256={sig}"


def verify_webhook_signature(
    payload: bytes,
    signature: str,
    secret: str,
) -> bool:
    """Verify webhook signature."""
    if not signature.startswith("sha256="):
        return False
    expected = sign_webhook(payload, secret)
    return hmac.compare_digest(signature, expected)
