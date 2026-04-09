"""Webhook Handler Action Module.

Handle incoming webhooks with signature verification and event routing.
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable
import uuid


class WebhookEventType(Enum):
    """Webhook event types."""
    UNKNOWN = "unknown"
    PING = "ping"
    CONFIRMATION = "confirmation"


@dataclass
class IncomingWebhook:
    """Incoming webhook data."""
    id: str
    event_type: str
    payload: dict[str, Any]
    headers: dict[str, str]
    received_at: float
    signature: str | None = None


@dataclass
class WebhookRoute:
    """Webhook event route."""
    event_type: str
    handler: Callable[[IncomingWebhook], Any]
    verify_signature: bool = True


class SignatureVerificationError(Exception):
    """Raised when signature verification fails."""
    pass


class WebhookHandler:
    """Handle incoming webhooks with routing and verification."""

    def __init__(self, secret: str | None = None) -> None:
        self.secret = secret
        self._routes: dict[str, WebhookRoute] = {}
        self._default_handler: Callable[[IncomingWebhook], Any] | None = None
        self._processed_ids: set[str] = set()
        self._lock = asyncio.Lock()

    def register_route(
        self,
        event_type: str,
        handler: Callable[[IncomingWebhook], Any],
        verify: bool = True
    ) -> None:
        """Register a handler for an event type."""
        self._routes[event_type] = WebhookRoute(event_type, handler, verify)

    def set_default_handler(self, handler: Callable[[IncomingWebhook], Any]) -> None:
        """Set default handler for unregistered events."""
        self._default_handler = handler

    def verify_signature(self, payload: bytes, signature: str, timestamp: str | None = None) -> bool:
        """Verify webhook signature."""
        if not self.secret:
            return True
        if timestamp:
            expected_sig = hmac.new(
                self.secret.encode(),
                f"{timestamp}.".encode() + payload,
                hashlib.sha256
            ).hexdigest()
        else:
            expected_sig = hmac.new(self.secret.encode(), payload, hashlib.sha256).hexdigest()
        return hmac.compare_digest(f"sha256={expected_sig}", signature)

    async def handle_request(
        self,
        payload: bytes,
        headers: dict[str, str],
        raw_body: bytes | None = None
    ) -> tuple[Any, int]:
        """Handle incoming webhook request. Returns (response, status_code)."""
        event_type = headers.get("X-Webhook-Event", "unknown")
        webhook_id = headers.get("X-Webhook-ID", str(uuid.uuid4()))
        signature = headers.get("X-Webhook-Signature")
        timestamp = headers.get("X-Webhook-Timestamp")
        try:
            data = json.loads(payload)
        except json.JSONDecodeError:
            return {"error": "Invalid JSON"}, 400
        webhook = IncomingWebhook(
            id=webhook_id,
            event_type=event_type,
            payload=data,
            headers=headers,
            received_at=time.time(),
            signature=signature
        )
        async with self._lock:
            if webhook_id in self._processed_ids:
                return {"status": "already_processed"}, 200
            self._processed_ids.add(webhook_id)
        route = self._routes.get(event_type)
        if route and route.verify_signature:
            if not self.verify_signature(raw_body or payload, signature or "", timestamp):
                return {"error": "Invalid signature"}, 401
        try:
            if route:
                result = route.handler(webhook)
                if asyncio.iscoroutine(result):
                    result = await result
                return {"status": "ok", "result": result}, 200
            elif self._default_handler:
                result = self._default_handler(webhook)
                if asyncio.iscoroutine(result):
                    result = await result
                return {"status": "ok", "result": result}, 200
            else:
                return {"status": "no_handler"}, 202
        except Exception as e:
            return {"error": str(e)}, 500
