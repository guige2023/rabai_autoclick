"""Webhook utilities for RabAI AutoClick.

Provides:
- Webhook event handling
- Signature verification
- Payload parsing
"""

from __future__ import annotations

import hashlib
import hmac
import json
from typing import (
    Any,
    Callable,
    Dict,
    Optional,
)


class WebhookEvent:
    """A webhook event."""

    def __init__(
        self,
        event_type: str,
        payload: Dict[str, Any],
        headers: Dict[str, str],
    ) -> None:
        self.event_type = event_type
        self.payload = payload
        self.headers = headers


class WebhookHandler:
    """Handle incoming webhooks."""

    def __init__(self, secret: Optional[str] = None) -> None:
        self._secret = secret
        self._handlers: Dict[str, Callable[[WebhookEvent], None]] = {}

    def on(
        self,
        event_type: str,
        handler: Callable[[WebhookEvent], None],
    ) -> None:
        """Register an event handler.

        Args:
            event_type: Event type to handle.
            handler: Function to call.
        """
        self._handlers[event_type] = handler

    def verify_signature(
        self,
        payload: bytes,
        signature: str,
        timestamp: Optional[str] = None,
    ) -> bool:
        """Verify webhook signature.

        Args:
            payload: Raw payload bytes.
            signature: Signature from header.
            timestamp: Optional timestamp.

        Returns:
            True if signature is valid.
        """
        if not self._secret:
            return True

        expected = hmac.new(
            self._secret.encode(),
            payload,
            hashlib.sha256,
        ).hexdigest()

        return hmac.compare_digest(f"sha256={expected}", signature)

    def handle(
        self,
        payload: Dict[str, Any],
        headers: Dict[str, str],
    ) -> bool:
        """Handle an incoming webhook.

        Args:
            payload: Event payload.
            headers: Request headers.

        Returns:
            True if handled successfully.
        """
        event_type = headers.get("X-Event-Type", "unknown")
        event = WebhookEvent(event_type, payload, headers)

        handler = self._handlers.get(event_type)
        if handler:
            handler(event)
            return True
        return False


__all__ = [
    "WebhookEvent",
    "WebhookHandler",
]
