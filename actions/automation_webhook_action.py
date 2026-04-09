"""Automation Webhook Action module.

Handles webhook-based automation triggers with signature
verification, retry logic, and event parsing.
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Optional

import aiohttp


class WebhookEventType(Enum):
    """Standard webhook event types."""

    UNKNOWN = "unknown"
    PING = "ping"
    PUSH = "push"
    PULL_REQUEST = "pull_request"
    ISSUE = "issue"
    COMMENT = "comment"
    MEMBER = "member"
    BUILD = "build"
    DEPLOY = "deploy"


@dataclass
class WebhookEvent:
    """A parsed webhook event."""

    event_type: WebhookEventType
    payload: dict[str, Any]
    headers: dict[str, str]
    timestamp: float = field(default_factory=time.time)
    delivery_id: Optional[str] = None

    @property
    def is_valid(self) -> bool:
        """Check if event has required fields."""
        return self.event_type != WebhookEventType.UNKNOWN and bool(self.payload)


@dataclass
class WebhookConfig:
    """Configuration for webhook handling."""

    secret: Optional[str] = None
    timeout: float = 30.0
    max_retries: int = 3
    retry_delay: float = 1.0
    signature_header: str = "X-Hub-Signature-256"
    delivery_id_header: str = "X-GitHub-Delivery"


class SignatureVerificationError(Exception):
    """Raised when webhook signature verification fails."""
    pass


def verify_signature(
    payload: bytes,
    secret: str,
    signature: str,
    algorithm: str = "sha256",
) -> bool:
    """Verify webhook signature.

    Args:
        payload: Raw request body
        secret: Webhook secret
        signature: Signature from header
        algorithm: Hash algorithm

    Returns:
        True if signature is valid
    """
    if not signature or not signature.startswith(f"{algorithm}="):
        return False

    expected = hmac.new(
        secret.encode(),
        payload,
        hashlib.new(algorithm),
    ).hexdigest()

    received = signature.split("=", 1)[1]

    return hmac.compare_digest(expected, received)


def parse_github_event(headers: dict[str, str], payload: dict[str, Any]) -> WebhookEvent:
    """Parse GitHub webhook event.

    Args:
        headers: Request headers
        payload: Event payload

    Returns:
        WebhookEvent
    """
    event_name = headers.get("X-GitHub-Event", "").lower()
    delivery_id = headers.get("X-GitHub-Delivery")

    event_type_map = {
        "push": WebhookEventType.PUSH,
        "pull_request": WebhookEventType.PULL_REQUEST,
        "issues": WebhookEventType.ISSUE,
        "issue_comment": WebhookEventType.COMMENT,
        "member": WebhookEventType.MEMBER,
        "ping": WebhookEventType.PING,
        "create": WebhookEventType.PUSH,
        "delete": WebhookEventType.PUSH,
    }

    event_type = event_type_map.get(event_name, WebhookEventType.UNKNOWN)

    return WebhookEvent(
        event_type=event_type,
        payload=payload,
        headers=headers,
        delivery_id=delivery_id,
    )


def parse_generic_event(
    headers: dict[str, str],
    payload: dict[str, Any],
) -> WebhookEvent:
    """Parse generic webhook event.

    Args:
        headers: Request headers
        payload: Event payload

    Returns:
        WebhookEvent
    """
    event_type_str = (
        headers.get("X-Event-Type")
        or headers.get("Event")
        or payload.get("type", "unknown")
    ).lower()

    if "ping" in event_type_str:
        event_type = WebhookEventType.PING
    elif "push" in event_type_str:
        event_type = WebhookEventType.PUSH
    elif "build" in event_type_str:
        event_type = WebhookEventType.BUILD
    elif "deploy" in event_type_str:
        event_type = WebhookEventType.DEPLOY
    else:
        event_type = WebhookEventType.UNKNOWN

    return WebhookEvent(
        event_type=event_type,
        payload=payload,
        headers=headers,
    )


class WebhookHandler:
    """Handles incoming webhooks with handlers."""

    def __init__(self, config: Optional[WebhookConfig] = None):
        self.config = config or WebhookConfig()
        self._handlers: dict[WebhookEventType, list[Callable]] = {
            event_type: [] for event_type in WebhookEventType
        }

    def register(
        self,
        event_type: WebhookEventType,
        handler: Callable[[WebhookEvent], Any],
    ) -> None:
        """Register a handler for an event type."""
        self._handlers[event_type].append(handler)

    async def handle(
        self,
        headers: dict[str, str],
        payload: bytes,
        parser: Callable[[dict[str, str], dict], WebhookEvent] | None = None,
    ) -> list[Any]:
        """Handle incoming webhook.

        Args:
            headers: Request headers
            payload: Raw request body
            parser: Optional custom event parser

        Returns:
            List of handler results
        """
        try:
            data = json.loads(payload)
        except json.JSONDecodeError:
            return []

        if self.config.secret:
            signature = headers.get(self.config.signature_header, "")
            if not verify_signature(payload, self.config.secret, signature):
                raise SignatureVerificationError("Invalid webhook signature")

        if parser:
            event = parser(headers, data)
        else:
            event = parse_generic_event(headers, data)

        if event.event_type == WebhookEventType.PING:
            return [{"status": "pong"}]

        handlers = self._handlers.get(event.event_type, [])
        results = []

        for handler in handlers:
            try:
                if asyncio.iscoroutinefunction(handler):
                    result = await handler(event)
                else:
                    result = handler(event)
                results.append(result)
            except Exception as e:
                results.append({"error": str(e)})

        return results


class WebhookDelivery:
    """Webhook delivery service for outgoing webhooks."""

    def __init__(
        self,
        timeout: float = 30.0,
        max_retries: int = 3,
    ):
        self.timeout = timeout
        self.max_retries = max_retries

    async def deliver(
        self,
        url: str,
        payload: dict[str, Any],
        secret: Optional[str] = None,
        headers: Optional[dict[str, str]] = None,
    ) -> tuple[bool, int]:
        """Deliver webhook to URL.

        Args:
            url: Target URL
            payload: Payload to send
            secret: Optional signing secret
            headers: Additional headers

        Returns:
            Tuple of (success, status_code)
        """
        headers = headers or {}
        headers["Content-Type"] = "application/json"

        body = json.dumps(payload)

        if secret:
            signature = hmac.new(
                secret.encode(),
                body.encode(),
                hashlib.sha256,
            ).hexdigest()
            headers["X-Hub-Signature-256"] = f"sha256={signature}"

        for attempt in range(self.max_retries):
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.post(
                        url,
                        data=body,
                        headers=headers,
                        timeout=aiohttp.ClientTimeout(total=self.timeout),
                    ) as response:
                        if 200 <= response.status < 300:
                            return True, response.status
                        elif response.status >= 500:
                            await asyncio.sleep(2**attempt)
                            continue
                        else:
                            return False, response.status
            except asyncio.TimeoutError:
                if attempt < self.max_retries - 1:
                    await asyncio.sleep(2**attempt)
                    continue
                return False, 0
            except Exception:
                if attempt < self.max_retries - 1:
                    await asyncio.sleep(2**attempt)
                    continue
                return False, 0

        return False, 0
