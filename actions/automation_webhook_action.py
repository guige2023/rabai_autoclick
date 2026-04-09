"""Webhook handling utilities with signature verification and delivery management.

Supports HMAC, RSA signature verification, retry logic, and event parsing.
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import logging
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable
from urllib.parse import parse_qs

logger = logging.getLogger(__name__)


class WebhookError(Exception):
    """Raised for webhook processing errors."""

    pass


class SignatureError(WebhookError):
    """Raised when signature verification fails."""

    pass


class DeliveryStatus(Enum):
    """Webhook delivery status."""

    PENDING = "pending"
    DELIVERED = "delivered"
    FAILED = "failed"
    RETRYING = "retrying"
    DROPPED = "dropped"


@dataclass
class WebhookEvent:
    """Parsed webhook event."""

    id: str
    type: str
    payload: dict[str, Any]
    headers: dict[str, str]
    delivered_at: float = field(default_factory=time.time)
    delivery_status: DeliveryStatus = DeliveryStatus.PENDING
    delivery_attempts: int = 0
    last_attempt_at: float | None = None
    last_error: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class WebhookConfig:
    """Configuration for webhook handling."""

    secret: str | None = None
    signature_header: str = "X-Signature"
    timestamp_header: str = "X-Timestamp"
    event_type_header: str = "X-Event-Type"
    id_header: str = "X-Webhook-ID"
    tolerance_seconds: int = 300
    verify_signature: bool = True
    max_retries: int = 3
    retry_delay: float = 5.0
    retry_multiplier: float = 2.0


class SignatureVerifier:
    """Webhook signature verifier supporting multiple algorithms."""

    def __init__(self, secret: str, tolerance_seconds: int = 300) -> None:
        self.secret = secret.encode()
        self.tolerance_seconds = tolerance_seconds

    def verify_hmac_sha256(self, payload: bytes, signature: str, timestamp: str | None = None) -> bool:
        """Verify HMAC-SHA256 signature.

        Args:
            payload: Raw request body.
            signature: Signature from header.
            timestamp: Optional timestamp for replay protection.

        Returns:
            True if signature is valid.
        """
        if timestamp:
            try:
                ts = int(timestamp)
                if abs(time.time() - ts) > self.tolerance_seconds:
                    logger.warning("Webhook timestamp outside tolerance")
                    return False
            except ValueError:
                return False

        expected = self._compute_signature(payload, timestamp)
        return hmac.compare_digest(expected, signature)

    def verify_hmac_sha1(self, payload: bytes, signature: str, timestamp: str | None = None) -> bool:
        """Verify HMAC-SHA1 signature."""
        if timestamp:
            try:
                ts = int(timestamp)
                if abs(time.time() - ts) > self.tolerance_seconds:
                    return False
            except ValueError:
                return False

        mac = hmac.new(self.secret, payload, hashlib.sha1)
        if timestamp:
            mac.update(timestamp.encode())
        expected = f"sha1={mac.hexdigest()}"
        return hmac.compare_digest(expected, signature)

    def verify_signed_payload(self, payload: bytes, signature: str) -> bool:
        """Verify GitHub-style signed payload."""
        try:
            sig_parts = dict(p.split("=", 1) for p in signature.split(","))
            algorithm = sig_parts.get("sha1", "sha1")
            sig_hex = sig_parts.get(algorithm, "")

            mac = hmac.new(self.secret, payload, getattr(hashlib, algorithm))
            expected = mac.hexdigest()

            return hmac.compare_digest(expected, sig_hex)
        except Exception as e:
            logger.error("Signed payload verification failed: %s", e)
            return False

    def _compute_signature(self, payload: bytes, timestamp: str | None) -> str:
        """Compute HMAC signature for payload."""
        if timestamp:
            mac = hmac.new(self.secret, timestamp.encode() + b"." + payload, hashlib.sha256)
        else:
            mac = hmac.new(self.secret, payload, hashlib.sha256)
        return mac.hexdigest()


class WebhookParser:
    """Parse webhook payloads from various services."""

    @staticmethod
    def parse_github(payload: dict[str, Any], headers: dict[str, str]) -> WebhookEvent:
        """Parse GitHub webhook event."""
        event_type = headers.get("X-GitHub-Event", "unknown")
        event_id = headers.get("X-GitHub-Delivery", "")
        return WebhookEvent(id=event_id, type=event_type, payload=payload, headers=headers)

    @staticmethod
    def parse_stripe(payload: dict[str, Any], headers: dict[str, str]) -> WebhookEvent:
        """Parse Stripe webhook event."""
        event_type = payload.get("type", "unknown")
        event_id = payload.get("id", "")
        return WebhookEvent(id=event_id, type=event_type, payload=payload, headers=headers)

    @staticmethod
    def parse_slack(payload: dict[str, Any], headers: dict[str, str]) -> WebhookEvent:
        """Parse Slack webhook event."""
        event_type = payload.get("type", "event_callback")
        event_id = payload.get("event_id", "")
        inner_event = payload.get("event", {})
        return WebhookEvent(id=event_id, type=event_type, payload={**payload, "event": inner_event}, headers=headers)

    @staticmethod
    def parse_generic(payload: dict[str, Any], headers: dict[str, str], config: WebhookConfig) -> WebhookEvent:
        """Parse generic webhook event."""
        event_type = headers.get(config.event_type_header, "unknown")
        event_id = headers.get(config.id_header, str(time.time()))
        return WebhookEvent(id=event_id, type=event_type, payload=payload, headers=headers)


@dataclass
class WebhookDelivery:
    """Record of a webhook delivery attempt."""

    event_id: str
    url: str
    payload: bytes
    headers: dict[str, str]
    status: DeliveryStatus = DeliveryStatus.PENDING
    attempts: int = 0
    last_attempt: float = 0.0
    response_status: int | None = None
    response_body: str | None = None
    error: str | None = None


class WebhookDeliveryManager:
    """Manage webhook deliveries with retry logic."""

    def __init__(self, config: WebhookConfig | None = None) -> None:
        self.config = config or WebhookConfig()
        self._pending: dict[str, WebhookDelivery] = {}
        self._handlers: dict[str, Callable] = {}
        self._delivery_callbacks: list[Callable[[WebhookDelivery], None]] = []

    def register_handler(self, event_type: str, handler: Callable[[WebhookEvent], Any]) -> None:
        """Register handler for event type."""
        self._handlers[event_type] = handler

    def on_delivery(self, callback: Callable[[WebhookDelivery], None]) -> None:
        """Register callback for delivery events."""
        self._delivery_callbacks.append(callback)

    async def deliver(
        self,
        url: str,
        payload: bytes,
        headers: dict[str, str] | None = None,
        event_id: str | None = None,
    ) -> WebhookDelivery:
        """Deliver webhook to URL with retry logic."""
        delivery = WebhookDelivery(
            event_id=event_id or str(time.time()),
            url=url,
            payload=payload,
            headers=headers or {},
        )
        self._pending[delivery.event_id] = delivery

        attempt = 0
        last_error: str | None = None

        while attempt < self.config.max_retries:
            attempt += 1
            delivery.attempts = attempt
            delivery.last_attempt = time.time()

            try:
                status, response_body = await self._send_request(url, payload, headers or {})
                delivery.response_status = status
                delivery.response_body = response_body

                if 200 <= status < 300:
                    delivery.status = DeliveryStatus.DELIVERED
                    logger.info("Webhook delivered: %s", delivery.event_id)
                    break
                else:
                    last_error = f"HTTP {status}: {response_body}"
                    delivery.error = last_error

                    if status >= 500 or status == 429:
                        delivery.status = DeliveryStatus.RETRYING
                    else:
                        delivery.status = DeliveryStatus.FAILED
                        logger.error("Webhook delivery failed (non-retryable): %s", last_error)
                        break

            except Exception as e:
                last_error = str(e)
                delivery.error = last_error
                delivery.status = DeliveryStatus.RETRYING
                logger.warning("Webhook delivery error: %s", e)

            if attempt < self.config.max_retries:
                delay = self.config.retry_delay * (self.config.retry_multiplier ** (attempt - 1))
                logger.info("Retrying webhook in %.1fs (attempt %d)", delay, attempt)
                await asyncio.sleep(delay)

        if delivery.status != DeliveryStatus.DELIVERED:
            delivery.status = DeliveryStatus.FAILED
            logger.error("Webhook delivery failed after %d attempts", attempt)

        for callback in self._delivery_callbacks:
            try:
                callback(delivery)
            except Exception as e:
                logger.error("Delivery callback error: %s", e)

        self._pending.pop(delivery.event_id, None)
        return delivery

    async def _send_request(self, url: str, payload: bytes, headers: dict[str, str]) -> tuple[int, str]:
        """Send HTTP request. Override for custom HTTP client."""
        import aiohttp

        async with aiohttp.ClientSession() as session:
            async with session.post(url, data=payload, headers=headers, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                body = await resp.text()
                return resp.status, body

    def get_pending(self) -> list[WebhookDelivery]:
        """Get list of pending deliveries."""
        return list(self._pending.values())


class WebhookServer:
    """Simple webhook receiver server."""

    def __init__(self, config: WebhookConfig, delivery_manager: WebhookDeliveryManager) -> None:
        self.config = config
        self.manager = delivery_manager
        self.verifier: SignatureVerifier | None = None
        if config.secret and config.verify_signature:
            self.verifier = SignatureVerifier(config.secret, config.tolerance_seconds)

    def verify(self, payload: bytes, headers: dict[str, str]) -> None:
        """Verify webhook signature.

        Raises:
            SignatureError: If signature is invalid.
        """
        if not self.verifier or not self.config.verify_signature:
            return

        signature = headers.get(self.config.signature_header, "")
        timestamp = headers.get(self.config.timestamp_header)

        if not signature:
            raise SignatureError("Missing signature header")

        if signature.startswith("sha1="):
            if not self.verifier.verify_hmac_sha1(payload, signature, timestamp):
                raise SignatureError("Invalid HMAC-SHA1 signature")
        elif self.verifier.verify_hmac_sha256(payload, signature, timestamp):
            pass
        else:
            raise SignatureError("Invalid signature")

    def parse(self, payload: bytes, headers: dict[str, str]) -> WebhookEvent:
        """Parse webhook payload into event."""
        try:
            data = json.loads(payload)
        except json.JSONDecodeError as e:
            raise WebhookError(f"Invalid JSON payload: {e}")

        return WebhookParser.parse_generic(data, headers, self.config)

    async def handle(self, payload: bytes, headers: dict[str, str]) -> WebhookEvent:
        """Handle incoming webhook.

        Args:
            payload: Raw request body.
            headers: Request headers.

        Returns:
            Parsed webhook event.

        Raises:
            SignatureError: If signature verification fails.
            WebhookError: If parsing fails.
        """
        self.verify(payload, headers)
        event = self.parse(payload, headers)

        if event.type in self.manager._handlers:
            handler = self.manager._handlers[event.type]
            try:
                if asyncio.iscoroutinefunction(handler):
                    result = await handler(event)
                else:
                    result = handler(event)
                event.metadata["handler_result"] = result
                event.delivery_status = DeliveryStatus.DELIVERED
            except Exception as e:
                event.delivery_status = DeliveryStatus.FAILED
                event.last_error = str(e)
                logger.error("Handler error for %s: %s", event.type, e)
        else:
            logger.debug("No handler for event type: %s", event.type)

        return event
