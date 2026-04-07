"""
Webhook utilities for event delivery and processing.

Provides webhook delivery, signature verification, retry logic,
event queuing, and multi-destination fan-out.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import logging
import time
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Callable, Optional

import httpx

logger = logging.getLogger(__name__)


class DeliveryStatus(Enum):
    PENDING = auto()
    DELIVERED = auto()
    FAILED = auto()
    RETRYING = auto()


@dataclass
class WebhookEvent:
    """Represents a webhook event."""
    id: str
    event_type: str
    payload: dict[str, Any]
    timestamp: float = field(default_factory=time.time)
    headers: dict[str, str] = field(default_factory=dict)
    retries: int = 0


@dataclass
class WebhookEndpoint:
    """Webhook endpoint configuration."""
    url: str
    secret: Optional[str] = None
    content_type: str = "application/json"
    timeout: float = 10.0
    enabled: bool = True
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class DeliveryResult:
    """Result of a webhook delivery attempt."""
    status: DeliveryStatus
    status_code: Optional[int] = None
    response_body: Optional[str] = None
    duration_ms: float = 0.0
    error: Optional[str] = None


class WebhookSignature:
    """Webhook signature generation and verification."""

    @staticmethod
    def generate(payload: bytes | str, secret: str, algorithm: str = "sha256") -> str:
        """Generate HMAC signature for a payload."""
        if isinstance(payload, str):
            payload = payload.encode()
        if algorithm == "sha256":
            mac = hmac.new(secret.encode(), payload, hashlib.sha256)
        elif algorithm == "sha512":
            mac = hmac.new(secret.encode(), payload, hashlib.sha512)
        else:
            mac = hmac.new(secret.encode(), payload, hashlib.sha256)
        return f"{algorithm}={mac.hexdigest()}"

    @staticmethod
    def verify(payload: bytes | str, signature: str, secret: str) -> bool:
        """Verify a webhook signature."""
        expected = WebhookSignature.generate(payload, secret)
        return hmac.compare_digest(expected, signature)

    @staticmethod
    def generate_idempotency_key(event_id: str, endpoint_url: str) -> str:
        """Generate an idempotency key for deduplication."""
        raw = f"{event_id}:{endpoint_url}"
        return hashlib.sha256(raw.encode()).hexdigest()


class WebhookDeliverer:
    """Delivers webhook events to endpoints."""

    def __init__(self, timeout: float = 10.0, max_retries: int = 3) -> None:
        self.timeout = timeout
        self.max_retries = max_retries
        self._client: Optional[httpx.AsyncClient] = None

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None:
            self._client = httpx.AsyncClient(timeout=self.timeout)
        return self._client

    async def deliver(
        self,
        endpoint: WebhookEndpoint,
        event: WebhookEvent,
    ) -> DeliveryResult:
        """Deliver an event to a webhook endpoint."""
        start = time.perf_counter()
        payload = json.dumps(event.payload)
        headers = dict(event.headers)
        headers["Content-Type"] = endpoint.content_type
        headers["X-Webhook-Event"] = event.event_type
        headers["X-Webhook-Event-ID"] = event.id
        headers["X-Webhook-Timestamp"] = str(event.timestamp)

        if endpoint.secret:
            signature = WebhookSignature.generate(payload, endpoint.secret)
            headers["X-Webhook-Signature"] = signature

        for attempt in range(self.max_retries):
            try:
                client = await self._get_client()
                response = await client.post(
                    endpoint.url,
                    content=payload,
                    headers=headers,
                )
                duration_ms = (time.perf_counter() - start) * 1000

                if 200 <= response.status_code < 300:
                    return DeliveryResult(
                        status=DeliveryStatus.DELIVERED,
                        status_code=response.status_code,
                        response_body=response.text[:500],
                        duration_ms=duration_ms,
                    )
                elif response.status_code >= 500:
                    continue
                else:
                    return DeliveryResult(
                        status=DeliveryStatus.FAILED,
                        status_code=response.status_code,
                        response_body=response.text[:500],
                        duration_ms=duration_ms,
                        error=f"Client error: {response.status_code}",
                    )
            except httpx.TimeoutException:
                if attempt == self.max_retries - 1:
                    return DeliveryResult(
                        status=DeliveryStatus.FAILED,
                        duration_ms=(time.perf_counter() - start) * 1000,
                        error="Timeout",
                    )
            except Exception as e:
                if attempt == self.max_retries - 1:
                    return DeliveryResult(
                        status=DeliveryStatus.FAILED,
                        duration_ms=(time.perf_counter() - start) * 1000,
                        error=str(e),
                    )

        return DeliveryResult(
            status=DeliveryStatus.FAILED,
            duration_ms=(time.perf_counter() - start) * 1000,
            error="Max retries exceeded",
        )

    async def close(self) -> None:
        if self._client:
            await self._client.aclose()
            self._client = None


class WebhookManager:
    """Manages multiple webhook endpoints and event delivery."""

    def __init__(self) -> None:
        self._endpoints: dict[str, WebhookEndpoint] = {}
        self._deliverer = WebhookDeliverer()
        self._delivery_history: list[DeliveryResult] = []

    def register_endpoint(
        self,
        name: str,
        url: str,
        secret: Optional[str] = None,
        **kwargs: Any,
    ) -> None:
        """Register a webhook endpoint."""
        self._endpoints[name] = WebhookEndpoint(url=url, secret=secret, **kwargs)
        logger.info("Registered webhook endpoint: %s -> %s", name, url)

    def unregister_endpoint(self, name: str) -> None:
        """Unregister a webhook endpoint."""
        if name in self._endpoints:
            del self._endpoints[name]

    async def broadcast(
        self,
        event_type: str,
        payload: dict[str, Any],
        filter_names: Optional[list[str]] = None,
    ) -> dict[str, DeliveryResult]:
        """Broadcast an event to all registered endpoints."""
        event = WebhookEvent(
            id=f"{time.time()}-{hashlib.md5(str(time.time()).encode()).hexdigest()[:8]}",
            event_type=event_type,
            payload=payload,
        )

        results = {}
        targets = {k: v for k, v in self._endpoints.items() if v.enabled}
        if filter_names:
            targets = {k: v for k, v in targets.items() if k in filter_names}

        for name, endpoint in targets.items():
            result = await self._deliverer.deliver(endpoint, event)
            results[name] = result
            self._delivery_history.append(result)

        return results

    async def deliver_to(
        self,
        endpoint_name: str,
        event_type: str,
        payload: dict[str, Any],
    ) -> Optional[DeliveryResult]:
        """Deliver an event to a specific endpoint."""
        endpoint = self._endpoints.get(endpoint_name)
        if not endpoint:
            logger.error("Unknown endpoint: %s", endpoint_name)
            return None

        event = WebhookEvent(
            id=f"{time.time()}-{hashlib.md5(str(time.time()).encode()).hexdigest()[:8]}",
            event_type=event_type,
            payload=payload,
        )

        result = await self._deliverer.deliver(endpoint, event)
        self._delivery_history.append(result)
        return result

    def get_history(self, limit: int = 100) -> list[DeliveryResult]:
        """Get recent delivery history."""
        return self._delivery_history[-limit:]

    def get_endpoint_stats(self, name: str) -> dict[str, Any]:
        """Get delivery statistics for an endpoint."""
        endpoint = self._endpoints.get(name)
        if not endpoint:
            return {}

        relevant = [r for r in self._delivery_history]
        total = len(relevant)
        delivered = sum(1 for r in relevant if r.status == DeliveryStatus.DELIVERED)
        failed = sum(1 for r in relevant if r.status == DeliveryStatus.FAILED)
        avg_duration = sum(r.duration_ms for r in relevant) / total if total > 0 else 0

        return {
            "endpoint": name,
            "url": endpoint.url,
            "total_deliveries": total,
            "delivered": delivered,
            "failed": failed,
            "success_rate": (delivered / total * 100) if total > 0 else 0,
            "avg_duration_ms": avg_duration,
        }
