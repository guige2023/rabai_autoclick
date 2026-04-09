"""Webhook Dispatcher Action Module.

Dispatch webhooks to multiple endpoints with retry and delivery confirmation.
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable
import uuid


class DeliveryStatus(Enum):
    """Webhook delivery status."""
    PENDING = "pending"
    DELIVERED = "delivered"
    FAILED = "failed"
    RETRYING = "retrying"
    EXPIRED = "expired"


@dataclass
class WebhookPayload:
    """Webhook payload."""
    id: str
    event_type: str
    data: dict[str, Any]
    timestamp: float
    signature: str | None = None


@dataclass
class WebhookDelivery:
    """Webhook delivery record."""
    delivery_id: str
    payload: WebhookPayload
    endpoint: str
    status: DeliveryStatus
    attempts: int = 0
    max_attempts: int = 3
    next_retry: float | None = None
    last_error: str | None = None
    response_code: int | None = None
    delivered_at: float | None = None


@dataclass
class WebhookEndpoint:
    """Webhook endpoint configuration."""
    url: str
    secret: str | None = None
    headers: dict[str, str] | None = None
    enabled: bool = True
    timeout: float = 10.0


class WebhookDispatcher:
    """Dispatcher for sending webhooks to multiple endpoints."""

    def __init__(self) -> None:
        self._endpoints: dict[str, WebhookEndpoint] = {}
        self._deliveries: dict[str, WebhookDelivery] = {}
        self._retry_delays = [1, 5, 30]
        self._lock = asyncio.Lock()

    def register_endpoint(self, endpoint: WebhookEndpoint) -> str:
        """Register a webhook endpoint."""
        endpoint_id = str(uuid.uuid4())
        self._endpoints[endpoint_id] = endpoint
        return endpoint_id

    def remove_endpoint(self, endpoint_id: str) -> bool:
        """Remove a webhook endpoint."""
        if endpoint_id in self._endpoints:
            del self._endpoints[endpoint_id]
            return True
        return False

    def _sign_payload(self, payload: str, secret: str) -> str:
        """Generate HMAC signature for payload."""
        return hmac.new(secret.encode(), payload.encode(), hashlib.sha256).hexdigest()

    async def dispatch(
        self,
        event_type: str,
        data: dict[str, Any],
        endpoint_ids: list[str] | None = None,
    ) -> list[str]:
        """Dispatch webhook to registered endpoints."""
        payload = WebhookPayload(
            id=str(uuid.uuid4()),
            event_type=event_type,
            data=data,
            timestamp=time.time()
        )
        delivery_ids = []
        targets = endpoint_ids or list(self._endpoints.keys())
        for endpoint_id in targets:
            endpoint = self._endpoints.get(endpoint_id)
            if not endpoint or not endpoint.enabled:
                continue
            if endpoint.secret:
                payload_str = f"{payload.timestamp}.{str(data)}"
                payload.signature = self._sign_payload(payload_str, endpoint.secret)
            delivery = WebhookDelivery(
                delivery_id=str(uuid.uuid4()),
                payload=payload,
                endpoint=endpoint.url,
                status=DeliveryStatus.PENDING,
                max_attempts=3
            )
            self._deliveries[delivery.delivery_id] = delivery
            delivery_ids.append(delivery.delivery_id)
            asyncio.create_task(self._deliver_with_retry(delivery, endpoint))
        return delivery_ids

    async def _deliver_with_retry(self, delivery: WebhookDelivery, endpoint: WebhookEndpoint) -> None:
        """Deliver webhook with retry logic."""
        while delivery.attempts < delivery.max_attempts:
            delivery.attempts += 1
            try:
                success = await self._deliver(delivery, endpoint)
                if success:
                    delivery.status = DeliveryStatus.DELIVERED
                    delivery.delivered_at = time.time()
                    return
            except Exception as e:
                delivery.last_error = str(e)
            if delivery.attempts < delivery.max_attempts:
                delivery.status = DeliveryStatus.RETRYING
                delay = self._retry_delays[min(delivery.attempts - 1, len(self._retry_delays) - 1)]
                delivery.next_retry = time.time() + delay
                await asyncio.sleep(delay)
        delivery.status = DeliveryStatus.FAILED

    async def _deliver(self, delivery: WebhookDelivery, endpoint: WebhookEndpoint) -> bool:
        """Deliver webhook to endpoint. Override for actual HTTP delivery."""
        import aiohttp
        payload_dict = {
            "id": delivery.payload.id,
            "event_type": delivery.payload.event_type,
            "data": delivery.payload.data,
            "timestamp": delivery.payload.timestamp,
        }
        headers = {"Content-Type": "application/json"}
        if endpoint.headers:
            headers.update(endpoint.headers)
        if delivery.payload.signature:
            headers["X-Webhook-Signature"] = delivery.payload.signature
        async with aiohttp.ClientSession() as session:
            async with session.post(
                endpoint.url,
                json=payload_dict,
                headers=headers,
                timeout=aiohttp.ClientTimeout(total=endpoint.timeout)
            ) as resp:
                delivery.response_code = resp.status
                return 200 <= resp.status < 300

    def get_delivery_status(self, delivery_id: str) -> WebhookDelivery | None:
        """Get delivery status."""
        return self._deliveries.get(delivery_id)

    def get_pending_deliveries(self) -> list[WebhookDelivery]:
        """Get all pending deliveries."""
        return [d for d in self._deliveries.values() if d.status == DeliveryStatus.PENDING]
