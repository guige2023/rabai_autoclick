"""Webhook v2 Action Module.

Advanced webhook management with filtering, transformation, and delivery guarantees.
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable

from .webhook_handler_action import WebhookEventType


class DeliveryPolicy(Enum):
    """Webhook delivery policies."""
    AT_LEAST_ONCE = "at_least_once"
    AT_MOST_ONCE = "at_most_once"
    EXACTLY_ONCE = "exactly_once"


@dataclass
class WebhookFilter:
    """Filter for webhook events."""
    name: str
    predicate: Callable[[dict], bool]
    description: str | None = None


@dataclass
class WebhookTransform:
    """Transform webhook payload."""
    name: str
    transform_fn: Callable[[dict], dict]
    description: str | None = None


@dataclass
class WebhookSubscription:
    """Webhook subscription."""
    id: str
    endpoint_url: str
    event_types: list[str]
    filters: list[WebhookFilter] = field(default_factory=list)
    transforms: list[WebhookTransform] = field(default_factory=list)
    secret: str | None = None
    enabled: bool = True
    retry_policy: dict = field(default_factory=lambda: {"max_attempts": 3, "backoff": [1, 5, 30]})
    delivery_policy: DeliveryPolicy = DeliveryPolicy.AT_LEAST_ONCE


@dataclass
class WebhookDeliveryRecord:
    """Record of webhook delivery attempt."""
    delivery_id: str
    subscription_id: str
    event_type: str
    payload: dict
    status: str
    attempts: int = 0
    last_attempt: float | None = None
    response_code: int | None = None
    error: str | None = None


class WebhookManager:
    """Advanced webhook manager with filtering and transformation."""

    def __init__(self) -> None:
        self._subscriptions: dict[str, WebhookSubscription] = {}
        self._delivery_records: dict[str, WebhookDeliveryRecord] = {}
        self._lock = asyncio.Lock()
        self._idempotency_cache: dict[str, float] = {}
        self._cache_ttl: float = 3600.0

    def subscribe(self, subscription: WebhookSubscription) -> str:
        """Register a webhook subscription."""
        self._subscriptions[subscription.id] = subscription
        return subscription.id

    def unsubscribe(self, subscription_id: str) -> bool:
        """Unregister a webhook subscription."""
        if subscription_id in self._subscriptions:
            del self._subscriptions[subscription_id]
            return True
        return False

    def _generate_signature(self, payload: str, secret: str) -> str:
        """Generate HMAC signature for payload."""
        return hmac.new(
            secret.encode(),
            payload.encode(),
            hashlib.sha256
        ).hexdigest()

    async def dispatch(
        self,
        event_type: str,
        payload: dict[str, Any],
        source: str | None = None
    ) -> list[str]:
        """Dispatch event to all matching subscriptions."""
        delivery_ids = []
        matching_subs = [
            sub for sub in self._subscriptions.values()
            if sub.enabled and event_type in sub.event_types
        ]
        for sub in matching_subs:
            for webhook_filter in sub.filters:
                if not webhook_filter.predicate(payload):
                    break
            else:
                transformed = payload
                for transform in sub.transforms:
                    transformed = transform.transform_fn(transformed)
                delivery_id = await self._deliver(sub, event_type, transformed)
                delivery_ids.append(delivery_id)
        return delivery_ids

    async def _deliver(
        self,
        subscription: WebhookSubscription,
        event_type: str,
        payload: dict
    ) -> str:
        """Deliver webhook to subscription endpoint."""
        delivery_id = str(uuid.uuid4())
        record = WebhookDeliveryRecord(
            delivery_id=delivery_id,
            subscription_id=subscription.id,
            event_type=event_type,
            payload=payload,
            status="pending"
        )
        self._delivery_records[delivery_id] = record
        backoff = subscription.retry_policy.get("backoff", [1, 5, 30])
        max_attempts = subscription.retry_policy.get("max_attempts", 3)
        for attempt in range(max_attempts):
            record.attempts += 1
            record.last_attempt = time.time()
            try:
                async with asyncio.timeout(30.0):
                    response = await self._http_post(
                        subscription.endpoint_url,
                        payload,
                        subscription.secret
                    )
                    record.response_code = response
                    if 200 <= response < 300:
                        record.status = "delivered"
                        return delivery_id
            except Exception as e:
                record.error = str(e)
            if attempt < max_attempts - 1:
                await asyncio.sleep(backoff[min(attempt, len(backoff) - 1)])
        record.status = "failed"
        return delivery_id

    async def _http_post(
        self,
        url: str,
        payload: dict,
        secret: str | None
    ) -> int:
        """Make HTTP POST request to webhook endpoint."""
        import aiohttp
        headers = {"Content-Type": "application/json"}
        if secret:
            payload_str = json.dumps(payload, sort_keys=True)
            headers["X-Webhook-Signature"] = self._generate_signature(payload_str, secret)
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=payload, headers=headers, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                return resp.status

    def get_delivery_record(self, delivery_id: str) -> WebhookDeliveryRecord | None:
        """Get delivery record by ID."""
        return self._delivery_records.get(delivery_id)

    def get_subscription_stats(self, subscription_id: str) -> dict[str, Any]:
        """Get statistics for a subscription."""
        records = [r for r in self._delivery_records.values() if r.subscription_id == subscription_id]
        return {
            "total_deliveries": len(records),
            "delivered": sum(1 for r in records if r.status == "delivered"),
            "failed": sum(1 for r in records if r.status == "failed"),
            "pending": sum(1 for r in records if r.status == "pending"),
        }
