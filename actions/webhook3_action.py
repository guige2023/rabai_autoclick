"""Webhook v3 Action Module.

Advanced webhook dispatcher with retry queues and delivery tracking.
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

from .webhook_dispatcher_action import WebhookEndpoint, WebhookDelivery, DeliveryStatus


class RetryPolicy(Enum):
    """Webhook retry policies."""
    NONE = "none"
    EXPONENTIAL = "exponential"
    LINEAR = "linear"
    FIXED = "fixed"


@dataclass
class RetryConfig:
    """Retry configuration."""
    policy: RetryPolicy = RetryPolicy.EXPONENTIAL
    max_attempts: int = 5
    initial_delay: float = 1.0
    max_delay: float = 300.0
    backoff_factor: float = 2.0


@dataclass
class WebhookSubscriptionV3:
    """Enhanced webhook subscription."""
    id: str
    url: str
    events: list[str]
    secret: str | None = None
    enabled: bool = True
    retry_config: RetryConfig = field(default_factory=RetryConfig)
    headers: dict[str, str] = field(default_factory=dict)
    tags: list[str] = field(default_factory=list)


@dataclass
class DeliveryAttempt:
    """Single delivery attempt."""
    attempt_number: int
    timestamp: float
    response_code: int | None = None
    response_body: str | None = None
    error: str | None = None
    duration_ms: float = 0.0


class WebhookDispatcherV3:
    """Advanced webhook dispatcher with comprehensive tracking."""

    def __init__(self) -> None:
        self._subscriptions: dict[str, WebhookSubscriptionV3] = {}
        self._deliveries: dict[str, list[DeliveryAttempt]] = {}
        self._pending_retry: asyncio.Queue[tuple[str, str]] = asyncio.Queue()
        self._lock = asyncio.Lock()
        self._running = False

    def subscribe(self, subscription: WebhookSubscriptionV3) -> str:
        """Register a subscription."""
        self._subscriptions[subscription.id] = subscription
        return subscription.id

    async def dispatch(
        self,
        event_type: str,
        payload: dict[str, Any]
    ) -> dict[str, str]:
        """Dispatch event to matching subscriptions."""
        results = {}
        for sub in self._subscriptions.values():
            if not sub.enabled:
                continue
            if event_type not in sub.events:
                continue
            delivery_id = await self._create_delivery(sub, payload)
            results[sub.id] = delivery_id
            asyncio.create_task(self._process_delivery(sub, delivery_id, payload))
        return results

    async def _create_delivery(
        self,
        subscription: WebhookSubscriptionV3,
        payload: dict[str, Any]
    ) -> str:
        """Create a delivery record."""
        delivery_id = str(uuid.uuid4())
        self._deliveries[delivery_id] = []
        return delivery_id

    async def _process_delivery(
        self,
        subscription: WebhookSubscriptionV3,
        delivery_id: str,
        payload: dict[str, Any]
    ) -> None:
        """Process delivery with retry logic."""
        attempts = self._deliveries.get(delivery_id, [])
        max_attempts = subscription.retry_config.max_attempts
        for attempt_num in range(1, max_attempts + 1):
            attempt = await self._deliver_to_endpoint(subscription, payload)
            attempt.attempt_number = attempt_num
            attempts.append(attempt)
            self._deliveries[delivery_id] = attempts
            if attempt.response_code and 200 <= attempt.response_code < 300:
                return
            if attempt_num < max_attempts:
                delay = self._calculate_retry_delay(subscription.retry_config, attempt_num)
                await asyncio.sleep(delay)

    async def _deliver_to_endpoint(
        self,
        subscription: WebhookSubscriptionV3,
        payload: dict[str, Any]
    ) -> DeliveryAttempt:
        """Deliver webhook to endpoint."""
        import aiohttp
        attempt = DeliveryAttempt(attempt_number=0, timestamp=time.time())
        headers = {"Content-Type": "application/json", **subscription.headers}
        if subscription.secret:
            payload_str = json.dumps(payload, sort_keys=True)
            signature = hmac.new(
                subscription.secret.encode(),
                payload_str.encode(),
                hashlib.sha256
            ).hexdigest()
            headers["X-Webhook-Signature"] = f"sha256={signature}"
        start = time.monotonic()
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    subscription.url,
                    json=payload,
                    headers=headers,
                    timeout=aiohttp.ClientTimeout(total=30)
                ) as response:
                    attempt.response_code = response.status
                    attempt.response_body = await response.text()
                    attempt.duration_ms = (time.monotonic() - start) * 1000
        except asyncio.TimeoutError:
            attempt.error = "Request timed out"
        except Exception as e:
            attempt.error = str(e)
        return attempt

    def _calculate_retry_delay(self, config: RetryConfig, attempt: int) -> float:
        """Calculate delay for retry attempt."""
        if config.policy == RetryPolicy.EXPONENTIAL:
            delay = config.initial_delay * (config.backoff_factor ** (attempt - 1))
        elif config.policy == RetryPolicy.LINEAR:
            delay = config.initial_delay * attempt
        else:
            delay = config.initial_delay
        return min(delay, config.max_delay)

    def get_delivery_history(self, delivery_id: str) -> list[DeliveryAttempt] | None:
        """Get delivery attempt history."""
        return self._deliveries.get(delivery_id)

    def get_subscription_deliveries(self, subscription_id: str) -> list[list[DeliveryAttempt]]:
        """Get all deliveries for a subscription."""
        sub = self._subscriptions.get(subscription_id)
        if not sub:
            return []
        return [attempts for did, attempts in self._deliveries.items()
                if sub.id in str(did)]
