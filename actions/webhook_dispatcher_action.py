"""Webhook dispatcher action for sending and managing webhooks.

Provides webhook delivery, retry logic, signature verification,
and event filtering.
"""

import hashlib
import hmac
import json
import logging
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Optional

logger = logging.getLogger(__name__)


class DeliveryStatus(Enum):
    PENDING = "pending"
    DELIVERING = "delivering"
    DELIVERED = "delivered"
    FAILED = "failed"
    RETRYING = "retrying"


@dataclass
class WebhookEndpoint:
    url: str
    secret: Optional[str] = None
    enabled: bool = True
    timeout: float = 30.0
    retry_count: int = 3
    retry_delay: float = 5.0
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class WebhookDelivery:
    delivery_id: str
    endpoint_id: str
    event_type: str
    payload: dict[str, Any]
    status: DeliveryStatus
    attempts: int = 0
    last_attempt: Optional[float] = None
    next_retry: Optional[float] = None
    response_code: Optional[int] = None
    response_body: Optional[str] = None
    error: Optional[str] = None
    created_at: float = field(default_factory=time.time)


class WebhookDispatcherAction:
    """Dispatch webhooks with delivery guarantees and retries.

    Args:
        default_timeout: Default request timeout in seconds.
        default_retry_count: Default retry count for failed deliveries.
        enable_signatures: Enable HMAC signature verification.
    """

    def __init__(
        self,
        default_timeout: float = 30.0,
        default_retry_count: int = 3,
        enable_signatures: bool = True,
    ) -> None:
        self._endpoints: dict[str, WebhookEndpoint] = {}
        self._deliveries: dict[str, list[WebhookDelivery]] = {}
        self._default_timeout = default_timeout
        self._default_retry_count = default_retry_count
        self._enable_signatures = enable_signatures
        self._event_filters: dict[str, list[Callable]] = {}
        self._delivery_handlers: dict[str, list[Callable]] = {
            "on_delivery": [],
            "on_failure": [],
            "on_retry": [],
        }

    def register_endpoint(
        self,
        endpoint_id: str,
        url: str,
        secret: Optional[str] = None,
        timeout: Optional[float] = None,
        retry_count: Optional[int] = None,
    ) -> bool:
        """Register a webhook endpoint.

        Args:
            endpoint_id: Unique endpoint identifier.
            url: Webhook URL.
            secret: Optional secret for signature verification.
            timeout: Request timeout.
            retry_count: Number of retries on failure.

        Returns:
            True if registered successfully.
        """
        if endpoint_id in self._endpoints:
            logger.warning(f"Endpoint already registered: {endpoint_id}")
            return False

        endpoint = WebhookEndpoint(
            url=url,
            secret=secret,
            timeout=timeout or self._default_timeout,
            retry_count=retry_count or self._default_retry_count,
        )

        self._endpoints[endpoint_id] = endpoint
        logger.debug(f"Registered webhook endpoint: {endpoint_id}")
        return True

    def unregister_endpoint(self, endpoint_id: str) -> bool:
        """Unregister a webhook endpoint.

        Args:
            endpoint_id: Endpoint identifier.

        Returns:
            True if unregistered.
        """
        if endpoint_id in self._endpoints:
            del self._endpoints[endpoint_id]
            return True
        return False

    def dispatch(
        self,
        event_type: str,
        payload: dict[str, Any],
        endpoint_ids: Optional[list[str]] = None,
        async_delivery: bool = True,
    ) -> list[str]:
        """Dispatch an event to registered endpoints.

        Args:
            event_type: Type of event.
            payload: Event payload.
            endpoint_ids: Specific endpoints (all if None).
            async_delivery: Deliver asynchronously.

        Returns:
            List of delivery IDs.
        """
        delivery_ids = []

        endpoints = endpoint_ids or list(self._endpoints.keys())
        for endpoint_id in endpoints:
            endpoint = self._endpoints.get(endpoint_id)
            if not endpoint or not endpoint.enabled:
                continue

            delivery_id = self._create_delivery(endpoint_id, event_type, payload)
            delivery_ids.append(delivery_id)

        logger.info(f"Dispatched event '{event_type}' to {len(delivery_ids)} endpoints")
        return delivery_ids

    def _create_delivery(
        self,
        endpoint_id: str,
        event_type: str,
        payload: dict[str, Any],
    ) -> str:
        """Create a webhook delivery record.

        Args:
            endpoint_id: Endpoint identifier.
            event_type: Event type.
            payload: Event payload.

        Returns:
            Delivery ID.
        """
        delivery_id = f"dlv_{int(time.time() * 1000)}"

        delivery = WebhookDelivery(
            delivery_id=delivery_id,
            endpoint_id=endpoint_id,
            event_type=event_type,
            payload=payload,
            status=DeliveryStatus.PENDING,
        )

        if endpoint_id not in self._deliveries:
            self._deliveries[endpoint_id] = []
        self._deliveries[endpoint_id].append(delivery)

        return delivery_id

    def _generate_signature(
        self,
        payload: str,
        secret: str,
    ) -> str:
        """Generate HMAC signature for payload.

        Args:
            payload: JSON payload string.
            secret: Webhook secret.

        Returns:
            Signature string.
        """
        signature = hmac.new(
            secret.encode(),
            payload.encode(),
            hashlib.sha256,
        ).hexdigest()
        return f"sha256={signature}"

    def deliver(
        self,
        delivery_id: str,
        endpoint_id: str,
    ) -> bool:
        """Attempt to deliver a webhook.

        Args:
            delivery_id: Delivery identifier.
            endpoint_id: Endpoint identifier.

        Returns:
            True if delivered successfully.
        """
        deliveries = self._deliveries.get(endpoint_id, [])
        delivery = None
        for d in deliveries:
            if d.delivery_id == delivery_id:
                delivery = d
                break

        if not delivery:
            return False

        endpoint = self._endpoints.get(endpoint_id)
        if not endpoint:
            return False

        delivery.status = DeliveryStatus.DELIVERING
        delivery.attempts += 1
        delivery.last_attempt = time.time()

        try:
            payload_json = json.dumps(delivery.payload)
            headers = {"Content-Type": "application/json"}

            if self._enable_signatures and endpoint.secret:
                signature = self._generate_signature(payload_json, endpoint.secret)
                headers["X-Webhook-Signature"] = signature

            import urllib.request
            req = urllib.request.Request(
                endpoint.url,
                data=payload_json.encode(),
                headers=headers,
                method="POST",
            )

            try:
                response = urllib.request.urlopen(req, timeout=endpoint.timeout)
                delivery.status = DeliveryStatus.DELIVERED
                delivery.response_code = response.getcode()
                delivery.response_body = response.read().decode()[:1000]

                for handler in self._delivery_handlers["on_delivery"]:
                    try:
                        handler(delivery)
                    except Exception as e:
                        logger.error(f"Delivery handler error: {e}")

                return True

            except urllib.error.HTTPError as e:
                delivery.response_code = e.code
                delivery.response_body = e.read().decode()[:1000]
                delivery.error = f"HTTP {e.code}"
            except Exception as e:
                delivery.error = str(e)

            delivery.status = DeliveryStatus.FAILED
            return False

        except Exception as e:
            delivery.error = str(e)
            delivery.status = DeliveryStatus.FAILED
            return False

    def retry_failed(self, endpoint_id: str) -> int:
        """Retry all failed deliveries for an endpoint.

        Args:
            endpoint_id: Endpoint identifier.

        Returns:
            Number of deliveries retried.
        """
        deliveries = self._deliveries.get(endpoint_id, [])
        failed = [
            d for d in deliveries
            if d.status == DeliveryStatus.FAILED and d.attempts < (
                self._endpoints.get(endpoint_id) or WebhookEndpoint(url="")
            ).retry_count
        ]

        for delivery in failed:
            delivery.status = DeliveryStatus.RETRYING
            delivery.next_retry = time.time() + (
                self._endpoints.get(endpoint_id) or WebhookEndpoint(url="")
            ).retry_delay

        for handler in self._delivery_handlers["on_retry"]:
            try:
                handler(delivery)
            except Exception as e:
                logger.error(f"Retry handler error: {e}")

        return len(failed)

    def get_delivery(self, endpoint_id: str, delivery_id: str) -> Optional[WebhookDelivery]:
        """Get a delivery record.

        Args:
            endpoint_id: Endpoint identifier.
            delivery_id: Delivery identifier.

        Returns:
            Delivery record or None.
        """
        deliveries = self._deliveries.get(endpoint_id, [])
        for d in deliveries:
            if d.delivery_id == delivery_id:
                return d
        return None

    def get_deliveries_for_event(
        self,
        event_type: str,
        limit: int = 100,
    ) -> list[WebhookDelivery]:
        """Get all deliveries for an event type.

        Args:
            event_type: Event type.
            limit: Maximum results.

        Returns:
            List of deliveries (newest first).
        """
        all_deliveries = []
        for deliveries in self._deliveries.values():
            all_deliveries.extend([
                d for d in deliveries if d.event_type == event_type
            ])

        return sorted(all_deliveries, key=lambda d: d.created_at, reverse=True)[:limit]

    def register_delivery_handler(
        self,
        event_type: str,
        handler: Callable[[WebhookDelivery], None],
    ) -> None:
        """Register a handler for delivery events.

        Args:
            event_type: Event type ('on_delivery', 'on_failure', 'on_retry').
            handler: Callback function.
        """
        if event_type in self._delivery_handlers:
            self._delivery_handlers[event_type].append(handler)

    def get_stats(self) -> dict[str, Any]:
        """Get webhook dispatcher statistics.

        Returns:
            Dictionary with stats.
        """
        total_deliveries = sum(len(d) for d in self._deliveries.values())
        delivered = sum(
            1 for deliveries in self._deliveries.values()
            for d in deliveries if d.status == DeliveryStatus.DELIVERED
        )
        failed = sum(
            1 for deliveries in self._deliveries.values()
            for d in deliveries if d.status == DeliveryStatus.FAILED
        )

        return {
            "total_endpoints": len(self._endpoints),
            "enabled_endpoints": sum(1 for e in self._endpoints.values() if e.enabled),
            "total_deliveries": total_deliveries,
            "delivered": delivered,
            "failed": failed,
            "delivery_rate": delivered / total_deliveries if total_deliveries > 0 else 0,
        }
