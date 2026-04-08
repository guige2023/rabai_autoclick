"""Webhook Action Module.

Provides webhook delivery, retry logic, signature verification,
and event routing capabilities for building event-driven workflows.
"""
from __future__ import annotations

import hashlib
import hmac
import json
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)


class DeliveryStatus(Enum):
    """Webhook delivery status."""
    PENDING = "pending"
    SUCCESS = "success"
    FAILED = "failed"
    RETRYING = "retrying"
    DROPPED = "dropped"


@dataclass
class WebhookDelivery:
    """Single webhook delivery attempt."""
    id: str
    webhook_id: str
    endpoint: str
    payload: Dict[str, Any]
    headers: Dict[str, str]
    status: DeliveryStatus
    attempts: int = 0
    max_attempts: int = 3
    next_retry: float = 0.0
    last_attempt: float = 0.0
    response_status: Optional[int] = None
    response_body: str = ""
    error: Optional[str] = None
    created_at: float = field(default_factory=time.time)


@dataclass
class Webhook:
    """Webhook configuration."""
    id: str
    name: str
    endpoint: str
    secret: Optional[str] = None
    events: List[str] = field(default_factory=list)
    enabled: bool = True
    metadata: Dict[str, Any] = field(default_factory=dict)
    filters: Dict[str, Any] = field(default_factory=dict)


@dataclass
class DeliveryResult:
    """Result of webhook delivery."""
    success: bool
    delivery_id: str
    webhook_id: str
    status: DeliveryStatus
    attempts: int
    response_status: Optional[int]
    duration_ms: float
    error: Optional[str] = None


def _generate_signature(payload: str, secret: str, timestamp: Optional[str] = None) -> str:
    """Generate HMAC signature for webhook payload."""
    if timestamp:
        signed_payload = f"{timestamp}.{payload}"
    else:
        signed_payload = payload
    return hmac.new(
        secret.encode(),
        signed_payload.encode(),
        hashlib.sha256
    ).hexdigest()


class WebhookStore:
    """In-memory webhook store."""

    def __init__(self):
        self._webhooks: Dict[str, Webhook] = {}
        self._deliveries: Dict[str, WebhookDelivery] = {}

    def create_webhook(self, name: str, endpoint: str,
                       secret: Optional[str] = None,
                       events: Optional[List[str]] = None) -> Webhook:
        """Create new webhook."""
        webhook = Webhook(
            id=uuid.uuid4().hex,
            name=name,
            endpoint=endpoint,
            secret=secret,
            events=events or ["*"]
        )
        self._webhooks[webhook.id] = webhook
        return webhook

    def get_webhook(self, webhook_id: str) -> Optional[Webhook]:
        """Get webhook by ID."""
        return self._webhooks.get(webhook_id)

    def list_webhooks(self, event: Optional[str] = None) -> List[Webhook]:
        """List all webhooks, optionally filtered by event."""
        webhooks = list(self._webhooks.values())
        if event:
            webhooks = [w for w in webhooks if event in w.events or "*" in w.events]
        return webhooks

    def delete_webhook(self, webhook_id: str) -> bool:
        """Delete webhook."""
        if webhook_id in self._webhooks:
            del self._webhooks[webhook_id]
            return True
        return False

    def create_delivery(self, webhook_id: str, endpoint: str,
                        payload: Dict[str, Any],
                        headers: Optional[Dict[str, str]] = None) -> WebhookDelivery:
        """Create new delivery."""
        delivery = WebhookDelivery(
            id=uuid.uuid4().hex,
            webhook_id=webhook_id,
            endpoint=endpoint,
            payload=payload,
            headers=headers or {},
            status=DeliveryStatus.PENDING
        )
        self._deliveries[delivery.id] = delivery
        return delivery

    def get_delivery(self, delivery_id: str) -> Optional[WebhookDelivery]:
        """Get delivery by ID."""
        return self._deliveries.get(delivery_id)

    def update_delivery(self, delivery: WebhookDelivery) -> None:
        """Update delivery."""
        self._deliveries[delivery.id] = delivery


class WebhookSimulator:
    """Simulated webhook delivery for testing."""

    def __init__(self, success_rate: float = 0.9, avg_latency_ms: float = 100.0):
        self._success_rate = success_rate
        self._avg_latency_ms = avg_latency_ms

    def deliver(self, delivery: WebhookDelivery) -> tuple[int, str]:
        """Simulate webhook delivery.

        Returns:
            Tuple of (response_status, response_body)
        """
        import random
        time.sleep(self._avg_latency_ms / 1000.0 * random.uniform(0.5, 1.5))

        if random.random() < self._success_rate:
            return (200, '{"status": "received"}')
        else:
            status = random.choice([500, 502, 503, 504])
            return (status, f'{{"error": "Server error {status}"}}')


_global_store = WebhookStore()
_global_simulator = WebhookSimulator()


class WebhookAction:
    """Webhook delivery and management action.

    Example:
        action = WebhookAction()

        webhook = action.create_webhook(
            name="my-hook",
            endpoint="https://example.com/webhook",
            secret="my-secret",
            events=["user.created", "order.completed"]
        )

        result = action.send(webhook.id, {"event": "user.created", "data": {...}})
    """

    def __init__(self, simulator: Optional[WebhookSimulator] = None):
        self._store = _global_store
        self._simulator = simulator or _global_simulator

    def create_webhook(self, name: str, endpoint: str,
                       secret: Optional[str] = None,
                       events: Optional[List[str]] = None,
                       metadata: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Create new webhook.

        Args:
            name: Webhook name
            endpoint: Webhook endpoint URL
            secret: Optional signing secret
            events: List of events to subscribe
            metadata: Additional metadata

        Returns:
            Dict with webhook info
        """
        try:
            webhook = self._store.create_webhook(name, endpoint, secret, events)
            if metadata:
                webhook.metadata.update(metadata)
            return {
                "success": True,
                "webhook": {
                    "id": webhook.id,
                    "name": webhook.name,
                    "endpoint": webhook.endpoint,
                    "events": webhook.events,
                    "enabled": webhook.enabled
                },
                "message": f"Created webhook {webhook.id}"
            }
        except Exception as e:
            return {"success": False, "message": str(e)}

    def get_webhook(self, webhook_id: str) -> Dict[str, Any]:
        """Get webhook by ID.

        Args:
            webhook_id: Webhook ID

        Returns:
            Dict with webhook info
        """
        webhook = self._store.get_webhook(webhook_id)
        if webhook:
            return {
                "success": True,
                "webhook": {
                    "id": webhook.id,
                    "name": webhook.name,
                    "endpoint": webhook.endpoint,
                    "events": webhook.events,
                    "enabled": webhook.enabled,
                    "metadata": webhook.metadata
                }
            }
        return {"success": False, "message": "Webhook not found"}

    def list_webhooks(self, event: Optional[str] = None) -> Dict[str, Any]:
        """List webhooks.

        Args:
            event: Optional event filter

        Returns:
            Dict with list of webhooks
        """
        webhooks = self._store.list_webhooks(event)
        return {
            "success": True,
            "webhooks": [
                {
                    "id": w.id,
                    "name": w.name,
                    "endpoint": w.endpoint,
                    "events": w.events,
                    "enabled": w.enabled
                }
                for w in webhooks
            ],
            "count": len(webhooks)
        }

    def delete_webhook(self, webhook_id: str) -> Dict[str, Any]:
        """Delete webhook.

        Args:
            webhook_id: Webhook ID

        Returns:
            Dict with success status
        """
        if self._store.delete_webhook(webhook_id):
            return {"success": True, "message": "Webhook deleted"}
        return {"success": False, "message": "Webhook not found"}

    def send(self, webhook_id: str, payload: Dict[str, Any],
             headers: Optional[Dict[str, str]] = None,
             async_delivery: bool = True) -> DeliveryResult:
        """Send event to webhook.

        Args:
            webhook_id: Target webhook ID
            payload: Event payload
            headers: Additional headers
            async_delivery: If True, simulate async delivery

        Returns:
            DeliveryResult with delivery status
        """
        start = time.time()
        webhook = self._store.get_webhook(webhook_id)

        if not webhook:
            return DeliveryResult(
                success=False,
                delivery_id="",
                webhook_id=webhook_id,
                status=DeliveryStatus.DROPPED,
                attempts=0,
                response_status=None,
                duration_ms=(time.time() - start) * 1000,
                error="Webhook not found"
            )

        if not webhook.enabled:
            return DeliveryResult(
                success=False,
                delivery_id="",
                webhook_id=webhook_id,
                status=DeliveryStatus.DROPPED,
                attempts=0,
                response_status=None,
                duration_ms=(time.time() - start) * 1000,
                error="Webhook disabled"
            )

        event_type = payload.get("event", "*")
        if event_type not in webhook.events and "*" not in webhook.events:
            return DeliveryResult(
                success=False,
                delivery_id="",
                webhook_id=webhook_id,
                status=DeliveryStatus.DROPPED,
                attempts=0,
                response_status=None,
                duration_ms=(time.time() - start) * 1000,
                error=f"Event {event_type} not subscribed"
            )

        delivery = self._store.create_delivery(
            webhook_id,
            webhook.endpoint,
            payload,
            headers
        )

        if not async_delivery:
            return self._deliver_with_retry(delivery, webhook, start)

        return DeliveryResult(
            success=True,
            delivery_id=delivery.id,
            webhook_id=webhook_id,
            status=DeliveryStatus.PENDING,
            attempts=0,
            response_status=None,
            duration_ms=(time.time() - start) * 1000,
            error=None
        )

    def _deliver_with_retry(self, delivery: WebhookDelivery,
                            webhook: Webhook,
                            start_time: float) -> DeliveryResult:
        """Deliver webhook with retry logic."""
        retry_delays = [1, 5, 30]

        while delivery.attempts < delivery.max_attempts:
            delivery.attempts += 1
            delivery.last_attempt = time.time()

            try:
                response_status, response_body = self._simulator.deliver(delivery)
                delivery.response_status = response_status
                delivery.response_body = response_body

                if 200 <= response_status < 300:
                    delivery.status = DeliveryStatus.SUCCESS
                    self._store.update_delivery(delivery)
                    return DeliveryResult(
                        success=True,
                        delivery_id=delivery.id,
                        webhook_id=webhook.id,
                        status=DeliveryStatus.SUCCESS,
                        attempts=delivery.attempts,
                        response_status=response_status,
                        duration_ms=(time.time() - start_time) * 1000
                    )

                delivery.error = f"HTTP {response_status}: {response_body}"

                if delivery.attempts < delivery.max_attempts:
                    delivery.status = DeliveryStatus.RETRYING
                    delay = retry_delays[min(delivery.attempts - 1, len(retry_delays) - 1)]
                    delivery.next_retry = time.time() + delay
                    self._store.update_delivery(delivery)
                    time.sleep(0.1)

            except Exception as e:
                delivery.error = str(e)
                delivery.status = DeliveryStatus.RETRYING
                if delivery.attempts < delivery.max_attempts:
                    delay = retry_delays[min(delivery.attempts - 1, len(retry_delays) - 1)]
                    delivery.next_retry = time.time() + delay

        delivery.status = DeliveryStatus.FAILED
        self._store.update_delivery(delivery)

        return DeliveryResult(
            success=False,
            delivery_id=delivery.id,
            webhook_id=webhook.id,
            status=DeliveryStatus.FAILED,
            attempts=delivery.attempts,
            response_status=delivery.response_status,
            duration_ms=(time.time() - start_time) * 1000,
            error=delivery.error
        )

    def get_delivery(self, delivery_id: str) -> Dict[str, Any]:
        """Get delivery status.

        Args:
            delivery_id: Delivery ID

        Returns:
            Dict with delivery info
        """
        delivery = self._store.get_delivery(delivery_id)
        if delivery:
            return {
                "success": True,
                "delivery": {
                    "id": delivery.id,
                    "webhook_id": delivery.webhook_id,
                    "endpoint": delivery.endpoint,
                    "status": delivery.status.value,
                    "attempts": delivery.attempts,
                    "response_status": delivery.response_status,
                    "error": delivery.error,
                    "created_at": delivery.created_at,
                    "last_attempt": delivery.last_attempt
                }
            }
        return {"success": False, "message": "Delivery not found"}

    def verify_signature(self, payload: str, signature: str,
                         secret: str, timestamp: Optional[str] = None) -> bool:
        """Verify webhook signature.

        Args:
            payload: Raw payload string
            signature: Signature to verify
            secret: Webhook secret
            timestamp: Optional timestamp

        Returns:
            True if signature is valid
        """
        expected = _generate_signature(payload, secret, timestamp)
        return hmac.compare_digest(expected, signature)


def execute(context: Any, params: Dict[str, Any]) -> Dict[str, Any]:
    """Execute webhook action.

    Args:
        context: Execution context
        params: Dict with keys:
            - operation: "create", "get", "list", "delete", "send", "get_delivery", "verify"
            - name: Webhook name (for create)
            - endpoint: Webhook endpoint (for create)
            - secret: Webhook secret (for create)
            - events: Event list (for create)
            - webhook_id: Webhook ID
            - payload: Event payload (for send)
            - headers: Additional headers (for send)
            - async_delivery: Async delivery flag (for send)
            - delivery_id: Delivery ID (for get_delivery)
            - signature: Signature to verify (for verify)

    Returns:
        Dict with success, data, message
    """
    operation = params.get("operation", "send")
    action = WebhookAction()

    try:
        if operation == "create":
            name = params.get("name", "")
            endpoint = params.get("endpoint", "")
            if not name or not endpoint:
                return {"success": False, "message": "name and endpoint required"}
            return action.create_webhook(
                name=name,
                endpoint=endpoint,
                secret=params.get("secret"),
                events=params.get("events"),
                metadata=params.get("metadata")
            )

        elif operation == "get":
            webhook_id = params.get("webhook_id", "")
            if not webhook_id:
                return {"success": False, "message": "webhook_id required"}
            return action.get_webhook(webhook_id)

        elif operation == "list":
            return action.list_webhooks(params.get("event"))

        elif operation == "delete":
            webhook_id = params.get("webhook_id", "")
            if not webhook_id:
                return {"success": False, "message": "webhook_id required"}
            return action.delete_webhook(webhook_id)

        elif operation == "send":
            webhook_id = params.get("webhook_id", "")
            payload = params.get("payload", {})
            if not webhook_id:
                return {"success": False, "message": "webhook_id required"}
            result = action.send(
                webhook_id=webhook_id,
                payload=payload,
                headers=params.get("headers"),
                async_delivery=params.get("async_delivery", True)
            )
            return {
                "success": result.success,
                "delivery_id": result.delivery_id,
                "webhook_id": result.webhook_id,
                "status": result.status.value,
                "attempts": result.attempts,
                "response_status": result.response_status,
                "duration_ms": result.duration_ms,
                "error": result.error,
                "message": f"Delivery {result.status.value}"
            }

        elif operation == "get_delivery":
            delivery_id = params.get("delivery_id", "")
            if not delivery_id:
                return {"success": False, "message": "delivery_id required"}
            return action.get_delivery(delivery_id)

        elif operation == "verify":
            payload = params.get("payload", "")
            signature = params.get("signature", "")
            secret = params.get("secret", "")
            timestamp = params.get("timestamp")
            valid = action.verify_signature(payload, signature, secret, timestamp)
            return {
                "success": True,
                "valid": valid,
                "message": "Signature valid" if valid else "Invalid signature"
            }

        else:
            return {"success": False, "message": f"Unknown operation: {operation}"}

    except Exception as e:
        return {"success": False, "message": f"Webhook error: {str(e)}"}
