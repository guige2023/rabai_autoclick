"""Webhook integration for event-driven integrations.

Handles webhook operations including sending webhooks,
signature verification, retry logic, and event parsing.
"""

from typing import Any, Optional, Callable
import logging
from dataclasses import dataclass, field
from datetime import datetime
import hashlib
import hmac
import time
import json

try:
    import requests
except ImportError:
    requests = None

logger = logging.getLogger(__name__)


@dataclass
class WebhookConfig:
    """Configuration for webhook operations."""
    url: str
    secret: Optional[str] = None
    timeout: float = 30.0
    max_retries: int = 3
    retry_delay: float = 1.0
    headers: dict = field(default_factory=dict)


@dataclass
class WebhookEvent:
    """Represents a webhook event."""
    id: str
    type: str
    payload: dict
    timestamp: datetime
    headers: dict = field(default_factory=dict)
    signature: Optional[str] = None


@dataclass
class WebhookDelivery:
    """Result of a webhook delivery attempt."""
    success: bool
    status_code: Optional[int] = None
    response_body: Optional[str] = None
    attempt: int = 1
    error: Optional[str] = None


class WebhookError(Exception):
    """Raised when webhook operations fail."""
    def __init__(self, message: str, status_code: Optional[int] = None):
        super().__init__(message)
        self.status_code = status_code


class WebhookAction:
    """Webhook client for sending and receiving webhooks."""

    def __init__(self, config: Optional[WebhookConfig] = None):
        """Initialize Webhook processor with configuration.

        Args:
            config: WebhookConfig with endpoint and settings

        Raises:
            ImportError: If requests is not installed
        """
        if requests is None:
            raise ImportError("requests library required: pip install requests")

        self.config = config
        self._session = requests.Session()

    def send(self, payload: dict,
            event_type: Optional[str] = None,
            headers: Optional[dict] = None,
            signature_key: str = "signature",
            timestamp_key: str = "timestamp") -> WebhookDelivery:
        """Send a webhook with the configured URL.

        Args:
            payload: Event payload dict
            event_type: Event type header
            headers: Additional headers
            signature_key: Header name for signature
            timestamp_key: Header name for timestamp

        Returns:
            WebhookDelivery with result
        """
        if not self.config:
            return WebhookDelivery(success=False, error="No webhook URL configured")

        return self.send_to_url(
            url=self.config.url,
            payload=payload,
            secret=self.config.secret,
            event_type=event_type,
            headers=headers,
            timeout=self.config.timeout,
            max_retries=self.config.max_retries,
            retry_delay=self.config.retry_delay,
            signature_key=signature_key,
            timestamp_key=timestamp_key
        )

    def send_to_url(self, url: str,
                   payload: dict,
                   secret: Optional[str] = None,
                   event_type: Optional[str] = None,
                   headers: Optional[dict] = None,
                   timeout: float = 30.0,
                   max_retries: int = 3,
                   retry_delay: float = 1.0,
                   signature_key: str = "signature",
                   timestamp_key: str = "timestamp") -> WebhookDelivery:
        """Send a webhook to a specific URL.

        Args:
            url: Webhook URL
            payload: Event payload dict
            secret: Secret for HMAC signature
            event_type: Event type header
            headers: Additional headers
            timeout: Request timeout
            max_retries: Maximum retry attempts
            retry_delay: Delay between retries
            signature_key: Header name for signature
            timestamp_key: Header name for timestamp

        Returns:
            WebhookDelivery with result
        """
        request_headers: dict[str, str] = {
            "Content-Type": "application/json",
            "User-Agent": "WebhookAgent/1.0"
        }

        if self.config and self.config.headers:
            request_headers.update(self.config.headers)

        if headers:
            request_headers.update(headers)

        timestamp = str(int(time.time()))
        request_headers[f"X-Webhook-{timestamp_key}"] = timestamp

        if event_type:
            request_headers["X-Webhook-Event"] = event_type

        json_payload = json.dumps(payload, default=str)
        request_headers["X-Webhook-Content"] = hashlib.sha256(json_payload.encode()).hexdigest()

        if secret:
            signature = self._generate_signature(
                timestamp,
                json_payload,
                secret,
                signature_key
            )
            request_headers[f"X-Webhook-{signature_key}"] = signature

        attempt = 0
        last_error = None

        while attempt < max_retries:
            attempt += 1

            try:
                response = self._session.post(
                    url,
                    data=json_payload,
                    headers=request_headers,
                    timeout=timeout
                )

                delivery = WebhookDelivery(
                    success=response.ok,
                    status_code=response.status_code,
                    response_body=response.text[:1000] if response.text else None,
                    attempt=attempt
                )

                if response.ok:
                    return delivery

                last_error = f"HTTP {response.status_code}: {response.text[:200]}"

                if response.status_code >= 400 and response.status_code < 500:
                    return WebhookDelivery(
                        success=False,
                        status_code=response.status_code,
                        response_body=response.text[:1000],
                        attempt=attempt,
                        error=f"Client error: {response.status_code}"
                    )

            except requests.RequestException as e:
                last_error = str(e)

            if attempt < max_retries:
                time.sleep(retry_delay * attempt)

        return WebhookDelivery(
            success=False,
            attempt=attempt,
            error=last_error or "Max retries exceeded"
        )

    def verify_signature(self, payload: bytes,
                       signature: str,
                       timestamp: str,
                       secret: Optional[str] = None,
                       tolerance: int = 300) -> bool:
        """Verify webhook signature.

        Args:
            payload: Raw request body
            signature: Signature header value
            timestamp: Timestamp header value
            secret: Secret key
            tolerance: Timestamp tolerance in seconds

        Returns:
            True if signature is valid

        Raises:
            WebhookError: If verification fails
        """
        secret = secret or (self.config.secret if self.config else None)

        if not secret:
            logger.warning("No secret configured, skipping signature verification")
            return True

        try:
            ts = int(timestamp)
        except ValueError:
            raise WebhookError("Invalid timestamp format")

        current_time = int(time.time())
        if abs(current_time - ts) > tolerance:
            raise WebhookError("Timestamp outside tolerance window")

        expected_signature = self._compute_hmac(
            timestamp,
            payload.decode("utf-8"),
            secret
        )

        if not hmac.compare_digest(expected_signature, signature):
            raise WebhookError("Invalid webhook signature")

        return True

    def parse_event(self, payload: dict,
                   headers: Optional[dict] = None,
                   signature: Optional[str] = None) -> WebhookEvent:
        """Parse webhook payload into WebhookEvent.

        Args:
            payload: Event payload dict
            headers: Request headers
            signature: Signature for verification

        Returns:
            WebhookEvent object
        """
        headers = headers or {}

        event_id = headers.get("X-Webhook-Event-Id", headers.get("X-GitHub-Delivery", ""))
        event_type = headers.get("X-Webhook-Event", headers.get("X-GitHub-Event", ""))

        return WebhookEvent(
            id=event_id or self._generate_event_id(payload),
            type=event_type or "unknown",
            payload=payload,
            timestamp=datetime.now(),
            headers=headers,
            signature=signature
        )

    def create_response(self, event: WebhookEvent,
                       status_code: int = 200,
                       body: Optional[dict] = None) -> dict:
        """Create a webhook response.

        Args:
            event: Received event
            status_code: HTTP status code
            body: Response body

        Returns:
            Response dict for framework integration
        """
        response_body = json.dumps(body or {"received": True}) if body else ""

        return {
            "statusCode": status_code,
            "body": response_body,
            "headers": {
                "Content-Type": "application/json"
            }
        }

    def retry_failed_delivery(self, delivery: WebhookDelivery,
                            payload: dict,
                            event_type: Optional[str] = None) -> WebhookDelivery:
        """Retry a failed webhook delivery.

        Args:
            delivery: Previous failed delivery
            payload: Original payload
            event_type: Event type header

        Returns:
            New WebhookDelivery with retry result
        """
        if not self.config:
            return WebhookDelivery(success=False, error="No webhook URL configured")

        if delivery.attempt >= self.config.max_retries:
            return WebhookDelivery(
                success=False,
                error="Max retries exceeded",
                attempt=delivery.attempt
            )

        return self.send(
            payload=payload,
            event_type=event_type
        )

    def batch_send(self, url: str,
                  events: list[tuple[dict, Optional[str]]],
                  secret: Optional[str] = None,
                  concurrency: int = 5) -> list[WebhookDelivery]:
        """Send multiple webhooks in batch.

        Args:
            url: Webhook URL
            events: List of (payload, event_type) tuples
            secret: Secret for signature
            concurrency: Max concurrent requests

        Returns:
            List of WebhookDelivery results
        """
        results = []

        for i in range(0, len(events), concurrency):
            batch = events[i:i + concurrency]
            batch_results = []

            for payload, event_type in batch:
                result = self.send_to_url(
                    url=url,
                    payload=payload,
                    secret=secret,
                    event_type=event_type
                )
                batch_results.append(result)

            results.extend(batch_results)

        return results

    def generate_test_payload(self, event_type: str = "test.event",
                            custom_data: Optional[dict] = None) -> dict:
        """Generate a test webhook payload.

        Args:
            event_type: Event type name
            custom_data: Optional custom data to include

        Returns:
            Test payload dict
        """
        return {
            "event_type": event_type,
            "timestamp": datetime.now().isoformat(),
            "id": self._generate_event_id({"test": True}),
            "data": custom_data or {
                "test": True,
                "message": "This is a test webhook payload"
            }
        }

    def _generate_signature(self, timestamp: str,
                          payload: str,
                          secret: str,
                          signature_key: str) -> str:
        """Generate HMAC signature for webhook."""
        signed_payload = f"{timestamp}.{payload}"
        signature = hmac.new(
            secret.encode("utf-8"),
            signed_payload.encode("utf-8"),
            hashlib.sha256
        ).digest()
        return f"{signature_key}=v1={signature.hex()}"

    def _compute_hmac(self, timestamp: str, payload: str, secret: str) -> str:
        """Compute HMAC for verification."""
        signed_payload = f"{timestamp}.{payload}"
        signature = hmac.new(
            secret.encode("utf-8"),
            signed_payload.encode("utf-8"),
            hashlib.sha256
        ).digest()
        return f"v1={signature.hex()}"

    def _generate_event_id(self, payload: dict) -> str:
        """Generate a unique event ID."""
        content = json.dumps(payload, sort_keys=True, default=str)
        return hashlib.sha256(content.encode()).hexdigest()[:16]
