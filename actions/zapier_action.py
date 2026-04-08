"""Zapier platform integration for workflow automation.

Handles Zapier operations including trigger polling, action execution,
and webhook management for automation workflows.
"""

from typing import Any, Optional
import logging
from dataclasses import dataclass, field
from datetime import datetime
import hashlib
import hmac
import base64
import json

try:
    import requests
except ImportError:
    requests = None

logger = logging.getLogger(__name__)


@dataclass
class ZapierConfig:
    """Configuration for Zapier integration."""
    api_key: Optional[str] = None
    webhook_secret: Optional[str] = None
    timeout: int = 30
    max_retries: int = 3


@dataclass
class ZapierTriggerEvent:
    """Represents a trigger event from a Zap."""
    event_id: str
    event_type: str
    payload: dict
    timestamp: Optional[str] = None
    meta: dict = field(default_factory=dict)


@dataclass
class ZapierActionResult:
    """Result of a Zapier action execution."""
    success: bool
    output: Optional[dict] = None
    error: Optional[str] = None
    execution_id: Optional[str] = None


class ZapierAPIError(Exception):
    """Raised when Zapier API returns an error."""
    def __init__(self, message: str, code: Optional[str] = None):
        super().__init__(message)
        self.code = code


class ZapierAction:
    """Zapier integration client for automation workflows."""

    BASE_URL = "https://hooks.zapier.com/hooks/catch"

    def __init__(self, config: ZapierConfig):
        """Initialize Zapier client with configuration.

        Args:
            config: ZapierConfig with API key and optional webhook secret
        """
        if requests is None:
            raise ImportError("requests library required: pip install requests")

        self.config = config
        self.session = requests.Session()
        self.session.headers.update({
            "Content-Type": "application/json",
            "Accept": "application/json"
        })

    def _request(self, method: str, url: str, **kwargs) -> dict:
        """Make HTTP request with retries.

        Args:
            method: HTTP method
            url: Request URL
            **kwargs: Additional request parameters

        Returns:
            Parsed JSON response

        Raises:
            ZapierAPIError: On request failure
        """
        retries = self.config.max_retries

        while retries > 0:
            try:
                response = self.session.request(
                    method,
                    url,
                    timeout=self.config.timeout,
                    **kwargs
                )

                if not response.ok:
                    raise ZapierAPIError(
                        message=f"Request failed with status {response.status_code}",
                        code=str(response.status_code)
                    )

                if response.content:
                    return response.json()

                return {}

            except requests.RequestException as e:
                retries -= 1
                if retries == 0:
                    raise ZapierAPIError(f"Request failed: {e}")

    def verify_webhook_signature(self, payload: bytes, signature: str,
                                  timestamp: Optional[str] = None) -> bool:
        """Verify webhook signature from Zapier.

        Args:
            payload: Raw request body bytes
            signature: X-Zapier-Signature header value
            timestamp: X-Zapier-Signature-Timestamp header value

        Returns:
            True if signature is valid

        Raises:
            ZapierAPIError: If signature verification fails
        """
        if not self.config.webhook_secret:
            logger.warning("No webhook secret configured, skipping verification")
            return True

        if timestamp:
            signed_payload = f"{timestamp}{payload.decode('utf-8')}"
        else:
            signed_payload = payload.decode("utf-8")

        expected_signature = hmac.new(
            self.config.webhook_secret.encode("utf-8"),
            signed_payload.encode("utf-8"),
            hashlib.sha256
        ).digest()

        expected_b64 = base64.b64encode(expected_signature).decode("utf-8")

        if not hmac.compare_digest(expected_b64, signature):
            raise ZapierAPIError("Invalid webhook signature", code="INVALID_SIGNATURE")

        return True

    def poll_webhook(self, webhook_url: str, params: Optional[dict] = None) -> list[dict]:
        """Poll a webhook URL for pending events.

        Args:
            webhook_url: Full webhook URL to poll
            params: Optional query parameters

        Returns:
            List of event payloads (usually max 1)
        """
        response = self._request("GET", webhook_url, params=params or {})
        return response if isinstance(response, list) else [response] if response else []

    def send_to_webhook(self, webhook_url: str,
                         data: dict,
                         headers: Optional[dict] = None) -> ZapierActionResult:
        """Send data to a Zapier webhook URL.

        Args:
            webhook_url: Catch hook URL from Zapier
            data: Data payload to send
            headers: Optional custom headers

        Returns:
            ZapierActionResult with execution outcome
        """
        request_headers = {}
        if headers:
            request_headers.update(headers)

        try:
            response = self._request(
                "POST",
                webhook_url,
                json=data,
                headers=request_headers
            )

            return ZapierActionResult(
                success=True,
                output=response,
                execution_id=response.get("id") or response.get("execution_id")
            )

        except ZapierAPIError as e:
            return ZapierActionResult(
                success=False,
                error=str(e)
            )

    def trigger_zap(self, zap_id: str, poll_url: str,
                    data: Optional[dict] = None) -> list[ZapierTriggerEvent]:
        """Poll a Zap trigger for new events.

        Args:
            zap_id: Zap ID for reference
            poll_url: Poll URL from Zapier trigger
            data: Optional filter data

        Returns:
            List of TriggerEvent objects
        """
        payload = data or {}
        response = self._request("POST", poll_url, json=payload)

        events = []

        if isinstance(response, list):
            for item in response:
                events.append(self._parse_trigger_event(item))
        elif response:
            events.append(self._parse_trigger_event(response))

        return events

    def _parse_trigger_event(self, data: dict) -> ZapierTriggerEvent:
        """Parse raw trigger data into TriggerEvent object."""
        return ZapierTriggerEvent(
            event_id=data.get("id", data.get("zapier_hook_id", "")),
            event_type=data.get("type", data.get("event", "poll")),
            payload=data,
            timestamp=data.get("timestamp") or data.get("created_at"),
            meta=data.get("meta", {})
        )

    def get_hook_status(self, hook_id: str, api_key: Optional[str] = None) -> dict:
        """Get status of a webhook hook.

        Args:
            hook_id: Hook ID to check
            api_key: API key (uses config if not provided)

        Returns:
            Hook status information
        """
        key = api_key or self.config.api_key

        if not key:
            raise ZapierAPIError("API key required for hook status", code="AUTH_REQUIRED")

        url = f"https://hooks.zapier.com/hooks/status/{hook_id}/"
        headers = {"Authorization": f"Bearer {key}"}

        response = self._request("GET", url, headers=headers)
        return response

    def list_active_hooks(self, api_key: Optional[str] = None) -> list[dict]:
        """List all active webhook hooks for the authenticated user.

        Args:
            api_key: API key (uses config if not provided)

        Returns:
            List of active hook objects
        """
        key = api_key or self.config.api_key

        if not key:
            raise ZapierAPIError("API key required", code="AUTH_REQUIRED")

        headers = {"Authorization": f"Bearer {key}"}

        response = self._request(
            "GET",
            "https://hooks.zapier.com/hooks/hooks/",
            headers=headers
        )

        return response.get("hooks", [])

    def unsubscribe_hook(self, hook_id: str, api_key: Optional[str] = None) -> bool:
        """Unsubscribe/disable a webhook hook.

        Args:
            hook_id: Hook ID to unsubscribe
            api_key: API key (uses config if not provided)

        Returns:
            True if successful
        """
        key = api_key or self.config.api_key

        if not key:
            raise ZapierAPIError("API key required", code="AUTH_REQUIRED")

        headers = {"Authorization": f"Bearer {key}"}

        self._request(
            "DELETE",
            f"https://hooks.zapier.com/hooks/hooks/{hook_id}/",
            headers=headers
        )

        return True

    def create_subscribe_hook(self, zap_id: str,
                               target_url: str,
                               api_key: Optional[str] = None,
                               event: Optional[str] = None) -> dict:
        """Subscribe to a Zapier trigger event.

        Args:
            zap_id: Zap ID to subscribe
            target_url: URL to receive events
            api_key: API key (uses config if not provided)
            event: Specific event name to subscribe

        Returns:
            Created hook object
        """
        key = api_key or self.config.api_key

        if not key:
            raise ZapierAPIError("API key required", code="AUTH_REQUIRED")

        headers = {
            "Authorization": f"Bearer {key}",
            "Content-Type": "application/json"
        }

        payload: dict[str, Any] = {
            "zap_id": zap_id,
            "target_url": target_url
        }

        if event:
            payload["event"] = event

        return self._request(
            "POST",
            "https://hooks.zapier.com/hooks/hooks/",
            json=payload,
            headers=headers
        )
