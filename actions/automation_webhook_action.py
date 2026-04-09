"""Automation Webhook Action Module.

Provides webhook utilities: webhook handling, signature verification,
retry logic, event routing, and delivery tracking.

Example:
    result = execute(context, {"action": "verify_signature", "payload": ...})
"""
from typing import Any, Optional, Callable
from dataclasses import dataclass, field
from datetime import datetime
import hashlib
import hmac
import json
import time


@dataclass
class WebhookEvent:
    """A webhook event."""
    
    id: str
    event_type: str
    payload: dict[str, Any]
    headers: dict[str, str] = field(default_factory=dict)
    received_at: datetime = field(default_factory=datetime.now)
    source_ip: Optional[str] = None
    retry_count: int = 0
    
    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "id": self.id,
            "event_type": self.event_type,
            "payload": self.payload,
            "headers": self.headers,
            "received_at": self.received_at.isoformat(),
            "source_ip": self.source_ip,
            "retry_count": self.retry_count,
        }


@dataclass
class WebhookSubscription:
    """Webhook subscription configuration."""
    
    id: str
    url: str
    event_types: list[str]
    secret: Optional[str] = None
    active: bool = True
    filters: dict[str, Any] = field(default_factory=dict)
    created_at: datetime = field(default_factory=datetime.now)


class WebhookRouter:
    """Routes webhook events to handlers."""
    
    def __init__(self) -> None:
        """Initialize webhook router."""
        self._handlers: dict[str, list[Callable]] = {}
        self._subscriptions: dict[str, WebhookSubscription] = {}
    
    def subscribe(self, subscription: WebhookSubscription) -> None:
        """Subscribe to webhook events.
        
        Args:
            subscription: Subscription configuration
        """
        self._subscriptions[subscription.id] = subscription
        
        for event_type in subscription.event_types:
            if event_type not in self._handlers:
                self._handlers[event_type] = []
    
    def unsubscribe(self, subscription_id: str) -> bool:
        """Unsubscribe from webhook events.
        
        Args:
            subscription_id: Subscription ID to remove
            
        Returns:
            True if subscription was removed
        """
        if subscription_id in self._subscriptions:
            del self._subscriptions[subscription_id]
            return True
        return False
    
    def register_handler(
        self,
        event_type: str,
        handler: Callable[[WebhookEvent], Any],
    ) -> None:
        """Register event handler.
        
        Args:
            event_type: Event type to handle
            handler: Handler function
        """
        if event_type not in self._handlers:
            self._handlers[event_type] = []
        self._handlers[event_type].append(handler)
    
    def route(self, event: WebhookEvent) -> list[Any]:
        """Route event to appropriate handlers.
        
        Args:
            event: Webhook event to route
            
        Returns:
            List of handler results
        """
        handlers = self._handlers.get(event.event_type, [])
        results = []
        
        for handler in handlers:
            try:
                result = handler(event)
                results.append({"success": True, "result": result})
            except Exception as e:
                results.append({"success": False, "error": str(e)})
        
        return results
    
    def get_subscriptions(self) -> list[WebhookSubscription]:
        """Get all active subscriptions."""
        return [
            sub for sub in self._subscriptions.values()
            if sub.active
        ]


class SignatureVerifier:
    """Verifies webhook signatures for security."""
    
    def __init__(self, secret: Optional[str] = None) -> None:
        """Initialize signature verifier.
        
        Args:
            secret: Webhook secret for verification
        """
        self.secret = secret
    
    def compute_signature(
        self,
        payload: str,
        timestamp: Optional[str] = None,
        algorithm: str = "sha256",
    ) -> str:
        """Compute signature for payload.
        
        Args:
            payload: Raw payload string
            timestamp: Optional timestamp
            algorithm: Hash algorithm
            
        Returns:
            Computed signature
        """
        if timestamp:
            signed_payload = f"{timestamp}.{payload}"
        else:
            signed_payload = payload
        
        return hmac.new(
            self.secret.encode() if self.secret else b"",
            signed_payload.encode(),
            hashlib.new(algorithm),
        ).hexdigest()
    
    def verify(
        self,
        payload: str,
        signature: str,
        timestamp: Optional[str] = None,
        tolerance: int = 300,
    ) -> tuple[bool, str]:
        """Verify webhook signature.
        
        Args:
            payload: Raw payload string
            signature: Signature to verify
            timestamp: Optional timestamp for replay protection
            tolerance: Timestamp tolerance in seconds
            
        Returns:
            Tuple of (is_valid, error_message)
        """
        if not self.secret:
            return False, "No secret configured"
        
        if timestamp:
            try:
                ts = int(timestamp)
                current = int(time.time())
                if abs(current - ts) > tolerance:
                    return False, "Timestamp outside tolerance"
            except ValueError:
                return False, "Invalid timestamp format"
        
        expected = self.compute_signature(payload, timestamp)
        
        if hmac.compare_digest(expected, signature):
            return True, ""
        
        return False, "Signature mismatch"
    
    def verify_headers(
        self,
        headers: dict[str, str],
        payload: str,
    ) -> tuple[bool, str]:
        """Verify signature from headers.
        
        Args:
            headers: Request headers
            payload: Raw payload
            
        Returns:
            Tuple of (is_valid, error_message)
        """
        signature_header = headers.get("X-Webhook-Signature", "")
        timestamp_header = headers.get("X-Webhook-Timestamp", "")
        
        if not signature_header:
            return False, "Missing signature header"
        
        return self.verify(payload, signature_header, timestamp_header or None)


class DeliveryTracker:
    """Tracks webhook delivery attempts and retries."""
    
    def __init__(self) -> None:
        """Initialize delivery tracker."""
        self._deliveries: dict[str, list[dict[str, Any]]] = {}
    
    def record_delivery(
        self,
        event_id: str,
        attempt: int,
        status: str,
        response_code: Optional[int] = None,
        error: Optional[str] = None,
    ) -> None:
        """Record a delivery attempt.
        
        Args:
            event_id: Event ID
            attempt: Attempt number
            status: Delivery status (success, failed, pending)
            response_code: HTTP response code
            error: Error message if failed
        """
        if event_id not in self._deliveries:
            self._deliveries[event_id] = []
        
        self._deliveries[event_id].append({
            "attempt": attempt,
            "status": status,
            "response_code": response_code,
            "error": error,
            "timestamp": datetime.now().isoformat(),
        })
    
    def get_delivery_status(self, event_id: str) -> dict[str, Any]:
        """Get delivery status for an event.
        
        Args:
            event_id: Event ID
            
        Returns:
            Delivery status
        """
        attempts = self._deliveries.get(event_id, [])
        
        if not attempts:
            return {"status": "pending", "attempts": 0}
        
        latest = attempts[-1]
        
        if latest["status"] == "success":
            status = "delivered"
        elif latest["status"] == "failed":
            status = "failed"
        else:
            status = "pending"
        
        return {
            "status": status,
            "attempts": len(attempts),
            "last_attempt": latest,
        }
    
    def should_retry(self, event_id: str, max_retries: int = 5) -> bool:
        """Check if event should be retried.
        
        Args:
            event_id: Event ID
            max_retries: Maximum retry attempts
            
        Returns:
            True if should retry
        """
        attempts = self._deliveries.get(event_id, [])
        
        if not attempts:
            return True
        
        latest = attempts[-1]
        
        if latest["status"] == "success":
            return False
        
        return len(attempts) < max_retries


class WebhookFormatter:
    """Formats webhook payloads for different destinations."""
    
    @staticmethod
    def to_json(event: WebhookEvent) -> str:
        """Format event as JSON.
        
        Args:
            event: Webhook event
            
        Returns:
            JSON string
        """
        return json.dumps(event.to_dict(), indent=2)
    
    @staticmethod
    def to_slack(event: WebhookEvent) -> dict[str, Any]:
        """Format event for Slack.
        
        Args:
            event: Webhook event
            
        Returns:
            Slack message payload
        """
        return {
            "text": f"Webhook Event: {event.event_type}",
            "attachments": [
                {
                    "color": "#36a64f",
                    "fields": [
                        {"title": "Event ID", "value": event.id, "short": True},
                        {"title": "Type", "value": event.event_type, "short": True},
                        {"title": "Received", "value": event.received_at.isoformat(), "short": True},
                    ],
                }
            ],
        }
    
    @staticmethod
    def to_teams(event: WebhookEvent) -> dict[str, Any]:
        """Format event for Microsoft Teams.
        
        Args:
            event: Webhook event
            
        Returns:
            Teams message payload
        """
        return {
            "@type": "MessageCard",
            "@context": "http://schema.org/extensions",
            "themeColor": "0076D7",
            "summary": f"Webhook Event: {event.event_type}",
            "sections": [
                {
                    "activityTitle": event.event_type,
                    "facts": [
                        {"name": "Event ID", "value": event.id},
                        {"name": "Received", "value": event.received_at.isoformat()},
                    ],
                }
            ],
        }


def execute(context: dict[str, Any], params: dict[str, Any]) -> dict[str, Any]:
    """Execute webhook action.
    
    Args:
        context: Execution context
        params: Parameters including action type
        
    Returns:
        Result dictionary with status and data
    """
    action = params.get("action", "status")
    result: dict[str, Any] = {"status": "success"}
    
    if action == "verify_signature":
        verifier = SignatureVerifier(secret=params.get("secret"))
        is_valid, error = verifier.verify(
            params.get("payload", ""),
            params.get("signature", ""),
            params.get("timestamp"),
        )
        result["data"] = {"valid": is_valid, "error": error}
    
    elif action == "compute_signature":
        verifier = SignatureVerifier(secret=params.get("secret"))
        sig = verifier.compute_signature(
            params.get("payload", ""),
            params.get("timestamp"),
        )
        result["data"] = {"signature": sig}
    
    elif action == "subscribe":
        subscription = WebhookSubscription(
            id=params.get("id", ""),
            url=params.get("url", ""),
            event_types=params.get("event_types", []),
            secret=params.get("secret"),
        )
        result["data"] = {"subscription_id": subscription.id}
    
    elif action == "route":
        router = WebhookRouter()
        event = WebhookEvent(
            id=params.get("event_id", ""),
            event_type=params.get("event_type", ""),
            payload=params.get("payload", {}),
        )
        results = router.route(event)
        result["data"] = {"routed": len(results)}
    
    elif action == "record_delivery":
        tracker = DeliveryTracker()
        tracker.record_delivery(
            params.get("event_id", ""),
            params.get("attempt", 1),
            params.get("status", "success"),
            params.get("response_code"),
        )
        result["data"] = {"recorded": True}
    
    elif action == "delivery_status":
        tracker = DeliveryTracker()
        status = tracker.get_delivery_status(params.get("event_id", ""))
        result["data"] = status
    
    elif action == "format_slack":
        event = WebhookEvent(
            id="",
            event_type=params.get("event_type", ""),
            payload={},
        )
        formatted = WebhookFormatter.to_slack(event)
        result["data"] = {"formatted": formatted}
    
    elif action == "format_teams":
        event = WebhookEvent(
            id="",
            event_type=params.get("event_type", ""),
            payload={},
        )
        formatted = WebhookFormatter.to_teams(event)
        result["data"] = {"formatted": formatted}
    
    else:
        result["status"] = "error"
        result["error"] = f"Unknown action: {action}"
    
    return result
