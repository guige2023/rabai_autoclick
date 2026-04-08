"""Webhook action module for RabAI AutoClick.

Provides webhook operations for receiving and processing webhooks,
including signature verification, event parsing, and response handling.
"""

import os
import sys
import time
import hmac
import hashlib
import json
import secrets
from typing import Any, Dict, List, Optional, Union, Callable
from dataclasses import dataclass, field
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


@dataclass
class WebhookEvent:
    """Represents a received webhook event.
    
    Attributes:
        event_type: Type of the webhook event.
        payload: Event payload data.
        headers: HTTP headers from the request.
        timestamp: When the event was received.
        delivery_id: Unique delivery ID (if available).
        signature: Webhook signature (if available).
        verified: Whether the signature was verified.
    """
    event_type: str
    payload: Dict[str, Any]
    headers: Dict[str, str]
    timestamp: float = field(default_factory=time.time)
    delivery_id: Optional[str] = None
    signature: Optional[str] = None
    verified: bool = False


class WebhookVerifier:
    """Webhook signature verifier for various providers.
    
    Supports verification for:
    - GitHub (HMAC-SHA256)
    - Slack (SHA256 HMAC)
    - Stripe (HMAC-SHA256)
    - Custom HMAC verification
    """
    
    @staticmethod
    def verify_github(
        payload: bytes,
        signature: str,
        secret: str
    ) -> bool:
        """Verify GitHub webhook signature.
        
        Args:
            payload: Raw request body bytes.
            signature: X-Hub-Signature-256 header value.
            secret: Webhook secret.
            
        Returns:
            True if signature is valid.
        """
        if not signature.startswith("sha256="):
            return False
        
        expected = hmac.new(
            secret.encode(),
            payload,
            hashlib.sha256
        ).hexdigest()
        
        received = signature[7:]
        
        return secrets.compare_digest(expected, received)
    
    @staticmethod
    def verify_slack(
        payload: bytes,
        signature: str,
        timestamp: str,
        secret: str
    ) -> bool:
        """Verify Slack webhook signature.
        
        Args:
            payload: Raw request body bytes.
            signature: X-Slack-Signature header value.
            timestamp: X-Slack-Request-Timestamp header value.
            secret: Signing secret.
            
        Returns:
            True if signature is valid.
        """
        if not signature.startswith("v0="):
            return False
        
        base_string = f"v0:{timestamp}:".encode() + payload
        
        expected = "v0=" + hmac.new(
            secret.encode(),
            base_string,
            hashlib.sha256
        ).hexdigest()
        
        received = signature
        
        return secrets.compare_digest(expected, received)
    
    @staticmethod
    def verify_stripe(
        payload: bytes,
        signature: str,
        secret: str
    ) -> bool:
        """Verify Stripe webhook signature.
        
        Args:
            payload: Raw request body bytes.
            signature: Stripe-Signature header value.
            secret: Webhook signing secret.
            
        Returns:
            True if signature is valid.
        """
        try:
            parts = dict(item.split("=") for item in signature.split(","))
            timestamp = parts.get("t", "")
            expected_sig = parts.get("v1", "")
            
            if not timestamp or not expected_sig:
                return False
            
            signed_payload = f"{timestamp}.".encode() + payload
            
            computed = hmac.new(
                secret.encode(),
                signed_payload,
                hashlib.sha256
            ).hexdigest()
            
            return secrets.compare_digest(computed, expected_sig)
        
        except (ValueError, KeyError):
            return False
    
    @staticmethod
    def verify_hmac(
        payload: bytes,
        signature: str,
        secret: str,
        algorithm: str = "sha256"
    ) -> bool:
        """Verify a generic HMAC signature.
        
        Args:
            payload: Raw request body bytes.
            signature: Signature to verify.
            secret: Shared secret.
            algorithm: Hash algorithm ('sha256', 'sha1', 'md5').
            
        Returns:
            True if signature is valid.
        """
        algorithms = {
            "sha256": hashlib.sha256,
            "sha1": hashlib.sha1,
            "md5": hashlib.md5
        }
        
        hash_func = algorithms.get(algorithm, hashlib.sha256)
        
        expected = hmac.new(
            secret.encode(),
            payload,
            hash_func
        ).hexdigest()
        
        return secrets.compare_digest(expected, signature)


class WebhookAction(BaseAction):
    """Webhook action for receiving and processing webhooks.
    
    Supports signature verification for GitHub, Slack, Stripe,
    and custom webhooks.
    """
    action_type: str = "webhook"
    display_name: str = "Webhook动作"
    description: str = "Webhook接收和处理，支持签名验证"
    
    def __init__(self) -> None:
        super().__init__()
        self._handlers: Dict[str, Callable] = {}
        self._events: List[WebhookEvent] = []
        self._secrets: Dict[str, str] = {}
    
    def get_required_params(self) -> List[str]:
        """Return required parameters for this action."""
        return ["operation"]
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute webhook operation.
        
        Args:
            context: Execution context.
            params: Operation and parameters.
            
        Returns:
            ActionResult with operation outcome.
        """
        start_time = time.time()
        
        try:
            operation = params.get("operation", "verify")
            
            if operation == "verify":
                return self._verify_webhook(params, start_time)
            elif operation == "register_secret":
                return self._register_secret(params, start_time)
            elif operation == "list_events":
                return self._list_events(start_time)
            elif operation == "clear_events":
                return self._clear_events(start_time)
            elif operation == "create_signature":
                return self._create_signature(params, start_time)
            elif operation == "parse_event":
                return self._parse_event(params, start_time)
            else:
                return ActionResult(
                    success=False,
                    message=f"Unknown operation: {operation}",
                    duration=time.time() - start_time
                )
        
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Webhook operation failed: {str(e)}",
                duration=time.time() - start_time
            )
    
    def _verify_webhook(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Verify a webhook signature."""
        provider = params.get("provider", "custom")
        payload_bytes = params.get("payload", b"")
        signature = params.get("signature", "")
        secret = params.get("secret", "")
        timestamp = params.get("timestamp", "")
        
        if isinstance(payload_bytes, str):
            payload_bytes = payload_bytes.encode("utf-8")
        
        if not secret and provider in self._secrets:
            secret = self._secrets[provider]
        
        if not secret:
            return ActionResult(
                success=False,
                message="Secret is required for verification",
                duration=time.time() - start_time
            )
        
        verified = False
        
        if provider == "github":
            verified = WebhookVerifier.verify_github(payload_bytes, signature, secret)
        elif provider == "slack":
            verified = WebhookVerifier.verify_slack(payload_bytes, signature, timestamp, secret)
        elif provider == "stripe":
            verified = WebhookVerifier.verify_stripe(payload_bytes, signature, secret)
        elif provider == "custom":
            verified = WebhookVerifier.verify_hmac(
                payload_bytes,
                signature,
                secret,
                params.get("algorithm", "sha256")
            )
        else:
            return ActionResult(
                success=False,
                message=f"Unknown provider: {provider}",
                duration=time.time() - start_time
            )
        
        try:
            payload_data = json.loads(payload_bytes.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError):
            payload_data = {}
        
        event = WebhookEvent(
            event_type=params.get("event_type", provider),
            payload=payload_data,
            headers=params.get("headers", {}),
            signature=signature,
            verified=verified
        )
        
        self._events.append(event)
        
        return ActionResult(
            success=True,
            message=f"Webhook verified: {verified}",
            data={
                "verified": verified,
                "event_type": event.event_type,
                "timestamp": event.timestamp
            },
            duration=time.time() - start_time
        )
    
    def _register_secret(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Register a webhook secret for a provider."""
        provider = params.get("provider", "")
        secret = params.get("secret", "")
        
        if not provider or not secret:
            return ActionResult(
                success=False,
                message="provider and secret are required",
                duration=time.time() - start_time
            )
        
        self._secrets[provider] = secret
        
        return ActionResult(
            success=True,
            message=f"Registered secret for provider: {provider}",
            duration=time.time() - start_time
        )
    
    def _list_events(self, start_time: float) -> ActionResult:
        """List received webhook events."""
        events = [
            {
                "event_type": e.event_type,
                "timestamp": e.timestamp,
                "verified": e.verified,
                "delivery_id": e.delivery_id
            }
            for e in reversed(self._events)
        ]
        
        return ActionResult(
            success=True,
            message=f"Found {len(events)} events",
            data={"events": events, "count": len(events)},
            duration=time.time() - start_time
        )
    
    def _clear_events(self, start_time: float) -> ActionResult:
        """Clear all stored events."""
        count = len(self._events)
        self._events.clear()
        
        return ActionResult(
            success=True,
            message=f"Cleared {count} events",
            data={"cleared": count},
            duration=time.time() - start_time
        )
    
    def _create_signature(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Create a webhook signature for sending."""
        payload = params.get("payload", "")
        secret = params.get("secret", "")
        algorithm = params.get("algorithm", "sha256")
        provider = params.get("provider", "custom")
        
        if not secret:
            return ActionResult(
                success=False,
                message="secret is required",
                duration=time.time() - start_time
            )
        
        if isinstance(payload, dict):
            payload = json.dumps(payload)
        
        if isinstance(payload, str):
            payload = payload.encode("utf-8")
        
        signature = ""
        
        if provider == "github":
            sig = hmac.new(secret.encode(), payload, hashlib.sha256).hexdigest()
            signature = f"sha256={sig}"
        elif provider == "stripe":
            timestamp = str(int(time.time()))
            signed_payload = f"{timestamp}.".encode() + payload
            sig = hmac.new(secret.encode(), signed_payload, hashlib.sha256).hexdigest()
            signature = f"t={timestamp},v1={sig}"
        else:
            sig = hmac.new(secret.encode(), payload, hashlib.sha256).hexdigest()
            signature = sig
        
        return ActionResult(
            success=True,
            message="Created webhook signature",
            data={"signature": signature, "provider": provider},
            duration=time.time() - start_time
        )
    
    def _parse_event(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Parse a webhook payload."""
        payload = params.get("payload", {})
        provider = params.get("provider", "custom")
        
        if isinstance(payload, str):
            try:
                payload = json.loads(payload)
            except json.JSONDecodeError:
                payload = {"raw": payload}
        
        event_type = "unknown"
        
        if provider == "github":
            event_type = params.get("headers", {}).get("X-GitHub-Event", "push")
        elif provider == "slack":
            event_type = payload.get("type", "event_callback")
        elif provider == "stripe":
            event_type = payload.get("type", "unknown")
        elif provider == "custom":
            event_type = params.get("event_type", "custom_event")
        
        return ActionResult(
            success=True,
            message=f"Parsed {provider} event: {event_type}",
            data={
                "event_type": event_type,
                "payload": payload,
                "provider": provider
            },
            duration=time.time() - start_time
        )
