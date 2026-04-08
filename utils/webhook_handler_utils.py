"""Webhook handler utilities for automation trigger integration.

Provides tools for receiving, validating, and processing webhooks
from external services, supporting signature verification,
event parsing, and automated action dispatch.

Example:
    >>> from utils.webhook_handler_utils import WebhookHandler, verify_signature
    >>> handler = WebhookHandler(secret='my_secret')
    >>> handler.on('push', lambda event: process_push(event))
    >>> handler.start(port=8080)
"""

from __future__ import annotations

import hashlib
import hmac
import json
import time
from typing import Any, Callable, Optional

__all__ = [
    "WebhookHandler",
    "verify_signature",
    "parse_github_webhook",
    "parse_slack_webhook",
    "WebhookEvent",
    "WebhookError",
]


class WebhookError(Exception):
    """Raised when webhook processing fails."""
    pass


@dataclass
class WebhookEvent:
    """A received webhook event."""

    headers: dict
    body: dict
    event_type: str
    delivery_id: Optional[str]
    timestamp: float


def verify_signature(
    payload: bytes,
    signature: str,
    secret: str,
    algorithm: str = "sha256",
) -> bool:
    """Verify the HMAC signature of a webhook payload.

    Args:
        payload: Raw request body bytes.
        signature: Signature from the request header.
        secret: Shared secret key.
        algorithm: Hash algorithm ('sha256' or 'sha1').

    Returns:
        True if the signature is valid.
    """
    if algorithm == "sha256":
        h = hmac.new(secret.encode(), payload, hashlib.sha256)
    elif algorithm == "sha1":
        h = hmac.new(secret.encode(), payload, hashlib.sha1)
    else:
        raise WebhookError(f"Unknown algorithm: {algorithm}")

    expected = h.hexdigest()
    return hmac.compare_digest(expected, signature)


def parse_github_webhook(headers: dict, body: dict) -> WebhookEvent:
    """Parse a GitHub webhook event.

    Args:
        headers: Request headers.
        body: Parsed JSON body.

    Returns:
        WebhookEvent with extracted fields.
    """
    event_type = headers.get("X-GitHub-Event", "unknown")
    delivery_id = headers.get("X-GitHub-Delivery")
    return WebhookEvent(
        headers=headers,
        body=body,
        event_type=event_type,
        delivery_id=delivery_id,
        timestamp=time.time(),
    )


def parse_slack_webhook(headers: dict, body: dict) -> WebhookEvent:
    """Parse a Slack webhook event.

    Args:
        headers: Request headers.
        body: Parsed JSON body.

    Returns:
        WebhookEvent with extracted fields.
    """
    return WebhookEvent(
        headers=headers,
        body=body,
        event_type=body.get("type", "event_callback"),
        delivery_id=body.get("team_id"),
        timestamp=time.time(),
    )


class WebhookHandler:
    """HTTP webhook receiver and dispatcher.

    Example:
        >>> handler = WebhookHandler(secret='webhook_secret')
        >>> @handler.on('push')
        ... def handle_push(event):
        ...     print(f"Got push: {event.body}")
        >>> handler.start(port=8080)  # starts HTTP server
    """

    def __init__(
        self,
        secret: Optional[str] = None,
        signature_header: str = "X-Signature-256",
        algorithm: str = "sha256",
    ):
        self.secret = secret
        self.signature_header = signature_header
        self.algorithm = algorithm
        self._handlers: dict[str, list[Callable]] = {}
        self._running = False

    def on(self, event_type: str) -> Callable:
        """Decorator to register a handler for an event type.

        Args:
            event_type: Event type to handle.

        Returns:
            Decorator function.
        """
        def decorator(fn: Callable) -> Callable:
            self._handlers.setdefault(event_type, []).append(fn)
            return fn
        return decorator

    def handle(self, headers: dict, body: bytes) -> list[Any]:
        """Process an incoming webhook request.

        Args:
            headers: HTTP headers.
            body: Raw request body bytes.

        Returns:
            List of handler return values.

        Raises:
            WebhookError: If validation fails.
        """
        # Parse body
        try:
            parsed = json.loads(body.decode("utf-8"))
        except Exception as e:
            raise WebhookError(f"Invalid JSON body: {e}")

        # Verify signature if secret is configured
        if self.secret:
            sig = headers.get(self.signature_header, headers.get("X-Hub-Signature-256", ""))
            if not sig:
                raise WebhookError("Missing signature header")
            # Strip 'sha256=' prefix if present
            if sig.startswith("sha256="):
                sig = sig[7:]
            if not verify_signature(body, sig, self.secret, self.algorithm):
                raise WebhookError("Invalid signature")

        # Determine event type
        event_type = headers.get("X-GitHub-Event", "unknown")
        delivery_id = headers.get("X-GitHub-Delivery")

        event = WebhookEvent(
            headers=headers,
            body=parsed,
            event_type=event_type,
            delivery_id=delivery_id,
            timestamp=time.time(),
        )

        # Dispatch to handlers
        results = []
        handlers = self._handlers.get(event_type, [])
        for handler in handlers:
            try:
                results.append(handler(event))
            except Exception as e:
                results.append(e)

        return results

    def start(self, host: str = "0.0.0.0", port: int = 8080) -> None:
        """Start the webhook HTTP server.

        Args:
            host: Bind address.
            port: Listen port.
        """
        try:
            from http.server import HTTPServer, BaseHTTPRequestHandler
        except ImportError:
            raise WebhookError("HTTP server not available")

        self._running = True

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self):
                content_length = int(self.headers.get("Content-Length", 0))
                body = self.rfile.read(content_length)

                try:
                    results = self.server.server.handle(
                        {k: v for k, v in self.headers.items()},
                        body,
                    )
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json")
                    self.end_headers()
                    self.wfile.write(json.dumps({"status": "ok", "results": len(results)}).encode())
                except WebhookError as e:
                    self.send_response(400)
                    self.send_header("Content-Type", "application/json")
                    self.end_headers()
                    self.wfile.write(json.dumps({"error": str(e)}).encode())
                except Exception as e:
                    self.send_response(500)
                    self.send_header("Content-Type", "application/json")
                    self.end_headers()
                    self.wfile.write(json.dumps({"error": "Internal error"}).encode())

            def log_message(self, format, *args):
                pass  # silence default logging

        server = HTTPServer((host, port), Handler)
        server.server = self
        server.serve_forever()
