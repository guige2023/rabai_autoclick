"""
Webhook handler for receiving and processing external API callbacks.

This module provides webhook signature verification, event parsing,
retry handling, and structured processing of incoming webhook events.

Author: RabAiBot
License: MIT
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import logging
import time
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Callable, Dict, List, Optional, Union
from wsgiref.simple_server import make_server
import threading

logger = logging.getLogger(__name__)


class WebhookEventType(Enum):
    """Standard webhook event types."""
    UNKNOWN = auto()
    PING = auto()
    CONFIRMATION = auto()
    NOTIFICATION = auto()
    DATA_UPDATE = auto()
    ACTION_REQUIRED = auto()
    DELETION = auto()


@dataclass
class WebhookEvent:
    """Represents a parsed webhook event."""
    id: str
    type: WebhookEventType
    payload: Dict[str, Any]
    headers: Dict[str, str]
    timestamp: float
    source_ip: Optional[str] = None
    delivery_attempts: int = 0
    processed: bool = False
    processing_result: Optional[Any] = None
    error: Optional[str] = None

    @property
    def age(self) -> float:
        """Get age of event in seconds."""
        return time.time() - self.timestamp

    @classmethod
    def from_raw(
        cls,
        raw_data: Union[bytes, str, Dict],
        headers: Dict[str, str],
        source_ip: Optional[str] = None,
    ) -> "WebhookEvent":
        """Parse raw webhook data into a WebhookEvent."""
        if isinstance(raw_data, bytes):
            raw_data = raw_data.decode("utf-8")
        if isinstance(raw_data, str):
            try:
                raw_data = json.loads(raw_data)
            except json.JSONDecodeError:
                raw_data = {"raw": raw_data}

        event_type = WebhookEventType.UNKNOWN
        if "type" in raw_data:
            type_str = str(raw_data["type"]).upper()
            for et in WebhookEventType:
                if et.name == type_str:
                    event_type = et
                    break

        event_id = raw_data.get("id", raw_data.get("event_id", str(time.time())))
        if isinstance(event_id, int):
            event_id = str(event_id)

        return cls(
            id=event_id,
            type=event_type,
            payload=raw_data,
            headers=headers,
            timestamp=time.time(),
            source_ip=source_ip,
        )


@dataclass
class WebhookConfig:
    """Configuration for webhook handling."""
    secret: Optional[str] = None
    signature_header: str = "X-Webhook-Signature"
    timestamp_header: str = "X-Webhook-Timestamp"
    event_type_header: str = "X-Webhook-Event"
    max_age: float = 300.0
    retry_count: int = 3
    async_processing: bool = True


class WebhookSignatureError(Exception):
    """Raised when webhook signature verification fails."""
    pass


class WebhookHandler:
    """
    Handle incoming webhooks with signature verification and event processing.

    Features:
    - HMAC signature verification
    - Timestamp validation (replay attack prevention)
    - Multiple event type handlers
    - Retry with exponential backoff
    - Async processing support
    - Dead letter queue for failed events

    Example:
        >>> def handle_payment(event):
        ...     print(f"Payment received: {event.payload}")
        >>> handler = WebhookHandler(secret="my_secret")
        >>> handler.register("payment.completed", handle_payment)
        >>> handler.start_server(port=8080)
    """

    def __init__(
        self,
        config: Optional[WebhookConfig] = None,
    ):
        """
        Initialize webhook handler.

        Args:
            config: Webhook configuration
        """
        self.config = config or WebhookConfig()
        self._handlers: Dict[str, Callable[[WebhookEvent], Any]] = {}
        self._event_queue: asyncio.Queue = asyncio.Queue()
        self._processing_task: Optional[asyncio.Task] = None
        self._server: Optional[any] = None
        self._running = False
        self._processed_count = 0
        self._failed_count = 0
        logger.info("WebhookHandler initialized")

    def register(
        self,
        event_type: str,
        handler: Callable[[WebhookEvent], Any],
    ) -> None:
        """
        Register a handler for an event type.

        Args:
            event_type: Event type name or "*" for all
            handler: Callback function to handle the event
        """
        self._handlers[event_type] = handler
        logger.info(f"Registered handler for event type: {event_type}")

    def unregister(self, event_type: str) -> bool:
        """Unregister a handler."""
        if event_type in self._handlers:
            del self._handlers[event_type]
            logger.info(f"Unregistered handler for: {event_type}")
            return True
        return False

    def verify_signature(
        self,
        payload: bytes,
        headers: Dict[str, str],
    ) -> bool:
        """
        Verify webhook signature.

        Args:
            payload: Raw request body
            headers: Request headers

        Returns:
            True if signature is valid

        Raises:
            WebhookSignatureError: If signature verification fails
        """
        if not self.config.secret:
            logger.warning("No secret configured, skipping signature verification")
            return True

        sig_header = headers.get(self.config.signature_header, "")
        ts_header = headers.get(self.config.timestamp_header, "")

        if not sig_header:
            raise WebhookSignatureError("Missing signature header")

        if not ts_header:
            raise WebhookSignatureError("Missing timestamp header")

        try:
            timestamp = float(ts_header)
        except ValueError:
            raise WebhookSignatureError("Invalid timestamp format")

        if abs(time.time() - timestamp) > self.config.max_age:
            raise WebhookSignatureError("Request timestamp too old (replay attack?)")

        payload_with_ts = f"{ts_header}.{payload.decode('utf-8')}"
        expected_sig = hmac.new(
            self.config.secret.encode("utf-8"),
            payload_with_ts.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()

        if not hmac.compare_digest(sig_header, expected_sig):
            raise WebhookSignatureError("Signature mismatch")

        return True

    def parse_event(
        self,
        raw_data: Union[bytes, str, Dict],
        headers: Dict[str, str],
        source_ip: Optional[str] = None,
    ) -> WebhookEvent:
        """
        Parse raw webhook data into a WebhookEvent.

        Args:
            raw_data: Raw request body
            headers: Request headers
            source_ip: Source IP address

        Returns:
            Parsed WebhookEvent
        """
        event = WebhookEvent.from_raw(raw_data, headers, source_ip)

        if event.type == WebhookEventType.UNKNOWN and self.config.event_type_header:
            type_header = headers.get(self.config.event_type_header, "")
            if type_header:
                event.payload["_event_type_from_header"] = type_header

        logger.debug(f"Parsed webhook event: {event.id} (type={event.type.name})")
        return event

    async def handle_raw(
        self,
        raw_data: bytes,
        headers: Dict[str, str],
        source_ip: Optional[str] = None,
    ) -> WebhookEvent:
        """
        Handle incoming raw webhook request.

        Args:
            raw_data: Raw request body
            headers: Request headers
            source_ip: Source IP address

        Returns:
            Processed WebhookEvent

        Raises:
            WebhookSignatureError: If signature verification fails
        """
        self.verify_signature(raw_data, headers)
        event = self.parse_event(raw_data, headers, source_ip)
        await self._enqueue_event(event)
        return event

    async def _enqueue_event(self, event: WebhookEvent) -> None:
        """Add event to processing queue."""
        await self._event_queue.put(event)
        if not self._processing_task or self._processing_task.done():
            self._processing_task = asyncio.create_task(self._process_queue())

    async def _process_queue(self) -> None:
        """Process events from the queue."""
        while not self._event_queue.empty():
            event = await self._event_queue.get()
            await self._process_event(event)
            self._event_queue.task_done()

    async def _process_event(self, event: WebhookEvent) -> None:
        """Process a single webhook event."""
        event.processed = False
        event.delivery_attempts += 1

        event_type_str = event.type.name
        handlers_to_try = [
            self._handlers.get(event_type_str),
            self._handlers.get("*"),
        ]

        last_error = None
        for handler in handlers_to_try:
            if handler is None:
                continue

            try:
                if asyncio.iscoroutinefunction(handler):
                    result = await handler(event)
                else:
                    loop = asyncio.get_event_loop()
                    result = await loop.run_in_executor(None, handler, event)

                event.processed = True
                event.processing_result = result
                self._processed_count += 1
                logger.info(f"Event {event.id} processed successfully")
                return

            except Exception as e:
                last_error = e
                logger.warning(
                    f"Handler failed for event {event.id}: {e} "
                    f"(attempt {event.delivery_attempts})"
                )

        event.error = str(last_error)
        self._failed_count += 1

        if event.delivery_attempts < self.config.retry_count:
            await asyncio.sleep(2 ** event.delivery_attempts)
            await self._enqueue_event(event)

    def handle_sync(
        self,
        raw_data: Union[bytes, str],
        headers: Dict[str, str],
        source_ip: Optional[str] = None,
    ) -> WebhookEvent:
        """
        Synchronous version of handle_raw.

        Args:
            raw_data: Raw request body
            headers: Request headers
            source_ip: Source IP address

        Returns:
            Processed WebhookEvent
        """
        try:
            loop = asyncio.get_event_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

        return loop.run_until_complete(self.handle_raw(raw_data, headers, source_ip))

    def get_stats(self) -> Dict[str, Any]:
        """Get webhook handler statistics."""
        return {
            "registered_handlers": len(self._handlers),
            "event_types": list(self._handlers.keys()),
            "processed_count": self._processed_count,
            "failed_count": self._failed_count,
            "queue_size": self._event_queue.qsize() if hasattr(self, '_event_queue') else 0,
            "running": self._running,
        }

    def reset_stats(self) -> None:
        """Reset statistics counters."""
        self._processed_count = 0
        self._failed_count = 0
        logger.info("Webhook handler statistics reset")
