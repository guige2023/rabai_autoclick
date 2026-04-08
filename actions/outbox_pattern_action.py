"""Outbox Pattern Action Module.

Provides transactional outbox pattern for
reliable message delivery.
"""

import time
import threading
import json
from typing import Any, Dict, List, Optional
from dataclasses import dataclass, field
from enum import Enum
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class OutboxStatus(Enum):
    """Outbox message status."""
    PENDING = "pending"
    SENT = "sent"
    FAILED = "failed"


@dataclass
class OutboxMessage:
    """Outbox message."""
    message_id: str
    channel: str
    payload: Any
    headers: Dict[str, str] = field(default_factory=dict)
    status: OutboxStatus = OutboxStatus.PENDING
    created_at: float = field(default_factory=time.time)
    sent_at: Optional[float] = None
    retry_count: int = 0
    last_error: Optional[str] = None


class OutboxManager:
    """Manages outbox pattern."""

    def __init__(self):
        self._messages: Dict[str, OutboxMessage] = {}
        self._handlers: Dict[str, callable] = {}
        self._lock = threading.RLock()

    def register_handler(self, channel: str, handler: callable) -> None:
        """Register message handler."""
        self._handlers[channel] = handler

    def add_message(
        self,
        channel: str,
        payload: Any,
        headers: Optional[Dict] = None
    ) -> str:
        """Add message to outbox."""
        message_id = f"outbox_{int(time.time() * 1000)}"

        message = OutboxMessage(
            message_id=message_id,
            channel=channel,
            payload=payload,
            headers=headers or {}
        )

        with self._lock:
            self._messages[message_id] = message

        return message_id

    def send_pending(self, batch_size: int = 100) -> Dict[str, Any]:
        """Send pending messages."""
        with self._lock:
            pending = [
                m for m in self._messages.values()
                if m.status == OutboxStatus.PENDING
            ][:batch_size]

        sent = 0
        failed = 0

        for message in pending:
            handler = self._handlers.get(message.channel)

            if not handler:
                message.last_error = f"No handler for channel: {message.channel}"
                failed += 1
                continue

            try:
                handler(message.payload, message.headers)
                message.status = OutboxStatus.SENT
                message.sent_at = time.time()
                sent += 1

            except Exception as e:
                message.last_error = str(e)
                message.retry_count += 1

                if message.retry_count >= 3:
                    message.status = OutboxStatus.FAILED

                failed += 1

        return {
            "sent": sent,
            "failed": failed,
            "pending": len(pending) - sent - failed
        }

    def get_pending(self, channel: Optional[str] = None) -> List[OutboxMessage]:
        """Get pending messages."""
        with self._lock:
            pending = [
                m for m in self._messages.values()
                if m.status == OutboxStatus.PENDING
            ]

            if channel:
                pending = [m for m in pending if m.channel == channel]

            return pending

    def get_message(self, message_id: str) -> Optional[OutboxMessage]:
        """Get message by ID."""
        return self._messages.get(message_id)


class OutboxPatternAction(BaseAction):
    """Action for outbox pattern operations."""

    def __init__(self):
        super().__init__("outbox_pattern")
        self._manager = OutboxManager()

    def execute(self, params: Dict) -> ActionResult:
        """Execute outbox action."""
        try:
            operation = params.get("operation", "add")

            if operation == "add":
                return self._add(params)
            elif operation == "send":
                return self._send(params)
            elif operation == "pending":
                return self._pending(params)
            elif operation == "get":
                return self._get(params)
            else:
                return ActionResult(success=False, message=f"Unknown: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=str(e))

    def _add(self, params: Dict) -> ActionResult:
        """Add message."""
        message_id = self._manager.add_message(
            channel=params.get("channel", ""),
            payload=params.get("payload"),
            headers=params.get("headers")
        )
        return ActionResult(success=True, data={"message_id": message_id})

    def _send(self, params: Dict) -> ActionResult:
        """Send pending."""
        result = self._manager.send_pending(params.get("batch_size", 100))
        return ActionResult(success=True, data=result)

    def _pending(self, params: Dict) -> ActionResult:
        """Get pending."""
        messages = self._manager.get_pending(params.get("channel"))
        return ActionResult(success=True, data={
            "messages": [
                {
                    "message_id": m.message_id,
                    "channel": m.channel,
                    "created_at": m.created_at
                }
                for m in messages
            ]
        })

    def _get(self, params: Dict) -> ActionResult:
        """Get message."""
        message = self._manager.get_message(params.get("message_id", ""))
        if not message:
            return ActionResult(success=False, message="Not found")
        return ActionResult(success=True, data={
            "message_id": message.message_id,
            "channel": message.channel,
            "status": message.status.value,
            "retry_count": message.retry_count
        })
