"""Dead Letter Queue Action Module.

Provides dead letter queue for failed message
handling and reprocessing.
"""

import time
import json
import threading
from typing import Any, Dict, List, Optional
from dataclasses import dataclass, field
from enum import Enum
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class DLQStatus(Enum):
    """Dead letter status."""
    PENDING = "pending"
    RETRYING = "retrying"
    DISCARDED = "discarded"
    PROCESSED = "processed"


@dataclass
class DeadLetter:
    """Dead letter message."""
    message_id: str
    original_queue: str
    payload: Any
    error: str
    error_count: int = 0
    status: DLQStatus = DLQStatus.PENDING
    created_at: float = field(default_factory=time.time)
    last_attempt: Optional[float] = None
    next_retry: Optional[float] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


class DeadLetterQueueManager:
    """Manages dead letter queue."""

    def __init__(self):
        self._messages: Dict[str, DeadLetter] = {}
        self._lock = threading.RLock()

    def add(
        self,
        original_queue: str,
        payload: Any,
        error: str,
        metadata: Optional[Dict] = None
    ) -> str:
        """Add message to DLQ."""
        message_id = f"dlq_{int(time.time() * 1000)}"

        dlq_message = DeadLetter(
            message_id=message_id,
            original_queue=original_queue,
            payload=payload,
            error=error,
            metadata=metadata or {}
        )

        with self._lock:
            self._messages[message_id] = dlq_message

        return message_id

    def retry(self, message_id: str) -> bool:
        """Mark message for retry."""
        with self._lock:
            message = self._messages.get(message_id)
            if not message:
                return False

            message.status = DLQStatus.RETRYING
            message.error_count += 1
            message.last_attempt = time.time()
            message.next_retry = None

            return True

    def discard(self, message_id: str) -> bool:
        """Discard message."""
        with self._lock:
            message = self._messages.get(message_id)
            if not message:
                return False

            message.status = DLQStatus.DISCARDED
            return True

    def mark_processed(self, message_id: str) -> bool:
        """Mark as processed."""
        with self._lock:
            message = self._messages.get(message_id)
            if not message:
                return False

            message.status = DLQStatus.PROCESSED
            return True

    def get(self, message_id: str) -> Optional[DeadLetter]:
        """Get message."""
        return self._messages.get(message_id)

    def get_pending(self, limit: int = 100) -> List[DeadLetter]:
        """Get pending messages."""
        with self._lock:
            pending = [
                m for m in self._messages.values()
                if m.status in (DLQStatus.PENDING, DLQStatus.RETRYING)
            ]
            return pending[:limit]

    def get_stats(self) -> Dict:
        """Get DLQ statistics."""
        with self._lock:
            total = len(self._messages)
            pending = sum(
                1 for m in self._messages.values()
                if m.status == DLQStatus.PENDING
            )
            retrying = sum(
                1 for m in self._messages.values()
                if m.status == DLQStatus.RETRYING
            )
            discarded = sum(
                1 for m in self._messages.values()
                if m.status == DLQStatus.DISCARDED
            )
            processed = sum(
                1 for m in self._messages.values()
                if m.status == DLQStatus.PROCESSED
            )

            return {
                "total": total,
                "pending": pending,
                "retrying": retrying,
                "discarded": discarded,
                "processed": processed
            }


class DeadLetterQueueAction(BaseAction):
    """Action for DLQ operations."""

    def __init__(self):
        super().__init__("dead_letter_queue")
        self._manager = DeadLetterQueueManager()

    def execute(self, params: Dict) -> ActionResult:
        """Execute DLQ action."""
        try:
            operation = params.get("operation", "add")

            if operation == "add":
                return self._add(params)
            elif operation == "retry":
                return self._retry(params)
            elif operation == "discard":
                return self._discard(params)
            elif operation == "mark_processed":
                return self._mark_processed(params)
            elif operation == "get":
                return self._get(params)
            elif operation == "pending":
                return self._pending(params)
            elif operation == "stats":
                return self._stats(params)
            else:
                return ActionResult(success=False, message=f"Unknown: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=str(e))

    def _add(self, params: Dict) -> ActionResult:
        """Add to DLQ."""
        message_id = self._manager.add(
            original_queue=params.get("original_queue", ""),
            payload=params.get("payload"),
            error=params.get("error", ""),
            metadata=params.get("metadata")
        )
        return ActionResult(success=True, data={"message_id": message_id})

    def _retry(self, params: Dict) -> ActionResult:
        """Mark for retry."""
        success = self._manager.retry(params.get("message_id", ""))
        return ActionResult(success=success)

    def _discard(self, params: Dict) -> ActionResult:
        """Discard message."""
        success = self._manager.discard(params.get("message_id", ""))
        return ActionResult(success=success)

    def _mark_processed(self, params: Dict) -> ActionResult:
        """Mark processed."""
        success = self._manager.mark_processed(params.get("message_id", ""))
        return ActionResult(success=success)

    def _get(self, params: Dict) -> ActionResult:
        """Get message."""
        message = self._manager.get(params.get("message_id", ""))
        if not message:
            return ActionResult(success=False, message="Not found")
        return ActionResult(success=True, data={
            "message_id": message.message_id,
            "original_queue": message.original_queue,
            "error": message.error,
            "status": message.status.value,
            "error_count": message.error_count
        })

    def _pending(self, params: Dict) -> ActionResult:
        """Get pending."""
        messages = self._manager.get_pending(params.get("limit", 100))
        return ActionResult(success=True, data={
            "messages": [
                {
                    "message_id": m.message_id,
                    "original_queue": m.original_queue,
                    "error": m.error,
                    "status": m.status.value
                }
                for m in messages
            ]
        })

    def _stats(self, params: Dict) -> ActionResult:
        """Get stats."""
        return ActionResult(success=True, data=self._manager.get_stats())
