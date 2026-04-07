"""
Mailbox Utilities

Provides a message mailbox system with persistence,
priorities, and consumer groups for distributed processing.
"""

from __future__ import annotations

import copy
import heapq
import json
import threading
import time
import uuid
from abc import ABC, abstractmethod
from collections.abc import Callable
from dataclasses import dataclass, field
from enum import Enum, auto
from pathlib import Path
from typing import Any, Generic, TypeVar

T = TypeVar("T")


class MessageStatus(Enum):
    """Status of a message."""
    PENDING = auto()
    PROCESSING = auto()
    COMPLETED = auto()
    FAILED = auto()
    DEAD_LETTER = auto()


@dataclass
class MailboxMessage(Generic[T]):
    """A message in the mailbox."""
    id: str = field(default_factory=lambda: uuid.uuid4().hex)
    data: T | None = None
    priority: int = 0
    status: MessageStatus = MessageStatus.PENDING
    created_at: float = field(default_factory=time.time)
    processed_at: float | None = None
    retry_count: int = 0
    max_retries: int = 3
    error: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
    tags: list[str] = field(default_factory=list)

    def __lt__(self, other: MailboxMessage) -> bool:
        """Compare by priority (lower = higher priority) and timestamp."""
        if self.priority != other.priority:
            return self.priority < other.priority
        return self.created_at < other.created_at


class MailboxBackend(ABC):
    """Abstract backend for mailbox storage."""

    @abstractmethod
    def enqueue(self, message: MailboxMessage) -> None:
        """Add a message to the mailbox."""
        pass

    @abstractmethod
    def dequeue(self) -> MailboxMessage | None:
        """Get and remove the next message."""
        pass

    @abstractmethod
    def peek(self) -> MailboxMessage | None:
        """View the next message without removing."""
        pass

    @abstractmethod
    def ack(self, message_id: str) -> bool:
        """Acknowledge a message was processed."""
        pass

    @abstractmethod
    def nack(self, message_id: str, error: str | None = None) -> bool:
        """Negative acknowledge a message."""
        pass

    @abstractmethod
    def size(self) -> int:
        """Get number of pending messages."""
        pass


class InMemoryMailboxBackend(MailboxBackend):
    """In-memory mailbox backend."""

    def __init__(self):
        self._heap: list[MailboxMessage] = []
        self._lock = threading.RLock()
        self._by_id: dict[str, MailboxMessage] = {}

    def enqueue(self, message: MailboxMessage) -> None:
        with self._lock:
            heapq.heappush(self._heap, message)
            self._by_id[message.id] = message

    def dequeue(self) -> MailboxMessage | None:
        with self._lock:
            while self._heap:
                msg = heapq.heappop(self._heap)
                if msg.status == MessageStatus.PENDING:
                    msg.status = MessageStatus.PROCESSING
                    msg.metadata["dequeued_at"] = time.time()
                    return msg
            return None

    def peek(self) -> MailboxMessage | None:
        with self._lock:
            for msg in sorted(self._heap):
                if msg.status == MessageStatus.PENDING:
                    return msg
            return None

    def ack(self, message_id: str) -> bool:
        with self._lock:
            if message_id in self._by_id:
                msg = self._by_id[message_id]
                msg.status = MessageStatus.COMPLETED
                msg.processed_at = time.time()
                return True
            return False

    def nack(self, message_id: str, error: str | None = None) -> bool:
        with self._lock:
            if message_id in self._by_id:
                msg = self._by_id[message_id]
                msg.retry_count += 1
                msg.error = error
                msg.metadata["last_failed_at"] = time.time()

                if msg.retry_count >= msg.max_retries:
                    msg.status = MessageStatus.DEAD_LETTER
                else:
                    msg.status = MessageStatus.PENDING
                    # Re-enqueue with updated retry count
                    heapq.heappush(self._heap, msg)

                return True
            return False

    def size(self) -> int:
        with self._lock:
            return sum(1 for m in self._heap if m.status == MessageStatus.PENDING)


class Mailbox(Generic[T]):
    """
    Mailbox for storing and retrieving messages with priorities.
    """

    def __init__(
        self,
        name: str = "",
        backend: MailboxBackend | None = None,
        default_max_retries: int = 3,
    ):
        self.name = name or f"mailbox_{uuid.uuid4().hex[:8]}"
        self._backend = backend or InMemoryMailboxBackend()
        self._default_max_retries = default_max_retries
        self._handlers: list[Callable[[T], Any]] = []
        self._metrics: dict[str, int] = {
            "enqueued": 0,
            "dequeued": 0,
            "completed": 0,
            "failed": 0,
            "dead_lettered": 0,
        }

    def send(
        self,
        data: T,
        priority: int = 0,
        max_retries: int | None = None,
        tags: list[str] | None = None,
        **metadata: Any,
    ) -> str:
        """
        Send a message to the mailbox.

        Returns:
            Message ID.
        """
        message = MailboxMessage(
            data=data,
            priority=priority,
            max_retries=max_retries or self._default_max_retries,
            tags=tags or [],
            metadata=metadata,
        )

        self._backend.enqueue(message)
        self._metrics["enqueued"] += 1
        return message.id

    def receive(self, timeout: float | None = None) -> T | None:
        """
        Receive a message from the mailbox.

        Args:
            timeout: Maximum time to wait for a message.

        Returns:
            The message data, or None if timeout.
        """
        if timeout:
            end_time = time.time() + timeout
            while time.time() < end_time:
                msg = self._backend.dequeue()
                if msg:
                    self._metrics["dequeued"] += 1
                    return msg.data
                time.sleep(0.01)
        else:
            msg = self._backend.dequeue()
            if msg:
                self._metrics["dequeued"] += 1
                return msg.data

        return None

    def acknowledge(self, message_id: str) -> bool:
        """Acknowledge successful processing."""
        if self._backend.ack(message_id):
            self._metrics["completed"] += 1
            return True
        return False

    def reject(self, message_id: str, error: str | None = None) -> bool:
        """Reject a message (with retry or dead-letter)."""
        if self._backend.nack(message_id, error):
            self._metrics["failed"] += 1
            # Check if it became dead-lettered
            return True
        return False

    def register_handler(self, handler: Callable[[T], Any]) -> None:
        """Register a message handler."""
        self._handlers.append(handler)

    def process_one(self) -> bool:
        """
        Process one message with registered handlers.

        Returns:
            True if a message was processed.
        """
        msg = self._backend.dequeue()
        if not msg:
            return False

        self._metrics["dequeued"] += 1
        success = False

        for handler in self._handlers:
            try:
                handler(msg.data)
                success = True
            except Exception as e:
                msg.error = str(e)

        if success:
            self._backend.ack(msg.id)
            self._metrics["completed"] += 1
        else:
            self._backend.nack(msg.id, msg.error)
            self._metrics["failed"] += 1

        return True

    @property
    def pending_count(self) -> int:
        """Get number of pending messages."""
        return self._backend.size()

    @property
    def metrics(self) -> dict[str, int]:
        """Get mailbox metrics."""
        return copy.copy(self._metrics)


class MailboxPool:
    """
    Pool of mailboxes for different message types.
    """

    def __init__(self):
        self._mailboxes: dict[str, Mailbox] = {}
        self._lock = threading.RLock()

    def get_mailbox(self, name: str) -> Mailbox:
        """Get or create a mailbox."""
        with self._lock:
            if name not in self._mailboxes:
                self._mailboxes[name] = Mailbox(name)
            return self._mailboxes[name]

    def list_mailboxes(self) -> list[str]:
        """List all mailbox names."""
        with self._lock:
            return list(self._mailboxes.keys())

    def delete_mailbox(self, name: str) -> bool:
        """Delete a mailbox."""
        with self._lock:
            if name in self._mailboxes:
                del self._mailboxes[name]
                return True
            return False

    def get_all_metrics(self) -> dict[str, dict[str, int]]:
        """Get metrics for all mailboxes."""
        with self._lock:
            return {name: mb.metrics for name, mb in self._mailboxes.items()}
