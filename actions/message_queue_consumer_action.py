"""Message Queue Consumer Action Module.

Async message queue consumer with acknowledgment and dead letter queue.
"""

from __future__ import annotations

import asyncio
import json
import time
import uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Generic, TypeVar

T = TypeVar("T")


class MessageStatus(Enum):
    """Message processing status."""
    PENDING = "pending"
    PROCESSING = "processing"
    ACKNOWLEDGED = "acknowledged"
    REJECTED = "rejected"
    DEAD_LETTER = "dead_letter"


@dataclass
class Message(Generic[T]):
    """Message envelope."""
    id: str
    body: T
    topic: str
    status: MessageStatus
    created_at: float
    processed_at: float | None = None
    retry_count: int = 0
    max_retries: int = 3
    headers: dict[str, str] = field(default_factory=dict)
    error: str | None = None


class MessageHandler(ABC, Generic[T]):
    """Abstract message handler."""

    @abstractmethod
    async def handle(self, message: Message[T]) -> bool:
        """Handle a message. Returns True if successful."""
        pass


class DeadLetterQueue:
    """Dead letter queue for failed messages."""

    def __init__(self, max_size: int = 1000) -> None:
        self._queue: asyncio.Queue[Message] = asyncio.Queue(maxsize=max_size)
        self._lock = asyncio.Lock()

    async def put(self, message: Message) -> None:
        """Add failed message to DLQ."""
        message.status = MessageStatus.DEAD_LETTER
        await self._queue.put(message)

    async def get(self) -> Message | None:
        """Get message from DLQ."""
        try:
            return self._queue.get_nowait()
        except asyncio.QueueEmpty:
            return None

    async def size(self) -> int:
        return self._queue.qsize()


class MessageQueueConsumer(Generic[T]):
    """Async message queue consumer."""

    def __init__(
        self,
        name: str,
        handler: MessageHandler[T],
        dlq: DeadLetterQueue | None = None,
    ) -> None:
        self.name = name
        self.handler = handler
        self.dlq = dlq or DeadLetterQueue()
        self._running = False
        self._task: asyncio.Task | None = None
        self._queue: asyncio.Queue[Message[T]] = asyncio.Queue()
        self._stats = {"processed": 0, "failed": 0, "rejected": 0}

    async def start(self) -> None:
        """Start the consumer."""
        self._running = True
        self._task = asyncio.create_task(self._consume_loop())

    async def stop(self) -> None:
        """Stop the consumer."""
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

    async def publish(self, topic: str, body: T, headers: dict[str, str] | None = None) -> Message[T]:
        """Publish a message to the consumer's queue."""
        message = Message(
            id=str(uuid.uuid4()),
            body=body,
            topic=topic,
            status=MessageStatus.PENDING,
            created_at=time.time(),
            headers=headers or {}
        )
        await self._queue.put(message)
        return message

    async def _consume_loop(self) -> None:
        """Main consume loop."""
        while self._running:
            try:
                message = await asyncio.wait_for(self._queue.get(), timeout=1.0)
                await self._process_message(message)
            except asyncio.TimeoutError:
                continue
            except asyncio.CancelledError:
                break
            except Exception:
                pass

    async def _process_message(self, message: Message[T]) -> None:
        """Process a single message."""
        message.status = MessageStatus.PROCESSING
        try:
            success = await self.handler.handle(message)
            if success:
                message.status = MessageStatus.ACKNOWLEDGED
                message.processed_at = time.time()
                self._stats["processed"] += 1
            else:
                await self._handle_failure(message)
        except Exception as e:
            message.error = str(e)
            await self._handle_failure(message)

    async def _handle_failure(self, message: Message[T]) -> None:
        """Handle message processing failure."""
        message.retry_count += 1
        if message.retry_count >= message.max_retries:
            message.status = MessageStatus.REJECTED
            self._stats["failed"] += 1
            await self.dlq.put(message)
        else:
            message.status = MessageStatus.PENDING
            await asyncio.sleep(min(2 ** message.retry_count, 30))
            await self._queue.put(message)

    def get_stats(self) -> dict[str, int]:
        """Get consumer statistics."""
        return dict(self._stats)
