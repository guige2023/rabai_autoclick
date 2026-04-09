"""
API SQS Action Module.

Provides Amazon SQS queue operations for message sending,
receiving, batch processing, and dead letter queue management.

Author: RabAi Team
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Callable, Optional


class QueueType(Enum):
    """SQS queue types."""
    STANDARD = "standard"
    FIFO = "fifo"
    DLQ = "dlq"


@dataclass
class SQSConfig:
    """SQS client configuration."""
    region: str = "us-east-1"
    endpoint_url: Optional[str] = None
    credentials: Optional[dict[str, str]] = None
    visibility_timeout: int = 30
    receive_wait_time: int = 0
    max_receive_count: int = 3


@dataclass
class Message:
    """SQS message representation."""
    message_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    receipt_handle: str = ""
    body: str = ""
    attributes: dict[str, str] = field(default_factory=dict)
    message_attributes: dict[str, dict[str, Any]] = field(default_factory=dict)
    MD5_of_body: str = ""
    MD5_of_message_attributes: str = ""
    timestamp: float = field(default_factory=time.time)
    delay_seconds: int = 0
    receive_count: int = 0


@dataclass
class QueueAttributes:
    """SQS queue attributes."""
    queue_name: str
    queue_url: str = ""
    queue_type: QueueType = QueueType.STANDARD
    visibility_timeout: int = 30
    message_retention_period: int = 345600
    maximum_message_size: int = 262144
    delay_seconds: int = 0
    receive_wait_time_seconds: int = 0
    approximate_number_of_messages: int = 0
    approximate_number_of_messages_not_visible: int = 0


@dataclass
class SendResult:
    """Result of send operation."""
    message_id: str
    MD5_of_message_body: str
    sequence_number: Optional[str] = None


@dataclass
class ReceiveResult:
    """Result of receive operation."""
    messages: list[Message] = field(default_factory=list)
    receipt_handles: list[str] = field(default_factory=list)


class SQSClient:
    """Amazon SQS client."""

    def __init__(self, config: Optional[SQSConfig] = None):
        self.config = config or SQSConfig()
        self._queues: dict[str, list[Message]] = {}
        self._queue_attrs: dict[str, QueueAttributes] = {}
        self._visibility_timers: dict[str, float] = {}

    async def create_queue(
        self,
        queue_name: str,
        queue_type: QueueType = QueueType.STANDARD,
        attributes: Optional[dict[str, Any]] = None,
        tags: Optional[dict[str, str]] = None,
    ) -> str:
        """Create a new SQS queue."""
        queue_url = f"https://sqs.{self.config.region}.amazonaws.com/{queue_name}"
        if queue_name in self._queues:
            return queue_url
        self._queues[queue_name] = []
        attrs = QueueAttributes(
            queue_name=queue_name,
            queue_url=queue_url,
            queue_type=queue_type,
        )
        if attributes:
            attrs.visibility_timeout = attributes.get("VisibilityTimeout", attrs.visibility_timeout)
            attrs.message_retention_period = attributes.get("MessageRetentionPeriod", attrs.message_retention_period)
        self._queue_attrs[queue_name] = attrs
        await asyncio.sleep(0.02)
        return queue_url

    async def get_queue_url(self, queue_name: str) -> str:
        """Get queue URL by name."""
        if queue_name in self._queue_attrs:
            return self._queue_attrs[queue_name].queue_url
        return f"https://sqs.{self.config.region}.amazonaws.com/{queue_name}"

    async def send_message(
        self,
        queue_name: str,
        message_body: str,
        delay_seconds: int = 0,
        message_attributes: Optional[dict[str, dict[str, Any]]] = None,
        deduplication_id: Optional[str] = None,
        group_id: Optional[str] = None,
    ) -> SendResult:
        """Send a single message to queue."""
        if queue_name not in self._queues:
            await self.create_queue(queue_name)
        message = Message(
            body=message_body,
            delay_seconds=delay_seconds,
            message_attributes=message_attributes or {},
            MD5_of_body=hashlib.md5(message_body.encode()).hexdigest(),
        )
        self._queues[queue_name].append(message)
        await asyncio.sleep(0.01)
        return SendResult(
            message_id=message.message_id,
            MD5_of_message_body=message.MD5_of_body,
        )

    async def send_message_batch(
        self,
        queue_name: str,
        messages: list[tuple[str, Optional[str]]],
    ) -> dict[str, Any]:
        """Send multiple messages in batch."""
        if queue_name not in self._queues:
            await self.create_queue(queue_name)
        results = {"Successful": [], "Failed": []}
        for body, dedup_id in messages:
            result = await self.send_message(queue_name, body, deduplication_id=dedup_id)
            results["Successful"].append({
                "Id": str(len(results["Successful"])),
                "MessageId": result.message_id,
                "MD5OfMessageBody": result.MD5_of_message_body,
            })
        return results

    async def receive_messages(
        self,
        queue_name: str,
        max_number_of_messages: int = 1,
        visibility_timeout: Optional[int] = None,
        wait_time_seconds: int = 0,
    ) -> ReceiveResult:
        """Receive messages from queue."""
        if queue_name not in self._queues:
            return ReceiveResult()
        now = time.time()
        available = [
            (i, m) for i, m in enumerate(self._queues[queue_name])
            if m.delay_seconds == 0 and
            (m.receipt_handle not in self._visibility_timers or
             self._visibility_timers[m.receipt_handle] <= now)
        ]
        if not available and wait_time_seconds > 0:
            await asyncio.sleep(min(wait_time_seconds, 0.5))
            available = [
                (i, m) for i, m in enumerate(self._queues[queue_name])
                if m.delay_seconds == 0 and
                (m.receipt_handle not in self._visibility_timers or
                 self._visibility_timers[m.receipt_handle] <= now)
            ]
        messages = []
        receipt_handles = []
        for idx, (orig_idx, msg) in enumerate(available[:max_number_of_messages]):
            if not msg.receipt_handle:
                msg.receipt_handle = str(uuid.uuid4())
            self._visibility_timers[msg.receipt_handle] = now + (visibility_timeout or self.config.visibility_timeout)
            msg.receive_count += 1
            messages.append(msg)
            receipt_handles.append(msg.receipt_handle)
        return ReceiveResult(messages=messages, receipt_handles=receipt_handles)

    async def delete_message(
        self,
        queue_name: str,
        receipt_handle: str,
    ) -> bool:
        """Delete a message from queue."""
        if queue_name not in self._queues:
            return False
        for i, msg in enumerate(self._queues[queue_name]):
            if msg.receipt_handle == receipt_handle:
                self._queues[queue_name].pop(i)
                if receipt_handle in self._visibility_timers:
                    del self._visibility_timers[receipt_handle]
                return True
        return False

    async def delete_message_batch(
        self,
        queue_name: str,
        receipt_handles: list[str],
    ) -> dict[str, Any]:
        """Delete multiple messages."""
        results = {"Successful": [], "Failed": []}
        for i, handle in enumerate(receipt_handles):
            success = await self.delete_message(queue_name, handle)
            if success:
                results["Successful"].append({"Id": str(i)})
            else:
                results["Failed"].append({"Id": str(i), "SenderFault": True, "Code": "ReceiptHandleIsInvalid"})
        return results

    async def purge_queue(self, queue_name: str) -> bool:
        """Purge all messages from queue."""
        if queue_name in self._queues:
            self._queues[queue_name].clear()
        return True

    async def get_queue_attributes(
        self,
        queue_name: str,
        attribute_names: Optional[list[str]] = None,
    ) -> dict[str, Any]:
        """Get queue attributes."""
        if queue_name not in self._queue_attrs:
            return {}
        attrs = self._queue_attrs[queue_name]
        result = {
            "QueueUrl": attrs.queue_url,
            "ApproximateNumberOfMessages": str(len(self._queues.get(queue_name, []))),
            "VisibilityTimeout": str(attrs.visibility_timeout),
        }
        return result

    async def change_message_visibility(
        self,
        queue_name: str,
        receipt_handle: str,
        visibility_timeout: int,
    ) -> bool:
        """Change message visibility timeout."""
        if receipt_handle in self._visibility_timers:
            self._visibility_timers[receipt_handle] = time.time() + visibility_timeout
            return True
        return False

    async def set_queue_attributes(
        self,
        queue_name: str,
        attributes: dict[str, str],
    ) -> bool:
        """Set queue attributes."""
        if queue_name not in self._queue_attrs:
            return False
        attrs = self._queue_attrs[queue_name]
        if "VisibilityTimeout" in attributes:
            attrs.visibility_timeout = int(attributes["VisibilityTimeout"])
        if "MessageRetentionPeriod" in attributes:
            attrs.message_retention_period = int(attributes["MessageRetentionPeriod"])
        return True


class DLQManager:
    """Dead Letter Queue manager for SQS."""

    def __init__(self, client: SQSClient):
        self.client = client
        self._dlq_mappings: dict[str, str] = {}

    async def setup_dlq(self, source_queue: str, dlq_name: str) -> str:
        """Setup a DLQ for a source queue."""
        dlq_url = await self.client.create_queue(dlq_name, QueueType.DLQ)
        self._dlq_mappings[source_queue] = dlq_name
        return dlq_url

    async def move_to_dlq(
        self,
        source_queue: str,
        receipt_handle: str,
        message_body: str,
        error_info: str = "",
    ) -> bool:
        """Move a failed message to DLQ."""
        dlq_name = self._dlq_mappings.get(source_queue)
        if not dlq_name:
            return False
        enriched_body = json.dumps({
            "original_body": message_body,
            "error": error_info,
            "moved_at": datetime.now(timezone.utc).isoformat(),
        })
        result = await self.client.send_message(dlq_name, enriched_body)
        if result.message_id:
            await self.client.delete_message(source_queue, receipt_handle)
            return True
        return False


async def demo():
    """Demo SQS operations."""
    client = SQSClient(SQSConfig(region="us-east-1"))

    url = await client.create_queue("test-queue", QueueType.STANDARD)
    print(f"Created queue: {url}")

    result = await client.send_message("test-queue", "Hello, SQS!")
    print(f"Sent message: {result.message_id}")

    receive_result = await client.receive_messages("test-queue", max_number_of_messages=5)
    print(f"Received {len(receive_result.messages)} messages")


if __name__ == "__main__":
    asyncio.run(demo())
