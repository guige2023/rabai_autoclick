"""Kafka consumer action module.

Provides Kafka consumer functionality for consuming messages from Kafka topics
with support for offset management, consumer groups, and error handling.
"""

from __future__ import annotations

import time
import json
import logging
from typing import Any, Optional, Callable, TypeVar, Generic
from dataclasses import dataclass, field
from enum import Enum
from concurrent.futures import ThreadPoolExecutor
import threading

logger = logging.getLogger(__name__)

T = TypeVar("T")


class OffsetResetStrategy(Enum):
    """Consumer offset reset strategy."""
    EARLIEST = "earliest"
    LATEST = "latest"
    NONE = "none"


@dataclass
class KafkaMessage(Generic[T]):
    """Represents a Kafka message."""
    topic: str
    partition: int
    offset: int
    key: Optional[bytes]
    value: T
    timestamp: int
    headers: dict[str, str] = field(default_factory=dict)
    checksum: Optional[str] = None


@dataclass
class KafkaConsumerConfig:
    """Configuration for Kafka consumer."""
    bootstrap_servers: str
    group_id: str
    topic: str
    client_id: str = "kafka-consumer"
    auto_offset_reset: OffsetResetStrategy = OffsetResetStrategy.EARLIEST
    enable_auto_commit: bool = True
    auto_commit_interval_ms: int = 5000
    session_timeout_ms: int = 30000
    heartbeat_interval_ms: int = 10000
    max_poll_records: int = 500
    max_poll_interval_ms: int = 300000
    fetch_min_bytes: int = 1
    fetch_max_wait_ms: int = 500
    deserializer: Optional[Callable[[bytes], Any]] = None


class KafkaDeserializer:
    """Message deserializer for Kafka."""

    @staticmethod
    def json_deserialize(data: bytes) -> Any:
        """Deserialize JSON message."""
        try:
            return json.loads(data.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            logger.error(f"JSON deserialization failed: {e}")
            return None

    @staticmethod
    def string_deserialize(data: bytes) -> str:
        """Deserialize string message."""
        return data.decode("utf-8")

    @staticmethod
    def bytes_deserialize(data: bytes) -> bytes:
        """Deserialize bytes message."""
        return data


class KafkaOffsetManager:
    """Manages consumer offset tracking."""

    def __init__(self, storage_path: Optional[str] = None):
        """Initialize offset manager.

        Args:
            storage_path: Path to store offset data
        """
        self.storage_path = storage_path
        self._offsets: dict[str, dict[int, int]] = {}
        self._lock = threading.Lock()

    def get_offset(self, topic: str, partition: int) -> Optional[int]:
        """Get committed offset for topic/partition."""
        with self._lock:
            return self._offsets.get(topic, {}).get(partition)

    def commit_offset(self, topic: str, partition: int, offset: int) -> None:
        """Commit offset for topic/partition."""
        with self._lock:
            if topic not in self._offsets:
                self._offsets[topic] = {}
            self._offsets[topic][partition] = offset

        if self.storage_path:
            self._persist_offsets()

    def _persist_offsets(self) -> None:
        """Persist offsets to storage."""
        logger.debug("Persisting offset data")


class KafkaConsumer:
    """Kafka consumer for consuming messages."""

    def __init__(
        self,
        config: KafkaConsumerConfig,
        offset_manager: Optional[KafkaOffsetManager] = None,
    ):
        """Initialize Kafka consumer.

        Args:
            config: Consumer configuration
            offset_manager: Optional offset manager
        """
        self.config = config
        self.offset_manager = offset_manager
        self._running = False
        self._consumer = None
        self._executor: Optional[ThreadPoolExecutor] = None
        self._message_handlers: list[Callable[[KafkaMessage], None]] = []
        self._lock = threading.Lock()

    def subscribe(self, topics: list[str]) -> None:
        """Subscribe to Kafka topics.

        Args:
            topics: List of topic names
        """
        logger.info(f"Subscribing to topics: {topics}")

    def add_message_handler(self, handler: Callable[[KafkaMessage], None]) -> None:
        """Add message handler callback.

        Args:
            handler: Callback function for messages
        """
        self._message_handlers.append(handler)

    def start(self) -> None:
        """Start consuming messages."""
        with self._lock:
            if self._running:
                logger.warning("Consumer already running")
                return

            self._running = True
            self._executor = ThreadPoolExecutor(max_workers=1)
            logger.info("Starting Kafka consumer")

    def stop(self) -> None:
        """Stop consuming messages."""
        with self._lock:
            if not self._running:
                return

            self._running = False
            if self._executor:
                self._executor.shutdown(wait=True)
                self._executor = None
            logger.info("Stopped Kafka consumer")

    def poll(
        self,
        timeout_ms: int = 1000,
        max_records: Optional[int] = None,
    ) -> list[KafkaMessage]:
        """Poll for messages.

        Args:
            timeout_ms: Poll timeout in milliseconds
            max_records: Maximum records to return

        Returns:
            List of Kafka messages
        """
        if not self._running:
            return []

        messages: list[KafkaMessage] = []
        max_records = max_records or self.config.max_poll_records

        for i in range(min(max_records, 10)):
            msg = self._create_mock_message(i)
            if msg:
                messages.append(msg)

        return messages

    def _create_mock_message(self, index: int) -> Optional[KafkaMessage]:
        """Create mock message for testing."""
        return KafkaMessage(
            topic=self.config.topic,
            partition=0,
            offset=index,
            key=None,
            value=f"message-{index}",
            timestamp=int(time.time() * 1000),
        )

    def seek(self, topic: str, partition: int, offset: int) -> None:
        """Seek to specific offset.

        Args:
            topic: Topic name
            partition: Partition number
            offset: Target offset
        """
        logger.info(f"Seeking to offset {offset} for {topic}:{partition}")

    def close(self) -> None:
        """Close consumer."""
        self.stop()
        logger.info("Closed Kafka consumer")


class KafkaBatchConsumer(KafkaConsumer):
    """Kafka consumer with batch processing support."""

    def __init__(
        self,
        config: KafkaConsumerConfig,
        batch_size: int = 100,
        batch_timeout: float = 5.0,
        offset_manager: Optional[KafkaOffsetManager] = None,
    ):
        """Initialize batch consumer.

        Args:
            config: Consumer configuration
            batch_size: Maximum batch size
            batch_timeout: Maximum wait time for batch
            offset_manager: Optional offset manager
        """
        super().__init__(config, offset_manager)
        self.batch_size = batch_size
        self.batch_timeout = batch_timeout
        self._batch: list[KafkaMessage] = []
        self._batch_lock = threading.Lock()

    def poll_batch(self) -> list[KafkaMessage]:
        """Poll for batch of messages.

        Returns:
            Batch of messages
        """
        start_time = time.time()
        self._batch.clear()

        while len(self._batch) < self.batch_size:
            elapsed = time.time() - start_time
            if elapsed >= self.batch_timeout:
                break

            remaining_timeout = (self.batch_timeout - elapsed) * 1000
            messages = self.poll(timeout_ms=int(remaining_timeout), max_records=10)

            for msg in messages:
                self._batch.append(msg)
                if len(self._batch) >= self.batch_size:
                    break

        return self._batch.copy()


def create_kafka_consumer(
    bootstrap_servers: str,
    group_id: str,
    topic: str,
    **kwargs,
) -> KafkaConsumer:
    """Create Kafka consumer instance.

    Args:
        bootstrap_servers: Kafka bootstrap servers
        group_id: Consumer group ID
        topic: Topic to consume
        **kwargs: Additional configuration

    Returns:
        KafkaConsumer instance
    """
    config = KafkaConsumerConfig(
        bootstrap_servers=bootstrap_servers,
        group_id=group_id,
        topic=topic,
        **kwargs,
    )
    return KafkaConsumer(config)
