"""
Kafka Action Module

Provides Kafka producer and consumer functionality for message queue
operations in UI automation workflows. Supports async production and
consumption with configurable partitioning.

Author: AI Agent
Version: 1.0.0
"""

from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum, auto
from typing import Any, Callable, Optional

logger = logging.getLogger(__name__)


class KafkaCompressionType(Enum):
    """Kafka compression types."""
    NONE = "none"
    GZIP = "gzip"
    SNAPPY = "snappy"
    LZ4 = "lz4"
    ZSTD = "zstd"


class Kafkaacks(Enum):
    """Kafka acknowledgment levels."""
    LEADER = 1
    ALL = -1
    NONE = 0


@dataclass
class KafkaProducerConfig:
    """Kafka producer configuration."""
    bootstrap_servers: str
    client_id: str = "rabai_producer"
    acks: Kafkaacks = Kafkaacks.LEADER
    compression_type: KafkaCompressionType = KafkaCompressionType.NONE
    batch_size: int = 16384
    linger_ms: int = 0
    max_in_flight_requests_per_connection: int = 5
    retry_backoff_ms: int = 100
    request_timeout_ms: int = 30000
    max_batch_size: int = 128 * 1024


@dataclass
class KafkaConsumerConfig:
    """Kafka consumer configuration."""
    bootstrap_servers: str
    group_id: str
    client_id: str = "rabai_consumer"
    auto_offset_reset: str = "earliest"
    enable_auto_commit: bool = True
    auto_commit_interval_ms: int = 5000
    max_poll_records: int = 500
    max_poll_interval_ms: int = 300000
    session_timeout_ms: int = 10000
    heartbeat_interval_ms: int = 3000


@dataclass
class KafkaMessage:
    """Represents a Kafka message."""
    topic: str
    partition: int
    offset: int
    key: Optional[bytes] = None
    value: bytes = field(default_factory=bytes)
    timestamp: int = field(default_factory=lambda: int(datetime.utcnow().timestamp() * 1000))
    headers: dict[str, str] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class ProducerRecord:
    """Record to be produced to Kafka."""
    topic: str
    value: bytes | str
    key: Optional[bytes | str] = None
    partition: Optional[int] = None
    headers: Optional[dict[str, str]] = None

    def __post_init__(self) -> None:
        if isinstance(self.value, str):
            self.value = self.value.encode("utf-8")
        if isinstance(self.key, str):
            self.key = self.key.encode("utf-8")
        if self.headers is None:
            self.headers = {}


class KafkaProducer:
    """
    Async Kafka producer for message sending.

    Example:
        >>> async def main():
        ...     config = KafkaProducerConfig(bootstrap_servers="localhost:9092")
        ...     producer = KafkaProducer(config)
        ...     await producer.start()
        ...     await producer.send("my_topic", value="hello", key="key1")
        ...     await producer.flush()
        ...     await producer.stop()
    """

    def __init__(self, config: KafkaProducerConfig) -> None:
        self.config = config
        self._producer: Optional[Any] = None
        self._running = False
        self._pending: list[asyncio.Future] = []

    async def start(self) -> None:
        """Start the Kafka producer."""
        try:
            from aiokafka import AIOKafkaProducer

            self._producer = AIOKafkaProducer(
                bootstrap_servers=self.config.bootstrap_servers,
                client_id=self.config.client_id,
                acks=self.config.acks.value,
                compression_type=self.config.compression_type.value,
                batch_size=self.config.batch_size,
                linger_ms=self.config.linger_ms,
                max_in_flight_requests_per_connection=(
                    self.config.max_in_flight_requests_per_connection
                ),
                retry_backoff_ms=self.config.retry_backoff_ms,
                request_timeout_ms=self.config.request_timeout_ms,
                max_batch_size=self.config.max_batch_size,
            )
            await self._producer.start()
            self._running = True
            logger.info(f"Kafka producer started: {self.config.bootstrap_servers}")
        except ImportError:
            logger.warning("aiokafka not installed, using mock producer")
            self._producer = MockKafkaProducer(self.config)
            self._running = True

    async def stop(self) -> None:
        """Stop the Kafka producer."""
        self._running = False
        if self._producer:
            await self._producer.stop()
            self._producer = None
        logger.info("Kafka producer stopped")

    async def send(
        self,
        topic: str,
        value: bytes | str,
        key: Optional[bytes | str] = None,
        partition: Optional[int] = None,
        headers: Optional[dict[str, str]] = None,
    ) -> Optional[KafkaMessage]:
        """Send message to topic."""
        if not self._running or not self._producer:
            raise RuntimeError("Producer not started")

        record = ProducerRecord(
            topic=topic,
            value=value,
            key=key,
            partition=partition,
            headers=headers,
        )

        future = await self._producer.send_and_wait(
            record=record.topic,
            value=record.value,
            key=record.key,
            partition=record.partition,
            headers=list(record.headers.items()) if record.headers else None,
        )

        message = KafkaMessage(
            topic=topic,
            partition=future.partition,
            offset=future.offset,
            key=record.key,
            value=record.value,
        )
        logger.debug(f"Sent message to {topic}:{future.partition}@{future.offset}")
        return message

    async def send_batch(self, records: list[ProducerRecord]) -> list[KafkaMessage]:
        """Send batch of messages."""
        messages = []
        for record in records:
            msg = await self.send(
                topic=record.topic,
                value=record.value,
                key=record.key,
                partition=record.partition,
                headers=record.headers,
            )
            if msg:
                messages.append(msg)
        return messages

    async def flush(self, timeout: float = 10.0) -> None:
        """Flush pending messages."""
        if self._producer:
            await asyncio.wait_for(self._producer.flush(), timeout=timeout)
        logger.debug("Producer flushed")


class MockKafkaProducer:
    """Mock producer for testing without Kafka."""

    def __init__(self, config: KafkaProducerConfig) -> None:
        self.config = config
        self._messages: list[KafkaMessage] = []
        self._counter = 0

    async def start(self) -> None:
        pass

    async def stop(self) -> None:
        pass

    async def send_and_wait(self, record: str, value: bytes, key: Optional[bytes], partition: Optional[int], headers: Optional[list]) -> Any:
        class FutureResult:
            partition = 0
            offset = self._counter
            topic = record

        self._counter += 1
        return FutureResult()

    async def flush(self) -> None:
        pass


class KafkaConsumer:
    """
    Async Kafka consumer for message consumption.

    Example:
        >>> async def main():
        ...     config = KafkaConsumerConfig(
        ...         bootstrap_servers="localhost:9092",
        ...         group_id="my_group"
        ...     )
        ...     consumer = KafkaConsumer(config)
        ...     await consumer.start()
        ...     async for message in consumer:
        ...         print(message.value)
        ...     await consumer.stop()
    """

    def __init__(self, config: KafkaConsumerConfig) -> None:
        self.config = config
        self._consumer: Optional[Any] = None
        self._running = False
        self._handlers: dict[str, Callable] = {}

    async def start(self, topics: list[str]) -> None:
        """Start consumer and subscribe to topics."""
        try:
            from aiokafka import AIOKafkaConsumer

            self._consumer = AIOKafkaConsumer(
                *topics,
                bootstrap_servers=self.config.bootstrap_servers,
                group_id=self.config.group_id,
                client_id=self.config.client_id,
                auto_offset_reset=self.config.auto_offset_reset,
                enable_auto_commit=self.config.enable_auto_commit,
                auto_commit_interval_ms=self.config.auto_commit_interval_ms,
                max_poll_records=self.config.max_poll_records,
                max_poll_interval_ms=self.config.max_poll_interval_ms,
                session_timeout_ms=self.config.session_timeout_ms,
                heartbeat_interval_ms=self.config.heartbeat_interval_ms,
            )
            await self._consumer.start()
            self._running = True
            logger.info(f"Kafka consumer started: {topics}")
        except ImportError:
            logger.warning("aiokafka not installed, using mock consumer")
            self._consumer = MockKafkaConsumer(self.config, topics)
            self._running = True

    async def stop(self) -> None:
        """Stop the consumer."""
        self._running = False
        if self._consumer:
            await self._consumer.stop()
            self._consumer = None
        logger.info("Kafka consumer stopped")

    def subscribe(self, topic: str, handler: Callable[[KafkaMessage], None]) -> None:
        """Register handler for topic."""
        self._handlers[topic] = handler

    async def consume(self, timeout: float = 1.0) -> list[KafkaMessage]:
        """Consume messages from subscribed topics."""
        if not self._running or not self._consumer:
            return []

        messages: list[KafkaMessage] = []
        try:
            async for msg in self._consumer:
                if not self._running:
                    break

                kafka_message = KafkaMessage(
                    topic=msg.topic,
                    partition=msg.partition,
                    offset=msg.offset,
                    key=msg.key,
                    value=msg.value,
                    timestamp=msg.timestamp,
                )

                if msg.topic in self._handlers:
                    self._handlers[msg.topic](kafka_message)

                messages.append(kafka_message)

        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.error(f"Consumer error: {e}")

        return messages

    async def __aiter__(self) -> AsyncIterator:
        return self

    async def __anext__(self) -> KafkaMessage:
        if not self._running:
            raise StopAsyncIteration
        messages = await self.consume()
        if not messages:
            await asyncio.sleep(0.1)
            return await self.__anext__()
        return messages[0]


class AsyncIterator:
    """Async iterator helper."""

    def __init__(self, consumer: KafkaConsumer) -> None:
        self._consumer = consumer

    def __aiter__(self):
        return self

    async def __anext__(self) -> KafkaMessage:
        return await self._consumer.__anext__()


class MockKafkaConsumer:
    """Mock consumer for testing without Kafka."""

    def __init__(self, config: KafkaConsumerConfig, topics: list[str]) -> None:
        self.config = config
        self.topics = topics
        self._messages: asyncio.Queue[KafkaMessage] = asyncio.Queue()

    async def start(self) -> None:
        pass

    async def stop(self) -> None:
        pass

    async def __aiter__(self):
        return self

    async def __anext__(self) -> KafkaMessage:
        return await self._messages.get()


class KafkaTopicManager:
    """
    Manager for Kafka topic operations.

    Example:
        >>> from kafka.admin import KafkaAdminClient
        >>> manager = KafkaTopicManager("localhost:9092")
        >>> topics = await manager.list_topics()
    """

    def __init__(self, bootstrap_servers: str) -> None:
        self.bootstrap_servers = bootstrap_servers

    async def list_topics(self) -> list[str]:
        """List all available topics."""
        try:
            from kafka import KafkaAdminClient
            admin = KafkaAdminClient(
                bootstrap_servers=self.bootstrap_servers,
                client_id="rabai_topic_manager",
            )
            topics = admin.list_topics()
            admin.close()
            return topics
        except Exception as e:
            logger.warning(f"Cannot list topics: {e}")
            return []

    async def create_topic(
        self,
        name: str,
        num_partitions: int = 3,
        replication_factor: int = 1,
    ) -> bool:
        """Create a new topic."""
        try:
            from kafka import KafkaAdminClient
            from kafka.admin import NewTopic

            admin = KafkaAdminClient(
                bootstrap_servers=self.bootstrap_servers,
                client_id="rabai_topic_manager",
            )
            topic = NewTopic(
                name=name,
                num_partitions=num_partitions,
                replication_factor=replication_factor,
            )
            admin.create_topics([topic])
            admin.close()
            logger.info(f"Created topic: {name}")
            return True
        except Exception as e:
            logger.error(f"Failed to create topic: {e}")
            return False

    async def delete_topic(self, name: str) -> bool:
        """Delete a topic."""
        try:
            from kafka import KafkaAdminClient
            admin = KafkaAdminClient(
                bootstrap_servers=self.bootstrap_servers,
                client_id="rabai_topic_manager",
            )
            admin.delete_topics([name])
            admin.close()
            logger.info(f"Deleted topic: {name}")
            return True
        except Exception as e:
            logger.error(f"Failed to delete topic: {e}")
            return False
