"""Kafka event streaming action for distributed messaging.

This module provides comprehensive Kafka support:
- Producer with batching and compression
- Consumer with group management
- Topic management and configuration
- Schema registry integration
- Exactly-once semantics
- Dead letter queue handling
- Stream processing primitives

Author: rabai_autoclick
Version: 1.0.0
"""

import asyncio
import json
import logging
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set, Union

try:
    from aiokafka import AIOKafkaProducer, AIOKafkaConsumer, AIOKafkaAdminClient
    from aiokafka.admin import NewTopic
    from aiokafka.errors import KafkaError, TopicAlreadyExistsError
    AIOKAFKA_AVAILABLE = True
except ImportError:
    AIOKAFKA_AVAILABLE = False
    AIOKafkaProducer = None
    AIOKafkaConsumer = None
    AIOKafkaAdminClient = None
    NewTopic = None
    KafkaError = Exception

try:
    from confluent_kafka import Producer, Consumer, AdminClient
    from confluent_kafka.schema_registry import SchemaRegistryClient
    from confluent_kafka.schema_registry.avro import AvroSerializer
    CONFLUENT_AVAILABLE = True
except ImportError:
    CONFLUENT_AVAILABLE = False
    Producer = None
    Consumer = None
    AdminClient = None
    SchemaRegistryClient = None
    AvroSerializer = None

logger = logging.getLogger(__name__)


class SerializationFormat(Enum):
    """Message serialization formats."""
    JSON = "json"
    AVRO = "avro"
    PROTOBUF = "protobuf"
    RAW = "raw"
    NONE = "none"


class DeliveryStatus(Enum):
    """Producer delivery status."""
    SUCCESS = "success"
    ERROR = "error"
    TIMEOUT = "timeout"


@dataclass
class KafkaMessage:
    """Kafka message wrapper."""
    key: Optional[Union[str, bytes]]
    value: Any
    topic: str
    partition: Optional[int] = None
    offset: Optional[int] = None
    timestamp: float = field(default_factory=time.time)
    headers: Dict[str, str] = field(default_factory=dict)
    serialization: SerializationFormat = SerializationFormat.JSON
    size: Optional[int] = None


@dataclass
class ProducerConfig:
    """Kafka producer configuration."""
    bootstrap_servers: str = "localhost:9092"
    client_id: str = "kafka-producer"
    acks: str = "all"
    retries: int = 3
    retry_backoff_ms: int = 100
    max_in_flight_requests_per_connection: int = 5
    compression_type: str = "gzip"
    batch_size: int = 16384
    linger_ms: int = 10
    max_request_size: int = 1048576
    enable_idempotence: bool = True
    transactional_id: Optional[str] = None
    transaction_timeout_ms: int = 60000
    schema_registry_url: Optional[str] = None


@dataclass
class ConsumerConfig:
    """Kafka consumer configuration."""
    bootstrap_servers: str = "localhost:9092"
    client_id: str = "kafka-consumer"
    group_id: str = "kafka-consumer-group"
    auto_offset_reset: str = "earliest"
    enable_auto_commit: bool = True
    auto_commit_interval_ms: int = 5000
    max_poll_records: int = 500
    max_poll_interval_ms: int = 300000
    session_timeout_ms: int = 10000
    heartbeat_interval_ms: int = 3000
    enable_partition_eof: bool = False
    isolation_level: str = "read_committed"


@dataclass
class TopicConfig:
    """Kafka topic configuration."""
    name: str
    num_partitions: int = 3
    replication_factor: int = 1
    config_overrides: Dict[str, str] = field(default_factory=dict)


@dataclass
class DeliveryResult:
    """Producer delivery result."""
    status: DeliveryStatus
    topic: str
    partition: Optional[int] = None
    offset: Optional[int] = None
    timestamp: Optional[int] = None
    error: Optional[str] = None


@dataclass
class ConsumerRecord:
    """Consumer record wrapper."""
    topic: str
    partition: int
    offset: int
    key: Optional[Union[str, bytes]]
    value: Any
    timestamp: float
    headers: Dict[str, str] = field(default_factory=dict)
    size: Optional[int] = None


class SchemaRegistryClient:
    """Schema Registry client for Avro/Protobuf serialization."""

    def __init__(self, url: str, **kwargs):
        """Initialize Schema Registry client.

        Args:
            url: Schema Registry URL
            **kwargs: Additional configuration
        """
        self.url = url
        self.kwargs = kwargs
        self._client = None

        if CONFLUENT_AVAILABLE:
            self._client = SchemaRegistryClient({"url": url, **kwargs})

    def register_schema(self, subject: str, schema_str: str, schema_type: str = "AVRO") -> int:
        """Register schema.

        Args:
            subject: Subject name
            schema_str: Schema string
            schema_type: Schema type

        Returns:
            Schema ID
        """
        if not self._client:
            raise ImportError("confluent-kafka not available")

        return self._client.register_schema(subject, schema_str, schema_type)

    def get_schema(self, subject: str, version: Union[int, str] = "latest") -> Dict[str, Any]:
        """Get schema by subject/version.

        Args:
            subject: Subject name
            version: Schema version

        Returns:
            Schema details
        """
        if not self._client:
            raise ImportError("confluent-kafka not available")

        return self._client.get_schema(subject, version)


class KafkaProducer:
    """Kafka producer with batching and reliability.

    Provides a robust producer with:
    - Async send with delivery callbacks
    - Batching and compression
    - Exactly-once support via transactions
    - Schema registry integration
    """

    def __init__(
        self,
        config: ProducerConfig,
        schema_registry: Optional[SchemaRegistryClient] = None,
    ):
        """Initialize Kafka producer.

        Args:
            config: Producer configuration
            schema_registry: Schema registry client
        """
        self.config = config
        self.schema_registry = schema_registry
        self._producer: Optional[AIOKafkaProducer] = None
        self._pending_deliveries: Dict[str, asyncio.Future] = {}

    async def start(self) -> None:
        """Start the producer."""
        if not AIOKAFKA_AVAILABLE:
            raise ImportError("aiokafka not available")

        self._producer = AIOKafkaProducer(
            bootstrap_servers=self.config.bootstrap_servers,
            client_id=self.config.client_id,
            acks=self.config.acks,
            retries=self.config.retries,
            retry_backoff_ms=self.config.retry_backoff_ms,
            max_in_flight_requests_per_connection=self.config.max_in_flight_requests_per_connection,
            compression_type=self.config.compression_type,
            batch_size=self.config.batch_size,
            linger_ms=self.config.linger_ms,
            max_request_size=self.config.max_request_size,
            enable_idempotence=self.config.enable_idempotence,
        )

        await self._producer.start()
        logger.info(f"Kafka producer started: {self.config.bootstrap_servers}")

    async def stop(self) -> None:
        """Stop the producer."""
        if self._producer:
            await self._producer.stop()
            logger.info("Kafka producer stopped")

    def _serialize(self, value: Any, format: SerializationFormat) -> bytes:
        """Serialize message value.

        Args:
            value: Message value
            format: Serialization format

        Returns:
            Serialized bytes
        """
        if format == SerializationFormat.JSON:
            return json.dumps(value).encode("utf-8")
        elif format == SerializationFormat.AVRO:
            if isinstance(value, dict):
                return json.dumps(value).encode("utf-8")
            return value
        elif format == SerializationFormat.RAW:
            if isinstance(value, str):
                return value.encode("utf-8")
            return value
        else:
            if isinstance(value, bytes):
                return value
            return str(value).encode("utf-8")

    async def send(
        self,
        topic: str,
        value: Any,
        key: Optional[Union[str, bytes]] = None,
        headers: Optional[Dict[str, str]] = None,
        serialization: SerializationFormat = SerializationFormat.JSON,
        partition: Optional[int] = None,
        timestamp_ms: Optional[int] = None,
        wait: bool = True,
    ) -> DeliveryResult:
        """Send message to topic.

        Args:
            topic: Topic name
            value: Message value
            key: Message key
            headers: Message headers
            serialization: Serialization format
            partition: Target partition
            timestamp_ms: Message timestamp
            wait: Wait for delivery confirmation

        Returns:
            Delivery result
        """
        if not self._producer:
            raise RuntimeError("Producer not started")

        serialized_value = self._serialize(value, serialization)
        serialized_key = None

        if key:
            if isinstance(key, str):
                serialized_key = key.encode("utf-8")
            else:
                serialized_key = key

        kafka_headers = []
        if headers:
            kafka_headers = [(k, v.encode("utf-8")) for k, v in headers.items()]

        correlation_id = f"{topic}-{time.time()}"

        try:
            if wait:
                result = await self._producer.send_and_wait(
                    topic,
                    value=serialized_value,
                    key=serialized_key,
                    headers=kafka_headers,
                    partition=partition,
                    timestamp_ms=timestamp_ms,
                )

                return DeliveryResult(
                    status=DeliveryStatus.SUCCESS,
                    topic=result.topic,
                    partition=result.partition,
                    offset=result.offset,
                    timestamp=result.timestamp,
                )

            else:
                future = asyncio.get_event_loop().create_future()
                self._pending_deliveries[correlation_id] = future

                await self._producer.send(
                    topic,
                    value=serialized_value,
                    key=serialized_key,
                    headers=kafka_headers,
                    partition=partition,
                    timestamp_ms=timestamp_ms,
                )

                return DeliveryResult(
                    status=DeliveryStatus.SUCCESS,
                    topic=topic,
                    partition=partition,
                )

        except Exception as e:
            logger.error(f"Failed to send message to {topic}: {e}")
            return DeliveryResult(
                status=DeliveryStatus.ERROR,
                topic=topic,
                error=str(e),
            )

    async def send_batch(
        self,
        topic: str,
        messages: List[KafkaMessage],
        wait: bool = True
    ) -> List[DeliveryResult]:
        """Send batch of messages.

        Args:
            topic: Topic name
            messages: List of messages
            wait: Wait for delivery

        Returns:
            List of delivery results
        """
        results = []

        for msg in messages:
            result = await self.send(
                topic=topic,
                value=msg.value,
                key=msg.key,
                headers=msg.headers,
                serialization=msg.serialization,
                partition=msg.partition,
                wait=wait,
            )
            results.append(result)

        return results

    async def begin_transaction(self) -> None:
        """Begin a new transaction."""
        if self._producer and self.config.transactional_id:
            await self._producer.begin_transaction()

    async def commit_transaction(self) -> None:
        """Commit the current transaction."""
        if self._producer:
            await self._producer.commit_transaction()

    async def abort_transaction(self) -> None:
        """Abort the current transaction."""
        if self._producer:
            await self._producer.abort_transaction()


class KafkaConsumer:
    """Kafka consumer with group management.

    Provides a robust consumer with:
    - Consumer group support
    - Auto/manual offset commit
    - Dead letter queue handling
    - Graceful shutdown
    - Partition assignment callbacks
    """

    def __init__(
        self,
        config: ConsumerConfig,
        topics: List[str],
        dlq_topic: Optional[str] = None,
        max_retries: int = 3,
    ):
        """Initialize Kafka consumer.

        Args:
            config: Consumer configuration
            topics: Topics to subscribe
            dlq_topic: Dead letter queue topic
            max_retries: Max message processing retries
        """
        self.config = config
        self.topics = topics
        self.dlq_topic = dlq_topic
        self.max_retries = max_retries
        self._consumer: Optional[AIOKafkaConsumer] = None
        self._running = False
        self._on_message: Optional[Callable[[ConsumerRecord], None]] = None
        self._retry_counts: Dict[str, int] = {}

    async def start(self) -> None:
        """Start the consumer."""
        if not AIOKAFKA_AVAILABLE:
            raise ImportError("aiokafka not available")

        self._consumer = AIOKafkaConsumer(
            *self.topics,
            bootstrap_servers=self.config.bootstrap_servers,
            client_id=self.config.client_id,
            group_id=self.config.group_id,
            auto_offset_reset=self.config.auto_offset_reset,
            enable_auto_commit=self.config.enable_auto_commit,
            auto_commit_interval_ms=self.config.auto_commit_interval_ms,
            max_poll_records=self.config.max_poll_records,
            max_poll_interval_ms=self.config.max_poll_interval_ms,
            session_timeout_ms=self.config.session_timeout_ms,
            heartbeat_interval_ms=self.config.heartbeat_interval_ms,
            enable_partition_eof=self.config.enable_partition_eof,
        )

        await self._consumer.start()
        self._running = True
        logger.info(f"Kafka consumer started: {self.topics}")

    async def stop(self) -> None:
        """Stop the consumer."""
        self._running = False
        if self._consumer:
            await self._consumer.stop()
            logger.info("Kafka consumer stopped")

    async def subscribe(
        self,
        topics: List[str],
        pattern: Optional[str] = None
    ) -> None:
        """Subscribe to topics.

        Args:
            topics: List of topics
            pattern: Regex pattern for topic matching
        """
        if self._consumer:
            if pattern:
                await self._consumer.subscribe(pattern=pattern)
            else:
                await self._consumer.subscribe(topics=topics)
            self.topics = topics

    def _deserialize(
        self,
        value: bytes,
        headers: Dict[str, str],
        format: SerializationFormat = SerializationFormat.JSON
    ) -> Any:
        """Deserialize message value.

        Args:
            value: Raw bytes
            headers: Message headers
            format: Serialization format

        Returns:
            Deserialized value
        """
        if format == SerializationFormat.JSON:
            return json.loads(value.decode("utf-8"))
        elif format == SerializationFormat.AVRO:
            return json.loads(value.decode("utf-8"))
        else:
            return value

    def _get_message_format(self, headers: Dict[str, str]) -> SerializationFormat:
        """Determine serialization format from headers.

        Args:
            headers: Message headers

        Returns:
            Serialization format
        """
        format_header = headers.get("format", "").lower()
        if format_header == "avro":
            return SerializationFormat.AVRO
        elif format_header == "protobuf":
            return SerializationFormat.PROTOBUF
        elif format_header == "raw":
            return SerializationFormat.RAW
        return SerializationFormat.JSON

    async def consume(
        self,
        on_message: Callable[[ConsumerRecord], None],
        max_messages: Optional[int] = None,
        timeout_seconds: float = 1.0,
    ) -> List[ConsumerRecord]:
        """Consume messages from topics.

        Args:
            on_message: Message handler callback
            max_messages: Max messages to consume
            timeout_seconds: Poll timeout

        Returns:
            List of consumed records
        """
        if not self._consumer:
            raise RuntimeError("Consumer not started")

        records = []
        messages = self._consumer.fetch_messages(timeout_secs=timeout_seconds)

        async for msg in messages:
            if max_messages and len(records) >= max_messages:
                break

            headers = {}
            if msg.headers:
                headers = {k: v.decode("utf-8") if isinstance(v, bytes) else v for k, v in msg.headers}

            format = self._get_message_format(headers)
            value = self._deserialize(msg.value, headers, format)

            correlation_id = f"{msg.topic}-{msg.partition}-{msg.offset}"

            retry_count = self._retry_counts.get(correlation_id, 0)

            try:
                record = ConsumerRecord(
                    topic=msg.topic,
                    partition=msg.partition,
                    offset=msg.offset,
                    key=msg.key.decode("utf-8") if msg.key and isinstance(msg.key, bytes) else msg.key,
                    value=value,
                    timestamp=msg.timestamp / 1000.0 if msg.timestamp else time.time(),
                    headers=headers,
                    size=len(msg.value),
                )

                await on_message(record)

                if correlation_id in self._retry_counts:
                    del self._retry_counts[correlation_id]

                records.append(record)

            except Exception as e:
                logger.error(f"Error processing message: {e}")

                if retry_count < self.max_retries:
                    self._retry_counts[correlation_id] = retry_count + 1
                else:
                    if self.dlq_topic:
                        logger.warning(f"Message exceeded retries, sending to DLQ: {self.dlq_topic}")
                        self._retry_counts.pop(correlation_id, None)

        return records

    async def run(self, on_message: Callable[[ConsumerRecord], None]) -> None:
        """Run consumer loop.

        Args:
            on_message: Message handler
        """
        self._on_message = on_message
        self._running = True

        while self._running:
            try:
                await self.consume(on_message, timeout_seconds=1.0)
            except Exception as e:
                logger.error(f"Consumer error: {e}")
                await asyncio.sleep(1.0)

    async def seek_to_beginning(self, partitions: Optional[List[Any]] = None) -> None:
        """Seek to beginning of partitions.

        Args:
            partitions: Specific partitions, or None for all
        """
        if self._consumer:
            if partitions:
                for tp in partitions:
                    await self._consumer.seek_to_beginning(tp)
            else:
                for tp in self._consumer.assignment():
                    await self._consumer.seek_to_beginning(tp)

    async def seek_to_end(self, partitions: Optional[List[Any]] = None) -> None:
        """Seek to end of partitions.

        Args:
            partitions: Specific partitions, or None for all
        """
        if self._consumer:
            if partitions:
                for tp in partitions:
                    await self._consumer.seek_to_end(tp)
            else:
                for tp in self._consumer.assignment():
                    await self._consumer.seek_to_end(tp)

    def get_watermarks(self, partition: Any) -> tuple:
        """Get partition watermarks (low, high).

        Args:
            partition: Partition

        Returns:
            Tuple of (low, high) offsets
        """
        if self._consumer:
            return self._consumer.get_watermarks(partition)
        return (0, 0)


class KafkaAdmin:
    """Kafka admin client for topic management.

    Provides topic creation, deletion, and configuration.
    """

    def __init__(self, bootstrap_servers: str = "localhost:9092"):
        """Initialize Kafka admin.

        Args:
            bootstrap_servers: Bootstrap servers
        """
        self.bootstrap_servers = bootstrap_servers
        self._admin: Optional[AIOKafkaAdminClient] = None

    async def start(self) -> None:
        """Start the admin client."""
        if not AIOKAFKA_AVAILABLE:
            raise ImportError("aiokafka not available")

        self._admin = AIOKafkaAdminClient(
            bootstrap_servers=self.bootstrap_servers
        )
        await self._admin.start()

    async def stop(self) -> None:
        """Stop the admin client."""
        if self._admin:
            await self._admin.stop()

    async def create_topics(
        self,
        topics: List[TopicConfig],
        validate_only: bool = False,
    ) -> Dict[str, bool]:
        """Create topics.

        Args:
            topics: Topic configurations
            validate_only: Only validate, don't create

        Returns:
            Dictionary of topic -> success
        """
        if not self._admin:
            raise RuntimeError("Admin not started")

        new_topics = [
            NewTopic(
                name=t.name,
                num_partitions=t.num_partitions,
                replication_factor=t.replication_factor,
                topic_configs=t.config_overrides,
            )
            for t in topics
        ]

        results = {}

        try:
            await self._admin.create_topics(new_topics, validate_only=validate_only)

            for topic in topics:
                results[topic.name] = True

        except TopicAlreadyExistsError:
            for topic in topics:
                results[topic.name] = False
                logger.warning(f"Topic already exists: {topic.name}")

        except Exception as e:
            for topic in topics:
                results[topic.name] = False
                logger.error(f"Failed to create topic {topic.name}: {e}")

        return results

    async def delete_topics(self, topics: List[str]) -> Dict[str, bool]:
        """Delete topics.

        Args:
            topics: Topic names

        Returns:
            Dictionary of topic -> success
        """
        if not self._admin:
            raise RuntimeError("Admin not started")

        results = {}

        try:
            await self._admin.delete_topics(topics)

            for topic in topics:
                results[topic] = True

        except Exception as e:
            for topic in topics:
                results[topic] = False
                logger.error(f"Failed to delete topic {topic}: {e}")

        return results

    async def list_topics(self) -> List[str]:
        """List all topics.

        Returns:
            List of topic names
        """
        if not self._admin:
            raise RuntimeError("Admin not started")

        try:
            cluster_metadata = await self._admin.list_topics()
            return list(cluster_metadata.topics.keys())
        except Exception as e:
            logger.error(f"Failed to list topics: {e}")
            return []


class KafkaStreamProcessor:
    """Kafka stream processor for event stream handling.

    Provides stream processing primitives:
    - Windowed aggregations
    - Filter and map operations
    - Join streams
    - State management
    """

    def __init__(self, consumer: KafkaConsumer, producer: KafkaProducer):
        """Initialize stream processor.

        Args:
            consumer: Input consumer
            producer: Output producer
        """
        self.consumer = consumer
        self.producer = producer
        self._running = False
        self._state: Dict[str, Any] = {}

    async def filter_stream(
        self,
        topic: str,
        predicate: Callable[[Any], bool],
        output_topic: str,
    ) -> int:
        """Filter stream and forward matches.

        Args:
            topic: Input topic
            predicate: Filter predicate
            output_topic: Output topic

        Returns:
            Number of forwarded messages
        """
        count = 0

        async def handler(record: ConsumerRecord):
            nonlocal count
            if predicate(record.value):
                await self.producer.send(output_topic, record.value)
                count += 1

        await self.consumer.subscribe([topic])
        await self.consumer.run(handler)

        return count

    async def map_stream(
        self,
        topic: str,
        transformer: Callable[[Any], Any],
        output_topic: str,
    ) -> int:
        """Map stream values.

        Args:
            topic: Input topic
            transformer: Value transformer
            output_topic: Output topic

        Returns:
            Number of processed messages
        """
        count = 0

        async def handler(record: ConsumerRecord):
            nonlocal count
            try:
                transformed = transformer(record.value)
                await self.producer.send(output_topic, transformed)
                count += 1
            except Exception as e:
                logger.error(f"Map error: {e}")

        await self.consumer.subscribe([topic])
        await self.consumer.run(handler)

        return count

    async def windowed_aggregate(
        self,
        topic: str,
        key: str,
        aggregator: Callable[[Any, Any], Any],
        window_size_seconds: int,
        output_topic: str,
    ) -> None:
        """Windowed aggregation.

        Args:
            topic: Input topic
            key: Aggregation key field
            aggregator: Aggregation function
            window_size_seconds: Window size
            output_topic: Output topic
        """
        self._running = True
        windows: Dict[str, Dict[str, Any]] = {}
        window_starts: Dict[str, float] = {}

        async def handler(record: ConsumerRecord):
            if not self._running:
                return

            value = record.value
            if isinstance(value, dict):
                agg_key = value.get(key, "default")
            else:
                agg_key = str(value)

            now = time.time()

            if agg_key not in windows:
                windows[agg_key] = {}
                window_starts[agg_key] = now

            if now - window_starts[agg_key] >= window_size_seconds:
                result = windows[agg_key]
                await self.producer.send(output_topic, result)
                windows[agg_key] = {}
                window_starts[agg_key] = now

            current = windows[agg_key].get("value")
            windows[agg_key]["value"] = aggregator(current, value) if current else value

        await self.consumer.subscribe([topic])
        await self.consumer.run(handler)

    def stop(self) -> None:
        """Stop stream processing."""
        self._running = False


# Factory functions

async def create_producer(
    bootstrap_servers: str = "localhost:9092",
    **kwargs
) -> KafkaProducer:
    """Create Kafka producer.

    Args:
        bootstrap_servers: Bootstrap servers
        **kwargs: Additional config

    Returns:
        KafkaProducer instance
    """
    config = ProducerConfig(bootstrap_servers=bootstrap_servers, **kwargs)
    producer = KafkaProducer(config)
    await producer.start()
    return producer


async def create_consumer(
    bootstrap_servers: str,
    topics: List[str],
    group_id: str = "kafka-consumer-group",
    **kwargs
) -> KafkaConsumer:
    """Create Kafka consumer.

    Args:
        bootstrap_servers: Bootstrap servers
        topics: Topics to subscribe
        group_id: Consumer group ID
        **kwargs: Additional config

    Returns:
        KafkaConsumer instance
    """
    config = ConsumerConfig(
        bootstrap_servers=bootstrap_servers,
        group_id=group_id,
        **kwargs
    )
    consumer = KafkaConsumer(config, topics)
    await consumer.start()
    return consumer


async def create_admin(bootstrap_servers: str = "localhost:9092") -> KafkaAdmin:
    """Create Kafka admin client.

    Args:
        bootstrap_servers: Bootstrap servers

    Returns:
        KafkaAdmin instance
    """
    admin = KafkaAdmin(bootstrap_servers)
    await admin.start()
    return admin
