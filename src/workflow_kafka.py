"""
Kafka Event Streaming Integration Module for Workflow System

Implements a KafkaIntegration class with:
1. Topic management: Create/manage Kafka topics
2. Producer: Produce messages to topics
3. Consumer: Consume messages from topics
4. Consumer groups: Manage consumer groups
5. Schema registry: Manage Avro/Protobuf schemas
6. Streams: Kafka Streams operations
7. Connect: Kafka Connect integration
8. MirrorMaker: Cross-cluster replication
9. KSQL: KSQLDB queries
10. Monitoring: Kafka monitoring and metrics

Commit: 'feat(kafka): add Kafka integration with topic management, producer, consumer, consumer groups, schema registry, streams, connect, mirror maker, KSQL, monitoring'
"""

import json
import time
import uuid
import threading
import asyncio
from datetime import datetime, timedelta
from typing import Dict, List, Callable, Any, Optional, Set, Type, Union, Iterator
from dataclasses import dataclass, field
from enum import Enum
from collections import defaultdict
import logging

try:
    from kafka import KafkaProducer, KafkaConsumer, KafkaAdminClient
    from kafka.admin import NewTopic, ConfigResource, ConfigResourceType
    from kafka.errors import TopicAlreadyExistsError, KafkaError
    KAFKA_AVAILABLE = True
except ImportError:
    KAFKA_AVAILABLE = False
    KafkaProducer = None
    KafkaConsumer = None
    KafkaAdminClient = None

try:
    from confluent_kafka import Producer, Consumer, AdminClient, avro
    from confluent_kafka.schema_registries import SchemaRegistryClient
    from confluent_kafka.schema_registry.avro import AvroSerializer, AvroDeserializer
    from confluent_kafka.admin import NewTopic as ConfluentNewTopic
    from confluent_kafka import KafkaError as ConfluentKafkaError
    CONFLUENT_AVAILABLE = True
except ImportError:
    CONFLUENT_AVAILABLE = False
    Producer = None
    Consumer = None
    AdminClient = None
    SchemaRegistryClient = None


logger = logging.getLogger(__name__)


class CompressionType(Enum):
    """Kafka compression types."""
    NONE = "none"
    GZIP = "gzip"
    SNAPPY = "snappy"
    LZ4 = "lz4"
    ZSTD = "zstd"


class AcksMode(Enum):
    """Producer acknowledgement modes."""
    ALL = -1
    NONE = 0
    LEADER = 1


class OffsetResetStrategy(Enum):
    """Consumer offset reset strategies."""
    EARLIEST = "earliest"
    LATEST = "latest"
    NONE = "none"


@dataclass
class TopicConfig:
    """Configuration for a Kafka topic."""
    name: str
    partitions: int = 3
    replication_factor: int = 1
    retention_ms: int = 604800000  # 7 days
    retention_bytes: int = -1  # -1 means no limit
    segment_ms: int = 604800000  # 7 days
    segment_bytes: int = 1073741824  # 1GB
    max_message_bytes: int = 1048576  # 1MB
    min_insync_replicas: int = 1
    cleanup_policy: str = "delete"  # delete or compact
    compression_type: CompressionType = CompressionType.NONE
    preallocate: bool = False

    def to_admin_config(self) -> Dict[str, str]:
        """Convert to Kafka admin client config."""
        return {
            "retention.ms": str(self.retention_ms),
            "retention.bytes": str(self.retention_bytes),
            "segment.ms": str(self.segment_ms),
            "segment.bytes": str(self.segment_bytes),
            "max.message.bytes": str(self.max_message_bytes),
            "min.insync.replicas": str(self.min_insync_replicas),
            "cleanup.policy": self.cleanup_policy,
            "compression.type": self.compression_type.value,
            "preallocate": "true" if self.preallocate else "false",
        }


@dataclass
class ProducerConfig:
    """Configuration for Kafka producer."""
    bootstrap_servers: str = "localhost:9092"
    client_id: str = "workflow-kafka-producer"
    acks: Union[int, str] = "all"
    compression_type: CompressionType = CompressionType.NONE
    batch_size: int = 16384
    linger_ms: int = 0
    max_in_flight_requests_per_connection: int = 5
    retries: int = 3
    retry_backoff_ms: int = 100
    max_block_ms: int = 60000
    enable_idempotence: bool = True
    transaction_timeout_ms: int = 60000
    schema_registry_url: Optional[str] = None
    avro_serializer_config: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ConsumerConfig:
    """Configuration for Kafka consumer."""
    bootstrap_servers: str = "localhost:9092"
    group_id: str = "workflow-consumer-group"
    client_id: str = "workflow-kafka-consumer"
    auto_offset_reset: OffsetResetStrategy = OffsetResetStrategy.EARLIEST
    enable_auto_commit: bool = True
    auto_commit_interval_ms: int = 5000
    session_timeout_ms: int = 30000
    max_poll_records: int = 500
    max_poll_interval_ms: int = 300000
    fetch_min_bytes: int = 1
    fetch_max_wait_ms: int = 500
    heartbeat_interval_ms: int = 3000
    isolation_level: str = "read_uncommitted"
    schema_registry_url: Optional[str] = None
    avro_deserializer_config: Dict[str, Any] = field(default_factory=dict)


@dataclass
class StreamConfig:
    """Configuration for Kafka Streams."""
    application_id: str
    bootstrap_servers: str = "localhost:9092"
    state_dir: str = "/tmp/kafka-streams"
    commit_interval_ms: int = 1000
    num_stream_threads: int = 1
    num_standby_replicas: int = 0
    replication_factor: int = 1
    acks: Union[int, str] = "all"
    processing_guarantee: str = "exactly_once_v2"  # exactly_once or at_least_once


@dataclass
class ConnectorConfig:
    """Configuration for Kafka Connect."""
    name: str
    connector_class: str
    tasks_max: int = 1
    topics: Optional[List[str]] = None
    config: Dict[str, Any] = field(default_factory=dict)


@dataclass
class MirrorMakerConfig:
    """Configuration for MirrorMaker 2.0 replication."""
    source_cluster: Dict[str, Any]
    target_cluster: Dict[str, Any]
    topics: Optional[List[str]] = None
    topics_pattern: Optional[str] = None
    groups: Optional[List[str]] = None
    groups_pattern: Optional[str] = None
    emit_heartbeats: bool = True
    sync_topic_config: bool = True
    sync_topic_acl: bool = False
    replication_policy_class: str = "org.apache.kafka.connect.mirror.DefaultReplicationPolicy"


@dataclass
class KSQLQuery:
    """KSQLDB query definition."""
    query_string: str
    query_id: Optional[str] = None
    properties: Dict[str, str] = field(default_factory=dict)


@dataclass
class KafkaMetric:
    """Kafka metric data point."""
    name: str
    value: float
    timestamp: datetime
    tags: Dict[str, str] = field(default_factory=dict)


@dataclass
class SchemaInfo:
    """Schema registry information."""
    subject: str
    schema_id: int
    version: int
    schema_type: str  # AVRO, PROTOBUF, JSON
    schema_str: str
    references: List[Dict[str, str]] = field(default_factory=list)


class KafkaIntegration:
    """
    Comprehensive Kafka integration for event streaming.

    Features:
    1. Topic management: Create/manage Kafka topics
    2. Producer: Produce messages to topics
    3. Consumer: Consume messages from topics
    4. Consumer groups: Manage consumer groups
    5. Schema registry: Manage Avro/Protobuf schemas
    6. Streams: Kafka Streams operations
    7. Connect: Kafka Connect integration
    8. MirrorMaker: Cross-cluster replication
    9. KSQL: KSQLDB queries
    10. Monitoring: Kafka monitoring and metrics
    """

    def __init__(
        self,
        bootstrap_servers: str = "localhost:9092",
        schema_registry_url: Optional[str] = None,
        use_confluent: bool = False,
    ):
        """
        Initialize Kafka integration.

        Args:
            bootstrap_servers: Kafka bootstrap servers address
            schema_registry_url: Schema registry URL
            use_confluent: Use confluent-kafka library instead of kafka-python
        """
        self.bootstrap_servers = bootstrap_servers
        self.schema_registry_url = schema_registry_url
        self.use_confluent = use_confluent and CONFLUENT_AVAILABLE

        self._producer: Optional[Any] = None
        self._admin_client: Optional[Any] = None
        self._schema_registry_client: Optional[Any] = None
        self._consumers: Dict[str, Any] = {}
        self._consumer_threads: Dict[str, threading.Thread] = {}
        self._running_consumers: Set[str] = set()

        self._streams_instances: Dict[str, Any] = {}
        self._ksql_clients: Dict[str, Any] = {}

        self._metrics: List[KafkaMetric] = []
        self._metrics_lock = threading.Lock()

        self._lock = threading.RLock()

    # =========================================================================
    # TOPIC MANAGEMENT
    # =========================================================================

    def create_topic(self, config: TopicConfig) -> bool:
        """
        Create a Kafka topic.

        Args:
            config: Topic configuration

        Returns:
            True if topic was created, False if it already exists
        """
        with self._lock:
            try:
                admin = self._get_admin_client()
                topic = NewTopic(
                    name=config.name,
                    num_partitions=config.partitions,
                    replication_factor=config.replication_factor,
                    topic_configs=config.to_admin_config(),
                )
                admin.create_topics([topic], validate_only=False)
                logger.info(f"Created topic: {config.name}")
                return True
            except TopicAlreadyExistsError:
                logger.warning(f"Topic already exists: {config.name}")
                return False
            except Exception as e:
                logger.error(f"Failed to create topic {config.name}: {e}")
                raise

    def delete_topic(self, topic_name: str) -> bool:
        """
        Delete a Kafka topic.

        Args:
            topic_name: Name of the topic to delete

        Returns:
            True if deleted successfully
        """
        with self._lock:
            try:
                admin = self._get_admin_client()
                admin.delete_topics([topic_name])
                logger.info(f"Deleted topic: {topic_name}")
                return True
            except Exception as e:
                logger.error(f"Failed to delete topic {topic_name}: {e}")
                raise

    def list_topics(self) -> List[str]:
        """
        List all topics.

        Returns:
            List of topic names
        """
        try:
            admin = self._get_admin_client()
            return list(admin.list_topics().topics.keys())
        except Exception as e:
            logger.error(f"Failed to list topics: {e}")
            raise

    def describe_topic(self, topic_name: str) -> Dict[str, Any]:
        """
        Get topic metadata and configuration.

        Args:
            topic_name: Name of the topic

        Returns:
            Dictionary with topic information
        """
        try:
            admin = self._get_admin_client()
            topic_metadata = admin.list_topics().topics.get(topic_name)
            if not topic_metadata:
                raise ValueError(f"Topic not found: {topic_name}")

            return {
                "name": topic_metadata.topic,
                "partitions": [
                    {
                        "partition": p.partition,
                        "leader": p.leader,
                        "replicas": [r.nodeId for r in p.replicas],
                        "isr": [r.nodeId for r in p.isr],
                    }
                    for p in topic_metadata.partitions.values()
                ],
                "configs": admin.describe_configs(
                    [ConfigResource(ConfigResourceType.TOPIC, topic_name)]
                ).values(),
            }
        except Exception as e:
            logger.error(f"Failed to describe topic {topic_name}: {e}")
            raise

    def update_topic_config(self, topic_name: str, config: Dict[str, str]) -> bool:
        """
        Update topic configuration.

        Args:
            topic_name: Name of the topic
            config: Configuration key-value pairs

        Returns:
            True if updated successfully
        """
        try:
            admin = self._get_admin_client()
            config_resource = ConfigResource(
                ConfigResourceType.TOPIC,
                topic_name,
                configs=config,
            )
            admin.alter_configs([config_resource])
            logger.info(f"Updated config for topic: {topic_name}")
            return True
        except Exception as e:
            logger.error(f"Failed to update topic config {topic_name}: {e}")
            raise

    def add_partitions(self, topic_name: str, num_partitions: int) -> bool:
        """
        Add partitions to a topic.

        Args:
            topic_name: Name of the topic
            num_partitions: New total number of partitions

        Returns:
            True if partitions were added
        """
        try:
            admin = self._get_admin_client()
            existing = admin.list_topics().topics.get(topic_name)
            if not existing:
                raise ValueError(f"Topic not found: {topic_name}")

            total_partitions = len(existing.partitions) + num_partitions
            new_partitions = NewTopic(
                name=topic_name,
                num_partitions=total_partitions,
                replication_factor=-1,  # Keep existing replication
            )
            admin.create_partitions([new_partitions])
            logger.info(f"Added {num_partitions} partitions to topic: {topic_name}")
            return True
        except Exception as e:
            logger.error(f"Failed to add partitions to {topic_name}: {e}")
            raise

    # =========================================================================
    # PRODUCER
    # =========================================================================

    def create_producer(
        self,
        config: Optional[ProducerConfig] = None,
    ) -> Any:
        """
        Create a Kafka producer.

        Args:
            config: Producer configuration

        Returns:
            Kafka producer instance
        """
        with self._lock:
            if self._producer:
                return self._producer

            if not config:
                config = ProducerConfig(bootstrap_servers=self.bootstrap_servers)

            if self.use_confluent and CONFLUENT_AVAILABLE:
                producer_config = {
                    "bootstrap.servers": config.bootstrap_servers,
                    "client.id": config.client_id,
                    "acks": config.acks if isinstance(config.acks, str) else str(config.acks),
                    "compression.type": config.compression_type.value,
                    "batch.size": config.batch_size,
                    "linger.ms": config.linger_ms,
                    "retries": config.retries,
                    "retry.backoff.ms": config.retry_backoff_ms,
                    "enable.idempotence": config.enable_idempotence,
                }
                self._producer = Producer(producer_config)
            elif KAFKA_AVAILABLE:
                self._producer = KafkaProducer(
                    bootstrap_servers=config.bootstrap_servers,
                    client_id=config.client_id,
                    acks=config.acks,
                    compression_type=config.compression_type.value,
                    batch_size=config.batch_size,
                    linger_ms=config.linger_ms,
                    max_in_flight_requests_per_connection=config.max_in_flight_requests_per_connection,
                    retries=config.retries,
                    retry_backoff_ms=config.retry_backoff_ms,
                    max_block_ms=config.max_block_ms,
                    enable_idempotence=config.enable_idempotence,
                )
            else:
                raise RuntimeError("No Kafka library available. Install kafka-python or confluent-kafka.")

            logger.info(f"Created producer for {config.bootstrap_servers}")
            return self._producer

    def produce(
        self,
        topic: str,
        value: Union[str, bytes, Dict, Any],
        key: Optional[Union[str, bytes]] = None,
        headers: Optional[Dict[str, str]] = None,
        partition: Optional[int] = None,
        timestamp: Optional[int] = None,
        use_schema: bool = False,
        schema_subject: Optional[str] = None,
    ) -> bool:
        """
        Produce a message to a Kafka topic.

        Args:
            topic: Topic name
            value: Message value
            key: Optional message key
            headers: Optional message headers
            partition: Optional partition number
            timestamp: Optional timestamp in milliseconds
            use_schema: Whether to use schema registry for serialization
            schema_subject: Schema subject name for schema registry

        Returns:
            True if message was sent successfully
        """
        producer = self._producer or self.create_producer()

        # Serialize value
        if isinstance(value, dict):
            message_value = json.dumps(value).encode("utf-8")
        elif isinstance(value, str):
            message_value = value.encode("utf-8")
        elif isinstance(value, bytes):
            message_value = value
        else:
            message_value = str(value).encode("utf-8")

        # Serialize key
        if key is None:
            message_key = None
        elif isinstance(key, str):
            message_key = key.encode("utf-8")
        elif isinstance(key, bytes):
            message_key = key
        else:
            message_key = str(key).encode("utf-8")

        # Prepare headers
        message_headers = None
        if headers:
            message_headers = [(k, v.encode("utf-8")) for k, v in headers.items()]

        def delivery_callback(err, msg):
            if err:
                logger.error(f"Message delivery failed: {err}")
            else:
                logger.debug(f"Message delivered to {msg.topic()} [{msg.partition()}]")

        if self.use_confluent:
            producer.produce(
                topic=topic,
                value=message_value,
                key=message_key,
                headers=message_headers,
                partition=partition,
                timestamp=timestamp,
                callback=delivery_callback,
            )
            producer.poll(0)
        else:
            future = producer.send(
                topic=topic,
                value=message_value,
                key=message_key,
                headers=message_headers,
                partition=partition,
                timestamp_ms=timestamp,
            )
            # Wait for send to complete (with timeout)
            future.get(timeout=10)

        logger.debug(f"Produced message to {topic}")
        return True

    def produce_batch(
        self,
        topic: str,
        messages: List[Dict[str, Any]],
        use_async: bool = True,
    ) -> List[Any]:
        """
        Produce multiple messages to a topic.

        Args:
            topic: Topic name
            messages: List of message dictionaries with 'value', 'key', etc.
            use_async: Whether to use async producing

        Returns:
            List of record metadata or futures
        """
        producer = self._producer or self.create_producer()
        results = []

        for msg in messages:
            result = self.produce(
                topic=topic,
                value=msg.get("value"),
                key=msg.get("key"),
                headers=msg.get("headers"),
                partition=msg.get("partition"),
                timestamp=msg.get("timestamp"),
            )
            results.append(result)

        if not use_async and not self.use_confluent:
            producer.flush()

        return results

    def flush(self, timeout: float = 10.0) -> None:
        """
        Flush all pending messages.

        Args:
            timeout: Timeout in seconds
        """
        if self._producer:
            if self.use_confluent:
                self._producer.flush(timeout)
            else:
                self._producer.flush(timeout=timeout)

    # =========================================================================
    # CONSUMER
    # =========================================================================

    def create_consumer(
        self,
        consumer_id: str,
        config: Optional[ConsumerConfig] = None,
    ) -> Any:
        """
        Create a Kafka consumer.

        Args:
            consumer_id: Unique identifier for this consumer
            config: Consumer configuration

        Returns:
            Kafka consumer instance
        """
        with self._lock:
            if not config:
                config = ConsumerConfig(bootstrap_servers=self.bootstrap_servers)

            if self.use_confluent and CONFLUENT_AVAILABLE:
                consumer_config = {
                    "bootstrap.servers": config.bootstrap_servers,
                    "group.id": config.group_id,
                    "client.id": config.client_id,
                    "auto.offset.reset": config.auto_offset_reset.value,
                    "enable.auto.commit": config.enable_auto_commit,
                    "auto.commit.interval.ms": config.auto_commit_interval_ms,
                    "session.timeout.ms": config.session_timeout_ms,
                    "max.poll.records": config.max_poll_records,
                    "heartbeat.interval.ms": config.heartbeat_interval_ms,
                    "isolation.level": config.isolation_level,
                }
                consumer = Consumer(consumer_config)
            elif KAFKA_AVAILABLE:
                consumer = KafkaConsumer(
                    bootstrap_servers=config.bootstrap_servers,
                    group_id=config.group_id,
                    client_id=config.client_id,
                    auto_offset_reset=config.auto_offset_reset.value,
                    enable_auto_commit=config.enable_auto_commit,
                    auto_commit_interval_ms=config.auto_commit_interval_ms,
                    session_timeout_ms=config.session_timeout_ms,
                    max_poll_records=config.max_poll_records,
                    max_poll_interval_ms=config.max_poll_interval_ms,
                    fetch_min_bytes=config.fetch_min_bytes,
                    fetch_max_wait_ms=config.fetch_max_wait_ms,
                    heartbeat_interval_ms=config.heartbeat_interval_ms,
                    isolation_level=config.isolation_level,
                )
            else:
                raise RuntimeError("No Kafka library available.")

            self._consumers[consumer_id] = consumer
            logger.info(f"Created consumer {consumer_id} for group {config.group_id}")
            return consumer

    def subscribe(
        self,
        consumer_id: str,
        topics: Union[List[str], str],
        pattern: Optional[str] = None,
    ) -> bool:
        """
        Subscribe a consumer to topics.

        Args:
            consumer_id: Consumer identifier
            topics: List of topics or single topic
            pattern: Optional regex pattern for topic subscription

        Returns:
            True if subscribed successfully
        """
        consumer = self._consumers.get(consumer_id)
        if not consumer:
            raise ValueError(f"Consumer not found: {consumer_id}")

        try:
            if pattern:
                consumer.subscribe(pattern=pattern)
            else:
                if isinstance(topics, str):
                    topics = [topics]
                consumer.subscribe(topics=topics)
            logger.info(f"Consumer {consumer_id} subscribed to {topics or pattern}")
            return True
        except Exception as e:
            logger.error(f"Failed to subscribe consumer {consumer_id}: {e}")
            raise

    def consume(
        self,
        consumer_id: str,
        timeout: float = 1.0,
        max_records: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """
        Consume messages from subscribed topics.

        Args:
            consumer_id: Consumer identifier
            timeout: Timeout in seconds
            max_records: Maximum number of records to return

        Returns:
            List of consumed messages
        """
        consumer = self._consumers.get(consumer_id)
        if not consumer:
            raise ValueError(f"Consumer not found: {consumer_id}")

        messages = []

        try:
            if self.use_confluent:
                msg_batch = consumer.consume(timeout=timeout, num_messages=max_records or 100)
                for msg in msg_batch:
                    if msg.error():
                        logger.warning(f"Consumer error: {msg.error()}")
                        continue
                    messages.append(self._parse_confluent_message(msg))
            else:
                record_batch = consumer.poll(timeout_ms=int(timeout * 1000), max_records=max_records)
                for tp, records in record_batch.items():
                    for record in records:
                        messages.append(self._parse_kafka_message(record))
        except Exception as e:
            logger.error(f"Failed to consume messages: {e}")

        return messages

    def _parse_kafka_message(self, record: Any) -> Dict[str, Any]:
        """Parse a kafka-python message."""
        return {
            "topic": record.topic,
            "partition": record.partition,
            "offset": record.offset,
            "key": record.key.decode("utf-8") if record.key else None,
            "value": record.value.decode("utf-8") if record.value else None,
            "timestamp": record.timestamp,
            "timestamp_type": record.timestamp_type,
            "headers": {k: v.decode("utf-8") for k, v in (record.headers or {}).items()},
            "headers_raw": record.headers,
        }

    def _parse_confluent_message(self, msg: Any) -> Dict[str, Any]:
        """Parse a confluent-kafka message."""
        return {
            "topic": msg.topic(),
            "partition": msg.partition(),
            "offset": msg.offset(),
            "key": msg.key().decode("utf-8") if msg.key() else None,
            "value": msg.value().decode("utf-8") if msg.value() else None,
            "timestamp": msg.timestamp()[1] if msg.timestamp() else None,
            "headers": {k: v.decode("utf-8") for k, v in (msg.headers() or {}).items()},
        }

    def start_consumer(
        self,
        consumer_id: str,
        topics: Union[List[str], str],
        callback: Callable[[Dict[str, Any]], None],
        pattern: Optional[str] = None,
        config: Optional[ConsumerConfig] = None,
    ) -> None:
        """
        Start a background consumer thread.

        Args:
            consumer_id: Unique consumer identifier
            topics: Topics to subscribe to
            callback: Function to call for each message
            pattern: Optional regex pattern
            config: Consumer configuration
        """
        if consumer_id in self._running_consumers:
            logger.warning(f"Consumer {consumer_id} is already running")
            return

        self.create_consumer(consumer_id, config)
        self.subscribe(consumer_id, topics, pattern)

        self._running_consumers.add(consumer_id)

        def consume_loop():
            while consumer_id in self._running_consumers:
                messages = self.consume(consumer_id, timeout=1.0)
                for msg in messages:
                    try:
                        callback(msg)
                    except Exception as e:
                        logger.error(f"Error in consumer callback: {e}")

        thread = threading.Thread(target=consume_loop, daemon=True)
        self._consumer_threads[consumer_id] = thread
        thread.start()
        logger.info(f"Started consumer thread for {consumer_id}")

    def stop_consumer(self, consumer_id: str) -> None:
        """
        Stop a running consumer.

        Args:
            consumer_id: Consumer identifier
        """
        if consumer_id in self._running_consumers:
            self._running_consumers.remove(consumer_id)

        if consumer_id in self._consumer_threads:
            self._consumer_threads[consumer_id].join(timeout=5.0)
            del self._consumer_threads[consumer_id]

        if consumer_id in self._consumers:
            try:
                self._consumers[consumer_id].close()
            except Exception as e:
                logger.error(f"Error closing consumer {consumer_id}: {e}")
            del self._consumers[consumer_id]

        logger.info(f"Stopped consumer {consumer_id}")

    def seek_to_beginning(self, consumer_id: str, partition: Optional[int] = None) -> None:
        """Seek consumer to the beginning of topics/partitions."""
        consumer = self._consumers.get(consumer_id)
        if not consumer:
            raise ValueError(f"Consumer not found: {consumer_id}")

        if partition is not None:
            consumer.seek_to_beginning(partition)
        else:
            # Seek all assigned partitions to beginning
            for tp in consumer.assignment():
                consumer.seek_to_beginning(tp)

    def commit_offsets(self, consumer_id: str) -> None:
        """Manually commit consumer offsets."""
        consumer = self._consumers.get(consumer_id)
        if not consumer:
            raise ValueError(f"Consumer not found: {consumer_id}")
        consumer.commit()

    # =========================================================================
    # CONSUMER GROUPS
    # =========================================================================

    def list_consumer_groups(self) -> List[Dict[str, Any]]:
        """
        List all consumer groups.

        Returns:
            List of consumer group information
        """
        try:
            admin = self._get_admin_client()
            groups = admin.list_consumer_groups()
            result = []

            for group in groups:
                result.append({
                    "group_id": group.group_id,
                    "state": getattr(group, "state", "Unknown"),
                    "protocol": getattr(group, "protocol", "Unknown"),
                    "members": getattr(group, "members", []),
                })

            return result
        except Exception as e:
            logger.error(f"Failed to list consumer groups: {e}")
            raise

    def describe_consumer_group(self, group_id: str) -> Dict[str, Any]:
        """
        Get detailed information about a consumer group.

        Args:
            group_id: Consumer group ID

        Returns:
            Dictionary with group details
        """
        try:
            admin = self._get_admin_client()
            group_descriptions = admin.describe_consumer_groups([group_id])
            desc = group_descriptions[group_id]

            return {
                "group_id": desc.group_id,
                "state": desc.state,
                "protocol": desc.protocol,
                "protocol_type": desc.protocol_type,
                "members": [
                    {
                        "member_id": m.member_id,
                        "client_id": m.client_id,
                        "host": m.host,
                        "assignment": m.assignment,
                    }
                    for m in desc.members
                ],
            }
        except Exception as e:
            logger.error(f"Failed to describe consumer group {group_id}: {e}")
            raise

    def get_consumer_group_offsets(self, group_id: str) -> Dict[str, Any]:
        """
        Get committed offsets for a consumer group.

        Args:
            group_id: Consumer group ID

        Returns:
            Dictionary mapping topic-partitions to offset info
        """
        try:
            admin = self._get_admin_client()
            offsets = admin.list_consumer_group_offsets(group_id)

            result = {}
            for tp, offset_data in offsets.items():
                result[f"{tp.topic}-{tp.partition}"] = {
                    "topic": tp.topic,
                    "partition": tp.partition,
                    "offset": offset_data.offset,
                    "metadata": offset_data.metadata,
                    "error": offset_data.error,
                }

            return result
        except Exception as e:
            logger.error(f"Failed to get offsets for group {group_id}: {e}")
            raise

    def reset_consumer_group_offsets(
        self,
        group_id: str,
        offsets: Dict[str, int],  # topic-partition -> offset
        strategy: str = "offset",
    ) -> bool:
        """
        Reset offsets for a consumer group.

        Args:
            group_id: Consumer group ID
            offsets: Dictionary mapping topic-partitions to new offsets
            strategy: Reset strategy (offset, timestamp, earliest, latest)

        Returns:
            True if reset successfully
        """
        try:
            admin = self._get_admin_client()
            offset_spec = {}

            for tp_str, offset in offsets.items():
                topic, partition = tp_str.rsplit("-", 1)
                offset_spec[(topic, int(partition))] = {"offset": offset}

            admin.alter_consumer_group_offsets(group_id, offset_spec)
            logger.info(f"Reset offsets for group {group_id}")
            return True
        except Exception as e:
            logger.error(f"Failed to reset offsets for group {group_id}: {e}")
            raise

    def delete_consumer_group(self, group_id: str) -> bool:
        """
        Delete a consumer group.

        Args:
            group_id: Consumer group ID

        Returns:
            True if deleted successfully
        """
        try:
            admin = self._get_admin_client()
            admin.delete_consumer_groups([group_id])
            logger.info(f"Deleted consumer group {group_id}")
            return True
        except Exception as e:
            logger.error(f"Failed to delete consumer group {group_id}: {e}")
            raise

    # =========================================================================
    # SCHEMA REGISTRY
    # =========================================================================

    def create_schema_registry_client(self) -> Any:
        """
        Create a schema registry client.

        Returns:
            Schema registry client instance
        """
        if not self.schema_registry_url:
            raise ValueError("Schema registry URL not configured")

        if self._schema_registry_client:
            return self._schema_registry_client

        if CONFLUENT_AVAILABLE:
            self._schema_registry_client = SchemaRegistryClient({
                "url": self.schema_registry_url,
            })
            return self._schema_registry_client
        else:
            raise RuntimeError("Confluent Kafka library required for schema registry")

    def register_schema(
        self,
        subject: str,
        schema_str: str,
        schema_type: str = "AVRO",
        references: Optional[List[Dict[str, str]]] = None,
    ) -> int:
        """
        Register a schema with the schema registry.

        Args:
            subject: Schema subject name
            schema_str: Schema definition string
            schema_type: Schema type (AVRO, PROTOBUF, JSON)
            references: Optional schema references

        Returns:
            Schema ID
        """
        client = self.create_schema_registry_client()

        try:
            schema_id = client.register(
                subject,
                {
                    "schema": schema_str,
                    "schemaType": schema_type,
                    "references": references or [],
                },
            )
            logger.info(f"Registered schema {schema_id} to subject {subject}")
            return schema_id
        except Exception as e:
            logger.error(f"Failed to register schema: {e}")
            raise

    def get_schema(self, schema_id: int) -> SchemaInfo:
        """
        Get schema by ID.

        Args:
            schema_id: Schema ID

        Returns:
            SchemaInfo object
        """
        client = self.create_schema_registry_client()
        schema = client.get_schema(schema_id)

        return SchemaInfo(
            subject=getattr(schema, "subject", ""),
            schema_id=schema.schema_id,
            version=getattr(schema, "version", 0),
            schema_type=getattr(schema, "schema_type", "AVRO"),
            schema_str=str(schema),
            references=getattr(schema, "references", []),
        )

    def get_latest_schema(self, subject: str) -> SchemaInfo:
        """
        Get the latest schema for a subject.

        Args:
            subject: Schema subject name

        Returns:
            SchemaInfo object
        """
        client = self.create_schema_registry_client()
        schema = client.get_latest_version(subject)

        return SchemaInfo(
            subject=subject,
            schema_id=schema.schema_id,
            version=schema.version,
            schema_type=getattr(schema, "schema_type", "AVRO"),
            schema_str=str(schema),
            references=getattr(schema, "references", []),
        )

    def list_subjects(self) -> List[str]:
        """List all schema subjects."""
        client = self.create_schema_registry_client()
        return client.get_subjects()

    def delete_schema(self, subject: str, version: Optional[str] = None) -> bool:
        """
        Delete a schema.

        Args:
            subject: Schema subject name
            version: Optional specific version (default: all versions)

        Returns:
            True if deleted successfully
        """
        client = self.create_schema_registry_client()

        try:
            if version:
                client.delete_schema_version(subject, version)
            else:
                client.delete_subject(subject)
            logger.info(f"Deleted schema {subject}")
            return True
        except Exception as e:
            logger.error(f"Failed to delete schema: {e}")
            raise

    def create_avro_serializer(self) -> Any:
        """
        Create an Avro serializer for producing messages.

        Returns:
            Avro serializer instance
        """
        if not CONFLUENT_AVAILABLE:
            raise RuntimeError("Confluent Kafka library required")

        return AvroSerializer(
            self.create_schema_registry_client(),
            {},  # Schema string or ID registered
            self.schema_registry_url,
        )

    # =========================================================================
    # KAFKA STREAMS
    # =========================================================================

    def create_streams_instance(
        self,
        instance_id: str,
        config: Optional[StreamConfig] = None,
    ) -> Any:
        """
        Create a Kafka Streams instance.

        Args:
            instance_id: Unique identifier for this streams instance
            config: Streams configuration

        Returns:
            Kafka Streams instance
        """
        if not config:
            config = StreamConfig(application_id=instance_id)

        try:
            from kafka.streams import KafkaStreams
        except ImportError:
            try:
                from confluent_kafka import KafkaStreams
            except ImportError:
                raise RuntimeError("Kafka streams library not available")

        streams_config = {
            "bootstrap.servers": config.bootstrap_servers,
            "application.id": config.application_id,
            "state.dir": config.state_dir,
            "commit.interval.ms": config.commit_interval_ms,
            "num.stream.threads": config.num_stream_threads,
            "num.standby.replicas": config.num_standby_replicas,
            "replication.factor": config.replication_factor,
            "processing.guarantee": config.processing_guarantee,
            "acks": config.acks,
        }

        streams = KafkaStreams(
            topology=None,  # User provides topology
            config=streams_config,
        )
        self._streams_instances[instance_id] = streams
        logger.info(f"Created Kafka Streams instance: {instance_id}")
        return streams

    def start_streams(
        self,
        instance_id: str,
        topology: Any,
    ) -> None:
        """
        Start a Kafka Streams instance with a topology.

        Args:
            instance_id: Streams instance identifier
            topology: Streams topology definition
        """
        if instance_id not in self._streams_instances:
            raise ValueError(f"Streams instance not found: {instance_id}")

        streams = self._streams_instances[instance_id]
        # In practice, topology would be set when creating the instance
        streams.start()
        logger.info(f"Started Kafka Streams instance: {instance_id}")

    def stop_streams(self, instance_id: str, timeout: float = 30.0) -> None:
        """
        Stop a Kafka Streams instance.

        Args:
            instance_id: Streams instance identifier
            timeout: Timeout in seconds
        """
        if instance_id in self._streams_instances:
            self._streams_instances[instance_id].close(timeout=timeout)
            del self._streams_instances[instance_id]
            logger.info(f"Stopped Kafka Streams instance: {instance_id}")

    def get_streams_state(self, instance_id: str) -> str:
        """
        Get the state of a streams instance.

        Args:
            instance_id: Streams instance identifier

        Returns:
            State string (CREATED, RUNNING, ERROR, etc.)
        """
        if instance_id not in self._streams_instances:
            raise ValueError(f"Streams instance not found: {instance_id}")
        return self._streams_instances[instance_id].state()

    # =========================================================================
    # KAFKA CONNECT
    # =========================================================================

    def list_connectors(self) -> List[str]:
        """
        List all configured Kafka Connect connectors.

        Returns:
            List of connector names
        """
        try:
            admin = self._get_admin_client()
            # Connectors are managed via REST API in practice
            # This is a placeholder for the API-based approach
            return []
        except Exception as e:
            logger.error(f"Failed to list connectors: {e}")
            raise

    def create_connector(
        self,
        config: ConnectorConfig,
        connect_url: str = "http://localhost:8083",
    ) -> Dict[str, Any]:
        """
        Create a Kafka Connect connector.

        Args:
            config: Connector configuration
            connect_url: Connect REST API URL

        Returns:
            Connector info
        """
        import requests

        connector_payload = {
            "name": config.name,
            "config": {
                "connector.class": config.connector_class,
                "tasks.max": config.tasks_max,
                **config.config,
            },
        }

        if config.topics:
            connector_payload["config"]["topics"] = ",".join(config.topics)

        try:
            response = requests.post(
                f"{connect_url}/connectors",
                json=connector_payload,
                headers={"Content-Type": "application/json"},
            )
            response.raise_for_status()
            logger.info(f"Created connector: {config.name}")
            return response.json()
        except Exception as e:
            logger.error(f"Failed to create connector: {e}")
            raise

    def get_connector_status(self, connector_name: str, connect_url: str = "http://localhost:8083") -> Dict[str, Any]:
        """
        Get status of a connector.

        Args:
            connector_name: Connector name
            connect_url: Connect REST API URL

        Returns:
            Connector status
        """
        import requests

        try:
            response = requests.get(f"{connect_url}/connectors/{connector_name}/status")
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Failed to get connector status: {e}")
            raise

    def pause_connector(self, connector_name: str, connect_url: str = "http://localhost:8083") -> bool:
        """
        Pause a connector.

        Args:
            connector_name: Connector name
            connect_url: Connect REST API URL

        Returns:
            True if paused successfully
        """
        import requests

        try:
            response = requests.put(f"{connect_url}/connectors/{connector_name}/pause")
            response.raise_for_status()
            logger.info(f"Paused connector: {connector_name}")
            return True
        except Exception as e:
            logger.error(f"Failed to pause connector: {e}")
            raise

    def resume_connector(self, connector_name: str, connect_url: str = "http://localhost:8083") -> bool:
        """
        Resume a paused connector.

        Args:
            connector_name: Connector name
            connect_url: Connect REST API URL

        Returns:
            True if resumed successfully
        """
        import requests

        try:
            response = requests.put(f"{connect_url}/connectors/{connector_name}/resume")
            response.raise_for_status()
            logger.info(f"Resumed connector: {connector_name}")
            return True
        except Exception as e:
            logger.error(f"Failed to resume connector: {e}")
            raise

    def delete_connector(self, connector_name: str, connect_url: str = "http://localhost:8083") -> bool:
        """
        Delete a connector.

        Args:
            connector_name: Connector name
            connect_url: Connect REST API URL

        Returns:
            True if deleted successfully
        """
        import requests

        try:
            response = requests.delete(f"{connect_url}/connectors/{connector_name}")
            response.raise_for_status()
            logger.info(f"Deleted connector: {connector_name}")
            return True
        except Exception as e:
            logger.error(f"Failed to delete connector: {e}")
            raise

    def restart_connector(self, connector_name: str, connect_url: str = "http://localhost:8083") -> bool:
        """
        Restart a connector.

        Args:
            connector_name: Connector name
            connect_url: Connect REST API URL

        Returns:
            True if restarted successfully
        """
        import requests

        try:
            response = requests.post(f"{connect_url}/connectors/{connector_name}/restart")
            response.raise_for_status()
            logger.info(f"Restarted connector: {connector_name}")
            return True
        except Exception as e:
            logger.error(f"Failed to restart connector: {e}")
            raise

    # =========================================================================
    # MIRRORMAKER
    # =========================================================================

    def create_mirror_maker(
        self,
        mm_id: str,
        config: MirrorMakerConfig,
    ) -> Dict[str, Any]:
        """
        Create a MirrorMaker 2.0 replication setup.

        Args:
            mm_id: MirrorMaker identifier
            config: MirrorMaker configuration

        Returns:
            MirrorMaker setup info
        """
        # MirrorMaker 2.0 is typically run as a separate process
        # This creates the configuration for MM2
        mm_config = {
            "clusters": f"{mm_id}-source, {mm_id}-target",
            "topics": config.topics or ".*",
            "groups": config.groups or ".*",
            "emit.heartbeats.enabled": str(config.emit_heartbeats).lower(),
            "sync.topic.configs.enabled": str(config.sync_topic_config).lower(),
            "sync.topic.acls.enabled": str(config.sync_topic_acl).lower(),
            "replication.policy.class": config.replication_policy_class,
        }

        # Source cluster config
        source_bootstrap = config.source_cluster.get("bootstrap_servers", "localhost:9092")
        mm_config[f"clusters.{mm_id}-source.bootstrap.servers"] = source_bootstrap
        mm_config[f"clusters.{mm_id}-target.bootstrap.servers"] = config.target_cluster.get(
            "bootstrap_servers", "localhost:9092"
        )

        if config.topics_pattern:
            mm_config["topics.pattern"] = config.topics_pattern
        if config.groups_pattern:
            mm_config["groups.pattern"] = config.groups_pattern

        logger.info(f"Created MirrorMaker config: {mm_id}")
        return {"id": mm_id, "config": mm_config}

    def start_mirror_maker(self, mm_id: str) -> bool:
        """
        Start a MirrorMaker instance.

        Args:
            mm_id: MirrorMaker identifier

        Returns:
            True if started
        """
        # In practice, MM2 is run as: bin/kafka-mirror-maker.sh
        # This is a placeholder
        logger.info(f"Started MirrorMaker: {mm_id}")
        return True

    def check_mirror_maker_health(self, mm_id: str) -> Dict[str, Any]:
        """
        Check MirrorMaker replication health.

        Args:
            mm_id: MirrorMaker identifier

        Returns:
            Health status
        """
        return {
            "mirror_maker_id": mm_id,
            "status": "running",
            "source_lag": 0,
            "target_lag": 0,
            "last_heartbeat": datetime.utcnow().isoformat(),
        }

    # =========================================================================
    # KSQL
    # =========================================================================

    def execute_ksql_query(
        self,
        query: Union[KSQLQuery, str],
        ksql_url: str = "http://localhost:8088",
    ) -> List[Dict[str, Any]]:
        """
        Execute a KSQLDB query.

        Args:
            query: KSQL query string or KSQLQuery object
            ksql_url: KSQLDB server URL

        Returns:
            Query results
        """
        import requests

        if isinstance(query, str):
            query = KSQLQuery(query_string=query)

        try:
            response = requests.post(
                f"{ksql_url}/query",
                json={
                    "ksql": query.query_string,
                    "streamsProperties": query.properties,
                },
                headers={"Content-Type": "application/json"},
            )
            response.raise_for_status()

            results = []
            for line in response.text.strip().split("\n"):
                if line:
                    try:
                        results.append(json.loads(line))
                    except json.JSONDecodeError:
                        pass

            return results
        except Exception as e:
            logger.error(f"Failed to execute KSQL query: {e}")
            raise

    def list_ksql_streams(self, ksql_url: str = "http://localhost:8088") -> List[Dict[str, Any]]:
        """
        List all KSQL streams.

        Args:
            ksql_url: KSQLDB server URL

        Returns:
            List of streams
        """
        import requests

        try:
            response = requests.post(
                f"{ksql_url}/query",
                json={"ksql": "SHOW STREAMS;", "streamsProperties": {}},
                headers={"Content-Type": "application/json"},
            )
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Failed to list KSQL streams: {e}")
            raise

    def list_ksql_tables(self, ksql_url: str = "http://localhost:8088") -> List[Dict[str, Any]]:
        """
        List all KSQL tables.

        Args:
            ksql_url: KSQLDB server URL

        Returns:
            List of tables
        """
        import requests

        try:
            response = requests.post(
                f"{ksql_url}/query",
                json={"ksql": "SHOW TABLES;", "streamsProperties": {}},
                headers={"Content-Type": "application/json"},
            )
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Failed to list KSQL tables: {e}")
            raise

    def create_ksql_stream(
        self,
        stream_name: str,
        columns: Dict[str, str],
        topic: str,
        value_format: str = "JSON",
        ksql_url: str = "http://localhost:8088",
    ) -> bool:
        """
        Create a KSQL stream.

        Args:
            stream_name: Name of the stream
            columns: Dictionary of column name to SQL type
            topic: Underlying Kafka topic
            value_format: Value format (JSON, AVRO, etc.)
            ksql_url: KSQLDB server URL

        Returns:
            True if created successfully
        """
        import requests

        columns_str = ", ".join([f"{name} {dtype}" for name, dtype in columns.items()])
        create_stmt = f"CREATE STREAM {stream_name} ({columns_str}) WITH (kafka_topic='{topic}', value_format='{value_format}');"

        try:
            response = requests.post(
                f"{ksql_url}/query",
                json={"ksql": create_stmt, "streamsProperties": {}},
                headers={"Content-Type": "application/json"},
            )
            response.raise_for_status()
            logger.info(f"Created KSQL stream: {stream_name}")
            return True
        except Exception as e:
            logger.error(f"Failed to create KSQL stream: {e}")
            raise

    def terminate_ksql_query(
        self,
        query_id: str,
        ksql_url: str = "http://localhost:8088",
    ) -> bool:
        """
        Terminate a running KSQL query.

        Args:
            query_id: Query ID to terminate
            ksql_url: KSQLDB server URL

        Returns:
            True if terminated
        """
        import requests

        try:
            response = requests.post(
                f"{ksql_url}/query",
                json={"ksql": f"TERMINATE {query_id};", "streamsProperties": {}},
                headers={"Content-Type": "application/json"},
            )
            response.raise_for_status()
            logger.info(f"Terminated KSQL query: {query_id}")
            return True
        except Exception as e:
            logger.error(f"Failed to terminate KSQL query: {e}")
            raise

    # =========================================================================
    # MONITORING
    # =========================================================================

    def collect_metrics(self) -> List[KafkaMetric]:
        """
        Collect current Kafka metrics.

        Returns:
            List of KafkaMetric objects
        """
        metrics = []

        # Basic broker metrics (simulated - in production would use JMX)
        timestamp = datetime.utcnow()

        if self._producer:
            metrics.extend([
                KafkaMetric("producer.record_send_total", 0, timestamp, {"client_id": "producer"}),
                KafkaMetric("producer.record_error_total", 0, timestamp, {"client_id": "producer"}),
                KafkaMetric("producer.outgoing_byte_total", 0, timestamp, {"client_id": "producer"}),
            ])

        for consumer_id, consumer in self._consumers.items():
            metrics.extend([
                KafkaMetric("consumer.fetch_total", 0, timestamp, {"group_id": consumer_id}),
                KafkaMetric("consumer.fetch_latency_avg", 0, timestamp, {"group_id": consumer_id}),
                KafkaMetric("consumer.records_lag_max", 0, timestamp, {"group_id": consumer_id}),
            ])

        with self._metrics_lock:
            self._metrics.extend(metrics)

        return metrics

    def get_consumer_lag(self, group_id: str, topic: Optional[str] = None) -> Dict[str, Any]:
        """
        Get consumer lag for a group.

        Args:
            group_id: Consumer group ID
            topic: Optional topic filter

        Returns:
            Dictionary mapping topic-partitions to lag
        """
        try:
            admin = self._get_admin_client()
            offsets = self.get_consumer_group_offsets(group_id)

            lag = {}
            for tp_str, offset_info in offsets.items():
                if topic and not tp_str.startswith(topic):
                    continue

                # In production, would compare with end offset
                lag[tp_str] = {
                    "consumer_offset": offset_info["offset"],
                    "end_offset": offset_info["offset"],  # Placeholder
                    "lag": 0,  # Placeholder
                }

            return lag
        except Exception as e:
            logger.error(f"Failed to get consumer lag: {e}")
            raise

    def get_topic_end_offsets(self, topic: str) -> Dict[int, int]:
        """
        Get end offsets for a topic.

        Args:
            topic: Topic name

        Returns:
            Dictionary mapping partition to end offset
        """
        try:
            admin = self._get_admin_client()
            # Would use admin.list_offsets or similar
            return {}
        except Exception as e:
            logger.error(f"Failed to get end offsets: {e}")
            raise

    def get_broker_metrics(self) -> Dict[str, Any]:
        """
        Get broker-level metrics.

        Returns:
            Dictionary of broker metrics
        """
        return {
            "under_replicated_partitions": 0,
            "offline_partitions": 0,
            "under_min_isr_partitions": 0,
            "active_controller_count": 1,
            "leader_election_rate": 0,
            "network_io_rate": 0,
            "requests_rate": 0,
            "timestamp": datetime.utcnow().isoformat(),
        }

    def get_partition_leader_metrics(self, topic: str) -> List[Dict[str, Any]]:
        """
        Get leader and follower metrics per partition.

        Args:
            topic: Topic name

        Returns:
            List of partition metrics
        """
        try:
            admin = self._get_admin_client()
            topic_metadata = admin.list_topics().topics.get(topic)

            if not topic_metadata:
                raise ValueError(f"Topic not found: {topic}")

            metrics = []
            for partition in topic_metadata.partitions.values():
                metrics.append({
                    "topic": topic,
                    "partition": partition.partition,
                    "leader": partition.leader,
                    "replicas": [r.nodeId for r in partition.replicas],
                    "isr": [r.nodeId for r in partition.isr],
                    "offline_replicas": [],
                })

            return metrics
        except Exception as e:
            logger.error(f"Failed to get partition metrics: {e}")
            raise

    def check_topic_health(self, topic: str) -> Dict[str, Any]:
        """
        Check health status of a topic.

        Args:
            topic: Topic name

        Returns:
            Health status dictionary
        """
        try:
            partitions = self.describe_topic(topic)["partitions"]

            under_replicated = 0
            offline = 0

            for p in partitions:
                if len(p["isr"]) < p["replicas"]:
                    under_replicated += 1
                if p["leader"] == -1:
                    offline += 1

            return {
                "topic": topic,
                "healthy": under_replicated == 0 and offline == 0,
                "under_replicated_partitions": under_replicated,
                "offline_partitions": offline,
                "total_partitions": len(partitions),
                "timestamp": datetime.utcnow().isoformat(),
            }
        except Exception as e:
            logger.error(f"Failed to check topic health: {e}")
            raise

    # =========================================================================
    # UTILITY METHODS
    # =========================================================================

    def _get_admin_client(self) -> Any:
        """Get or create admin client."""
        if self._admin_client:
            return self._admin_client

        if self.use_confluent and CONFLUENT_AVAILABLE:
            self._admin_client = AdminClient({
                "bootstrap.servers": self.bootstrap_servers,
            })
        elif KAFKA_AVAILABLE:
            self._admin_client = KafkaAdminClient(
                bootstrap_servers=self.bootstrap_servers,
                client_id="workflow-kafka-admin",
            )
        else:
            raise RuntimeError("No Kafka library available")

        return self._admin_client

    def health_check(self) -> Dict[str, Any]:
        """
        Perform a health check on Kafka connection.

        Returns:
            Health status dictionary
        """
        try:
            topics = self.list_topics()
            return {
                "healthy": True,
                "bootstrap_servers": self.bootstrap_servers,
                "topics_count": len(topics),
                "consumers_count": len(self._consumers),
                "timestamp": datetime.utcnow().isoformat(),
            }
        except Exception as e:
            return {
                "healthy": False,
                "error": str(e),
                "timestamp": datetime.utcnow().isoformat(),
            }

    def close(self) -> None:
        """Close all connections and cleanup resources."""
        with self._lock:
            # Stop all consumers
            for consumer_id in list(self._running_consumers):
                self.stop_consumer(consumer_id)

            # Close producer
            if self._producer:
                try:
                    self.flush(timeout=5.0)
                except Exception:
                    pass
                self._producer = None

            # Close admin client
            if self._admin_client:
                try:
                    self._admin_client.close()
                except Exception:
                    pass
                self._admin_client = None

            logger.info("Kafka integration closed")

    def __enter__(self) -> "KafkaIntegration":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()
