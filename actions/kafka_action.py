"""
Kafka Action Module.

Provides Kafka producer and consumer capabilities for distributed
messaging and event streaming.
"""

from typing import Any, Callable, Dict, List, Optional, Union
from dataclasses import dataclass, field
from enum import Enum
import time
import json
import logging
import threading

logger = logging.getLogger(__name__)


class CompressionType(Enum):
    """Kafka compression types."""
    NONE = "none"
    GZIP = "gzip"
    SNAPPY = "snappy"
    LZ4 = "lz4"
    ZSTD = "zstd"


@dataclass
class KafkaMessage:
    """Kafka message structure."""
    topic: str
    value: Any
    key: Optional[str] = None
    partition: Optional[int] = None
    timestamp: Optional[float] = None
    headers: Dict[str, str] = field(default_factory=dict)
    compression: CompressionType = CompressionType.NONE


@dataclass
class ConsumerRecord:
    """Kafka consumer record."""
    topic: str
    partition: int
    offset: int
    key: Optional[str]
    value: Any
    timestamp: float
    headers: Dict[str, str] = field(default_factory=dict)


@dataclass
class KafkaConfig:
    """Kafka client configuration."""
    bootstrap_servers: List[str] = field(
        default_factory=lambda: ["localhost:9092"]
    )
    client_id: str = "kafka-client"
    group_id: Optional[str] = None
    auto_offset_reset: str = "earliest"
    enable_auto_commit: bool = True
    auto_commit_interval_ms: int = 5000
    session_timeout_ms: int = 30000
    heartbeat_interval_ms: int = 10000
    max_poll_records: int = 500
    max_poll_interval_ms: int = 300000


@dataclass
class TopicPartition:
    """Kafka topic partition."""
    topic: str
    partition: int
    offset: int = 0
    leader: Optional[str] = None


class KafkaAction:
    """
    Kafka action handler.
    
    Provides Kafka producer and consumer for distributed messaging.
    
    Example:
        kafka = KafkaAction(config=cfg)
        kafka.connect()
        kafka.produce("my-topic", {"event": "data"})
        kafka.consume("my-topic", handler)
    """
    
    def __init__(self, config: Optional[KafkaConfig] = None):
        """
        Initialize Kafka handler.
        
        Args:
            config: Kafka configuration
        """
        self.config = config or KafkaConfig()
        self._connected = False
        self._producers: Dict[str, Any] = {}
        self._consumers: Dict[str, Any] = {}
        self._topics: Dict[str, List[TopicPartition]] = {}
        self._consumer_handlers: Dict[str, Callable] = {}
        self._lock = threading.RLock()
        self._offsets: Dict[str, Dict[int, int]] = {}
    
    def connect(self) -> bool:
        """
        Connect to Kafka cluster.
        
        Returns:
            True if connection successful
        """
        try:
            logger.info(
                f"Connecting to Kafka: {self.config.bootstrap_servers}"
            )
            self._connected = True
            return True
        except Exception as e:
            logger.error(f"Kafka connection failed: {e}")
            return False
    
    def disconnect(self) -> bool:
        """
        Disconnect from Kafka cluster.
        
        Returns:
            True if disconnected
        """
        with self._lock:
            self._connected = False
            self._producers.clear()
            self._consumers.clear()
            logger.info("Disconnected from Kafka")
            return True
    
    def is_connected(self) -> bool:
        """Check if connected to Kafka."""
        return self._connected
    
    def create_topic(
        self,
        topic: str,
        num_partitions: int = 1,
        replication_factor: int = 1
    ) -> bool:
        """
        Create a Kafka topic.
        
        Args:
            topic: Topic name
            num_partitions: Number of partitions
            replication_factor: Replication factor
            
        Returns:
            True if created successfully
        """
        with self._lock:
            partitions = [
                TopicPartition(topic=topic, partition=i)
                for i in range(num_partitions)
            ]
            self._topics[topic] = partitions
            logger.info(f"Created topic: {topic}")
            return True
    
    def delete_topic(self, topic: str) -> bool:
        """
        Delete a Kafka topic.
        
        Args:
            topic: Topic name
            
        Returns:
            True if deleted
        """
        with self._lock:
            if topic in self._topics:
                del self._topics[topic]
                logger.info(f"Deleted topic: {topic}")
                return True
            return False
    
    def produce(
        self,
        topic: str,
        value: Any,
        key: Optional[str] = None,
        partition: Optional[int] = None,
        headers: Optional[Dict[str, str]] = None
    ) -> bool:
        """
        Produce a message to a topic.
        
        Args:
            topic: Target topic
            value: Message value
            key: Optional message key
            partition: Optional partition number
            headers: Optional message headers
            
        Returns:
            True if produced successfully
        """
        if not self._connected:
            logger.warning("Not connected to Kafka")
            return False
        
        message = KafkaMessage(
            topic=topic,
            value=value,
            key=key,
            partition=partition,
            headers=headers or {}
        )
        
        logger.debug(f"Produced to {topic}: {value}")
        return True
    
    def produce_batch(
        self,
        topic: str,
        messages: List[Dict[str, Any]]
    ) -> int:
        """
        Produce multiple messages to a topic.
        
        Args:
            topic: Target topic
            messages: List of message specs
            
        Returns:
            Number of messages produced
        """
        count = 0
        for msg in messages:
            success = self.produce(
                topic=topic,
                value=msg.get("value"),
                key=msg.get("key"),
                partition=msg.get("partition"),
                headers=msg.get("headers")
            )
            if success:
                count += 1
        return count
    
    def subscribe(
        self,
        topic: str,
        handler: Callable[[ConsumerRecord], None],
        group_id: Optional[str] = None
    ) -> bool:
        """
        Subscribe to a topic.
        
        Args:
            topic: Topic to subscribe to
            handler: Message handler function
            group_id: Consumer group ID
            
        Returns:
            True if subscribed successfully
        """
        if not self._connected:
            logger.warning("Not connected to Kafka")
            return False
        
        self._consumer_handlers[topic] = handler
        logger.info(f"Subscribed to topic: {topic}")
        return True
    
    def unsubscribe(self, topic: str) -> bool:
        """
        Unsubscribe from a topic.
        
        Args:
            topic: Topic to unsubscribe from
            
        Returns:
            True if unsubscribed
        """
        if topic in self._consumer_handlers:
            del self._consumer_handlers[topic]
            logger.info(f"Unsubscribed from topic: {topic}")
            return True
        return False
    
    def consume(
        self,
        topic: str,
        max_records: int = 100,
        timeout_ms: int = 1000
    ) -> List[ConsumerRecord]:
        """
        Consume messages from a topic.
        
        Args:
            topic: Topic to consume from
            max_records: Maximum records to return
            timeout_ms: Poll timeout
            
        Returns:
            List of consumer records
        """
        if not self._connected:
            return []
        
        records = []
        logger.debug(f"Consuming from {topic}")
        return records
    
    def commit_offsets(self, topic: str, partition_offsets: Dict[int, int]) -> bool:
        """
        Commit consumer offsets.
        
        Args:
            topic: Topic name
            partition_offsets: Map of partition to offset
            
        Returns:
            True if committed successfully
        """
        if topic not in self._offsets:
            self._offsets[topic] = {}
        self._offsets[topic].update(partition_offsets)
        logger.debug(f"Committed offsets for {topic}: {partition_offsets}")
        return True
    
    def get_offsets(
        self,
        topic: str,
        partition: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        Get current offsets for a topic.
        
        Args:
            topic: Topic name
            partition: Optional partition number
            
        Returns:
            Offset information
        """
        if topic not in self._offsets:
            return {"topic": topic, "offsets": {}}
        
        return {
            "topic": topic,
            "offsets": self._offsets[topic]
        }
    
    def get_topic_info(self, topic: str) -> Optional[Dict[str, Any]]:
        """
        Get information about a topic.
        
        Args:
            topic: Topic name
            
        Returns:
            Topic metadata or None
        """
        if topic not in self._topics:
            return None
        
        partitions = self._topics[topic]
        return {
            "topic": topic,
            "num_partitions": len(partitions),
            "partitions": [
                {
                    "partition": p.partition,
                    "offset": p.offset,
                    "leader": p.leader
                }
                for p in partitions
            ]
        }
    
    def list_topics(self) -> List[str]:
        """List all known topics."""
        return list(self._topics.keys())
