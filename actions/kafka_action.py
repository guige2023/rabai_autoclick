"""Kafka Action Module for message streaming and processing.

Provides Kafka producer, consumer, and stream processing capabilities
with support for topic management, consumer groups, and offset tracking.
"""
from __future__ import annotations

import json
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)


class DeliveryStatus(Enum):
    """Message delivery status."""
    PENDING = "pending"
    DELIVERED = "delivered"
    FAILED = "failed"


@dataclass
class KafkaMessage:
    """Single Kafka message."""
    topic: str
    key: Optional[str]
    value: Any
    partition: Optional[int] = None
    offset: Optional[int] = None
    timestamp: float = field(default_factory=time.time)
    headers: Dict[str, str] = field(default_factory=dict)
    delivery_status: DeliveryStatus = DeliveryStatus.PENDING
    error: Optional[str] = None


@dataclass
class ProducerResult:
    """Result of producing messages."""
    success: bool
    messages_sent: int
    messages_failed: int
    topic: str
    partition: Optional[int] = None
    errors: List[str] = field(default_factory=list)
    duration_ms: float = 0.0


@dataclass
class ConsumerResult:
    """Result of consuming messages."""
    success: bool
    messages_received: int
    topic: str
    messages: List[KafkaMessage] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)
    duration_ms: float = 0.0


class KafkaBrokerSimulator:
    """In-memory Kafka broker simulator for testing and demos.

    In production, replace with confluent-kafka or kafka-python.
    """

    def __init__(self):
        self._topics: Dict[str, Dict[int, List[KafkaMessage]]] = {}
        self._consumer_offsets: Dict[str, Dict[str, Dict[str, int]]] = {}
        self._partitions_per_topic = 3

    def create_topic(self, topic: str, partitions: int = 3) -> bool:
        """Create a topic with specified partitions."""
        if topic not in self._topics:
            self._topics[topic] = {
                i: [] for i in range(partitions)
            }
        return True

    def produce(self, topic: str, messages: List[KafkaMessage]) -> ProducerResult:
        """Produce messages to topic."""
        if topic not in self._topics:
            self.create_topic(topic)

        start = time.time()
        sent = 0
        failed = 0
        errors = []
        partition = None

        for msg in messages:
            if topic not in self._topics:
                errors.append(f"Topic {topic} not found")
                failed += 1
                continue

            p = msg.partition if msg.partition is not None else hash(msg.key or str(uuid.uuid4())) % self._partitions_per_topic
            if p not in self._topics[topic]:
                self._topics[topic][p] = []

            msg.partition = p
            msg.offset = len(self._topics[topic][p])
            msg.delivery_status = DeliveryStatus.DELIVERED
            self._topics[topic][p].append(msg)
            sent += 1
            partition = p

        duration_ms = (time.time() - start) * 1000

        return ProducerResult(
            success=failed == 0,
            messages_sent=sent,
            messages_failed=failed,
            topic=topic,
            partition=partition,
            errors=errors,
            duration_ms=duration_ms
        )

    def consume(self, topic: str, group_id: str, count: int = 10,
                auto_commit: bool = True) -> ConsumerResult:
        """Consume messages from topic."""
        if topic not in self._topics:
            return ConsumerResult(
                success=False,
                messages_received=0,
                topic=topic,
                errors=[f"Topic {topic} not found"]
            )

        start = time.time()
        messages: List[KafkaMessage] = []

        if group_id not in self._consumer_offsets:
            self._consumer_offsets[group_id] = {}
        if topic not in self._consumer_offsets[group_id]:
            self._consumer_offsets[group_id][topic] = {}

        for partition, msgs in self._topics[topic].items():
            current_offset = self._consumer_offsets[group_id][topic].get(str(partition), 0)
            for msg in msgs[current_offset:current_offset + count]:
                messages.append(msg)
            if auto_commit and messages:
                self._consumer_offsets[group_id][topic][str(partition)] = current_offset + len(messages)

        duration_ms = (time.time() - start) * 1000

        return ConsumerResult(
            success=True,
            messages_received=len(messages),
            topic=topic,
            messages=messages[:count],
            duration_ms=duration_ms
        )

    def get_offsets(self, topic: str) -> Dict[int, int]:
        """Get end offsets for all partitions of a topic."""
        if topic not in self._topics:
            return {}
        return {
            partition: len(msgs)
            for partition, msgs in self._topics[topic].items()
        }


_kafka_broker = KafkaBrokerSimulator()


class KafkaAction:
    """Kafka message streaming action.

    Example:
        action = KafkaAction()

        action.create_topic("events", partitions=6)
        action.send("events", [{"key": "user:1", "value": {"event": "click"}}])
        messages = action.receive("events", group_id="processor-1", count=5)
    """

    def __init__(self, bootstrap_servers: Optional[str] = None):
        """Initialize Kafka action.

        Args:
            bootstrap_servers: Kafka bootstrap servers (used in production)
        """
        self._broker = _kafka_broker
        self._connected = True

    def create_topic(self, topic: str, partitions: int = 3) -> Dict[str, Any]:
        """Create a new topic.

        Args:
            topic: Topic name
            partitions: Number of partitions

        Returns:
            Dict with success status
        """
        try:
            self._broker.create_topic(topic, partitions)
            return {
                "success": True,
                "topic": topic,
                "partitions": partitions,
                "message": f"Topic {topic} created with {partitions} partitions"
            }
        except Exception as e:
            return {
                "success": False,
                "topic": topic,
                "message": f"Failed to create topic: {str(e)}"
            }

    def send(self, topic: str, messages: List[Dict[str, Any]],
             key_field: Optional[str] = None) -> ProducerResult:
        """Send messages to topic.

        Args:
            topic: Target topic
            messages: List of message dicts with 'key' and 'value'
            key_field: Field name to use as message key

        Returns:
            ProducerResult with send status
        """
        kafka_messages = []
        for msg in messages:
            key = msg.get("key") or (msg.get(key_field) if key_field else None)
            value = msg.get("value", msg)
            kafka_messages.append(KafkaMessage(
                topic=topic,
                key=str(key) if key else None,
                value=value
            ))

        return self._broker.produce(topic, kafka_messages)

    def receive(self, topic: str, group_id: str,
                count: int = 10, auto_commit: bool = True) -> ConsumerResult:
        """Receive messages from topic.

        Args:
            topic: Source topic
            group_id: Consumer group ID
            count: Max messages to receive
            auto_commit: Auto commit offsets after receive

        Returns:
            ConsumerResult with received messages
        """
        return self._broker.consume(topic, group_id, count, auto_commit)

    def get_topic_offsets(self, topic: str) -> Dict[int, int]:
        """Get current offsets for topic partitions."""
        return self._broker.get_offsets(topic)

    def list_topics(self) -> List[str]:
        """List all topics."""
        return list(self._kafka_broker._topics.keys())


def execute(context: Any, params: Dict[str, Any]) -> Dict[str, Any]:
    """Execute Kafka action.

    Args:
        context: Execution context
        params: Dict with keys:
            - operation: "create_topic", "send", "receive", "list_topics", "offsets"
            - topic: Topic name
            - messages: List of messages (for send)
            - key_field: Field to use as key (for send)
            - group_id: Consumer group ID (for receive)
            - count: Number of messages (for receive)

    Returns:
        Dict with success, data, message
    """
    operation = params.get("operation", "")

    action = KafkaAction()

    try:
        if operation == "create_topic":
            topic = params.get("topic", "")
            partitions = params.get("partitions", 3)
            if not topic:
                return {"success": False, "message": "topic required"}
            return action.create_topic(topic, partitions)

        elif operation == "send":
            topic = params.get("topic", "")
            messages = params.get("messages", [])
            key_field = params.get("key_field")
            if not topic:
                return {"success": False, "message": "topic required"}
            result = action.send(topic, messages, key_field)
            return {
                "success": result.success,
                "messages_sent": result.messages_sent,
                "messages_failed": result.messages_failed,
                "partition": result.partition,
                "duration_ms": result.duration_ms,
                "message": f"Sent {result.messages_sent} messages"
            }

        elif operation == "receive":
            topic = params.get("topic", "")
            group_id = params.get("group_id", "default")
            count = params.get("count", 10)
            if not topic:
                return {"success": False, "message": "topic required"}
            result = action.receive(topic, group_id, count)
            return {
                "success": result.success,
                "messages_received": result.messages_received,
                "messages": [
                    {"key": m.key, "value": m.value, "offset": m.offset}
                    for m in result.messages
                ],
                "duration_ms": result.duration_ms,
                "message": f"Received {result.messages_received} messages"
            }

        elif operation == "list_topics":
            return {
                "success": True,
                "topics": action.list_topics(),
                "message": "Topics listed"
            }

        elif operation == "offsets":
            topic = params.get("topic", "")
            if not topic:
                return {"success": False, "message": "topic required"}
            return {
                "success": True,
                "offsets": action.get_topic_offsets(topic),
                "message": "Offsets retrieved"
            }

        else:
            return {
                "success": False,
                "message": f"Unknown operation: {operation}"
            }

    except Exception as e:
        return {
            "success": False,
            "message": f"Kafka error: {str(e)}"
        }
