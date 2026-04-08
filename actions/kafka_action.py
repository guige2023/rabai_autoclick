"""Kafka action module for RabAI AutoClick.

Provides Kafka message queue operations including producing messages,
consuming messages, topic management, and consumer group control.
"""

import os
import sys
import time
import json
from typing import Any, Dict, List, Optional, Union, Callable

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class KafkaProducer:
    """Kafka message producer wrapper.
    
    Provides methods for producing messages to Kafka topics.
    """
    
    def __init__(
        self,
        bootstrap_servers: str = "localhost:9092",
        client_id: str = "rabai-producer",
        acks: str = "all",
        retries: int = 3,
        retry_backoff_ms: int = 100,
        max_in_flight_requests_per_connection: int = 5,
        compression_type: Optional[str] = None
    ) -> None:
        """Initialize Kafka producer.
        
        Args:
            bootstrap_servers: Comma-separated list of broker addresses.
            client_id: Client identifier for logging.
            acks: Acknowledgement level ('all', '0', '1').
            retries: Number of retry attempts on failure.
            retry_backoff_ms: Backoff time between retries.
            max_in_flight_requests_per_connection: Max requests per connection.
            compression_type: Compression type ('gzip', 'snappy', 'lz4', None).
        """
        self.bootstrap_servers = bootstrap_servers
        self.client_id = client_id
        self.acks = acks
        self.retries = retries
        self._producer = None
        self._producer_kwargs: Dict[str, Any] = {
            "bootstrap_servers": bootstrap_servers,
            "client_id": client_id,
            "acks": acks,
            "retries": retries,
            "retry_backoff_ms": retry_backoff_ms,
            "max_in_flight_requests_per_connection": max_in_flight_requests_per_connection,
        }
        if compression_type:
            self._producer_kwargs["compression_type"] = compression_type
    
    def connect(self) -> bool:
        """Connect to Kafka brokers.
        
        Returns:
            True if connection successful, False otherwise.
        """
        try:
            from kafka import KafkaProducer as KafkaProducerImpl
        except ImportError:
            raise ImportError(
                "kafka-python is required for Kafka support. Install with: pip install kafka-python"
            )
        
        try:
            self._producer = KafkaProducerImpl(**self._producer_kwargs)
            return True
        except Exception:
            return False
    
    def disconnect(self) -> bool:
        """Disconnect from Kafka brokers."""
        if self._producer:
            try:
                self._producer.flush()
                self._producer.close()
            except Exception:
                pass
            self._producer = None
        return True
    
    def send(
        self,
        topic: str,
        value: Union[str, bytes, Dict[str, Any]],
        key: Optional[Union[str, bytes]] = None,
        headers: Optional[List[Tuple[str, bytes]]] = None,
        partition: Optional[int] = None,
        timestamp_ms: Optional[int] = None
    ) -> Dict[str, Any]:
        """Send a message to a Kafka topic.
        
        Args:
            topic: Topic name to send to.
            value: Message value (string, bytes, or dict for JSON).
            key: Optional message key for partitioning.
            headers: Optional message headers.
            partition: Optional partition number.
            timestamp_ms: Optional message timestamp in milliseconds.
            
        Returns:
            Send result with record metadata.
        """
        if not self._producer:
            raise RuntimeError("Producer not connected")
        
        if isinstance(value, dict):
            value = json.dumps(value).encode("utf-8")
        elif isinstance(value, str):
            value = value.encode("utf-8")
        
        if isinstance(key, str):
            key = key.encode("utf-8")
        
        future = self._producer.send(
            topic,
            value=value,
            key=key,
            headers=headers,
            partition=partition,
            timestamp_ms=timestamp_ms
        )
        
        try:
            record_metadata = future.get(timeout=10)
            
            return {
                "topic": record_metadata.topic,
                "partition": record_metadata.partition,
                "offset": record_metadata.offset,
                "timestamp": record_metadata.timestamp,
                "success": True
            }
        except Exception as e:
            return {"success": False, "error": str(e)}
    
    def send_batch(
        self,
        topic: str,
        messages: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """Send multiple messages to a topic.
        
        Args:
            topic: Topic name.
            messages: List of message dictionaries with 'value' and optional 'key'.
            
        Returns:
            List of send results.
        """
        if not self._producer:
            raise RuntimeError("Producer not connected")
        
        results = []
        futures = []
        
        for msg in messages:
            value = msg.get("value", "")
            key = msg.get("key")
            
            if isinstance(value, dict):
                value = json.dumps(value).encode("utf-8")
            elif isinstance(value, str):
                value = value.encode("utf-8")
            
            if isinstance(key, str):
                key = key.encode("utf-8")
            
            future = self._producer.send(topic, value=value, key=key)
            futures.append(future)
        
        self._producer.flush()
        
        for future in futures:
            try:
                record_metadata = future.get(timeout=10)
                results.append({
                    "topic": record_metadata.topic,
                    "partition": record_metadata.partition,
                    "offset": record_metadata.offset,
                    "success": True
                })
            except Exception as e:
                results.append({"success": False, "error": str(e)})
        
        return results
    
    @property
    def is_connected(self) -> bool:
        """Check if producer is connected."""
        return self._producer is not None


class KafkaConsumer:
    """Kafka message consumer wrapper.
    
    Provides methods for consuming messages from Kafka topics.
    """
    
    def __init__(
        self,
        bootstrap_servers: str = "localhost:9092",
        group_id: str = "rabai-consumer",
        client_id: str = "rabai-consumer",
        auto_offset_reset: str = "earliest",
        enable_auto_commit: bool = True,
        auto_commit_interval_ms: int = 5000,
        session_timeout_ms: int = 30000,
        heartbeat_interval_ms: int = 10000,
        max_poll_records: int = 100,
        max_poll_interval_ms: int = 300000
    ) -> None:
        """Initialize Kafka consumer.
        
        Args:
            bootstrap_servers: Comma-separated list of broker addresses.
            group_id: Consumer group ID.
            client_id: Client identifier.
            auto_offset_reset: Where to start reading ('earliest', 'latest').
            enable_auto_commit: Whether to auto-commit offsets.
            auto_commit_interval_ms: Auto-commit interval.
            session_timeout_ms: Session timeout for consumer group.
            heartbeat_interval_ms: Heartbeat interval.
            max_poll_records: Maximum records per poll.
            max_poll_interval_ms: Maximum poll interval.
        """
        self.bootstrap_servers = bootstrap_servers
        self.group_id = group_id
        self._consumer = None
        self._consumer_kwargs: Dict[str, Any] = {
            "bootstrap_servers": bootstrap_servers,
            "group_id": group_id,
            "client_id": client_id,
            "auto_offset_reset": auto_offset_reset,
            "enable_auto_commit": enable_auto_commit,
            "auto_commit_interval_ms": auto_commit_interval_ms,
            "session_timeout_ms": session_timeout_ms,
            "heartbeat_interval_ms": heartbeat_interval_ms,
            "max_poll_records": max_poll_records,
            "max_poll_interval_ms": max_poll_interval_ms,
        }
    
    def connect(self) -> bool:
        """Connect to Kafka brokers and join consumer group.
        
        Returns:
            True if connection successful, False otherwise.
        """
        try:
            from kafka import KafkaConsumer as KafkaConsumerImpl
        except ImportError:
            raise ImportError(
                "kafka-python is required for Kafka support. Install with: pip install kafka-python"
            )
        
        try:
            self._consumer = KafkaConsumerImpl(**self._consumer_kwargs)
            return True
        except Exception:
            return False
    
    def disconnect(self) -> bool:
        """Disconnect from Kafka brokers."""
        if self._consumer:
            try:
                self._consumer.close()
            except Exception:
                pass
            self._consumer = None
        return True
    
    def subscribe(self, topics: Union[str, List[str]]) -> bool:
        """Subscribe to one or more topics.
        
        Args:
            topics: Topic name or list of topic names.
            
        Returns:
            True if subscription successful.
        """
        if not self._consumer:
            raise RuntimeError("Consumer not connected")
        
        if isinstance(topics, str):
            topics = [topics]
        
        self._consumer.subscribe(topics)
        return True
    
    def unsubscribe(self) -> bool:
        """Unsubscribe from all topics.
        
        Returns:
            True if unsubscription successful.
        """
        if not self._consumer:
            raise RuntimeError("Consumer not connected")
        
        self._consumer.unsubscribe()
        return True
    
    def poll(self, timeout_ms: int = 1000, max_records: Optional[int] = None) -> List[Dict[str, Any]]:
        """Poll for new messages.
        
        Args:
            timeout_ms: Poll timeout in milliseconds.
            max_records: Maximum records to return.
            
        Returns:
            List of consumed messages.
        """
        if not self._consumer:
            raise RuntimeError("Consumer not connected")
        
        records = []
        
        while True:
            message_batch = self._consumer.poll(timeout_ms=timeout_ms)
            
            if not message_batch:
                break
            
            for topic_partition, messages in message_batch.items():
                for message in messages:
                    record = {
                        "topic": message.topic,
                        "partition": message.partition,
                        "offset": message.offset,
                        "timestamp": message.timestamp,
                        "timestamp_type": message.timestamp_type,
                        "key": message.key.decode("utf-8") if message.key else None,
                        "value": message.value.decode("utf-8") if message.value else None,
                        "headers": dict(message.headers) if message.headers else {},
                    }
                    
                    try:
                        record["value_json"] = json.loads(record["value"])
                    except (json.JSONDecodeError, TypeError):
                        pass
                    
                    records.append(record)
            
            if max_records and len(records) >= max_records:
                break
        
        if max_records:
            records = records[:max_records]
        
        return records
    
    def consume(
        self,
        topics: Union[str, List[str]],
        max_messages: int = 10,
        timeout_ms: int = 5000
    ) -> List[Dict[str, Any]]:
        """Consume messages from topics.
        
        Args:
            topics: Topic name or list of topic names.
            max_messages: Maximum messages to consume.
            timeout_ms: Total timeout in milliseconds.
            
        Returns:
            List of consumed messages.
        """
        if not self._consumer:
            raise RuntimeError("Consumer not connected")
        
        self.subscribe(topics)
        
        start_time = time.time()
        messages = []
        
        while len(messages) < max_messages:
            elapsed = (time.time() - start_time) * 1000
            remaining = max(0, timeout_ms - elapsed)
            
            if remaining <= 0:
                break
            
            new_messages = self.poll(timeout_ms=int(remaining), max_records=max_messages - len(messages))
            messages.extend(new_messages)
        
        return messages
    
    def commit(self) -> bool:
        """Manually commit current offsets.
        
        Returns:
            True if commit successful.
        """
        if not self._consumer:
            raise RuntimeError("Consumer not connected")
        
        try:
            self._consumer.commit()
            return True
        except Exception:
            return False
    
    @property
    def is_connected(self) -> bool:
        """Check if consumer is connected."""
        return self._consumer is not None
    
    @property
    def assignment(self) -> List[Any]:
        """Get current partition assignments."""
        if not self._consumer:
            return []
        return list(self._consumer.assignment())
    
    def seek_to_beginning(self, partitions: Optional[List[Any]] = None) -> None:
        """Seek to the beginning of specified partitions.
        
        Args:
            partitions: Optional list of partitions to seek.
        """
        if not self._consumer:
            raise RuntimeError("Consumer not connected")
        
        if partitions is None:
            partitions = list(self._consumer.assignment())
        
        for partition in partitions:
            self._consumer.seek_to_beginning(partition)
    
    def seek_to_end(self, partitions: Optional[List[Any]] = None) -> None:
        """Seek to the end of specified partitions.
        
        Args:
            partitions: Optional list of partitions to seek.
        """
        if not self._consumer:
            raise RuntimeError("Consumer not connected")
        
        if partitions is None:
            partitions = list(self._consumer.assignment())
        
        for partition in partitions:
            self._consumer.seek_to_end(partition)


class KafkaAdmin:
    """Kafka admin client for topic and broker management."""
    
    def __init__(
        self,
        bootstrap_servers: str = "localhost:9092"
    ) -> None:
        """Initialize Kafka admin client.
        
        Args:
            bootstrap_servers: Comma-separated list of broker addresses.
        """
        self.bootstrap_servers = bootstrap_servers
        self._admin = None
    
    def connect(self) -> bool:
        """Connect to Kafka brokers."""
        try:
            from kafka import KafkaAdminClient as KafkaAdminImpl
        except ImportError:
            raise ImportError(
                "kafka-python is required. Install with: pip install kafka-python"
            )
        
        try:
            self._admin = KafkaAdminImpl(bootstrap_servers=self.bootstrap_servers)
            return True
        except Exception:
            return False
    
    def disconnect(self) -> bool:
        """Disconnect from Kafka brokers."""
        if self._admin:
            try:
                self._admin.close()
            except Exception:
                pass
            self._admin = None
        return True
    
    def list_topics(self) -> List[str]:
        """List all topics.
        
        Returns:
            List of topic names.
        """
        if not self._admin:
            raise RuntimeError("Admin not connected")
        
        return list(self._admin.list_topics())
    
    def create_topic(
        self,
        name: str,
        num_partitions: int = 1,
        replication_factor: int = 1,
        topic_configs: Optional[Dict[str, str]] = None
    ) -> bool:
        """Create a new topic.
        
        Args:
            name: Topic name.
            num_partitions: Number of partitions.
            replication_factor: Replication factor.
            topic_configs: Optional topic configuration.
            
        Returns:
            True if created successfully.
        """
        if not self._admin:
            raise RuntimeError("Admin not connected")
        
        try:
            from kafka.admin import TopicProperties, NewTopic
            
            topic = NewTopic(
                name=name,
                num_partitions=num_partitions,
                replication_factor=replication_factor,
                topic_configs=topic_configs or {}
            )
            
            self._admin.create_topics([topic], validate_only=False)
            return True
        except Exception:
            return False
    
    def delete_topic(self, name: str) -> bool:
        """Delete a topic.
        
        Args:
            name: Topic name.
            
        Returns:
            True if deleted successfully.
        """
        if not self._admin:
            raise RuntimeError("Admin not connected")
        
        try:
            self._admin.delete_topics([name])
            return True
        except Exception:
            return False


class KafkaAction(BaseAction):
    """Kafka action for message queue operations.
    
    Supports producing, consuming, and topic management.
    """
    action_type: str = "kafka"
    display_name: str = "Kafka动作"
    description: str = "Kafka消息队列操作，支持消息生产、消费和主题管理"
    
    def __init__(self) -> None:
        super().__init__()
        self._producer: Optional[KafkaProducer] = None
        self._consumer: Optional[KafkaConsumer] = None
        self._admin: Optional[KafkaAdmin] = None
    
    def get_required_params(self) -> List[str]:
        """Return required parameters for this action."""
        return ["operation"]
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute Kafka operation.
        
        Args:
            context: Execution context.
            params: Operation and parameters.
            
        Returns:
            ActionResult with operation outcome.
        """
        start_time = time.time()
        
        try:
            operation = params.get("operation", "connect")
            
            if operation == "producer_connect":
                return self._producer_connect(params, start_time)
            elif operation == "producer_disconnect":
                return self._producer_disconnect(start_time)
            elif operation == "send":
                return self._send_message(params, start_time)
            elif operation == "send_batch":
                return self._send_batch(params, start_time)
            elif operation == "consumer_connect":
                return self._consumer_connect(params, start_time)
            elif operation == "consumer_disconnect":
                return self._consumer_disconnect(start_time)
            elif operation == "subscribe":
                return self._subscribe(params, start_time)
            elif operation == "poll":
                return self._poll_messages(params, start_time)
            elif operation == "consume":
                return self._consume_messages(params, start_time)
            elif operation == "commit":
                return self._commit_offsets(start_time)
            elif operation == "admin_connect":
                return self._admin_connect(params, start_time)
            elif operation == "admin_disconnect":
                return self._admin_disconnect(start_time)
            elif operation == "list_topics":
                return self._list_topics(start_time)
            elif operation == "create_topic":
                return self._create_topic(params, start_time)
            elif operation == "delete_topic":
                return self._delete_topic(params, start_time)
            else:
                return ActionResult(
                    success=False,
                    message=f"Unknown operation: {operation}",
                    duration=time.time() - start_time
                )
        
        except ImportError as e:
            return ActionResult(
                success=False,
                message=f"Import error: {str(e)}",
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Kafka operation failed: {str(e)}",
                duration=time.time() - start_time
            )
    
    def _producer_connect(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Connect producer to Kafka."""
        servers = params.get("bootstrap_servers", "localhost:9092")
        
        self._producer = KafkaProducer(bootstrap_servers=servers)
        success = self._producer.connect()
        
        return ActionResult(
            success=success,
            message=f"Producer connected to {servers}" if success else "Failed to connect producer",
            duration=time.time() - start_time
        )
    
    def _producer_disconnect(self, start_time: float) -> ActionResult:
        """Disconnect producer."""
        if self._producer:
            self._producer.disconnect()
            self._producer = None
        
        return ActionResult(
            success=True,
            message="Producer disconnected",
            duration=time.time() - start_time
        )
    
    def _send_message(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Send a message to Kafka topic."""
        if not self._producer or not self._producer.is_connected:
            return ActionResult(
                success=False,
                message="Producer not connected",
                duration=time.time() - start_time
            )
        
        topic = params.get("topic", "")
        value = params.get("value", "")
        key = params.get("key")
        
        if not topic or value == "":
            return ActionResult(
                success=False,
                message="topic and value are required",
                duration=time.time() - start_time
            )
        
        result = self._producer.send(topic, value=value, key=key)
        
        return ActionResult(
            success=result.get("success", False),
            message=f"Sent to {result.get('topic')}:{result.get('partition')}@{result.get('offset')}" if result.get("success") else f"Failed: {result.get('error')}",
            data=result,
            duration=time.time() - start_time
        )
    
    def _send_batch(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Send multiple messages."""
        if not self._producer or not self._producer.is_connected:
            return ActionResult(
                success=False,
                message="Producer not connected",
                duration=time.time() - start_time
            )
        
        topic = params.get("topic", "")
        messages = params.get("messages", [])
        
        if not topic or not messages:
            return ActionResult(
                success=False,
                message="topic and messages are required",
                duration=time.time() - start_time
            )
        
        results = self._producer.send_batch(topic, messages)
        success_count = sum(1 for r in results if r.get("success"))
        
        return ActionResult(
            success=success_count > 0,
            message=f"Sent {success_count}/{len(messages)} messages",
            data={"results": results, "success_count": success_count},
            duration=time.time() - start_time
        )
    
    def _consumer_connect(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Connect consumer to Kafka."""
        servers = params.get("bootstrap_servers", "localhost:9092")
        group_id = params.get("group_id", "rabai-consumer")
        
        self._consumer = KafkaConsumer(bootstrap_servers=servers, group_id=group_id)
        success = self._consumer.connect()
        
        return ActionResult(
            success=success,
            message=f"Consumer connected to {servers}" if success else "Failed to connect consumer",
            duration=time.time() - start_time
        )
    
    def _consumer_disconnect(self, start_time: float) -> ActionResult:
        """Disconnect consumer."""
        if self._consumer:
            self._consumer.disconnect()
            self._consumer = None
        
        return ActionResult(
            success=True,
            message="Consumer disconnected",
            duration=time.time() - start_time
        )
    
    def _subscribe(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Subscribe to topics."""
        if not self._consumer or not self._consumer.is_connected:
            return ActionResult(
                success=False,
                message="Consumer not connected",
                duration=time.time() - start_time
            )
        
        topics = params.get("topics", "")
        if isinstance(topics, str):
            topics = [topics]
        
        if not topics:
            return ActionResult(
                success=False,
                message="topics is required",
                duration=time.time() - start_time
            )
        
        success = self._consumer.subscribe(topics)
        
        return ActionResult(
            success=success,
            message=f"Subscribed to {topics}",
            duration=time.time() - start_time
        )
    
    def _poll_messages(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Poll for messages."""
        if not self._consumer or not self._consumer.is_connected:
            return ActionResult(
                success=False,
                message="Consumer not connected",
                duration=time.time() - start_time
            )
        
        timeout_ms = params.get("timeout_ms", 1000)
        max_records = params.get("max_records")
        
        messages = self._consumer.poll(timeout_ms=timeout_ms, max_records=max_records)
        
        return ActionResult(
            success=True,
            message=f"Polled {len(messages)} messages",
            data={"messages": messages, "count": len(messages)},
            duration=time.time() - start_time
        )
    
    def _consume_messages(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Consume messages from topics."""
        if not self._consumer or not self._consumer.is_connected:
            return ActionResult(
                success=False,
                message="Consumer not connected",
                duration=time.time() - start_time
            )
        
        topics = params.get("topics", "")
        max_messages = params.get("max_messages", 10)
        timeout_ms = params.get("timeout_ms", 5000)
        
        if isinstance(topics, str):
            topics = [topics]
        
        if not topics:
            return ActionResult(
                success=False,
                message="topics is required",
                duration=time.time() - start_time
            )
        
        messages = self._consumer.consume(
            topics=topics,
            max_messages=max_messages,
            timeout_ms=timeout_ms
        )
        
        return ActionResult(
            success=True,
            message=f"Consumed {len(messages)} messages",
            data={"messages": messages, "count": len(messages)},
            duration=time.time() - start_time
        )
    
    def _commit_offsets(self, start_time: float) -> ActionResult:
        """Commit consumer offsets."""
        if not self._consumer or not self._consumer.is_connected:
            return ActionResult(
                success=False,
                message="Consumer not connected",
                duration=time.time() - start_time
            )
        
        success = self._consumer.commit()
        
        return ActionResult(
            success=success,
            message="Offsets committed" if success else "Commit failed",
            duration=time.time() - start_time
        )
    
    def _admin_connect(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Connect admin client."""
        servers = params.get("bootstrap_servers", "localhost:9092")
        
        self._admin = KafkaAdmin(bootstrap_servers=servers)
        success = self._admin.connect()
        
        return ActionResult(
            success=success,
            message=f"Admin connected to {servers}" if success else "Failed to connect admin",
            duration=time.time() - start_time
        )
    
    def _admin_disconnect(self, start_time: float) -> ActionResult:
        """Disconnect admin client."""
        if self._admin:
            self._admin.disconnect()
            self._admin = None
        
        return ActionResult(
            success=True,
            message="Admin disconnected",
            duration=time.time() - start_time
        )
    
    def _list_topics(self, start_time: float) -> ActionResult:
        """List all topics."""
        if not self._admin:
            return ActionResult(
                success=False,
                message="Admin not connected",
                duration=time.time() - start_time
            )
        
        topics = self._admin.list_topics()
        
        return ActionResult(
            success=True,
            message=f"Found {len(topics)} topics",
            data={"topics": topics, "count": len(topics)},
            duration=time.time() - start_time
        )
    
    def _create_topic(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Create a topic."""
        if not self._admin:
            return ActionResult(
                success=False,
                message="Admin not connected",
                duration=time.time() - start_time
            )
        
        name = params.get("name", "")
        num_partitions = params.get("num_partitions", 1)
        replication_factor = params.get("replication_factor", 1)
        
        if not name:
            return ActionResult(
                success=False,
                message="name is required",
                duration=time.time() - start_time
            )
        
        success = self._admin.create_topic(
            name=name,
            num_partitions=num_partitions,
            replication_factor=replication_factor
        )
        
        return ActionResult(
            success=success,
            message=f"Created topic: {name}" if success else f"Failed to create topic: {name}",
            duration=time.time() - start_time
        )
    
    def _delete_topic(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Delete a topic."""
        if not self._admin:
            return ActionResult(
                success=False,
                message="Admin not connected",
                duration=time.time() - start_time
            )
        
        name = params.get("name", "")
        
        if not name:
            return ActionResult(
                success=False,
                message="name is required",
                duration=time.time() - start_time
            )
        
        success = self._admin.delete_topic(name)
        
        return ActionResult(
            success=success,
            message=f"Deleted topic: {name}" if success else f"Failed to delete topic: {name}",
            duration=time.time() - start_time
        )
