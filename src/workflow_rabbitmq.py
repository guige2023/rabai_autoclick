"""
RabbitMQ Message Queue Integration Module for Workflow System

Implements a RabbitMQIntegration class with:
1. Connection management: Manage RabbitMQ connections
2. Channel management: Manage channels
3. Queue management: Create/manage queues
4. Exchange management: Create/manage exchanges
5. Binding management: Bind queues to exchanges
6. Message publishing: Publish messages
7. Message consuming: Consume messages
8. RPC: Request/reply pattern
9. Dead letter queue: DLQ support
10. Clustering: Cluster management

Commit: 'feat(rabbitmq): add RabbitMQ integration with connection management, channels, queues, exchanges, bindings, message publishing, consuming, RPC, DLQ, clustering'
"""

import uuid
import json
import threading
import time
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Callable, Any, Optional, Set, Type, Union
from dataclasses import dataclass, field
from collections import defaultdict
from queue import Queue, Empty
from enum import Enum
import copy

try:
    import pika
    from pika.exceptions import (
        AMQPConnectionError,
        AMQPChannelError,
        ChannelClosedByBroker,
        ConnectionClosedByBroker
    )
    PIKA_AVAILABLE = True
except ImportError:
    PIKA_AVAILABLE = False
    pika = None


logger = logging.getLogger(__name__)


class ExchangeType(Enum):
    """RabbitMQ exchange types."""
    DIRECT = "direct"
    FANOUT = "fanout"
    TOPIC = "topic"
    HEADERS = "headers"


class QueueMode(Enum):
    """Queue mode types."""
    CLASSIC = "classic"
    QUORUM = "quorum"


class MessagePriority(Enum):
    """Message priority levels."""
    LOW = 0
    NORMAL = 5
    HIGH = 10


@dataclass
class QueueConfig:
    """Configuration for a queue."""
    name: str
    durable: bool = True
    exclusive: bool = False
    auto_delete: bool = False
    arguments: Dict[str, Any] = field(default_factory=dict)
    mode: QueueMode = QueueMode.CLASSIC


@dataclass
class ExchangeConfig:
    """Configuration for an exchange."""
    name: str
    exchange_type: ExchangeType = ExchangeType.DIRECT
    durable: bool = True
    auto_delete: bool = False
    arguments: Dict[str, Any] = field(default_factory=dict)


@dataclass
class BindingConfig:
    """Configuration for a binding."""
    queue: str
    exchange: str
    routing_key: str = ""
    arguments: Dict[str, Any] = field(default_factory=dict)


@dataclass
class MessageProperties:
    """Message properties for publishing."""
    content_type: str = "application/json"
    delivery_mode: int = 2  # Persistent
    priority: int = 5
    message_id: Optional[str] = None
    timestamp: Optional[datetime] = None
    headers: Dict[str, Any] = field(default_factory=dict)
    reply_to: Optional[str] = None
    correlation_id: Optional[str] = None
    expiration: Optional[str] = None


@dataclass
class Message:
    """Represents a message received from RabbitMQ."""
    body: Any
    routing_key: str = ""
    delivery_tag: Optional[int] = None
    message_id: Optional[str] = None
    correlation_id: Optional[str] = None
    reply_to: Optional[str] = None
    headers: Dict[str, Any] = field(default_factory=dict)
    timestamp: Optional[datetime] = None
    redelivered: bool = False
    properties: Optional[MessageProperties] = None


@dataclass
class DeadLetterConfig:
    """Configuration for dead letter queue."""
    exchange: str = "dlx"
    queue: str = "dlq"
    routing_key: str = "dead"
    max_retries: int = 3
    ttl: Optional[int] = None  # milliseconds


@dataclass
class ClusterNode:
    """Represents a node in a RabbitMQ cluster."""
    host: str
    port: int = 5672
    node_type: str = "disk"  # or "ram"
    is_master: bool = False


class RabbitMQIntegration:
    """
    RabbitMQ integration class for workflow system.
    
    Provides comprehensive RabbitMQ message queue management including:
    - Connection and channel pooling
    - Exchange and queue management
    - Message publishing and consuming
    - RPC request/reply pattern
    - Dead letter queue support
    - Cluster management
    """
    
    def __init__(
        self,
        host: str = "localhost",
        port: int = 5672,
        username: str = "guest",
        password: str = "guest",
        virtual_host: str = "/",
        connection_attempts: int = 3,
        retry_delay: int = 5,
        heartbeat: int = 60,
        blocked_connection_timeout: float = 30.0,
        cluster_nodes: Optional[List[ClusterNode]] = None,
        use_clustering: bool = False,
        dlq_config: Optional[DeadLetterConfig] = None,
        max_channels: int = 100,
        channel_pool_size: int = 10
    ):
        """
        Initialize RabbitMQ integration.
        
        Args:
            host: RabbitMQ host
            port: RabbitMQ port
            username: Username for authentication
            password: Password for authentication
            virtual_host: Virtual host path
            connection_attempts: Number of connection retry attempts
            retry_delay: Delay between retry attempts in seconds
            heartbeat: Heartbeat interval in seconds
            blocked_connection_timeout: Timeout for blocked connections
            cluster_nodes: List of cluster nodes for HA
            use_clustering: Enable clustering mode
            dlq_config: Dead letter queue configuration
            max_channels: Maximum number of channels
            channel_pool_size: Size of channel pool
        """
        if not PIKA_AVAILABLE:
            raise ImportError("pika library is required. Install with: pip install pika")
        
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.virtual_host = virtual_host
        self.connection_attempts = connection_attempts
        self.retry_delay = retry_delay
        self.heartbeat = heartbeat
        self.blocked_connection_timeout = blocked_connection_timeout
        self.cluster_nodes = cluster_nodes or []
        self.use_clustering = use_clustering
        self.dlq_config = dlq_config or DeadLetterConfig()
        self.max_channels = max_channels
        self.channel_pool_size = channel_pool_size
        
        self._connection: Optional[Any] = None
        self._connection_lock = threading.RLock()
        self._channels: Dict[str, Any] = {}
        self._channel_lock = threading.RLock()
        self._channel_pool: Queue = Queue(maxsize=channel_pool_size)
        self._consumers: Dict[str, Callable] = {}
        self._consumer_threads: Dict[str, threading.Thread] = {}
        self._running = False
        
        self._exchanges: Set[str] = set()
        self._queues: Set[str] = set()
        self._bindings: List[BindingConfig] = []
        
        self._rpc_callbacks: Dict[str, Any] = {}
        self._rpc_lock = threading.RLock()
        self._rpc_timeout = 30.0
        
        self._setup_logging()
    
    def _setup_logging(self) -> None:
        """Setup logging for RabbitMQ integration."""
        self._logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
    
    def _get_connection_parameters(self, host: Optional[str] = None, port: Optional[int] = None) -> Any:
        """Get connection parameters for RabbitMQ."""
        credentials = pika.PlainCredentials(
            self.username,
            self.password
        )
        
        return pika.ConnectionParameters(
            host=host or self.host,
            port=port or self.port,
            virtual_host=self.virtual_host,
            credentials=credentials,
            connection_attempts=self.connection_attempts,
            retry_delay=self.retry_delay,
            heartbeat=self.heartbeat,
            blocked_connection_timeout=self.blocked_connection_timeout
        )
    
    # =========================================================================
    # Connection Management
    # =========================================================================
    
    def connect(self) -> bool:
        """
        Establish connection to RabbitMQ.
        
        Returns:
            True if connection successful, False otherwise
        """
        with self._connection_lock:
            try:
                if self._connection and self._connection.is_open:
                    return True
                
                self._connection = pika.BlockingConnection(
                    self._get_connection_parameters()
                )
                self._logger.info(f"Connected to RabbitMQ at {self.host}:{self.port}")
                self._setup_dead_letter_queue()
                return True
                
            except AMQPConnectionError as e:
                self._logger.error(f"Failed to connect to RabbitMQ: {e}")
                return False
    
    def disconnect(self) -> None:
        """Close connection to RabbitMQ."""
        with self._connection_lock:
            if self._connection and self._connection.is_open:
                self._connection.close()
                self._logger.info("Disconnected from RabbitMQ")
            self._connection = None
    
    def is_connected(self) -> bool:
        """Check if connected to RabbitMQ."""
        return self._connection is not None and self._connection.is_open
    
    def reconnect(self) -> bool:
        """Attempt to reconnect to RabbitMQ."""
        self.disconnect()
        return self.connect()
    
    def get_connection(self) -> Optional[Any]:
        """Get the current connection."""
        with self._connection_lock:
            return self._connection
    
    # =========================================================================
    # Channel Management
    # =========================================================================
    
    def create_channel(
        self,
        channel_id: Optional[str] = None,
        confirm_delivery: bool = False
    ) -> Optional[Any]:
        """
        Create a new channel.
        
        Args:
            channel_id: Optional identifier for the channel
            confirm_delivery: Enable publisher confirms
        
        Returns:
            Channel object or None if creation fails
        """
        if not self.is_connected():
            if not self.connect():
                return None
        
        channel_id = channel_id or str(uuid.uuid4())
        
        with self._channel_lock:
            try:
                channel = self._connection.channel()
                
                if confirm_delivery:
                    channel.confirm_delivery()
                
                channel.basic_qos(prefetch_count=10)
                self._channels[channel_id] = channel
                self._logger.debug(f"Created channel: {channel_id}")
                return channel
                
            except AMQPChannelError as e:
                self._logger.error(f"Failed to create channel: {e}")
                return None
    
    def get_channel(self, channel_id: str) -> Optional[Any]:
        """Get a channel by ID."""
        with self._channel_lock:
            return self._channels.get(channel_id)
    
    def close_channel(self, channel_id: str) -> None:
        """Close a specific channel."""
        with self._channel_lock:
            if channel_id in self._channels:
                try:
                    self._channels[channel_id].close()
                except Exception as e:
                    self._logger.warning(f"Error closing channel {channel_id}: {e}")
                finally:
                    del self._channels[channel_id]
                    self._logger.debug(f"Closed channel: {channel_id}")
    
    def close_all_channels(self) -> None:
        """Close all channels."""
        with self._channel_lock:
            for channel_id in list(self._channels.keys()):
                try:
                    self._channels[channel_id].close()
                except Exception:
                    pass
            self._channels.clear()
            self._logger.info("Closed all channels")
    
    def _acquire_channel_from_pool(self) -> Optional[Any]:
        """Acquire a channel from the pool."""
        try:
            return self._channel_pool.get_nowait()
        except Empty:
            return self.create_channel()
    
    def _release_channel_to_pool(self, channel: Any) -> None:
        """Release a channel back to the pool."""
        try:
            if channel and channel.is_open:
                self._channel_pool.put_nowait(channel)
        except Exception:
            pass
    
    # =========================================================================
    # Exchange Management
    # =========================================================================
    
    def declare_exchange(
        self,
        name: str,
        exchange_type: ExchangeType = ExchangeType.DIRECT,
        durable: bool = True,
        auto_delete: bool = False,
        arguments: Optional[Dict[str, Any]] = None,
        channel: Optional[Any] = None
    ) -> bool:
        """
        Declare an exchange.
        
        Args:
            name: Exchange name
            exchange_type: Type of exchange
            durable: Enable durable exchange
            auto_delete: Auto-delete when no longer in use
            arguments: Additional arguments
            channel: Specific channel to use
        
        Returns:
            True if successful
        """
        ch = channel or self._acquire_channel_from_pool()
        if not ch:
            return False
        
        try:
            ch.exchange_declare(
                exchange=name,
                exchange_type=exchange_type.value,
                durable=durable,
                auto_delete=auto_delete,
                arguments=arguments or {}
            )
            self._exchanges.add(name)
            self._logger.debug(f"Declared exchange: {name} ({exchange_type.value})")
            
            if not channel:
                self._release_channel_to_pool(ch)
            return True
            
        except AMQPChannelError as e:
            self._logger.error(f"Failed to declare exchange {name}: {e}")
            if not channel:
                self._release_channel_to_pool(ch)
            return False
    
    def delete_exchange(self, name: str, if_unused: bool = False) -> bool:
        """Delete an exchange."""
        if not self.is_connected():
            return False
        
        try:
            with self._channel_lock:
                for channel in self._channels.values():
                    try:
                        channel.exchange_delete(exchange=name, if_unused=if_unused)
                    except Exception:
                        pass
            self._exchanges.discard(name)
            self._logger.debug(f"Deleted exchange: {name}")
            return True
        except Exception as e:
            self._logger.error(f"Failed to delete exchange {name}: {e}")
            return False
    
    def get_declared_exchanges(self) -> Set[str]:
        """Get set of declared exchanges."""
        return self._exchanges.copy()
    
    # =========================================================================
    # Queue Management
    # =========================================================================
    
    def declare_queue(
        self,
        name: str,
        durable: bool = True,
        exclusive: bool = False,
        auto_delete: bool = False,
        arguments: Optional[Dict[str, Any]] = None,
        mode: QueueMode = QueueMode.CLASSIC,
        channel: Optional[Any] = None
    ) -> bool:
        """
        Declare a queue.
        
        Args:
            name: Queue name
            durable: Enable durable queue
            exclusive: Exclusive access
            auto_delete: Auto-delete when consumer disconnects
            arguments: Additional arguments
            mode: Queue mode (classic or quorum)
            channel: Specific channel to use
        
        Returns:
            True if successful
        """
        ch = channel or self._acquire_channel_from_pool()
        if not ch:
            return False
        
        queue_args = arguments or {}
        if mode == QueueMode.QUORUM:
            queue_args["x-queue-type"] = "quorum"
        
        try:
            ch.queue_declare(
                queue=name,
                durable=durable,
                exclusive=exclusive,
                auto_delete=auto_delete,
                arguments=queue_args
            )
            self._queues.add(name)
            self._logger.debug(f"Declared queue: {name} (mode: {mode.value})")
            
            if not channel:
                self._release_channel_to_pool(ch)
            return True
            
        except AMQPChannelError as e:
            self._logger.error(f"Failed to declare queue {name}: {e}")
            if not channel:
                self._release_channel_to_pool(ch)
            return False
    
    def delete_queue(self, name: str, if_unused: bool = False, if_empty: bool = False) -> bool:
        """Delete a queue."""
        if not self.is_connected():
            return False
        
        with self._channel_lock:
            for channel in self._channels.values():
                try:
                    channel.queue_delete(
                        queue=name,
                        if_unused=if_unused,
                        if_empty=if_empty
                    )
                except Exception:
                    pass
        
        self._queues.discard(name)
        self._logger.debug(f"Deleted queue: {name}")
        return True
    
    def purge_queue(self, name: str) -> bool:
        """Purge all messages from a queue."""
        channel = self._acquire_channel_from_pool()
        if not channel:
            return False
        
        try:
            channel.queue_purge(queue=name)
            self._logger.debug(f"Purged queue: {name}")
            return True
        except Exception as e:
            self._logger.error(f"Failed to purge queue {name}: {e}")
            return False
        finally:
            self._release_channel_to_pool(channel)
    
    def get_declared_queues(self) -> Set[str]:
        """Get set of declared queues."""
        return self._queues.copy()
    
    def get_queue_info(self, name: str) -> Optional[Dict[str, Any]]:
        """Get information about a queue."""
        channel = self._acquire_channel_from_pool()
        if not channel:
            return None
        
        try:
            result = channel.queue_declare(queue=name, passive=True)
            return {
                "name": result.method.queue,
                "message_count": result.method.message_count,
                "consumer_count": result.method.consumer_count
            }
        except Exception as e:
            self._logger.error(f"Failed to get queue info for {name}: {e}")
            return None
        finally:
            self._release_channel_to_pool(channel)
    
    # =========================================================================
    # Binding Management
    # =========================================================================
    
    def bind_queue(
        self,
        queue: str,
        exchange: str,
        routing_key: str = "",
        arguments: Optional[Dict[str, Any]] = None,
        channel: Optional[Any] = None
    ) -> bool:
        """
        Bind a queue to an exchange.
        
        Args:
            queue: Queue name
            exchange: Exchange name
            routing_key: Routing key for binding
            arguments: Additional binding arguments
            channel: Specific channel to use
        
        Returns:
            True if successful
        """
        ch = channel or self._acquire_channel_from_pool()
        if not ch:
            return False
        
        try:
            ch.queue_bind(
                queue=queue,
                exchange=exchange,
                routing_key=routing_key,
                arguments=arguments or {}
            )
            binding = BindingConfig(
                queue=queue,
                exchange=exchange,
                routing_key=routing_key,
                arguments=arguments or {}
            )
            self._bindings.append(binding)
            self._logger.debug(f"Bound queue {queue} to exchange {exchange} with key '{routing_key}'")
            
            if not channel:
                self._release_channel_to_pool(ch)
            return True
            
        except AMQPChannelError as e:
            self._logger.error(f"Failed to bind queue {queue} to exchange {exchange}: {e}")
            if not channel:
                self._release_channel_to_pool(ch)
            return False
    
    def unbind_queue(
        self,
        queue: str,
        exchange: str,
        routing_key: str = "",
        arguments: Optional[Dict[str, Any]] = None
    ) -> bool:
        """Unbind a queue from an exchange."""
        if not self.is_connected():
            return False
        
        channel = self._acquire_channel_from_pool()
        if not channel:
            return False
        
        try:
            channel.queue_unbind(
                queue=queue,
                exchange=exchange,
                routing_key=routing_key,
                arguments=arguments or {}
            )
            self._bindings = [
                b for b in self._bindings
                if not (b.queue == queue and b.exchange == exchange and b.routing_key == routing_key)
            ]
            self._logger.debug(f"Unbound queue {queue} from exchange {exchange}")
            return True
        except Exception as e:
            self._logger.error(f"Failed to unbind queue {queue}: {e}")
            return False
        finally:
            self._release_channel_to_pool(channel)
    
    def get_bindings(self) -> List[BindingConfig]:
        """Get all bindings."""
        return copy.deepcopy(self._bindings)
    
    # =========================================================================
    # Dead Letter Queue
    # =========================================================================
    
    def _setup_dead_letter_queue(self) -> None:
        """Setup dead letter exchange and queue."""
        if not self.dlq_config:
            return
        
        self.declare_exchange(
            name=self.dlq_config.exchange,
            exchange_type=ExchangeType.DIRECT,
            durable=True
        )
        
        queue_args = {}
        if self.dlq_config.ttl:
            queue_args["x-message-ttl"] = self.dlq_config.ttl
        
        self.declare_queue(
            name=self.dlq_config.queue,
            durable=True,
            arguments=queue_args
        )
        
        self.bind_queue(
            queue=self.dlq_config.queue,
            exchange=self.dlq_config.exchange,
            routing_key=self.dlq_config.routing_key
        )
        
        self._logger.info(f"Dead letter queue configured: {self.dlq_config.queue}")
    
    def enable_dlq_for_queue(
        self,
        queue: str,
        max_retries: int = 3,
        ttl: Optional[int] = None
    ) -> bool:
        """
        Enable dead letter queue for a specific queue.
        
        Args:
            queue: Queue name
            max_retries: Maximum retry attempts before DLQ
            ttl: Message TTL in milliseconds
        
        Returns:
            True if successful
        """
        arguments = {
            "x-dead-letter-exchange": self.dlq_config.exchange,
            "x-dead-letter-routing-key": self.dlq_config.routing_key
        }
        
        if ttl:
            arguments["x-message-ttl"] = ttl
        
        return self.declare_queue(
            name=queue,
            durable=True,
            arguments=arguments
        )
    
    def get_dlq_message_count(self) -> int:
        """Get the number of messages in the dead letter queue."""
        info = self.get_queue_info(self.dlq_config.queue)
        return info.get("message_count", 0) if info else 0
    
    def consume_dlq(self, callback: Callable[[Message], None]) -> None:
        """Consume messages from the dead letter queue."""
        self.consume(
            queue=self.dlq_config.queue,
            callback=callback,
            auto_ack=False
        )
    
    # =========================================================================
    # Message Publishing
    # =========================================================================
    
    def publish(
        self,
        message: Union[str, bytes, Dict],
        exchange: str,
        routing_key: str = "",
        properties: Optional[MessageProperties] = None,
        mandatory: bool = False,
        channel: Optional[Any] = None
    ) -> bool:
        """
        Publish a message to an exchange.
        
        Args:
            message: Message body (string, bytes, or dict)
            exchange: Exchange name
            routing_key: Routing key
            properties: Message properties
            mandatory: Require the message to be routed
            channel: Specific channel to use
        
        Returns:
            True if message was published
        """
        ch = channel or self._acquire_channel_from_pool()
        if not ch:
            return False
        
        try:
            if isinstance(message, dict):
                body = json.dumps(message).encode("utf-8")
                content_type = "application/json"
            elif isinstance(message, str):
                body = message.encode("utf-8")
                content_type = "text/plain"
            else:
                body = message
                content_type = properties.content_type if properties else "application/octet-stream"
            
            props = properties or MessageProperties()
            if not props.message_id:
                props.message_id = str(uuid.uuid4())
            if not props.timestamp:
                props.timestamp = datetime.utcnow()
            
            amqp_props = pika.BasicProperties(
                content_type=props.content_type,
                delivery_mode=props.delivery_mode,
                priority=props.priority,
                message_id=props.message_id,
                timestamp=int(props.timestamp.timestamp()),
                headers=props.headers,
                reply_to=props.reply_to,
                correlation_id=props.correlation_id,
                expiration=props.expiration
            )
            
            ch.basic_publish(
                exchange=exchange,
                routing_key=routing_key,
                body=body,
                properties=amqp_props,
                mandatory=mandatory
            )
            
            self._logger.debug(f"Published message to {exchange}/{routing_key}")
            return True
            
        except AMQPChannelError as e:
            self._logger.error(f"Failed to publish message: {e}")
            return False
        finally:
            if not channel:
                self._release_channel_to_pool(ch)
    
    def publish_batch(
        self,
        messages: List[Dict[str, Any]],
        exchange: str,
        routing_key: str = ""
    ) -> int:
        """
        Publish multiple messages in a batch.
        
        Args:
            messages: List of message dicts
            exchange: Exchange name
            routing_key: Routing key
        
        Returns:
            Number of successfully published messages
        """
        success_count = 0
        channel = self.create_channel()
        
        if not channel:
            return 0
        
        try:
            for msg in messages:
                if self.publish(
                    message=msg.get("body", msg),
                    exchange=exchange,
                    routing_key=routing_key,
                    properties=MessageProperties(**msg.get("properties", {})),
                    channel=channel
                ):
                    success_count += 1
            return success_count
        finally:
            self.close_channel(channel.channel_number)
    
    # =========================================================================
    # Message Consuming
    # =========================================================================
    
    def consume(
        self,
        queue: str,
        callback: Callable[[Message], None],
        auto_ack: bool = False,
        consumer_tag: Optional[str] = None,
        exclusive: bool = False
    ) -> Optional[str]:
        """
        Start consuming messages from a queue.
        
        Args:
            queue: Queue name
            callback: Function to call for each message
            auto_ack: Automatic acknowledgment
            consumer_tag: Optional consumer tag
            exclusive: Exclusive consumer
        
        Returns:
            Consumer tag or None if failed
        """
        if not self.is_connected():
            if not self.connect():
                return None
        
        consumer_tag = consumer_tag or str(uuid.uuid4())
        
        def on_message(channel, method, properties, body):
            msg = Message(
                body=body,
                routing_key=method.routing_key,
                delivery_tag=method.delivery_tag,
                message_id=properties.message_id,
                correlation_id=properties.correlation_id,
                reply_to=properties.reply_to,
                headers=properties.headers or {},
                timestamp=datetime.fromtimestamp(properties.timestamp) if properties.timestamp else None,
                redelivered=method.redelivered,
                properties=MessageProperties(
                    content_type=properties.content_type,
                    delivery_mode=properties.delivery_mode,
                    priority=properties.priority,
                    message_id=properties.message_id,
                    headers=properties.headers or {},
                    reply_to=properties.reply_to,
                    correlation_id=properties.correlation_id,
                    expiration=properties.expiration
                )
            )
            
            try:
                callback(msg)
                if not auto_ack:
                    channel.basic_ack(delivery_tag=method.delivery_tag)
            except Exception as e:
                self._logger.error(f"Error processing message: {e}")
                if not auto_ack:
                    channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
        
        try:
            channel = self._acquire_channel_from_pool()
            if not channel:
                return None
            
            channel.basic_consume(
                queue=queue,
                on_message_callback=on_message,
                auto_ack=auto_ack,
                consumer_tag=consumer_tag,
                exclusive=exclusive
            )
            
            self._consumers[consumer_tag] = callback
            self._logger.info(f"Started consuming from queue: {queue} (tag: {consumer_tag})")
            return consumer_tag
            
        except AMQPChannelError as e:
            self._logger.error(f"Failed to start consuming: {e}")
            return None
    
    def start_consuming(self, blocking: bool = True) -> None:
        """
        Start the consumer threads.
        
        Args:
            blocking: Run in blocking mode
        """
        self._running = True
        
        if blocking:
            with self._channel_lock:
                for channel in self._channels.values():
                    try:
                        channel.start_consuming()
                    except Exception as e:
                        self._logger.error(f"Error in consumer: {e}")
    
    def stop_consuming(self, consumer_tag: Optional[str] = None) -> None:
        """
        Stop consuming messages.
        
        Args:
            consumer_tag: Specific consumer tag to stop, or None for all
        """
        self._running = False
        
        if consumer_tag:
            if consumer_tag in self._consumers:
                del self._consumers[consumer_tag]
        else:
            self._consumers.clear()
        
        with self._channel_lock:
            for channel in self._channels.values():
                try:
                    channel.stop_consuming()
                except Exception:
                    pass
        
        self._logger.info(f"Stopped consuming (tag: {consumer_tag or 'all'})")
    
    def acknowledge(self, delivery_tag: int, channel: Optional[Any] = None) -> bool:
        """Acknowledge a message."""
        ch = channel or self._acquire_channel_from_pool()
        if not ch:
            return False
        
        try:
            ch.basic_ack(delivery_tag=delivery_tag)
            return True
        except Exception as e:
            self._logger.error(f"Failed to acknowledge message: {e}")
            return False
        finally:
            if not channel:
                self._release_channel_to_pool(ch)
    
    def reject(self, delivery_tag: int, requeue: bool = True) -> bool:
        """Reject a message."""
        channel = self._acquire_channel_from_pool()
        if not channel:
            return False
        
        try:
            channel.basic_nack(delivery_tag=delivery_tag, requeue=requeue)
            return True
        except Exception as e:
            self._logger.error(f"Failed to reject message: {e}")
            return False
        finally:
            self._release_channel_to_pool(channel)
    
    # =========================================================================
    # RPC (Request/Reply Pattern)
    # =========================================================================
    
    def rpc_call(
        self,
        exchange: str,
        routing_key: str,
        request: Any,
        reply_queue: Optional[str] = None,
        timeout: Optional[float] = None,
        properties: Optional[MessageProperties] = None
    ) -> Optional[Any]:
        """
        Make an RPC call and wait for response.
        
        Args:
            exchange: Exchange to publish to
            routing_key: Routing key
            request: Request body
            reply_queue: Reply queue name
            timeout: Timeout in seconds
            properties: Message properties
        
        Returns:
            Response body or None if failed
        """
        timeout = timeout or self._rpc_timeout
        reply_queue = reply_queue or f"rpc_reply_{uuid.uuid4().hex[:8]}"
        correlation_id = str(uuid.uuid4())
        
        response_event = threading.Event()
        response_container: List[Any] = [None]
        
        def on_response(message: Message):
            response_container[0] = message.body
            response_event.set()
        
        self.declare_queue(name=reply_queue, durable=False, exclusive=True, auto_delete=True)
        
        with self._rpc_lock:
            self._rpc_callbacks[correlation_id] = on_response
        
        try:
            props = properties or MessageProperties()
            props.correlation_id = correlation_id
            props.reply_to = reply_queue
            
            if not self.publish(
                message=request,
                exchange=exchange,
                routing_key=routing_key,
                properties=props
            ):
                return None
            
            self.consume(
                queue=reply_queue,
                callback=on_response,
                auto_ack=True,
                consumer_tag=f"rpc_consumer_{correlation_id}"
            )
            
            if response_event.wait(timeout=timeout):
                return response_container[0]
            else:
                self._logger.warning(f"RPC call timed out after {timeout}s")
                return None
                
        finally:
            with self._rpc_lock:
                self._rpc_callbacks.pop(correlation_id, None)
            self.stop_consuming(consumer_tag=f"rpc_consumer_{correlation_id}")
            self.delete_queue(reply_queue)
    
    def rpc_server(
        self,
        queue: str,
        handler: Callable[[Any], Any],
        exchange: Optional[str] = None,
        routing_key: Optional[str] = ""
    ) -> None:
        """
        Start an RPC server to handle requests.
        
        Args:
            queue: Queue to consume from
            handler: Function to handle requests
            exchange: Optional exchange to declare
            routing_key: Routing key for binding
        """
        if exchange:
            self.declare_exchange(name=exchange, exchange_type=ExchangeType.DIRECT)
            self.declare_queue(name=queue, durable=True)
            self.bind_queue(queue=queue, exchange=exchange, routing_key=routing_key)
        else:
            self.declare_queue(name=queue, durable=True)
        
        def on_request(message: Message):
            try:
                request_body = message.body
                if isinstance(request_body, bytes):
                    request_body = json.loads(request_body.decode("utf-8"))
                
                response = handler(request_body)
                
                if message.properties and message.properties.reply_to:
                    reply_props = MessageProperties(
                        correlation_id=message.properties.correlation_id
                    )
                    
                    if isinstance(response, dict):
                        self.publish(
                            message=json.dumps(response),
                            exchange="",
                            routing_key=message.properties.reply_to,
                            properties=reply_props
                        )
                    else:
                        self.publish(
                            message=response,
                            exchange="",
                            routing_key=message.properties.reply_to,
                            properties=reply_props
                        )
                
                self.acknowledge(message.delivery_tag)
                
            except Exception as e:
                self._logger.error(f"RPC handler error: {e}")
                self.reject(message.delivery_tag, requeue=True)
        
        self.consume(queue=queue, callback=on_request, auto_ack=False)
        self._logger.info(f"RPC server started on queue: {queue}")
    
    # =========================================================================
    # Clustering
    # =========================================================================
    
    def add_cluster_node(self, node: ClusterNode) -> None:
        """Add a node to the cluster configuration."""
        if node not in self.cluster_nodes:
            self.cluster_nodes.append(node)
            self._logger.info(f"Added cluster node: {node.host}:{node.port}")
    
    def remove_cluster_node(self, node: ClusterNode) -> None:
        """Remove a node from the cluster configuration."""
        if node in self.cluster_nodes:
            self.cluster_nodes.remove(node)
            self._logger.info(f"Removed cluster node: {node.host}:{node.port}")
    
    def get_cluster_nodes(self) -> List[ClusterNode]:
        """Get all cluster nodes."""
        return copy.deepcopy(self.cluster_nodes)
    
    def connect_to_cluster(self) -> bool:
        """
        Connect to RabbitMQ cluster with automatic failover.
        
        Returns:
            True if connected to any node
        """
        if not self.use_clustering or not self.cluster_nodes:
            return self.connect()
        
        for node in self.cluster_nodes:
            try:
                self._connection = pika.BlockingConnection(
                    self._get_connection_parameters(host=node.host, port=node.port)
                )
                self._logger.info(f"Connected to cluster node: {node.host}:{node.port}")
                self._setup_dead_letter_queue()
                return True
            except AMQPConnectionError:
                self._logger.warning(f"Failed to connect to cluster node: {node.host}:{node.port}")
                continue
        
        self._logger.error("Failed to connect to any cluster node")
        return False
    
    def get_cluster_status(self) -> Dict[str, Any]:
        """
        Get cluster status information.
        
        Returns:
            Dict with cluster status
        """
        status = {
            "connected": self.is_connected(),
            "host": self.host,
            "port": self.port,
            "clustering_enabled": self.use_clustering,
            "cluster_nodes": [
                {
                    "host": n.host,
                    "port": n.port,
                    "node_type": n.node_type,
                    "is_master": n.is_master
                }
                for n in self.cluster_nodes
            ]
        }
        
        if self.is_connected():
            try:
                channel = self._acquire_channel_from_pool()
                if channel:
                    result = channel.queue_declare(queue="cluster_health_check", passive=True)
                    self._release_channel_to_pool(channel)
                    status["channel_health"] = "ok"
                else:
                    status["channel_health"] = "failed"
            except Exception:
                status["channel_health"] = "degraded"
        
        return status
    
    def failover_to_node(self, node: ClusterNode) -> bool:
        """
        Failover to a specific cluster node.
        
        Args:
            node: Target node
        
        Returns:
            True if failover successful
        """
        self._logger.info(f"Failing over to node: {node.host}:{node.port}")
        
        self.close_all_channels()
        self.disconnect()
        
        try:
            self._connection = pika.BlockingConnection(
                self._get_connection_parameters(host=node.host, port=node.port)
            )
            self._logger.info(f"Successfully failed over to: {node.host}:{node.port}")
            self._setup_dead_letter_queue()
            return True
        except AMQPConnectionError as e:
            self._logger.error(f"Failover failed: {e}")
            return self.connect()
    
    def is_master_node(self, node: ClusterNode) -> bool:
        """Check if a node is the master in the cluster."""
        return node.is_master
    
    def get_optimal_node(self) -> Optional[ClusterNode]:
        """
        Get the optimal node for connection based on cluster configuration.
        
        Returns:
            Preferred node or None
        """
        for node in self.cluster_nodes:
            if node.is_master:
                return node
        
        return self.cluster_nodes[0] if self.cluster_nodes else None
    
    # =========================================================================
    # Utility Methods
    # =========================================================================
    
    def health_check(self) -> Dict[str, Any]:
        """
        Perform health check on RabbitMQ connection.
        
        Returns:
            Health status dict
        """
        return {
            "status": "healthy" if self.is_connected() else "unhealthy",
            "connection": self.is_connected(),
            "channels": len(self._channels),
            "exchanges": len(self._exchanges),
            "queues": len(self._queues),
            "bindings": len(self._bindings),
            "consumers": len(self._consumers)
        }
    
    def reset(self) -> None:
        """Reset all connections, channels, and local state."""
        self.stop_consuming()
        self.close_all_channels()
        self.disconnect()
        
        self._exchanges.clear()
        self._queues.clear()
        self._bindings.clear()
        self._consumers.clear()
        self._rpc_callbacks.clear()
        
        self._logger.info("RabbitMQ integration reset")
    
    def __enter__(self) -> "RabbitMQIntegration":
        """Context manager entry."""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager exit."""
        self.reset()
