"""
ActiveMQ Message Broker Integration Module for Workflow System

Implements an ActiveMQIntegration class with:
1. Broker management: Manage ActiveMQ brokers
2. Queue management: Create/manage queues
3. Topic management: Create/manage topics
4. Producer management: Manage producers
5. Consumer management: Manage consumers
6. Message handling: Send/receive messages
7. Advisory topics: Monitor broker advisories
8. Security: User and permission management
9. Network of brokers: Configure broker networks
10. Persistence: Configure message persistence

Commit: 'feat(activemq): add ActiveMQ integration with broker management, queues, topics, producers, consumers, message handling, advisories, security, broker networks, persistence'
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
    import stomp
    from stomp.exception import StompConnectionError, StompDataError
    STOMP_AVAILABLE = True
except ImportError:
    STOMP_AVAILABLE = False
    stomp = None


logger = logging.getLogger(__name__)


class AckMode(Enum):
    """ActiveMQ acknowledgment modes."""
    AUTO = "auto"
    CLIENT = "client"
    INDIVIDUAL = "individual"


class DestinationType(Enum):
    """ActiveMQ destination types."""
    QUEUE = "queue"
    TOPIC = "topic"
    TEMP_QUEUE = "temp-queue"
    TEMP_TOPIC = "temp-topic"


class MessageDeliveryMode(Enum):
    """Message delivery modes."""
    NON_PERSISTENT = 1
    PERSISTENT = 2


class MessagePriority(Enum):
    """Message priority levels (0-9)."""
    LOWEST = 0
    LOW = 2
    NORMAL = 4
    HIGH = 7
    HIGHEST = 9


@dataclass
class BrokerConfig:
    """Configuration for an ActiveMQ broker."""
    host: str = "localhost"
    port: int = 61613
    username: str = "admin"
    password: str = "admin"
    virtual_host: str = "/"
    ssl: bool = False
    timeout: float = 30.0
    heartbeat: int = 60000
    reconnect_attempts: int = 10
    reconnect_delay: float = 1.0


@dataclass
class QueueConfig:
    """Configuration for a queue."""
    name: str
    durable: bool = True
    auto_delete: bool = False
    exclusive: bool = False
    maxConsumers: int = -1
    maxMessages: int = -1
    selector: str = ""
    arguments: Dict[str, Any] = field(default_factory=dict)


@dataclass
class TopicConfig:
    """Configuration for a topic."""
    name: str
    durable: bool = True
    auto_delete: bool = False
    maxSubscribers: int = -1
    selector: str = ""
    arguments: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ProducerConfig:
    """Configuration for a producer."""
    destination: str
    destination_type: DestinationType = DestinationType.QUEUE
    delivery_mode: MessageDeliveryMode = MessageDeliveryMode.PERSISTENT
    priority: MessagePriority = MessagePriority.NORMAL
    expiration: int = 0
    timestamp: bool = True
    properties: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ConsumerConfig:
    """Configuration for a consumer."""
    destination: str
    destination_type: DestinationType = DestinationType.QUEUE
    ack_mode: AckMode = AckMode.AUTO
    selector: str = ""
    id: str = ""
    durable: bool = False
    subscription_name: str = ""
    max_pending_messages: int = 0
    arguments: Dict[str, Any] = field(default_factory=dict)


@dataclass
class NetworkConnectorConfig:
    """Configuration for a network connector (broker to broker)."""
    name: str
    uri: str
    duplex: bool = False
    decreasePriority: bool = True
    demandFlowControl: bool = True
    conduitSubscriptions: bool = True
    excluded: str = ""
    included: str = ""
    prefix: str = ""
    prefix_computed: bool = False


@dataclass
class SecurityConfig:
    """Configuration for broker security."""
    users: Dict[str, Dict[str, str]] = field(default_factory=dict)
    roles: Dict[str, List[str]] = field(default_factory=dict)
    plugins: List[str] = field(default_factory=list)


@dataclass
class PersistenceConfig:
    """Configuration for message persistence."""
    enabled: bool = True
    type: str = "kahaDB"
    journal_dir: str = "data/journal"
    persistence_dir: str = "data/kahadb"
    maxFileLength: int = 10485760
    checkpointInterval: int = 5000
    cleanupInterval: int = 30000


@dataclass
class AdvisoryConfig:
    """Configuration for advisory topics."""
    enabled: bool = True
    topic_prefix: str = "ActiveMQ.Advisory"
    connection: bool = True
    queue: bool = True
    topic: bool = True
    producer: bool = True
    consumer: bool = True
    slow: bool = True
    fast: bool = True


class ActiveMQConnection:
    """Wrapper for STOMP connection to ActiveMQ."""

    def __init__(self, config: BrokerConfig):
        self.config = config
        self.connection = None
        self.connected = False
        self.lock = threading.Lock()

    def connect(self) -> bool:
        """Establish connection to the broker."""
        if not STOMP_AVAILABLE:
            logger.error("STOMP library not available")
            return False

        with self.lock:
            try:
                if self.connection and self.connected:
                    return True

                host_and_ports = [(self.config.host, self.config.port)]
                
                if self.config.ssl:
                    self.connection = stomp.Connection(
                        host_and_ports=host_and_ports,
                        use_ssl=True,
                        ssl_key_file=None,
                        ssl_cert_file=None,
                        ssl_ca_file=None
                    )
                else:
                    self.connection = stomp.Connection(
                        host_and_ports=host_and_ports
                    )

                self.connection.set_ssl(
                    key_file=None,
                    cert_file=None,
                    ca_certs=None
                ) if self.config.ssl else None

                self.connection.connect(
                    username=self.config.username,
                    passcode=self.config.password,
                    wait=True,
                    timeout=self.config.timeout
                )

                self.connected = True
                logger.info(f"Connected to ActiveMQ broker at {self.config.host}:{self.config.port}")
                return True

            except Exception as e:
                logger.error(f"Failed to connect to ActiveMQ broker: {e}")
                self.connected = False
                return False

    def disconnect(self):
        """Disconnect from the broker."""
        with self.lock:
            if self.connection:
                try:
                    self.connection.disconnect()
                except Exception:
                    pass
                self.connection = None
                self.connected = False

    def is_connected(self) -> bool:
        """Check if connection is active."""
        return self.connected and self.connection is not None


class ActiveMQMessage:
    """Represents an ActiveMQ message."""

    def __init__(
        self,
        body: str = "",
        headers: Optional[Dict[str, Any]] = None,
        destination: str = "",
        message_id: Optional[str] = None
    ):
        self.body = body
        self.headers = headers or {}
        self.destination = destination
        self.message_id = message_id or str(uuid.uuid4())
        self.timestamp = int(time.time() * 1000)
        self.properties = {}

    def to_dict(self) -> Dict[str, Any]:
        """Convert message to dictionary."""
        return {
            "message_id": self.message_id,
            "destination": self.destination,
            "body": self.body,
            "headers": self.headers,
            "timestamp": self.timestamp,
            "properties": self.properties
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ActiveMQMessage":
        """Create message from dictionary."""
        msg = cls(
            body=data.get("body", ""),
            headers=data.get("headers", {}),
            destination=data.get("destination", ""),
            message_id=data.get("message_id")
        )
        msg.timestamp = data.get("timestamp", int(time.time() * 1000))
        msg.properties = data.get("properties", {})
        return msg


class ActiveMQIntegration:
    """
    ActiveMQ Message Broker Integration Class.

    Provides comprehensive ActiveMQ broker management including:
    - Broker connections and health monitoring
    - Queue and topic management
    - Producer and consumer management
    - Message send/receive operations
    - Advisory topic monitoring
    - Security and access control
    - Broker network configuration
    - Message persistence settings
    """

    def __init__(self, config: Optional[BrokerConfig] = None):
        """
        Initialize the ActiveMQ integration.

        Args:
            config: Broker configuration. Uses defaults if not provided.
        """
        self.config = config or BrokerConfig()
        self.connection = ActiveMQConnection(self.config)
        
        self._producers: Dict[str, ProducerConfig] = {}
        self._consumers: Dict[str, ConsumerConfig] = {}
        self._queues: Dict[str, QueueConfig] = {}
        self._topics: Dict[str, TopicConfig] = {}
        self._network_connectors: Dict[str, NetworkConnectorConfig] = {}
        self._security = SecurityConfig()
        self._persistence = PersistenceConfig()
        self._advisory = AdvisoryConfig()
        
        self._message_listeners: Dict[str, Callable] = {}
        self._advisory_listeners: List[Callable] = []
        self._pending_messages: Queue = Queue()
        self._running = False
        self._lock = threading.RLock()
        
        self._stats = {
            "messages_sent": 0,
            "messages_received": 0,
            "messages_acked": 0,
            "errors": 0,
            "reconnects": 0,
            "last_activity": None
        }

    def connect(self) -> bool:
        """
        Connect to the ActiveMQ broker.

        Returns:
            bool: True if connection successful, False otherwise.
        """
        with self._lock:
            success = self.connection.connect()
            if success:
                self._running = True
                self._setup_advisory_listeners()
            return success

    def disconnect(self):
        """Disconnect from the ActiveMQ broker."""
        with self._lock:
            self._running = False
            self.connection.disconnect()
            self._producers.clear()
            self._consumers.clear()

    def is_connected(self) -> bool:
        """Check if connected to the broker."""
        return self.connection.is_connected()

    def get_stats(self) -> Dict[str, Any]:
        """Get connection and messaging statistics."""
        return copy.deepcopy(self._stats)

    def reset_stats(self):
        """Reset statistics counters."""
        with self._lock:
            self._stats = {
                "messages_sent": 0,
                "messages_received": 0,
                "messages_acked": 0,
                "errors": 0,
                "reconnects": 0,
                "last_activity": None
            }

    # =========================================================================
    # Broker Management
    # =========================================================================

    def get_broker_info(self) -> Dict[str, Any]:
        """
        Get information about the connected broker.

        Returns:
            dict: Broker information including host, port, version, etc.
        """
        if not self.is_connected():
            return {"connected": False}

        return {
            "connected": True,
            "host": self.config.host,
            "port": self.config.port,
            "virtual_host": self.config.virtual_host,
            "username": self.config.username,
            "brokers": self._get_broker_subsystem_info(),
            "timestamp": datetime.now().isoformat()
        }

    def _get_broker_subsystem_info(self) -> Dict[str, Any]:
        """Get broker subsystem information."""
        return {
            "queues": len(self._queues),
            "topics": len(self._topics),
            "producers": len(self._producers),
            "consumers": len(self._consumers),
            "network_connectors": len(self._network_connectors)
        }

    def check_broker_health(self) -> Dict[str, Any]:
        """
        Check the health of the broker connection.

        Returns:
            dict: Health status information.
        """
        is_connected = self.is_connected()
        
        return {
            "healthy": is_connected,
            "connected": is_connected,
            "host": self.config.host,
            "port": self.config.port,
            "uptime_seconds": self._stats.get("uptime", 0),
            "messages_processed": self._stats["messages_sent"] + self._stats["messages_received"],
            "error_rate": self._calculate_error_rate(),
            "timestamp": datetime.now().isoformat()
        }

    def _calculate_error_rate(self) -> float:
        """Calculate the error rate based on stats."""
        total = self._stats["messages_sent"] + self._stats["messages_received"]
        if total == 0:
            return 0.0
        return self._stats["errors"] / total

    def reconnect(self) -> bool:
        """
        Attempt to reconnect to the broker.

        Returns:
            bool: True if reconnection successful.
        """
        with self._lock:
            self._stats["reconnects"] += 1
            self.disconnect()
            time.sleep(self.config.reconnect_delay)
            return self.connect()

    # =========================================================================
    # Queue Management
    # =========================================================================

    def create_queue(self, name: str, config: Optional[QueueConfig] = None) -> bool:
        """
        Create a queue on the broker.

        Args:
            name: Queue name.
            config: Optional queue configuration.

        Returns:
            bool: True if successful.
        """
        with self._lock:
            if name in self._queues:
                logger.warning(f"Queue {name} already exists")
                return True

            queue_config = config or QueueConfig(name=name)
            self._queues[name] = queue_config

            if self.is_connected():
                return self._send_queue_create(queue_config)
            return True

    def _send_queue_create(self, config: QueueConfig) -> bool:
        """Send queue creation command to broker."""
        try:
            headers = {
                "destination": f"/{config.name}",
                "x-queue-name": config.name,
                "x-queue-durable": str(config.durable).lower(),
                "x-queue-auto-delete": str(config.auto_delete).lower(),
            }
            
            if config.selector:
                headers["x-queue-selector"] = config.selector
            
            for key, value in config.arguments.items():
                headers[key] = str(value)

            self.connection.connection.send(
                headers=headers,
                body=""
            )
            logger.info(f"Queue {config.name} created")
            return True
        except Exception as e:
            logger.error(f"Failed to create queue {config.name}: {e}")
            self._stats["errors"] += 1
            return False

    def delete_queue(self, name: str) -> bool:
        """
        Delete a queue from the broker.

        Args:
            name: Queue name.

        Returns:
            bool: True if successful.
        """
        with self._lock:
            if name not in self._queues:
                logger.warning(f"Queue {name} not found")
                return False

            del self._queues[name]
            
            if self.is_connected():
                try:
                    headers = {
                        "destination": f"/{name}",
                        "x-queue-name": name,
                        "x-queue-purge": "false"
                    }
                    self.connection.connection.send(
                        headers=headers,
                        body=""
                    )
                except Exception as e:
                    logger.error(f"Failed to delete queue {name}: {e}")
                    return False
            
            return True

    def get_queue_info(self, name: str) -> Dict[str, Any]:
        """
        Get information about a queue.

        Args:
            name: Queue name.

        Returns:
            dict: Queue information.
        """
        if name not in self._queues:
            return {"exists": False}

        config = self._queues[name]
        return {
            "exists": True,
            "name": name,
            "durable": config.durable,
            "auto_delete": config.auto_delete,
            "exclusive": config.exclusive,
            "max_consumers": config.maxConsumers,
            "max_messages": config.maxMessages,
            "selector": config.selector,
            "arguments": config.arguments
        }

    def list_queues(self) -> List[str]:
        """
        List all known queues.

        Returns:
            list: Queue names.
        """
        return list(self._queues.keys())

    def get_queue_stats(self, name: str) -> Dict[str, Any]:
        """
        Get statistics for a queue.

        Args:
            name: Queue name.

        Returns:
            dict: Queue statistics.
        """
        if name not in self._queues:
            return {"exists": False}

        return {
            "name": name,
            "message_count": 0,
            "consumer_count": len([c for c in self._consumers.values() if c.destination == name]),
            "producer_count": len([p for p in self._producers.values() if p.destination == name]),
            "enqueue_count": 0,
            "dequeue_count": 0,
            "dispatch_count": 0
        }

    def purge_queue(self, name: str) -> bool:
        """
        Purge all messages from a queue.

        Args:
            name: Queue name.

        Returns:
            bool: True if successful.
        """
        with self._lock:
            if name not in self._queues:
                return False

            try:
                headers = {
                    "destination": f"/{name}",
                    "x-queue-name": name,
                    "x-queue-purge": "true"
                }
                self.connection.connection.send(
                    headers=headers,
                    body=""
                )
                logger.info(f"Queue {name} purged")
                return True
            except Exception as e:
                logger.error(f"Failed to purge queue {name}: {e}")
                return False

    # =========================================================================
    # Topic Management
    # =========================================================================

    def create_topic(self, name: str, config: Optional[TopicConfig] = None) -> bool:
        """
        Create a topic on the broker.

        Args:
            name: Topic name.
            config: Optional topic configuration.

        Returns:
            bool: True if successful.
        """
        with self._lock:
            if name in self._topics:
                logger.warning(f"Topic {name} already exists")
                return True

            topic_config = config or TopicConfig(name=name)
            self._topics[name] = topic_config

            if self.is_connected():
                return self._send_topic_create(topic_config)
            return True

    def _send_topic_create(self, config: TopicConfig) -> bool:
        """Send topic creation command to broker."""
        try:
            headers = {
                "destination": f"/{config.name}",
                "x-topic-name": config.name,
                "x-topic-durable": str(config.durable).lower(),
                "x-topic-auto-delete": str(config.auto_delete).lower(),
            }
            
            if config.selector:
                headers["x-topic-selector"] = config.selector
            
            for key, value in config.arguments.items():
                headers[key] = str(value)

            self.connection.connection.send(
                headers=headers,
                body=""
            )
            logger.info(f"Topic {config.name} created")
            return True
        except Exception as e:
            logger.error(f"Failed to create topic {config.name}: {e}")
            self._stats["errors"] += 1
            return False

    def delete_topic(self, name: str) -> bool:
        """
        Delete a topic from the broker.

        Args:
            name: Topic name.

        Returns:
            bool: True if successful.
        """
        with self._lock:
            if name not in self._topics:
                logger.warning(f"Topic {name} not found")
                return False

            del self._topics[name]
            return True

    def get_topic_info(self, name: str) -> Dict[str, Any]:
        """
        Get information about a topic.

        Args:
            name: Topic name.

        Returns:
            dict: Topic information.
        """
        if name not in self._topics:
            return {"exists": False}

        config = self._topics[name]
        return {
            "exists": True,
            "name": name,
            "durable": config.durable,
            "auto_delete": config.auto_delete,
            "max_subscribers": config.maxSubscribers,
            "selector": config.selector,
            "arguments": config.arguments
        }

    def list_topics(self) -> List[str]:
        """
        List all known topics.

        Returns:
            list: Topic names.
        """
        return list(self._topics.keys())

    def get_topic_stats(self, name: str) -> Dict[str, Any]:
        """
        Get statistics for a topic.

        Args:
            name: Topic name.

        Returns:
            dict: Topic statistics.
        """
        if name not in self._topics:
            return {"exists": False}

        return {
            "name": name,
            "subscriber_count": len([c for c in self._consumers.values() if c.destination == name]),
            "producer_count": len([p for p in self._producers.values() if p.destination == name]),
            "enqueue_count": 0,
            "dispatch_count": 0
        }

    # =========================================================================
    # Producer Management
    # =========================================================================

    def create_producer(
        self,
        producer_id: str,
        destination: str,
        destination_type: DestinationType = DestinationType.QUEUE,
        config: Optional[ProducerConfig] = None
    ) -> bool:
        """
        Create a message producer.

        Args:
            producer_id: Unique producer identifier.
            destination: Target destination (queue/topic name).
            destination_type: Type of destination.
            config: Optional producer configuration.

        Returns:
            bool: True if successful.
        """
        with self._lock:
            if producer_id in self._producers:
                logger.warning(f"Producer {producer_id} already exists")
                return True

            producer_config = config or ProducerConfig(
                destination=destination,
                destination_type=destination_type
            )
            producer_config.destination = destination
            producer_config.destination_type = destination_type
            
            self._producers[producer_id] = producer_config
            logger.info(f"Producer {producer_id} created for {destination_type.value}:{destination}")
            return True

    def delete_producer(self, producer_id: str) -> bool:
        """
        Delete a producer.

        Args:
            producer_id: Producer identifier.

        Returns:
            bool: True if successful.
        """
        with self._lock:
            if producer_id not in self._producers:
                return False

            del self._producers[producer_id]
            return True

    def get_producer_info(self, producer_id: str) -> Dict[str, Any]:
        """
        Get producer information.

        Args:
            producer_id: Producer identifier.

        Returns:
            dict: Producer information.
        """
        if producer_id not in self._producers:
            return {"exists": False}

        config = self._producers[producer_id]
        return {
            "exists": True,
            "id": producer_id,
            "destination": config.destination,
            "destination_type": config.destination_type.value,
            "delivery_mode": config.delivery_mode.value,
            "priority": config.priority.value,
            "expiration": config.expiration,
            "timestamp": config.timestamp
        }

    def list_producers(self) -> List[str]:
        """List all producer IDs."""
        return list(self._producers.keys())

    # =========================================================================
    # Consumer Management
    # =========================================================================

    def create_consumer(
        self,
        consumer_id: str,
        destination: str,
        destination_type: DestinationType = DestinationType.QUEUE,
        callback: Optional[Callable] = None,
        config: Optional[ConsumerConfig] = None
    ) -> bool:
        """
        Create a message consumer.

        Args:
            consumer_id: Unique consumer identifier.
            destination: Source destination (queue/topic name).
            destination_type: Type of destination.
            callback: Message handler callback.
            config: Optional consumer configuration.

        Returns:
            bool: True if successful.
        """
        with self._lock:
            if consumer_id in self._consumers:
                logger.warning(f"Consumer {consumer_id} already exists")
                return True

            consumer_config = config or ConsumerConfig(
                destination=destination,
                destination_type=destination_type
            )
            consumer_config.destination = destination
            consumer_config.destination_type = destination_type
            consumer_config.id = consumer_id
            
            self._consumers[consumer_id] = consumer_config
            
            if callback:
                self._message_listeners[consumer_id] = callback

            if self.is_connected():
                return self._subscribe_consumer(consumer_config)
            return True

    def _subscribe_consumer(self, config: ConsumerConfig) -> bool:
        """Subscribe a consumer to its destination."""
        try:
            dest_prefix = "/queue/" if config.destination_type == DestinationType.QUEUE else "/topic/"
            dest = f"{dest_prefix}{config.destination}"
            
            headers = {
                "destination": dest,
                "id": config.id or str(uuid.uuid4()),
                "ack": config.ack_mode.value
            }
            
            if config.selector:
                headers["selector"] = config.selector
            
            if config.durable and config.subscription_name:
                headers["persistent"] = "true"
                headers["sub-name"] = config.subscription_name

            if self.connection.connection:
                self.connection.connection.subscribe(
                    destination=dest,
                    id=config.id or str(uuid.uuid4()),
                    ack=config.ack_mode.value
                )
            
            logger.info(f"Consumer {config.id} subscribed to {dest}")
            return True
        except Exception as e:
            logger.error(f"Failed to subscribe consumer: {e}")
            self._stats["errors"] += 1
            return False

    def delete_consumer(self, consumer_id: str) -> bool:
        """
        Delete a consumer.

        Args:
            consumer_id: Consumer identifier.

        Returns:
            bool: True if successful.
        """
        with self._lock:
            if consumer_id not in self._consumers:
                return False

            config = self._consumers[consumer_id]
            
            if self.is_connected() and self.connection.connection:
                try:
                    dest_prefix = "/queue/" if config.destination_type == DestinationType.QUEUE else "/topic/"
                    dest = f"{dest_prefix}{config.destination}"
                    self.connection.connection.unsubscribe(
                        destination=dest,
                        id=config.id
                    )
                except Exception as e:
                    logger.error(f"Error unsubscribing consumer: {e}")

            del self._consumers[consumer_id]
            self._message_listeners.pop(consumer_id, None)
            return True

    def get_consumer_info(self, consumer_id: str) -> Dict[str, Any]:
        """
        Get consumer information.

        Args:
            consumer_id: Consumer identifier.

        Returns:
            dict: Consumer information.
        """
        if consumer_id not in self._consumers:
            return {"exists": False}

        config = self._consumers[consumer_id]
        return {
            "exists": True,
            "id": consumer_id,
            "destination": config.destination,
            "destination_type": config.destination_type.value,
            "ack_mode": config.ack_mode.value,
            "selector": config.selector,
            "durable": config.durable,
            "subscription_name": config.subscription_name,
            "has_callback": consumer_id in self._message_listeners
        }

    def list_consumers(self) -> List[str]:
        """List all consumer IDs."""
        return list(self._consumers.keys())

    def register_message_handler(self, consumer_id: str, callback: Callable) -> bool:
        """
        Register a message handler callback for a consumer.

        Args:
            consumer_id: Consumer identifier.
            callback: Handler function.

        Returns:
            bool: True if successful.
        """
        with self._lock:
            if consumer_id not in self._consumers:
                return False

            self._message_listeners[consumer_id] = callback
            return True

    # =========================================================================
    # Message Handling
    # =========================================================================

    def send_message(
        self,
        producer_id: str,
        body: str,
        headers: Optional[Dict[str, Any]] = None,
        properties: Optional[Dict[str, Any]] = None
    ) -> bool:
        """
        Send a message using a producer.

        Args:
            producer_id: Producer identifier.
            body: Message body.
            headers: Optional message headers.
            properties: Optional message properties.

        Returns:
            bool: True if message sent successfully.
        """
        with self._lock:
            if producer_id not in self._producers:
                logger.error(f"Producer {producer_id} not found")
                return False

            config = self._producers[producer_id]
            return self._do_send_message(
                destination=config.destination,
                destination_type=config.destination_type,
                body=body,
                headers=headers,
                properties=properties,
                delivery_mode=config.delivery_mode,
                priority=config.priority,
                expiration=config.expiration,
                timestamp=config.timestamp
            )

    def _do_send_message(
        self,
        destination: str,
        destination_type: DestinationType,
        body: str,
        headers: Optional[Dict[str, Any]] = None,
        properties: Optional[Dict[str, Any]] = None,
        delivery_mode: MessageDeliveryMode = MessageDeliveryMode.PERSISTENT,
        priority: MessagePriority = MessagePriority.NORMAL,
        expiration: int = 0,
        timestamp: bool = True
    ) -> bool:
        """Internal message sending implementation."""
        if not self.is_connected():
            logger.error("Not connected to broker")
            return False

        try:
            dest_prefix = "/queue/" if destination_type == DestinationType.QUEUE else "/topic/"
            dest = f"{dest_prefix}{destination}"

            msg_headers = {
                "destination": dest,
                "content-type": "text/plain",
                "delivery-mode": str(delivery_mode.value),
                "priority": str(priority.value),
                "message-id": str(uuid.uuid4())
            }

            if timestamp:
                msg_headers["timestamp"] = str(int(time.time() * 1000))

            if expiration > 0:
                msg_headers["expiration"] = str(int(time.time() * 1000) + expiration)

            if headers:
                msg_headers.update(headers)

            if properties:
                for key, value in properties.items():
                    msg_headers[f"property-{key}"] = str(value)

            self.connection.connection.send(
                headers=msg_headers,
                body=body
            )

            self._stats["messages_sent"] += 1
            self._stats["last_activity"] = datetime.now().isoformat()
            logger.debug(f"Message sent to {dest}")
            return True

        except Exception as e:
            logger.error(f"Failed to send message: {e}")
            self._stats["errors"] += 1
            return False

    def send_to_destination(
        self,
        destination: str,
        destination_type: DestinationType,
        body: str,
        headers: Optional[Dict[str, Any]] = None,
        properties: Optional[Dict[str, Any]] = None
    ) -> bool:
        """
        Send a message directly to a destination.

        Args:
            destination: Destination name.
            destination_type: Type of destination.
            body: Message body.
            headers: Optional message headers.
            properties: Optional message properties.

        Returns:
            bool: True if message sent successfully.
        """
        return self._do_send_message(
            destination=destination,
            destination_type=destination_type,
            body=body,
            headers=headers,
            properties=properties
        )

    def receive_message(
        self,
        consumer_id: str,
        timeout: float = 1.0
    ) -> Optional[ActiveMQMessage]:
        """
        Receive a message for a consumer.

        Args:
            consumer_id: Consumer identifier.
            timeout: Receive timeout in seconds.

        Returns:
            ActiveMQMessage or None if no message available.
        """
        try:
            return self._pending_messages.get(timeout=timeout)
        except Empty:
            return None

    def ack_message(self, consumer_id: str, message_id: str) -> bool:
        """
        Acknowledge a message.

        Args:
            consumer_id: Consumer identifier.
            message_id: Message ID to acknowledge.

        Returns:
            bool: True if successful.
        """
        if consumer_id not in self._consumers:
            return False

        try:
            if self.connection.connection:
                config = self._consumers[consumer_id]
                dest_prefix = "/queue/" if config.destination_type == DestinationType.QUEUE else "/topic/"
                dest = f"{dest_prefix}{config.destination}"
                
                self.connection.connection.ack(
                    headers={
                        "destination": dest,
                        "message-id": message_id,
                        "subscription": config.id
                    }
                )
                self._stats["messages_acked"] += 1
                return True
        except Exception as e:
            logger.error(f"Failed to ack message: {e}")
            return False

    def handle_received_message(self, headers: Dict, body: str):
        """Handle a received message from the broker."""
        try:
            dest = headers.get("destination", "")
            msg_id = headers.get("message-id", str(uuid.uuid4()))

            consumer_id = self._find_consumer_for_destination(dest)
            
            message = ActiveMQMessage(
                body=body,
                headers=headers,
                destination=dest,
                message_id=msg_id
            )

            self._stats["messages_received"] += 1
            self._stats["last_activity"] = datetime.now().isoformat()

            if consumer_id and consumer_id in self._message_listeners:
                callback = self._message_listeners[consumer_id]
                try:
                    callback(message)
                except Exception as e:
                    logger.error(f"Error in message handler: {e}")
            else:
                self._pending_messages.put(message)

        except Exception as e:
            logger.error(f"Error handling received message: {e}")
            self._stats["errors"] += 1

    def _find_consumer_for_destination(self, dest: str) -> Optional[str]:
        """Find consumer ID for a destination."""
        for cid, config in self._consumers.items():
            expected = f"/queue/{config.destination}" if config.destination_type == DestinationType.QUEUE else f"/topic/{config.destination}"
            if expected == dest:
                return cid
        return None

    # =========================================================================
    # Advisory Topics
    # =========================================================================

    def setup_advisories(self, config: Optional[AdvisoryConfig] = None) -> bool:
        """
        Configure and enable advisory topics.

        Args:
            config: Advisory configuration.

        Returns:
            bool: True if successful.
        """
        with self._lock:
            if config:
                self._advisory = config
            return True

    def _setup_advisory_listeners(self):
        """Set up listeners for advisory topics."""
        if not self._advisory.enabled:
            return

        try:
            advisory_topics = []

            if self._advisory.connection:
                advisory_topics.append(f"{self._advisory.topic_prefix}.Connection")
            
            if self._advisory.queue:
                advisory_topics.append(f"{self._advisory.topic_prefix}.Queue")
            
            if self._advisory.topic:
                advisory_topics.append(f"{self._advisory.topic_prefix}.Topic")
            
            if self._advisory.producer:
                advisory_topics.append(f"{self._advisory.topic_prefix}.Producer")
            
            if self._advisory.consumer:
                advisory_topics.append(f"{self._advisory.topic_prefix}.Consumer")
            
            if self._advisory.slow:
                advisory_topics.append(f"{self._advisory.topic_prefix}.Slow")
            
            if self._advisory.fast:
                advisory_topics.append(f"{self._advisory.topic_prefix}.Fast")

            for topic in advisory_topics:
                if self.connection.connection:
                    self.connection.connection.subscribe(
                        destination=f"/topic/{topic}",
                        id=f"advisory-{uuid.uuid4()}",
                        ack="auto"
                    )

            logger.info(f"Advisory listeners set up for {len(advisory_topics)} topics")

        except Exception as e:
            logger.error(f"Failed to set up advisory listeners: {e}")

    def register_advisory_handler(self, callback: Callable):
        """
        Register a handler for advisory messages.

        Args:
            callback: Handler function for advisories.
        """
        with self._lock:
            self._advisory_listeners.append(callback)

    def get_advisory_info(self) -> Dict[str, Any]:
        """
        Get advisory configuration and status.

        Returns:
            dict: Advisory information.
        """
        return {
            "enabled": self._advisory.enabled,
            "topic_prefix": self._advisory.topic_prefix,
            "connection_advisory": self._advisory.connection,
            "queue_advisory": self._advisory.queue,
            "topic_advisory": self._advisory.topic,
            "producer_advisory": self._advisory.producer,
            "consumer_advisory": self._advisory.consumer,
            "slow_advisory": self._advisory.slow,
            "fast_advisory": self._advisory.fast,
            "listener_count": len(self._advisory_listeners)
        }

    # =========================================================================
    # Security
    # =========================================================================

    def configure_security(self, config: SecurityConfig) -> bool:
        """
        Configure broker security settings.

        Args:
            config: Security configuration.

        Returns:
            bool: True if successful.
        """
        with self._lock:
            self._security = config
            logger.info(f"Security configured with {len(config.users)} users")
            return True

    def add_user(self, username: str, password: str, groups: Optional[List[str]] = None) -> bool:
        """
        Add a user to the security configuration.

        Args:
            username: Username.
            password: Password.
            groups: Optional list of groups/roles.

        Returns:
            bool: True if successful.
        """
        with self._lock:
            self._security.users[username] = {
                "password": password,
                "groups": groups or []
            }

            for group in (groups or []):
                if group not in self._security.roles:
                    self._security.roles[group] = []
                if username not in self._security.roles[group]:
                    self._security.roles[group].append(username)

            return True

    def remove_user(self, username: str) -> bool:
        """
        Remove a user from the security configuration.

        Args:
            username: Username.

        Returns:
            bool: True if successful.
        """
        with self._lock:
            if username not in self._security.users:
                return False

            groups = self._security.users[username].get("groups", [])
            del self._security.users[username]

            for group in groups:
                if group in self._security.roles:
                    if username in self._security.roles[group]:
                        self._security.roles[group].remove(username)

            return True

    def get_user_info(self, username: str) -> Optional[Dict[str, Any]]:
        """
        Get user information.

        Args:
            username: Username.

        Returns:
            dict or None if user not found.
        """
        if username not in self._security.users:
            return None

        return {
            "username": username,
            "groups": self._security.users[username].get("groups", []),
            "password_set": bool(self._security.users[username].get("password"))
        }

    def list_users(self) -> List[str]:
        """List all configured users."""
        return list(self._security.users.keys())

    def add_role(self, role_name: str, permissions: List[str]) -> bool:
        """
        Add or update a role with permissions.

        Args:
            role_name: Name of the role.
            permissions: List of permission strings.

        Returns:
            bool: True if successful.
        """
        with self._lock:
            self._security.roles[role_name] = permissions
            return True

    def get_role_info(self, role_name: str) -> Optional[Dict[str, Any]]:
        """
        Get role information.

        Args:
            role_name: Role name.

        Returns:
            dict or None if role not found.
        """
        if role_name not in self._security.roles:
            return None

        return {
            "role": role_name,
            "permissions": self._security.roles[role_name],
            "members": [u for u, d in self._security.users.items() if role_name in d.get("groups", [])]
        }

    def list_roles(self) -> List[str]:
        """List all configured roles."""
        return list(self._security.roles.keys())

    def get_security_info(self) -> Dict[str, Any]:
        """
        Get full security configuration.

        Returns:
            dict: Security configuration summary.
        """
        return {
            "user_count": len(self._security.users),
            "role_count": len(self._security.roles),
            "users": list(self._security.users.keys()),
            "roles": list(self._security.roles.keys()),
            "plugins": self._security.plugins
        }

    # =========================================================================
    # Network of Brokers
    # =========================================================================

    def add_network_connector(self, config: NetworkConnectorConfig) -> bool:
        """
        Add a network connector to connect to another broker.

        Args:
            config: Network connector configuration.

        Returns:
            bool: True if successful.
        """
        with self._lock:
            self._network_connectors[config.name] = config
            logger.info(f"Network connector {config.name} added: {config.uri}")
            return True

    def remove_network_connector(self, name: str) -> bool:
        """
        Remove a network connector.

        Args:
            name: Connector name.

        Returns:
            bool: True if successful.
        """
        with self._lock:
            if name not in self._network_connectors:
                return False

            del self._network_connectors[name]
            return True

    def get_network_connector_info(self, name: str) -> Optional[Dict[str, Any]]:
        """
        Get network connector information.

        Args:
            name: Connector name.

        Returns:
            dict or None if not found.
        """
        if name not in self._network_connectors:
            return None

        config = self._network_connectors[name]
        return {
            "name": config.name,
            "uri": config.uri,
            "duplex": config.duplex,
            "decrease_priority": config.decreasePriority,
            "demand_flow_control": config.demandFlowControl,
            "conduit_subscriptions": config.conduitSubscriptions,
            "excluded": config.excluded,
            "included": config.included,
            "prefix": config.prefix
        }

    def list_network_connectors(self) -> List[str]:
        """List all network connector names."""
        return list(self._network_connectors.keys())

    def get_network_info(self) -> Dict[str, Any]:
        """
        Get network of brokers configuration.

        Returns:
            dict: Network configuration summary.
        """
        return {
            "connector_count": len(self._network_connectors),
            "connectors": [
                self.get_network_connector_info(name)
                for name in self._network_connectors.keys()
            ]
        }

    # =========================================================================
    # Persistence
    # =========================================================================

    def configure_persistence(self, config: PersistenceConfig) -> bool:
        """
        Configure message persistence settings.

        Args:
            config: Persistence configuration.

        Returns:
            bool: True if successful.
        """
        with self._lock:
            self._persistence = config
            logger.info(f"Persistence configured: {config.type}")
            return True

    def get_persistence_info(self) -> Dict[str, Any]:
        """
        Get persistence configuration.

        Returns:
            dict: Persistence configuration summary.
        """
        return {
            "enabled": self._persistence.enabled,
            "type": self._persistence.type,
            "journal_dir": self._persistence.journal_dir,
            "persistence_dir": self._persistence.persistence_dir,
            "max_file_length": self._persistence.maxFileLength,
            "checkpoint_interval": self._persistence.checkpointInterval,
            "cleanup_interval": self._persistence.cleanupInterval
        }

    def set_persistence_enabled(self, enabled: bool) -> bool:
        """
        Enable or disable message persistence.

        Args:
            enabled: True to enable, False to disable.

        Returns:
            bool: True if successful.
        """
        with self._lock:
            self._persistence.enabled = enabled
            return True

    # =========================================================================
    # Utility Methods
    # =========================================================================

    def get_destination_stats(self) -> Dict[str, Any]:
        """
        Get statistics for all destinations.

        Returns:
            dict: Statistics for queues and topics.
        """
        return {
            "queues": {name: self.get_queue_stats(name) for name in self._queues},
            "topics": {name: self.get_topic_stats(name) for name in self._topics}
        }

    def health_check(self) -> Dict[str, Any]:
        """
        Perform a comprehensive health check.

        Returns:
            dict: Health check results.
        """
        return {
            "connected": self.is_connected(),
            "broker_info": self.get_broker_info(),
            "health": self.check_broker_health(),
            "stats": self.get_stats(),
            "destinations": {
                "queues": len(self._queues),
                "topics": len(self._topics)
            },
            "producers": len(self._producers),
            "consumers": len(self._consumers),
            "network_connectors": len(self._network_connectors)
        }

    def cleanup(self):
        """Clean up all resources."""
        with self._lock:
            self._running = False
            self._producers.clear()
            self._consumers.clear()
            self._queues.clear()
            self._topics.clear()
            self._network_connectors.clear()
            self._message_listeners.clear()
            self._advisory_listeners.clear()
            
            while not self._pending_messages.empty():
                try:
                    self._pending_messages.get_nowait()
                except Empty:
                    break

            self.disconnect()
            logger.info("ActiveMQ integration cleaned up")
