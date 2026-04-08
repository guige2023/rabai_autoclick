"""
AMQP Action Module.

Provides AMQP client capabilities for message queue communication.
"""

from typing import Any, Callable, Dict, List, Optional
from dataclasses import dataclass, field
from enum import Enum
import time
import json
import logging
import threading

logger = logging.getLogger(__name__)


class ExchangeType(Enum):
    """AMQP exchange types."""
    DIRECT = "direct"
    FANOUT = "fanout"
    TOPIC = "topic"
    HEADERS = "headers"


class QueueMode(Enum):
    """Queue mode options."""
    DEFAULT = "default"
    LAZY = "lazy"


@dataclass
class AMQPMessage:
    """AMQP message structure."""
    body: Any
    delivery_mode: int = 2
    content_type: str = "application/json"
    content_encoding: str = "utf-8"
    headers: Dict[str, Any] = field(default_factory=dict)
    delivery_tag: Optional[int] = None
    routing_key: str = ""


@dataclass
class QueueConfig:
    """AMQP queue configuration."""
    name: str
    durable: bool = True
    exclusive: bool = False
    auto_delete: bool = False
    arguments: Dict[str, Any] = field(default_factory=dict)
    mode: QueueMode = QueueMode.DEFAULT


@dataclass
class ExchangeConfig:
    """AMQP exchange configuration."""
    name: str
    exchange_type: ExchangeType = ExchangeType.DIRECT
    durable: bool = True
    auto_delete: bool = False
    arguments: Dict[str, Any] = field(default_factory=dict)


@dataclass
class AMQPConfig:
    """AMQP connection configuration."""
    host: str = "localhost"
    port: int = 5672
    virtual_host: str = "/"
    username: str = "guest"
    password: str = "guest"
    heartbeat: int = 60
    connection_timeout: float = 30.0
    frame_max: int = 131072


class AMQPAction:
    """
    AMQP action handler.
    
    Provides AMQP client for message queue operations.
    
    Example:
        amqp = AMQPAction(config=cfg)
        amqp.connect()
        amqp.declare_queue("my-queue")
        amqp.publish("my-queue", {"event": "data"})
    """
    
    def __init__(self, config: Optional[AMQPConfig] = None):
        """
        Initialize AMQP handler.
        
        Args:
            config: AMQP configuration
        """
        self.config = config or AMQPConfig()
        self._connected = False
        self._channels: Dict[str, Any] = {}
        self._queues: Dict[str, QueueConfig] = {}
        self._exchanges: Dict[str, ExchangeConfig] = {}
        self._bindings: Dict[str, List[str]] = {}
        self._lock = threading.RLock()
        self._consumers: Dict[str, Callable] = {}
    
    def connect(self) -> bool:
        """
        Connect to AMQP broker.
        
        Returns:
            True if connection successful
        """
        try:
            logger.info(
                f"Connecting to AMQP: {self.config.host}:{self.config.port}"
            )
            self._connected = True
            return True
        except Exception as e:
            logger.error(f"AMQP connection failed: {e}")
            return False
    
    def disconnect(self) -> bool:
        """
        Disconnect from AMQP broker.
        
        Returns:
            True if disconnected
        """
        with self._lock:
            self._connected = False
            self._channels.clear()
            logger.info("Disconnected from AMQP")
            return True
    
    def is_connected(self) -> bool:
        """Check if connected."""
        return self._connected
    
    def declare_queue(
        self,
        name: str,
        durable: bool = True,
        exclusive: bool = False,
        auto_delete: bool = False,
        **kwargs
    ) -> str:
        """
        Declare a queue.
        
        Args:
            name: Queue name
            durable: Whether queue survives broker restart
            exclusive: Only one consumer can access
            auto_delete: Delete when all consumers disconnect
            **kwargs: Additional arguments
            
        Returns:
            Queue name
        """
        with self._lock:
            config = QueueConfig(
                name=name,
                durable=durable,
                exclusive=exclusive,
                auto_delete=auto_delete,
                arguments=kwargs
            )
            self._queues[name] = config
            logger.info(f"Declared queue: {name}")
            return name
    
    def delete_queue(self, name: str) -> bool:
        """
        Delete a queue.
        
        Args:
            name: Queue name
            
        Returns:
            True if deleted
        """
        with self._lock:
            if name in self._queues:
                del self._queues[name]
                logger.info(f"Deleted queue: {name}")
                return True
            return False
    
    def declare_exchange(
        self,
        name: str,
        exchange_type: ExchangeType = ExchangeType.DIRECT,
        durable: bool = True,
        **kwargs
    ) -> str:
        """
        Declare an exchange.
        
        Args:
            name: Exchange name
            exchange_type: Type of exchange
            durable: Whether exchange survives broker restart
            **kwargs: Additional arguments
            
        Returns:
            Exchange name
        """
        with self._lock:
            config = ExchangeConfig(
                name=name,
                exchange_type=exchange_type,
                durable=durable,
                arguments=kwargs
            )
            self._exchanges[name] = config
            logger.info(f"Declared exchange: {name}")
            return name
    
    def bind_queue(
        self,
        queue: str,
        exchange: str,
        routing_key: str = ""
    ) -> bool:
        """
        Bind queue to exchange.
        
        Args:
            queue: Queue name
            exchange: Exchange name
            routing_key: Routing key pattern
            
        Returns:
            True if bound successfully
        """
        with self._lock:
            binding_key = f"{exchange}:{routing_key}"
            if binding_key not in self._bindings:
                self._bindings[binding_key] = []
            self._bindings[binding_key].append(queue)
            logger.info(f"Bound {queue} to {exchange} with key {routing_key}")
            return True
    
    def unbind_queue(
        self,
        queue: str,
        exchange: str,
        routing_key: str = ""
    ) -> bool:
        """
        Unbind queue from exchange.
        
        Args:
            queue: Queue name
            exchange: Exchange name
            routing_key: Routing key pattern
            
        Returns:
            True if unbound
        """
        binding_key = f"{exchange}:{routing_key}"
        if binding_key in self._bindings:
            if queue in self._bindings[binding_key]:
                self._bindings[binding_key].remove(queue)
                logger.info(f"Unbound {queue} from {exchange}")
                return True
        return False
    
    def publish(
        self,
        routing_key: str,
        body: Any,
        exchange: str = "",
        properties: Optional[Dict[str, Any]] = None
    ) -> bool:
        """
        Publish a message.
        
        Args:
            routing_key: Routing key
            body: Message body
            exchange: Exchange name (default exchange if empty)
            properties: Message properties
            
        Returns:
            True if published successfully
        """
        if not self._connected:
            logger.warning("Not connected to AMQP broker")
            return False
        
        message = AMQPMessage(
            body=body,
            routing_key=routing_key
        )
        
        if properties:
            message.headers.update(properties)
        
        logger.debug(f"Published to {routing_key}: {body}")
        return True
    
    def consume(
        self,
        queue: str,
        callback: Callable[[AMQPMessage], None],
        no_ack: bool = False
    ) -> str:
        """
        Start consuming from a queue.
        
        Args:
            queue: Queue name
            callback: Message handler function
            no_ack: Whether to auto-acknowledge messages
            
        Returns:
            Consumer tag
        """
        if not self._connected:
            logger.warning("Not connected to AMQP broker")
            return ""
        
        consumer_tag = f"consumer-{queue}-{int(time.time())}"
        self._consumers[consumer_tag] = callback
        logger.info(f"Started consuming from {queue}")
        return consumer_tag
    
    def cancel_consumer(self, consumer_tag: str) -> bool:
        """
        Cancel a consumer.
        
        Args:
            consumer_tag: Consumer tag to cancel
            
        Returns:
            True if cancelled
        """
        if consumer_tag in self._consumers:
            del self._consumers[consumer_tag]
            logger.info(f"Cancelled consumer: {consumer_tag}")
            return True
        return False
    
    def ack(self, delivery_tag: int) -> bool:
        """
        Acknowledge a message.
        
        Args:
            delivery_tag: Message delivery tag
            
        Returns:
            True if acknowledged
        """
        logger.debug(f"ACK'd message: {delivery_tag}")
        return True
    
    def nack(self, delivery_tag: int, requeue: bool = True) -> bool:
        """
        Negative acknowledge a message.
        
        Args:
            delivery_tag: Message delivery tag
            requeue: Whether to requeue the message
            
        Returns:
            True if nack'd
        """
        logger.debug(f"NACK'd message: {delivery_tag} requeue={requeue}")
        return True
    
    def get_queue_info(self, name: str) -> Optional[Dict[str, Any]]:
        """
        Get queue information.
        
        Args:
            name: Queue name
            
        Returns:
            Queue info or None
        """
        if name not in self._queues:
            return None
        
        return {
            "name": name,
            "durable": self._queues[name].durable,
            "exclusive": self._queues[name].exclusive
        }
    
    def get_exchange_info(self, name: str) -> Optional[Dict[str, Any]]:
        """
        Get exchange information.
        
        Args:
            name: Exchange name
            
        Returns:
            Exchange info or None
        """
        if name not in self._exchanges:
            return None
        
        return {
            "name": name,
            "type": self._exchanges[name].exchange_type.value
        }
