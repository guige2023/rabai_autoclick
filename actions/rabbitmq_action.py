"""
RabbitMQ Action Module

Provides RabbitMQ publisher and consumer functionality for message queue
operations in UI automation workflows. Supports exchanges, queues, and routing.

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


class ExchangeType(Enum):
    """RabbitMQ exchange types."""
    DIRECT = "direct"
    FANOUT = "fanout"
    TOPIC = "topic"
    HEADERS = "headers"


class RoutingPolicy(Enum):
    """Message routing policies."""
    ROUND_ROBIN = auto()
    RANDOM = auto()
    WEIGHTED = auto()
    BROADCAST = auto()


@dataclass
class RabbitMQConfig:
    """RabbitMQ connection configuration."""
    host: str = "localhost"
    port: int = 5672
    username: str = "guest"
    password: str = "guest"
    virtual_host: str = "/"
    heartbeat: int = 60
    connection_timeout: float = 30.0
    retry_delay: float = 1.0
    max_retries: int = 3
    ssl: bool = False


@dataclass
class ExchangeConfig:
    """Exchange configuration."""
    name: str
    exchange_type: ExchangeType = ExchangeType.DIRECT
    durable: bool = True
    auto_delete: bool = False
    internal: bool = False
    arguments: dict[str, Any] = field(default_factory=dict)


@dataclass
class QueueConfig:
    """Queue configuration."""
    name: str
    durable: bool = True
    auto_delete: bool = False
    exclusive: bool = False
    arguments: dict[str, Any] = field(default_factory=dict)
    routing_keys: list[str] = field(default_factory=list)


@dataclass
class Message:
    """Represents a RabbitMQ message."""
    body: bytes
    delivery_tag: Optional[int] = None
    content_type: str = "application/json"
    delivery_mode: int = 2
    headers: dict[str, Any] = field(default_factory=dict)
    message_id: Optional[str] = None
    timestamp: Optional[int] = None
    properties: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.message_id is None:
            import uuid
            self.message_id = str(uuid.uuid4())
        if self.timestamp is None:
            self.timestamp = int(datetime.utcnow().timestamp())


class RabbitMQConnection:
    """
    RabbitMQ connection manager.

    Example:
        >>> config = RabbitMQConfig(host="localhost")
        >>> conn = RabbitMQConnection(config)
        >>> await conn.connect()
    """

    def __init__(self, config: RabbitMQConfig) -> None:
        self.config = config
        self._connection: Optional[Any] = None
        self._channel: Optional[Any] = None
        self._connected = False

    @property
    def is_connected(self) -> bool:
        """Check if connected."""
        return self._connected and self._connection is not None

    async def connect(self) -> None:
        """Establish connection to RabbitMQ."""
        try:
            import aio_pika
            import aio_pika.abc

            url = f"amqp://{self.config.username}:{self.config.password}@{self.config.host}:{self.config.port}/{self.config.virtual_host}"
            if self.config.ssl:
                url = url.replace("amqp://", "amqps://")

            self._connection = await aio_pika.connect_robust(
                url,
                heartbeat=self.config.heartbeat,
                timeout=self.config.connection_timeout,
            )
            self._channel = await self._connection.channel()
            self._connected = True
            logger.info(f"Connected to RabbitMQ: {self.config.host}:{self.config.port}")
        except ImportError:
            logger.warning("aio_pika not installed, using mock connection")
            self._connected = True
        except Exception as e:
            logger.error(f"Failed to connect: {e}")
            raise

    async def disconnect(self) -> None:
        """Close connection."""
        self._connected = False
        if self._channel:
            await self._channel.close()
            self._channel = None
        if self._connection:
            await self._connection.close()
            self._connection = None
        logger.info("Disconnected from RabbitMQ")

    async def get_channel(self) -> Any:
        """Get channel."""
        if not self.is_connected:
            await self.connect()
        if self._channel is None:
            raise RuntimeError("Channel not available")
        return self._channel


class Exchange:
    """RabbitMQ exchange."""

    def __init__(
        self,
        connection: RabbitMQConnection,
        config: ExchangeConfig,
    ) -> None:
        self.connection = connection
        self.config = config
        self._exchange: Optional[Any] = None

    async def declare(self) -> None:
        """Declare exchange."""
        channel = await self.connection.get_channel()
        import aio_pika
        self._exchange = await channel.declare_exchange(
            name=self.config.name,
            type=aio_pika.ExchangeType[self.config.exchange_type.name],
            durable=self.config.durable,
            auto_delete=self.config.auto_delete,
            internal=self.config.internal,
            arguments=self.config.arguments,
        )
        logger.debug(f"Declared exchange: {self.config.name}")

    async def publish(
        self,
        message: Message,
        routing_key: str = "",
    ) -> None:
        """Publish message to exchange."""
        if self._exchange is None:
            await self.declare()

        if self._exchange is None:
            raise RuntimeError("Exchange not declared")

        import aio_pika

        body = message.body if isinstance(message.body, bytes) else message.body.encode()
        props = aio_pika.BasicProperties(
            content_type=message.content_type,
            delivery_mode=message.delivery_mode,
            headers=message.headers,
            message_id=message.message_id,
            timestamp=message.timestamp,
        )

        await self._exchange.publish(
            message=aio_pika.Message(body=body, properties=props),
            routing_key=routing_key,
        )
        logger.debug(f"Published to {self.config.name}/{routing_key}")

    def __repr__(self) -> str:
        return f"Exchange({self.config.name}, type={self.config.exchange_type.name})"


class Queue:
    """RabbitMQ queue."""

    def __init__(
        self,
        connection: RabbitMQConnection,
        config: QueueConfig,
    ) -> None:
        self.connection = connection
        self.config = config
        self._queue: Optional[Any] = None

    async def declare(self) -> None:
        """Declare queue."""
        channel = await self.connection.get_channel()
        import aio_pika
        self._queue = await channel.declare_queue(
            name=self.config.name,
            durable=self.config.durable,
            auto_delete=self.config.auto_delete,
            exclusive=self.config.exclusive,
            arguments=self.config.arguments,
        )
        logger.debug(f"Declared queue: {self.config.name}")

    async def bind(
        self,
        exchange: Exchange,
        routing_key: str = "",
    ) -> None:
        """Bind queue to exchange."""
        if self._queue is None:
            await self.declare()
        if exchange._exchange is None:
            await exchange.declare()

        if self._queue and exchange._exchange:
            await self._queue.bind(exchange._exchange, routing_key=routing_key)
            logger.debug(f"Bound {self.config.name} to {exchange.config.name}")

    async def consume(
        self,
        callback: Callable[[Message], Any],
        no_ack: bool = False,
    ) -> None:
        """Start consuming messages."""
        if self._queue is None:
            await self.declare()

        if self._queue is None:
            raise RuntimeError("Queue not declared")

        async def wrapper(message: Any) -> None:
            import aio_pika
            if isinstance(message, aio_pika.IncomingMessage):
                body = message.body
                msg = Message(
                    body=body,
                    delivery_tag=message.delivery_tag,
                    content_type=message.content_type,
                    headers=message.headers,
                    message_id=message.message_id,
                    timestamp=message.timestamp,
                )
                try:
                    result = await callback(msg)
                    if not no_ack and result is not False:
                        await message.ack()
                except Exception as e:
                    logger.error(f"Consumer callback error: {e}")
                    await message.nack(requeue=True)

        await self._queue.consume(wrapper, no_ack=no_ack)
        logger.debug(f"Started consuming from {self.config.name}")

    async def purge(self) -> int:
        """Purge all messages from queue."""
        if self._queue is None:
            await self.declare()
        if self._queue:
            result = await self._queue.purge()
            count = result if isinstance(result, int) else 0
            logger.debug(f"Purged {count} messages from {self.config.name}")
            return count
        return 0

    async def delete(self, if_unused: bool = False, if_empty: bool = False) -> None:
        """Delete the queue."""
        if self._queue:
            await self._queue.delete(if_unused=if_unused, if_empty=if_empty)
            logger.debug(f"Deleted queue: {self.config.name}")

    def __repr__(self) -> str:
        return f"Queue({self.config.name})"


class Publisher:
    """
    RabbitMQ message publisher.

    Example:
        >>> config = RabbitMQConfig(host="localhost")
        >>> conn = RabbitMQConnection(config)
        >>> await conn.connect()
        >>> pub = Publisher(conn)
        >>> await pub.publish("my_queue", {"data": "value"})
    """

    def __init__(self, connection: RabbitMQConnection) -> None:
        self.connection = connection
        self._exchanges: dict[str, Exchange] = {}

    async def get_exchange(self, config: ExchangeConfig) -> Exchange:
        """Get or create exchange."""
        if config.name not in self._exchanges:
            exchange = Exchange(self.connection, config)
            await exchange.declare()
            self._exchanges[config.name] = exchange
        return self._exchanges[config.name]

    async def publish(
        self,
        exchange_name: str,
        routing_key: str,
        message: Message,
    ) -> None:
        """Publish message to exchange."""
        if exchange_name not in self._exchanges:
            exchange = Exchange(
                self.connection,
                ExchangeConfig(name=exchange_name),
            )
            self._exchanges[exchange_name] = exchange

        await self._exchanges[exchange_name].publish(message, routing_key)

    async def publish_json(
        self,
        exchange_name: str,
        routing_key: str,
        data: dict[str, Any],
    ) -> None:
        """Publish JSON message."""
        body = json.dumps(data).encode("utf-8")
        message = Message(body=body, content_type="application/json")
        await self.publish(exchange_name, routing_key, message)


class Consumer:
    """
    RabbitMQ message consumer.

    Example:
        >>> config = RabbitMQConfig(host="localhost")
        >>> conn = RabbitMQConnection(config)
        >>> await conn.connect()
        >>> cons = Consumer(conn)
        >>> async def handler(msg):
        ...     print(msg.body)
        >>> await cons.consume("my_queue", handler)
    """

    def __init__(self, connection: RabbitMQConnection) -> None:
        self.connection = connection
        self._queues: dict[str, Queue] = {}

    async def get_queue(self, config: QueueConfig) -> Queue:
        """Get or create queue."""
        if config.name not in self._queues:
            queue = Queue(self.connection, config)
            await queue.declare()
            self._queues[config.name] = queue
        return self._queues[config.name]

    async def consume(
        self,
        queue_name: str,
        callback: Callable[[Message], Any],
        no_ack: bool = False,
    ) -> None:
        """Start consuming from queue."""
        if queue_name not in self._queues:
            queue = Queue(
                self.connection,
                QueueConfig(name=queue_name),
            )
            self._queues[queue_name] = queue

        await self._queues[queue_name].consume(callback, no_ack=no_ack)

    async def stop_consuming(self, queue_name: str) -> None:
        """Stop consuming from queue."""
        if queue_name in self._queues:
            await self._queues[queue_name].delete()
            del self._queues[queue_name]
