"""AMQP Consumer Action Module.

AMQP message consumer with connection pooling and error handling.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Generic, TypeVar

T = TypeVar("T")


class ConsumerStatus(Enum):
    """Consumer status."""
    DISCONNECTED = "disconnected"
    CONNECTING = "connecting"
    CONNECTED = "connected"
    CONSUMING = "consuming"
    STOPPED = "stopped"


@dataclass
class AMQPConfig:
    """AMQP connection configuration."""
    host: str = "localhost"
    port: int = 5672
    username: str = "guest"
    password: str = "guest"
    virtual_host: str = "/"
    heartbeat: int = 60
    connection_timeout: float = 10.0
    frame_max: int = 131072


@dataclass
class AMQPMessage(Generic[T]):
    """AMQP message wrapper."""
    delivery_tag: int
    body: T
    routing_key: str
    exchange: str
    properties: dict = field(default_factory=dict)
    redelivered: bool = False


class AMQPConsumerError(Exception):
    """AMQP consumer error."""
    pass


class AMQPConsumer(Generic[T]):
    """AMQP message consumer with reconnection."""

    def __init__(
        self,
        config: AMQPConfig,
        queue_name: str,
        handler: Callable[[AMQPMessage], Any]
    ) -> None:
        self.config = config
        self.queue_name = queue_name
        self.handler = handler
        self.status = ConsumerStatus.DISCONNECTED
        self._connection: Any = None
        self._channel: Any = None
        self._running = False
        self._reconnect_delay = 5.0
        self._max_reconnect_attempts = 10

    async def connect(self) -> None:
        """Connect to AMQP broker."""
        self.status = ConsumerStatus.CONNECTING
        try:
            import aio_pika
            self._connection = await aio_pika.connect_robust(
                self._build_url(),
                timeout=self.config.connection_timeout
            )
            self._channel = await self._connection.channel()
            await self._channel.set_qos(prefetch_count=10)
            self.status = ConsumerStatus.CONNECTED
        except Exception as e:
            self.status = ConsumerStatus.DISCONNECTED
            raise AMQPConsumerError(f"Connection failed: {e}") from e

    def _build_url(self) -> str:
        """Build AMQP connection URL."""
        return (
            f"amqp://{self.config.username}:{self.config.password}"
            f"@{self.config.host}:{self.config.port}/{self.config.virtual_host}"
        )

    async def start(self) -> None:
        """Start consuming messages."""
        if self.status != ConsumerStatus.CONNECTED:
            await self.connect()
        self._running = True
        self.status = ConsumerStatus.CONSUMING
        try:
            queue = await self._channel.declare_queue(self.queue_name, durable=True)
            await queue.consume(self._process_message)
            while self._running:
                await asyncio.sleep(1)
        except asyncio.CancelledError:
            self.status = ConsumerStatus.STOPPED
        except Exception as e:
            self.status = ConsumerStatus.DISCONNECTED
            raise AMQPConsumerError(f"Consumer error: {e}") from e

    async def _process_message(self, message: Any) -> None:
        """Process incoming message."""
        try:
            import json
            body = json.loads(message.body.decode())
            amqp_msg = AMQPMessage(
                delivery_tag=message.delivery_tag,
                body=body,
                routing_key=message.routing_key,
                exchange=message.exchange,
                properties=dict(message.properties),
                redelivered=message.redelivered
            )
            result = self.handler(amqp_msg)
            if asyncio.iscoroutine(result):
                result = await result
            await message.ack()
        except Exception:
            await message.nack(requeue=False)

    async def stop(self) -> None:
        """Stop consuming and disconnect."""
        self._running = False
        if self._channel:
            await self._channel.close()
        if self._connection:
            await self._connection.close()
        self.status = ConsumerStatus.STOPPED

    async def reconnect(self) -> None:
        """Attempt to reconnect with exponential backoff."""
        attempts = 0
        while attempts < self._max_reconnect_attempts and self._running:
            try:
                await self.connect()
                return
            except Exception:
                attempts += 1
                delay = min(self._reconnect_delay * (2 ** attempts), 60)
                await asyncio.sleep(delay)
        raise AMQPConsumerError("Max reconnection attempts reached")
