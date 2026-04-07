"""
Redis queue utilities for distributed task processing.

Provides message queue patterns: FIFO, priority, delayed, dead-letter,
rate limiting, and pub/sub broadcasting using Redis.
"""

from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Callable, Optional

import redis

logger = logging.getLogger(__name__)


class QueueStrategy(Enum):
    """Queue processing strategies."""
    FIFO = auto()
    LIFO = auto()
    PRIORITY = auto()
    DELAYED = auto()


@dataclass
class QueueConfig:
    """Configuration for Redis queues."""
    host: str = "localhost"
    port: int = 6379
    db: int = 0
    password: Optional[str] = None
    max_retry: int = 3
    retry_delay: float = 1.0
    visibility_timeout: int = 30  # seconds
    dead_letter_ttl: int = 86400  # seconds
    default_priority: int = 10


@dataclass
class QueueMessage:
    """Represents a queue message."""
    id: str
    body: dict[str, Any]
    priority: int = 10
    timestamp: float = field(default_factory=time.time)
    retry_count: int = 0
    metadata: dict[str, Any] = field(default_factory=dict)


class RedisQueue:
    """Redis-based message queue with multiple strategies."""

    def __init__(self, name: str, config: Optional[QueueConfig] = None, strategy: QueueStrategy = QueueStrategy.FIFO) -> None:
        self.name = name
        self.config = config or QueueConfig()
        self.strategy = strategy
        self._redis = redis.Redis(
            host=self.config.host,
            port=self.config.port,
            db=self.config.db,
            password=self.config.password,
            decode_responses=True,
        )
        self._queues: dict[str, str] = {
            "main": f"queue:{name}",
            "delayed": f"queue:{name}:delayed",
            "dead": f"queue:{name}:dead",
            "processing": f"queue:{name}:processing",
        }

    def enqueue(self, body: dict[str, Any], priority: int = 10, delay: float = 0) -> str:
        """Enqueue a message."""
        msg_id = f"{time.time()}:{self._redis.incr(f'queue:{self.name}:counter')}"
        message = QueueMessage(id=msg_id, body=body, priority=priority)
        payload = json.dumps(message.__dict__)

        if delay > 0:
            score = time.time() + delay
            self._redis.zadd(self._queues["delayed"], {payload: score})
            logger.debug("Enqueued message %s with delay %.1fs", msg_id, delay)
        else:
            if self.strategy == QueueStrategy.PRIORITY:
                self._redis.zadd(self._queues["main"], {payload: -priority})
            else:
                self._redis.rpush(self._queues["main"], payload)
            logger.debug("Enqueued message %s", msg_id)

        return msg_id

    def dequeue(self, timeout: int = 0) -> Optional[QueueMessage]:
        """Dequeue a message (blocking if timeout > 0)."""
        self._move_delayed()

        if timeout > 0:
            result = self._redis.blpop(self._queues["main"], timeout=timeout)
            if result:
                _, payload = result
            else:
                return None
        else:
            if self.strategy == QueueStrategy.PRIORITY:
                result = self._redis.zpopmin(self._queues["main"], 1)
                payload = result[0][0] if result else None
            elif self.strategy == QueueStrategy.LIFO:
                payload = self._redis.lpop(self._queues["main"])
            else:
                payload = self._redis.lpop(self._queues["main"])

        if not payload:
            return None

        self._redis.sadd(self._queues["processing"], payload)
        data = json.loads(payload)
        return QueueMessage(**data)

    def _move_delayed(self) -> int:
        """Move ready delayed messages to main queue."""
        count = 0
        now = time.time()
        while True:
            result = self._redis.zpopmin(self._queues["delayed"], 1)
            if not result:
                break
            payload, score = result
            if score <= now:
                if self.strategy == QueueStrategy.PRIORITY:
                    msg = QueueMessage(**json.loads(payload))
                    self._redis.zadd(self._queues["main"], {payload: -msg.priority})
                else:
                    self._redis.rpush(self._queues["main"], payload)
                count += 1
            else:
                self._redis.zadd(self._queues["delayed"], {payload: score})
                break
        return count

    def acknowledge(self, message: QueueMessage) -> None:
        """Remove message from processing queue."""
        payload = json.dumps(message.__dict__)
        self._redis.srem(self._queues["processing"], payload)
        logger.debug("Acknowledged message %s", message.id)

    def retry(self, message: QueueMessage) -> bool:
        """Retry a failed message."""
        message.retry_count += 1
        if message.retry_count >= self.config.max_retry:
            self._move_to_dead(message)
            return False

        payload = json.dumps(message.__dict__)
        self._redis.srem(self._queues["processing"], payload)
        delay = self.config.retry_delay * (2 ** (message.retry_count - 1))
        score = time.time() + delay
        self._redis.zadd(self._queues["delayed"], {payload: score})
        logger.info("Retrying message %s (attempt %d)", message.id, message.retry_count)
        return True

    def _move_to_dead(self, message: QueueMessage) -> None:
        """Move message to dead letter queue."""
        payload = json.dumps(message.__dict__)
        self._redis.srem(self._queues["processing"], payload)
        self._redis.rpush(self._queues["dead"], payload)
        logger.warning("Message %s moved to dead letter queue", message.id)

    def get_dead_letter(self) -> list[QueueMessage]:
        """Get all messages in dead letter queue."""
        payloads = self._redis.lrange(self._queues["dead"], 0, -1)
        return [QueueMessage(**json.loads(p)) for p in payloads]

    def purge_dead_letter(self) -> int:
        """Purge all dead letter messages."""
        count = self._redis.llen(self._queues["dead"])
        self._redis.delete(self._queues["dead"])
        return count

    def size(self) -> dict[str, int]:
        """Get queue sizes."""
        return {
            "main": self._redis.zcard(self._queues["main"]) if self.strategy == QueueStrategy.PRIORITY else self._redis.llen(self._queues["main"]),
            "delayed": self._redis.zcard(self._queues["delayed"]),
            "dead": self._redis.llen(self._queues["dead"]),
            "processing": self._redis.scard(self._queues["processing"]),
        }

    def ping(self) -> bool:
        """Check Redis connectivity."""
        try:
            return self._redis.ping()
        except redis.RedisError:
            return False


class RedisPubSub:
    """Redis pub/sub for broadcasting."""

    def __init__(self, channel: str, config: Optional[QueueConfig] = None) -> None:
        self.channel = channel
        self.config = config or QueueConfig()
        self._redis = redis.Redis(
            host=self.config.host,
            port=self.config.port,
            db=self.config.db,
            password=self.config.password,
            decode_responses=True,
        )
        self._pubsub = self._redis.pubsub()

    def publish(self, message: dict[str, Any]) -> int:
        """Publish a message to the channel."""
        payload = json.dumps(message)
        return self._redis.publish(self.channel, payload)

    def subscribe(self, handler: Callable[[dict[str, Any]], None]) -> None:
        """Subscribe to the channel with a handler."""
        self._pubsub.subscribe(self.channel)

        def wrapper(msg: dict[str, Any]) -> None:
            if msg["type"] == "message":
                data = json.loads(msg["data"])
                handler(data)

        self._pubsub.on_message(wrapper)

    def listen(self) -> None:
        """Start listening for messages."""
        for msg in self._pubsub.listen():
            if msg["type"] == "message":
                data = json.loads(msg["data"])
                yield data

    def unsubscribe(self) -> None:
        """Unsubscribe from the channel."""
        self._pubsub.unsubscribe(self.channel)

    def close(self) -> None:
        """Close the pub/sub connection."""
        self._pubsub.close()
