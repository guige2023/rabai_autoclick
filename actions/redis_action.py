"""
Redis Action Module

Provides Redis client functionality for caching and data storage
in UI automation workflows. Supports strings, hashes, lists, sets,
sorted sets, and pub/sub operations.

Author: AI Agent
Version: 1.0.0
"""

from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum, auto
from typing import Any, Callable, Optional

logger = logging.getLogger(__name__)


class RedisKeyType(Enum):
    """Redis key types."""
    STRING = "string"
    LIST = "list"
    SET = "set"
    ZSET = "zset"
    HASH = "hash"
    STREAM = "stream"


@dataclass
class RedisConfig:
    """Redis connection configuration."""
    host: str = "localhost"
    port: int = 6379
    db: int = 0
    password: Optional[str] = None
    ssl: bool = False
    socket_timeout: float = 5.0
    socket_connect_timeout: float = 5.0
    retry_delay: float = 1.0
    max_retries: int = 3
    decode_responses: bool = True


@dataclass
class CacheEntry:
    """Represents a cached entry."""
    key: str
    value: Any
    ttl: Optional[int] = None
    created_at: float = field(default_factory=lambda: datetime.utcnow().timestamp())
    metadata: dict[str, Any] = field(default_factory=dict)


class RedisClient:
    """
    Redis client with async operations.

    Example:
        >>> config = RedisConfig(host="localhost")
        >>> client = RedisClient(config)
        >>> await client.connect()
        >>> await client.set("key", "value", ttl=3600)
        >>> value = await client.get("key")
        >>> await client.disconnect()
    """

    def __init__(self, config: RedisConfig) -> None:
        self.config = config
        self._redis: Optional[Any] = None
        self._connected = False

    @property
    def is_connected(self) -> bool:
        """Check if connected."""
        return self._connected and self._redis is not None

    async def connect(self) -> None:
        """Connect to Redis."""
        try:
            import redis.asyncio as redis

            self._redis = redis.Redis(
                host=self.config.host,
                port=self.config.port,
                db=self.config.db,
                password=self.config.password,
                ssl=self.config.ssl,
                socket_timeout=self.config.socket_timeout,
                socket_connect_timeout=self.config.socket_connect_timeout,
                decode_responses=self.config.decode_responses,
            )
            await self._redis.ping()
            self._connected = True
            logger.info(f"Connected to Redis: {self.config.host}:{self.config.port}")
        except ImportError:
            logger.warning("redis not installed, using mock client")
            self._redis = MockRedis()
            self._connected = True
        except Exception as e:
            logger.error(f"Failed to connect: {e}")
            raise

    async def disconnect(self) -> None:
        """Disconnect from Redis."""
        self._connected = False
        if self._redis and hasattr(self._redis, "close"):
            await self._redis.close()
        self._redis = None
        logger.info("Disconnected from Redis")

    async def ping(self) -> bool:
        """Check connection."""
        if self._redis:
            return await self._redis.ping()
        return False

    async def get(self, key: str) -> Optional[str]:
        """Get string value."""
        if not self._redis:
            return None
        return await self._redis.get(key)

    async def set(
        self,
        key: str,
        value: str,
        ttl: Optional[int] = None,
        nx: bool = False,
    ) -> bool:
        """Set string value."""
        if not self._redis:
            return False
        if ttl:
            return await self._redis.setex(key, ttl, value)
        elif nx:
            return await self._redis.setnx(key, value)
        else:
            return await self._redis.set(key, value)

    async def delete(self, *keys: str) -> int:
        """Delete keys."""
        if not self._redis:
            return 0
        return await self._redis.delete(*keys)

    async def exists(self, key: str) -> bool:
        """Check if key exists."""
        if not self._redis:
            return False
        return await self._redis.exists(key) > 0

    async def expire(self, key: str, ttl: int) -> bool:
        """Set key expiration."""
        if not self._redis:
            return False
        return await self._redis.expire(key, ttl)

    async def ttl(self, key: str) -> int:
        """Get time to live."""
        if not self._redis:
            return -2
        return await self._redis.ttl(key)

    async def keys(self, pattern: str = "*") -> list[str]:
        """Get keys matching pattern."""
        if not self._redis:
            return []
        return await self._redis.keys(pattern)

    async def hset(self, name: str, key: str, value: str) -> int:
        """Set hash field."""
        if not self._redis:
            return 0
        return await self._redis.hset(name, key, value)

    async def hget(self, name: str, key: str) -> Optional[str]:
        """Get hash field."""
        if not self._redis:
            return None
        return await self._redis.hget(name, key)

    async def hgetall(self, name: str) -> dict[str, str]:
        """Get all hash fields."""
        if not self._redis:
            return {}
        return await self._redis.hgetall(name)

    async def hdel(self, name: str, *keys: str) -> int:
        """Delete hash fields."""
        if not self._redis:
            return 0
        return await self._redis.hdel(name, *keys)

    async def lpush(self, key: str, *values: str) -> int:
        """Push to list."""
        if not self._redis:
            return 0
        return await self._redis.lpush(key, *values)

    async def rpush(self, key: str, *values: str) -> int:
        """Push to list (right)."""
        if not self._redis:
            return 0
        return await self._redis.rpush(key, *values)

    async def lpop(self, key: str) -> Optional[str]:
        """Pop from list (left)."""
        if not self._redis:
            return None
        return await self._redis.lpop(key)

    async def rpop(self, key: str) -> Optional[str]:
        """Pop from list (right)."""
        if not self._redis:
            return None
        return await self._redis.rpop(key)

    async def llen(self, key: str) -> int:
        """Get list length."""
        if not self._redis:
            return 0
        return await self._redis.llen(key)

    async def lrange(self, key: str, start: int = 0, end: int = -1) -> list[str]:
        """Get list range."""
        if not self._redis:
            return []
        return await self._redis.lrange(key, start, end)

    async def sadd(self, key: str, *values: str) -> int:
        """Add to set."""
        if not self._redis:
            return 0
        return await self._redis.sadd(key, *values)

    async def smembers(self, key: str) -> set[str]:
        """Get set members."""
        if not self._redis:
            return set()
        return await self._redis.smembers(key)

    async def sismember(self, key: str, value: str) -> bool:
        """Check set membership."""
        if not self._redis:
            return False
        return await self._redis.sismember(key, value)

    async def zadd(self, key: str, mapping: dict[str, float]) -> int:
        """Add to sorted set."""
        if not self._redis:
            return 0
        return await self._redis.zadd(key, mapping)

    async def zrange(
        self,
        key: str,
        start: int = 0,
        end: int = -1,
        with_scores: bool = False,
    ) -> list[Any]:
        """Get sorted set range."""
        if not self._redis:
            return []
        return await self._redis.zrange(key, start, end, with_scores=with_scores)

    async def zrangebyscore(
        self,
        key: str,
        min_score: float,
        max_score: float,
        with_scores: bool = False,
    ) -> list[Any]:
        """Get sorted set by score range."""
        if not self._redis:
            return []
        return await self._redis.zrangebyscore(
            key, min_score, max_score, with_scores=with_scores
        )

    async def incr(self, key: str, amount: int = 1) -> int:
        """Increment integer value."""
        if not self._redis:
            return 0
        return await self._redis.incrby(key, amount)

    async def decr(self, key: str, amount: int = 1) -> int:
        """Decrement integer value."""
        if not self._redis:
            return 0
        return await self._redis.decrby(key, amount)

    async def incrbyfloat(self, key: str, amount: float = 1.0) -> float:
        """Increment float value."""
        if not self._redis:
            return 0.0
        return await self._redis.incrbyfloat(key, amount)

    async def type(self, key: str) -> str:
        """Get key type."""
        if not self._redis:
            return "none"
        return await self._redis.type(key)

    async def rename(self, old_key: str, new_key: str) -> bool:
        """Rename key."""
        if not self._redis:
            return False
        await self._redis.rename(old_key, new_key)
        return True


class MockRedis:
    """Mock Redis for testing."""

    def __init__(self) -> None:
        self._data: dict[str, Any] = {}
        self._ttls: dict[str, int] = {}

    async def ping(self) -> bool:
        return True

    async def close(self) -> None:
        pass

    async def get(self, key: str) -> Optional[str]:
        return self._data.get(key)

    async def set(self, key: str, value: str) -> bool:
        self._data[key] = value
        return True

    async def setex(self, key: str, ttl: int, value: str) -> bool:
        self._data[key] = value
        self._ttls[key] = ttl
        return True

    async def setnx(self, key: str, value: str) -> bool:
        if key in self._data:
            return False
        self._data[key] = value
        return True

    async def delete(self, *keys: str) -> int:
        count = 0
        for key in keys:
            if key in self._data:
                del self._data[key]
                count += 1
        return count

    async def exists(self, key: str) -> int:
        return 1 if key in self._data else 0

    async def expire(self, key: str, ttl: int) -> bool:
        if key in self._data:
            self._ttls[key] = ttl
            return True
        return False

    async def ttl(self, key: str) -> int:
        return self._ttls.get(key, -2)

    async def keys(self, pattern: str = "*") -> list[str]:
        import fnmatch
        return [k for k in self._data.keys() if fnmatch.fnmatch(k, pattern)]

    async def hset(self, name: str, key: str, value: str) -> int:
        if name not in self._data:
            self._data[name] = {}
        if key not in self._data[name]:
            self._data[name][key] = value
            return 1
        self._data[name][key] = value
        return 0

    async def hget(self, name: str, key: str) -> Optional[str]:
        if name in self._data and isinstance(self._data[name], dict):
            return self._data[name].get(key)
        return None

    async def hgetall(self, name: str) -> dict[str, str]:
        if name in self._data and isinstance(self._data[name], dict):
            return self._data[name].copy()
        return {}

    async def hdel(self, name: str, *keys: str) -> int:
        if name not in self._data or not isinstance(self._data[name], dict):
            return 0
        count = 0
        for key in keys:
            if key in self._data[name]:
                del self._data[name][key]
                count += 1
        return count

    async def lpush(self, key: str, *values: str) -> int:
        if key not in self._data:
            self._data[key] = []
        for v in reversed(values):
            self._data[key].insert(0, v)
        return len(self._data[key])

    async def rpush(self, key: str, *values: str) -> int:
        if key not in self._data:
            self._data[key] = []
        self._data[key].extend(values)
        return len(self._data[key])

    async def lpop(self, key: str) -> Optional[str]:
        if key not in self._data or not self._data[key]:
            return None
        return self._data[key].pop(0)

    async def rpop(self, key: str) -> Optional[str]:
        if key not in self._data or not self._data[key]:
            return None
        return self._data[key].pop()

    async def llen(self, key: str) -> int:
        if key not in self._data or not isinstance(self._data[key], list):
            return 0
        return len(self._data[key])

    async def lrange(self, key: str, start: int = 0, end: int = -1) -> list[str]:
        if key not in self._data or not isinstance(self._data[key], list):
            return []
        return self._data[key][start:end]

    async def sadd(self, key: str, *values: str) -> int:
        if key not in self._data:
            self._data[key] = set()
        count = 0
        for v in values:
            if v not in self._data[key]:
                self._data[key].add(v)
                count += 1
        return count

    async def smembers(self, key: str) -> set[str]:
        if key not in self._data or not isinstance(self._data[key], set):
            return set()
        return self._data[key].copy()

    async def sismember(self, key: str, value: str) -> bool:
        if key not in self._data or not isinstance(self._data[key], set):
            return False
        return value in self._data[key]

    async def zadd(self, key: str, mapping: dict[str, float]) -> int:
        if key not in self._data:
            self._data[key] = {}
        count = 0
        for member, score in mapping.items():
            if member not in self._data[key]:
                count += 1
            self._data[key][member] = score
        return count

    async def zrange(self, key: str, start: int = 0, end: int = -1, with_scores: bool = False) -> list[Any]:
        if key not in self._data or not isinstance(self._data[key], dict):
            return []
        items = sorted(self._data[key].items(), key=lambda x: x[1])
        result = items[start:end]
        if with_scores:
            return result
        return [item[0] for item in result]

    async def zrangebyscore(self, key: str, min_score: float, max_score: float, with_scores: bool = False) -> list[Any]:
        if key not in self._data or not isinstance(self._data[key], dict):
            return []
        result = [(m, s) for m, s in self._data[key].items() if min_score <= s <= max_score]
        result.sort(key=lambda x: x[1])
        if with_scores:
            return result
        return [item[0] for item in result]

    async def incrby(self, key: str, amount: int) -> int:
        if key not in self._data:
            self._data[key] = 0
        self._data[key] += amount
        return self._data[key]

    async def decrby(self, key: str, amount: int) -> int:
        if key not in self._data:
            self._data[key] = 0
        self._data[key] -= amount
        return self._data[key]

    async def incrbyfloat(self, key: str, amount: float) -> float:
        if key not in self._data:
            self._data[key] = 0.0
        self._data[key] += amount
        return self._data[key]

    async def type(self, key: str) -> str:
        if key not in self._data:
            return "none"
        v = self._data[key]
        if isinstance(v, str):
            return "string"
        if isinstance(v, list):
            return "list"
        if isinstance(v, set):
            return "set"
        if isinstance(v, dict):
            return "hash"
        return "string"

    async def rename(self, old_key: str, new_key: str) -> None:
        if old_key in self._data:
            self._data[new_key] = self._data.pop(old_key)


class RedisCache:
    """
    Redis-based cache with TTL support.

    Example:
        >>> cache = RedisCache(RedisConfig(host="localhost"))
        >>> await cache.connect()
        >>> await cache.set("user:1", {"name": "John"}, ttl=3600)
        >>> data = await cache.get("user:1")
    """

    def __init__(self, config: RedisConfig) -> None:
        self.client = RedisClient(config)

    async def connect(self) -> None:
        """Connect to Redis."""
        await self.client.connect()

    async def disconnect(self) -> None:
        """Disconnect from Redis."""
        await self.client.disconnect()

    async def get(self, key: str) -> Optional[Any]:
        """Get value from cache."""
        value = await self.client.get(key)
        if value is None:
            return None
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return value

    async def set(
        self,
        key: str,
        value: Any,
        ttl: Optional[int] = None,
    ) -> bool:
        """Set value in cache."""
        if isinstance(value, (dict, list)):
            value = json.dumps(value)
        return await self.client.set(key, str(value), ttl=ttl)

    async def delete(self, *keys: str) -> int:
        """Delete keys from cache."""
        return await self.client.delete(*keys)

    async def clear_pattern(self, pattern: str) -> int:
        """Clear all keys matching pattern."""
        keys = await self.client.keys(pattern)
        if keys:
            return await self.client.delete(*keys)
        return 0


class RedisPubSub:
    """
    Redis publish/subscribe client.

    Example:
        >>> pubsub = RedisPubSub(RedisConfig(host="localhost"))
        >>> await pubsub.connect()
        >>> await pubsub.subscribe("channel1", handler)
    """

    def __init__(self, config: RedisConfig) -> None:
        self.config = config
        self._redis: Optional[Any] = None
        self._pubsub: Optional[Any] = None
        self._handlers: dict[str, Callable] = {}

    async def connect(self) -> None:
        """Connect to Redis."""
        import redis.asyncio as redis
        self._redis = redis.Redis(
            host=self.config.host,
            port=self.config.port,
            db=self.config.db,
            password=self.config.password,
        )
        self._pubsub = self._redis.pubsub()
        logger.info("PubSub connected")

    async def disconnect(self) -> None:
        """Disconnect from Redis."""
        if self._pubsub:
            await self._pubsub.close()
        if self._redis:
            await self._redis.close()
        logger.info("PubSub disconnected")

    async def subscribe(self, channel: str, handler: Callable) -> None:
        """Subscribe to channel."""
        if not self._pubsub:
            raise RuntimeError("Not connected")
        await self._pubsub.subscribe(channel)
        self._handlers[channel] = handler
        logger.debug(f"Subscribed to {channel}")

    async def unsubscribe(self, channel: str) -> None:
        """Unsubscribe from channel."""
        if not self._pubsub:
            raise RuntimeError("Not connected")
        await self._pubsub.unsubscribe(channel)
        if channel in self._handlers:
            del self._handlers[channel]

    async def publish(self, channel: str, message: str) -> int:
        """Publish message to channel."""
        if not self._redis:
            raise RuntimeError("Not connected")
        return await self._redis.publish(channel, message)

    async def listen(self) -> None:
        """Listen for messages."""
        if not self._pubsub:
            raise RuntimeError("Not connected")
        async for message in self._pubsub.listen():
            if message["type"] == "message":
                channel = message["channel"]
                if channel in self._handlers:
                    await self._handlers[channel](message["data"])
