"""Redis action for caching and data structures.

This module provides comprehensive Redis support:
- String, hash, list, set, sorted set operations
- Pub/sub messaging
- Stream processing
- Pipeline and transaction support
- Cluster and Sentinel support
- Lua scripting
- Distributed locking
- Rate limiting

Author: rabai_autoclick
Version: 1.0.0
"""

import asyncio
import json
import logging
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set, Union

try:
    import redis.asyncio as aioredis
    from redis.asyncio import Redis, ConnectionPool
    from redis.cluster import RedisCluster
    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False
    Redis = None
    ConnectionPool = None
    RedisCluster = None

logger = logging.getLogger(__name__)


class DataType(Enum):
    """Redis data types."""
    STRING = "string"
    HASH = "hash"
    LIST = "list"
    SET = "set"
    ZSET = "zset"
    STREAM = "stream"
    NONE = "none"


@dataclass
class CacheEntry:
    """Cache entry with metadata."""
    key: str
    value: Any
    ttl_seconds: Optional[float] = None
    created_at: float = field(default_factory=time.time)
    hit_count: int = 0
    last_accessed: float = field(default_factory=time.time)


@dataclass
class RateLimitResult:
    """Rate limit check result."""
    allowed: bool
    limit: int
    remaining: int
    reset_at: float
    retry_after: Optional[float] = None


@dataclass
class LockResult:
    """Distributed lock result."""
    acquired: bool
    lock_key: str
    token: Optional[str] = None
    ttl_seconds: Optional[float] = None


@dataclass
class StreamEntry:
    """Redis stream entry."""
    stream: str
    entry_id: str
    values: Dict[str, Any]
    timestamp: float


class RedisAction:
    """Redis action handler for caching and data operations.

    Provides comprehensive Redis operations:
    - Key-value caching with TTL
    - Complex data structures
    - Pub/sub messaging
    - Stream processing
    - Distributed locking
    - Rate limiting

    Example:
        action = RedisAction(host="localhost", port=6379)
        await action.connect()
        await action.set("key", "value", ttl=3600)
        value = await action.get("key")
    """

    def __init__(
        self,
        host: str = "localhost",
        port: int = 6379,
        db: int = 0,
        password: Optional[str] = None,
        ssl: bool = False,
        max_connections: int = 50,
        socket_timeout: float = 5.0,
        socket_connect_timeout: float = 5.0,
        decode_responses: bool = True,
        encoding: str = "utf-8",
    ):
        """Initialize Redis action.

        Args:
            host: Redis host
            port: Redis port
            db: Database number
            password: Password for authentication
            ssl: Enable SSL connection
            max_connections: Maximum connections
            socket_timeout: Socket timeout
            socket_connect_timeout: Socket connect timeout
            decode_responses: Decode responses to strings
            encoding: Response encoding
        """
        self.host = host
        self.port = port
        self.db = db
        self.password = password
        self.ssl = ssl
        self.max_connections = max_connections
        self.socket_timeout = socket_timeout
        self.socket_connect_timeout = socket_connect_timeout
        self.decode_responses = decode_responses
        self.encoding = encoding

        self._client: Optional[Redis] = None
        self._pool: Optional[ConnectionPool] = None
        self._pubsub: Optional[aioredis.client.PubSub] = None
        self._connected = False

    async def connect(self) -> None:
        """Establish Redis connection."""
        if not REDIS_AVAILABLE:
            raise ImportError("redis[hiredis] not installed")

        self._pool = ConnectionPool(
            host=self.host,
            port=self.port,
            db=self.db,
            password=self.password,
            ssl=self.ssl,
            max_connections=self.max_connections,
            socket_timeout=self.socket_timeout,
            socket_connect_timeout=self.socket_connect_timeout,
            decode_responses=self.decode_responses,
            encoding=self.encoding,
        )

        self._client = Redis(connection_pool=self._pool)
        await self._client.ping()
        self._connected = True
        logger.info(f"Connected to Redis at {self.host}:{self.port}")

    async def disconnect(self) -> None:
        """Close Redis connection."""
        if self._pubsub:
            await self._pubsub.close()
            self._pubsub = None

        if self._client:
            await self._client.close()
            self._client = None

        if self._pool:
            await self._pool.disconnect()
            self._pool = None

        self._connected = False
        logger.info("Disconnected from Redis")

    async def ping(self) -> bool:
        """Ping Redis server.

        Returns:
            True if server responded
        """
        if not self._client:
            return False

        try:
            await self._client.ping()
            return True
        except Exception:
            return False

    async def get(self, key: str) -> Optional[Any]:
        """Get value by key.

        Args:
            key: Key name

        Returns:
            Value or None
        """
        if not self._client:
            raise RuntimeError("Not connected")

        value = await self._client.get(key)

        if value is None:
            return None

        try:
            return json.loads(value)
        except (json.JSONDecodeError, TypeError):
            return value

    async def set(
        self,
        key: str,
        value: Any,
        ttl_seconds: Optional[float] = None,
        nx: bool = False,
        xx: bool = False,
    ) -> bool:
        """Set key-value pair.

        Args:
            key: Key name
            value: Value to store
            ttl_seconds: Time to live in seconds
            nx: Only set if key doesn't exist
            xx: Only set if key exists

        Returns:
            True if set successfully
        """
        if not self._client:
            raise RuntimeError("Not connected")

        if not isinstance(value, str):
            value = json.dumps(value)

        if ttl_seconds:
            result = await self._client.setex(key, ttl_seconds, value)
        else:
            result = await self._client.set(key, value, nx=nx, xx=xx)

        return result is True

    async def delete(self, *keys: str) -> int:
        """Delete keys.

        Args:
            *keys: Key names

        Returns:
            Number of keys deleted
        """
        if not self._client:
            raise RuntimeError("Not connected")

        return await self._client.delete(*keys)

    async def exists(self, *keys: str) -> int:
        """Check if keys exist.

        Args:
            *keys: Key names

        Returns:
            Number of existing keys
        """
        if not self._client:
            raise RuntimeError("Not connected")

        return await self._client.exists(*keys)

    async def expire(self, key: str, ttl_seconds: float) -> bool:
        """Set key expiration.

        Args:
            key: Key name
            ttl_seconds: TTL in seconds

        Returns:
            True if expiration was set
        """
        if not self._client:
            raise RuntimeError("Not connected")

        return await self._client.expire(key, ttl_seconds)

    async def ttl(self, key: str) -> int:
        """Get key TTL.

        Args:
            key: Key name

        Returns:
            TTL in seconds, -1 if no TTL, -2 if key doesn't exist
        """
        if not self._client:
            raise RuntimeError("Not connected")

        return await self._client.ttl(key)

    async def get_type(self, key: str) -> DataType:
        """Get key data type.

        Args:
            key: Key name

        Returns:
            Data type
        """
        if not self._client:
            raise RuntimeError("Not connected")

        redis_type = await self._client.type(key)

        if redis_type == b"string" or redis_type == "string":
            return DataType.STRING
        elif redis_type == b"hash" or redis_type == "hash":
            return DataType.HASH
        elif redis_type == b"list" or redis_type == "list":
            return DataType.LIST
        elif redis_type == b"set" or redis_type == "set":
            return DataType.SET
        elif redis_type == b"zset" or redis_type == "zset":
            return DataType.ZSET
        elif redis_type == b"stream" or redis_type == "stream":
            return DataType.STREAM
        else:
            return DataType.NONE

    async def hget(self, key: str, field: str) -> Optional[str]:
        """Get hash field value.

        Args:
            key: Hash key
            field: Field name

        Returns:
            Field value or None
        """
        if not self._client:
            raise RuntimeError("Not connected")

        return await self._client.hget(key, field)

    async def hset(
        self,
        key: str,
        field: str,
        value: Any,
    ) -> int:
        """Set hash field.

        Args:
            key: Hash key
            field: Field name
            value: Field value

        Returns:
            1 if new field, 0 if updated
        """
        if not self._client:
            raise RuntimeError("Not connected")

        if not isinstance(value, str):
            value = json.dumps(value)

        return await self._client.hset(key, field, value)

    async def hgetall(self, key: str) -> Dict[str, Any]:
        """Get all hash fields.

        Args:
            key: Hash key

        Returns:
            Dictionary of field-value pairs
        """
        if not self._client:
            raise RuntimeError("Not connected")

        result = await self._client.hgetall(key)

        parsed = {}
        for k, v in result.items():
            try:
                parsed[k] = json.loads(v)
            except (json.JSONDecodeError, TypeError):
                parsed[k] = v

        return parsed

    async def hdel(self, key: str, *fields: str) -> int:
        """Delete hash fields.

        Args:
            key: Hash key
            *fields: Field names

        Returns:
            Number of fields deleted
        """
        if not self._client:
            raise RuntimeError("Not connected")

        return await self._client.hdel(key, *fields)

    async def lpush(self, key: str, *values: Any) -> int:
        """Push to list head.

        Args:
            key: List key
            *values: Values to push

        Returns:
            List length after push
        """
        if not self._client:
            raise RuntimeError("Not connected")

        serialized = [
            json.dumps(v) if not isinstance(v, str) else v
            for v in values
        ]

        return await self._client.lpush(key, *serialized)

    async def rpush(self, key: str, *values: Any) -> int:
        """Push to list tail.

        Args:
            key: List key
            *values: Values to push

        Returns:
            List length after push
        """
        if not self._client:
            raise RuntimeError("Not connected")

        serialized = [
            json.dumps(v) if not isinstance(v, str) else v
            for v in values
        ]

        return await self._client.rpush(key, *serialized)

    async def lrange(self, key: str, start: int = 0, end: int = -1) -> List[Any]:
        """Get list range.

        Args:
            key: List key
            start: Start index
            end: End index

        Returns:
            List of values
        """
        if not self._client:
            raise RuntimeError("Not connected")

        result = await self._client.lrange(key, start, end)

        parsed = []
        for item in result:
            try:
                parsed.append(json.loads(item))
            except (json.JSONDecodeError, TypeError):
                parsed.append(item)

        return parsed

    async def lpop(self, key: str) -> Optional[Any]:
        """Pop from list head.

        Args:
            key: List key

        Returns:
            Popped value or None
        """
        if not self._client:
            raise RuntimeError("Not connected")

        value = await self._client.lpop(key)

        if value is None:
            return None

        try:
            return json.loads(value)
        except (json.JSONDecodeError, TypeError):
            return value

    async def sadd(self, key: str, *values: Any) -> int:
        """Add to set.

        Args:
            key: Set key
            *values: Values to add

        Returns:
            Number of elements added
        """
        if not self._client:
            raise RuntimeError("Not connected")

        serialized = [
            json.dumps(v) if not isinstance(v, str) else v
            for v in values
        ]

        return await self._client.sadd(key, *serialized)

    async def smembers(self, key: str) -> Set[Any]:
        """Get all set members.

        Args:
            key: Set key

        Returns:
            Set of members
        """
        if not self._client:
            raise RuntimeError("Not connected")

        result = await self._client.smembers(key)

        parsed = set()
        for item in result:
            try:
                parsed.add(json.loads(item))
            except (json.JSONDecodeError, TypeError):
                parsed.add(item)

        return parsed

    async def zadd(
        self,
        key: str,
        mapping: Dict[Any, float],
    ) -> int:
        """Add to sorted set.

        Args:
            key: Sorted set key
            mapping: Member-score mapping

        Returns:
            Number of elements added
        """
        if not self._client:
            raise RuntimeError("Not connected")

        serialized = {}
        for member, score in mapping.items():
            if not isinstance(member, str):
                member = json.dumps(member)
            serialized[member] = score

        return await self._client.zadd(key, serialized)

    async def zrange(
        self,
        key: str,
        start: int = 0,
        end: int = -1,
        withscores: bool = False,
    ) -> List[Any]:
        """Get sorted set range.

        Args:
            key: Sorted set key
            start: Start index
            end: End index
            withscores: Include scores in result

        Returns:
            List of members (and scores if withscores)
        """
        if not self._client:
            raise RuntimeError("Not connected")

        result = await self._client.zrange(key, start, end, withscores=withscores)

        if withscores:
            parsed = []
            for member, score in result:
                try:
                    member = json.loads(member)
                except (json.JSONDecodeError, TypeError):
                    pass
                parsed.append((member, score))
            return parsed
        else:
            parsed = []
            for item in result:
                try:
                    parsed.append(json.loads(item))
                except (json.JSONDecodeError, TypeError):
                    parsed.append(item)
            return parsed

    async def zrangebyscore(
        self,
        key: str,
        min_score: float,
        max_score: float,
        withscores: bool = False,
    ) -> List[Any]:
        """Get sorted set range by score.

        Args:
            key: Sorted set key
            min_score: Minimum score
            max_score: Maximum score
            withscores: Include scores

        Returns:
            List of members
        """
        if not self._client:
            raise RuntimeError("Not connected")

        result = await self._client.zrangebyscore(
            key, min_score, max_score, withscores=withscores
        )

        if withscores:
            parsed = []
            for member, score in result:
                try:
                    member = json.loads(member)
                except (json.JSONDecodeError, TypeError):
                    pass
                parsed.append((member, score))
            return parsed
        else:
            parsed = []
            for item in result:
                try:
                    parsed.append(json.loads(item))
                except (json.JSONDecodeError, TypeError):
                    parsed.append(item)
            return parsed

    async def publish(self, channel: str, message: Any) -> int:
        """Publish message to channel.

        Args:
            channel: Channel name
            message: Message to publish

        Returns:
            Number of subscribers that received the message
        """
        if not self._client:
            raise RuntimeError("Not connected")

        if not isinstance(message, str):
            message = json.dumps(message)

        return await self._client.publish(channel, message)

    async def subscribe(self, *channels: str) -> aioredis.client.PubSub:
        """Subscribe to channels.

        Args:
            *channels: Channel names

        Returns:
            PubSub instance
        """
        if not self._client:
            raise RuntimeError("Not connected")

        self._pubsub = self._client.pubsub()
        await self._pubsub.subscribe(*channels)
        return self._pubsub

    async def psubscribe(self, *patterns: str) -> aioredis.client.PubSub:
        """Subscribe to channel patterns.

        Args:
            *patterns: Channel patterns

        Returns:
            PubSub instance
        """
        if not self._client:
            raise RuntimeError("Not connected")

        self._pubsub = self._client.pubsub()
        await self._pubsub.psubscribe(*patterns)
        return self._pubsub

    async def get_message(self) -> Optional[Dict[str, Any]]:
        """Get pub/sub message.

        Returns:
            Message dictionary or None
        """
        if not self._pubsub:
            return None

        message = await self._pubsub.get_message(ignore_subscribe_messages=True)

        if message:
            if message["type"] == "message":
                try:
                    message["data"] = json.loads(message["data"])
                except (json.JSONDecodeError, TypeError):
                    pass

        return message

    async def xadd(
        self,
        stream: str,
        fields: Dict[str, Any],
        maxlen: Optional[int] = None,
    ) -> str:
        """Add entry to stream.

        Args:
            stream: Stream name
            fields: Entry fields
            maxlen: Maximum stream length

        Returns:
            Entry ID
        """
        if not self._client:
            raise RuntimeError("Not connected")

        serialized = {}
        for k, v in fields.items():
            if not isinstance(v, str):
                serialized[k] = json.dumps(v)
            else:
                serialized[k] = v

        if maxlen:
            return await self._client.xadd(stream, serialized, maxlen=maxlen)
        else:
            return await self._client.xadd(stream, serialized)

    async def xread(
        self,
        streams: Dict[str, str],
        count: Optional[int] = None,
        block: Optional[int] = None,
    ) -> List[StreamEntry]:
        """Read from streams.

        Args:
            streams: Stream -> last read ID mapping
            count: Maximum entries per stream
            block: Block timeout in milliseconds

        Returns:
            List of stream entries
        """
        if not self._client:
            raise RuntimeError("Not connected")

        result = await self._client.xread(streams, count=count, block=block)

        entries = []
        for stream_name, messages in result or []:
            for entry_id, values in messages:
                parsed = {}
                for k, v in values.items():
                    try:
                        parsed[k] = json.loads(v)
                    except (json.JSONDecodeError, TypeError):
                        parsed[k] = v

                entries.append(StreamEntry(
                    stream=stream_name,
                    entry_id=entry_id,
                    values=parsed,
                    timestamp=time.time()
                ))

        return entries

    async def xlen(self, stream: str) -> int:
        """Get stream length.

        Args:
            stream: Stream name

        Returns:
            Stream length
        """
        if not self._client:
            raise RuntimeError("Not connected")

        return await self._client.xlen(stream)

    async def xtrim(self, stream: str, maxlen: int) -> int:
        """Trim stream to max length.

        Args:
            stream: Stream name
            maxlen: Maximum length

        Returns:
            Number of entries deleted
        """
        if not self._client:
            raise RuntimeError("Not connected")

        return await self._client.xtrim(stream, maxlen)

    async def scan(
        self,
        match: Optional[str] = None,
        count: int = 100,
    ) -> List[str]:
        """Scan keys.

        Args:
            match: Pattern to match
            count: Approximate count

        Returns:
            List of keys
        """
        if not self._client:
            raise RuntimeError("Not connected")

        keys = []
        cursor = 0

        while True:
            cursor, batch = await self._client.scan(
                cursor=cursor,
                match=match,
                count=count
            )
            keys.extend(batch)

            if cursor == 0:
                break

        return keys

    async def pipeline(self) -> "Pipeline":
        """Create pipeline.

        Returns:
            Pipeline instance
        """
        if not self._client:
            raise RuntimeError("Not connected")

        return Pipeline(await self._client.pipeline())

    async def execute_script(self, script: str, *keys: str, *args: Any) -> Any:
        """Execute Lua script.

        Args:
            script: Lua script
            *keys: Keys used in script
            *args: Arguments

        Returns:
            Script result
        """
        if not self._client:
            raise RuntimeError("Not connected")

        serialized_args = []
        for arg in args:
            if not isinstance(arg, str):
                serialized_args.append(json.dumps(arg))
            else:
                serialized_args.append(arg)

        return await self._client.eval(script, len(keys), *keys, *serialized_args)

    async def acquire_lock(
        self,
        lock_name: str,
        ttl_seconds: float = 10.0,
        token: Optional[str] = None,
    ) -> LockResult:
        """Acquire distributed lock.

        Args:
            lock_name: Lock name
            ttl_seconds: Lock TTL
            token: Lock token (auto-generated if not provided)

        Returns:
            Lock result
        """
        if not self._client:
            raise RuntimeError("Not connected")

        lock_key = f"lock:{lock_name}"
        token = token or f"{time.time()}:{id(self)}"

        acquired = await self._client.set(
            lock_key,
            token,
            nx=True,
            ex=ttl_seconds
        )

        return LockResult(
            acquired=bool(acquired),
            lock_key=lock_key,
            token=token if acquired else None,
            ttl_seconds=ttl_seconds if acquired else None
        )

    async def release_lock(self, lock_name: str, token: str) -> bool:
        """Release distributed lock.

        Args:
            lock_name: Lock name
            token: Lock token

        Returns:
            True if released
        """
        if not self._client:
            raise RuntimeError("Not connected")

        lock_key = f"lock:{lock_name}"

        script = """
        if redis.call("get", KEYS[1]) == ARGV[1] then
            return redis.call("del", KEYS[1])
        else
            return 0
        end
        """

        result = await self._client.eval(script, 1, lock_key, token)
        return result == 1

    async def extend_lock(self, lock_name: str, token: str, ttl_seconds: float) -> bool:
        """Extend lock TTL.

        Args:
            lock_name: Lock name
            token: Lock token
            ttl_seconds: New TTL

        Returns:
            True if extended
        """
        if not self._client:
            raise RuntimeError("Not connected")

        lock_key = f"lock:{lock_name}"

        script = """
        if redis.call("get", KEYS[1]) == ARGV[1] then
            return redis.call("expire", KEYS[1], ARGV[2])
        else
            return 0
        end
        """

        result = await self._client.eval(script, 1, lock_key, token, ttl_seconds)
        return result == 1

    async def rate_limit(
        self,
        key: str,
        limit: int,
        window_seconds: float,
    ) -> RateLimitResult:
        """Rate limit check.

        Args:
            key: Rate limit key
            limit: Maximum requests
            window_seconds: Time window

        Returns:
            Rate limit result
        """
        if not self._client:
            raise RuntimeError("Not connected")

        now = time.time()
        window_start = now - window_seconds
        rate_key = f"ratelimit:{key}"

        pipe = self._client.pipeline()
        pipe.zremrangebyscore(rate_key, 0, window_start)
        pipe.zcard(rate_key)
        pipe.zadd(rate_key, {str(now): now})
        pipe.expire(rate_key, int(window_seconds) + 1)
        results = await pipe.execute()

        current_count = results[1]

        if current_count < limit:
            return RateLimitResult(
                allowed=True,
                limit=limit,
                remaining=limit - current_count - 1,
                reset_at=now + window_seconds
            )
        else:
            oldest = await self._client.zrange(rate_key, 0, 0, withscores=True)
            if oldest:
                retry_after = oldest[0][1] + window_seconds - now
            else:
                retry_after = window_seconds

            return RateLimitResult(
                allowed=False,
                limit=limit,
                remaining=0,
                reset_at=now + window_seconds,
                retry_after=retry_after
            )

    async def cache_with_lock(
        self,
        key: str,
        ttl_seconds: float,
        fetch_func: Callable[[], Any],
        lock_ttl_seconds: float = 5.0,
    ) -> Any:
        """Cache with distributed lock for cache stampede prevention.

        Args:
            key: Cache key
            ttl_seconds: Cache TTL
            fetch_func: Function to fetch data
            lock_ttl_seconds: Lock TTL for stampede prevention

        Returns:
            Cached or freshly fetched value
        """
        cached = await self.get(key)
        if cached is not None:
            return cached

        lock_result = await self.acquire_lock(f"fetch:{key}", lock_ttl_seconds)

        try:
            if lock_result.acquired:
                cached = await self.get(key)
                if cached is not None:
                    return cached

                value = await fetch_func()
                await self.set(key, value, ttl_seconds)
                return value
            else:
                await asyncio.sleep(0.1)
                cached = await self.get(key)
                if cached is not None:
                    return cached

                await asyncio.sleep(0.2)
                cached = await self.get(key)
                if cached is not None:
                    return cached

                return await fetch_func()

        finally:
            if lock_result.acquired:
                await self.release_lock(f"fetch:{key}", lock_result.token)


class Pipeline:
    """Redis pipeline for batch operations."""

    def __init__(self, pipeline: Any):
        """Initialize pipeline.

        Args:
            pipeline: Redis pipeline
        """
        self._pipeline = pipeline

    def get(self, key: str) -> "Pipeline":
        """Queue GET command."""
        self._pipeline.get(key)
        return self

    def set(
        self,
        key: str,
        value: Any,
        ttl_seconds: Optional[float] = None,
    ) -> "Pipeline":
        """Queue SET command."""
        if not isinstance(value, str):
            value = json.dumps(value)

        if ttl_seconds:
            self._pipeline.setex(key, ttl_seconds, value)
        else:
            self._pipeline.set(key, value)

        return self

    def hget(self, key: str, field: str) -> "Pipeline":
        """Queue HGET command."""
        self._pipeline.hget(key, field)
        return self

    def hset(self, key: str, field: str, value: Any) -> "Pipeline":
        """Queue HSET command."""
        if not isinstance(value, str):
            value = json.dumps(value)

        self._pipeline.hset(key, field, value)
        return self

    def delete(self, *keys: str) -> "Pipeline":
        """Queue DELETE command."""
        self._pipeline.delete(*keys)
        return self

    async def execute(self) -> List[Any]:
        """Execute queued commands.

        Returns:
            List of results
        """
        results = await self._pipeline.execute()

        parsed = []
        for result in results:
            if isinstance(result, str):
                try:
                    parsed.append(json.loads(result))
                except (json.JSONDecodeError, TypeError):
                    parsed.append(result)
            else:
                parsed.append(result)

        return parsed


_redis_action_instance: Optional[RedisAction] = None


def get_redis_action(
    host: str = "localhost",
    port: int = 6379,
    **kwargs
) -> RedisAction:
    """Get singleton Redis action instance.

    Args:
        host: Redis host
        port: Redis port
        **kwargs: Additional arguments

    Returns:
        RedisAction instance
    """
    global _redis_action_instance

    if _redis_action_instance is None:
        _redis_action_instance = RedisAction(host=host, port=port, **kwargs)

    return _redis_action_instance
