"""Redis integration for in-memory data structure operations.

Handles Redis operations including strings, hashes, lists,
sets, sorted sets, pub/sub, and caching patterns.
"""

from typing import Any, Optional, Union
import logging
from dataclasses import dataclass, field
from datetime import timedelta

try:
    import redis
except ImportError:
    redis = None

logger = logging.getLogger(__name__)


@dataclass
class RedisConfig:
    """Configuration for Redis connection."""
    host: str = "localhost"
    port: int = 6379
    db: int = 0
    password: Optional[str] = None
    ssl: bool = False
    socket_timeout: int = 5
    socket_connect_timeout: int = 5
    max_connections: int = 50
    decode_responses: bool = True


@dataclass
class CacheEntry:
    """Represents a cached value with metadata."""
    key: str
    value: Any
    ttl: Optional[int] = None
    exists: bool = True


class RedisAPIError(Exception):
    """Raised when Redis operations fail."""
    def __init__(self, message: str, error_type: Optional[str] = None):
        super().__init__(message)
        self.error_type = error_type


class RedisAction:
    """Redis client for caching and data structure operations."""

    def __init__(self, config: RedisConfig):
        """Initialize Redis client with configuration.

        Args:
            config: RedisConfig with connection parameters

        Raises:
            ImportError: If redis is not installed
        """
        if redis is None:
            raise ImportError("redis required: pip install redis")

        self.config = config
        self._client: Optional[redis.Redis] = None

    def connect(self) -> None:
        """Establish connection to Redis.

        Raises:
            RedisAPIError: On connection failure
        """
        try:
            self._client = redis.Redis(
                host=self.config.host,
                port=self.config.port,
                db=self.config.db,
                password=self.config.password,
                ssl=self.config.ssl,
                socket_timeout=self.config.socket_timeout,
                socket_connect_timeout=self.config.socket_connect_timeout,
                max_connections=self.config.max_connections,
                decode_responses=self.config.decode_responses
            )
            self._client.ping()
            logger.info(f"Connected to Redis: {self.config.host}:{self.config.port}")

        except redis.RedisError as e:
            raise RedisAPIError(f"Connection failed: {e}")

    def disconnect(self) -> None:
        """Close Redis connection."""
        if self._client:
            self._client.close()
            self._client = None
            logger.info("Disconnected from Redis")

    @property
    def client(self) -> redis.Redis:
        """Get Redis client, connect if needed."""
        if self._client is None:
            self.connect()
        return self._client

    def ping(self) -> bool:
        """Check Redis connection.

        Returns:
            True if connected
        """
        try:
            return self.client.ping()
        except redis.RedisError:
            return False

    def set(self, key: str, value: Union[str, int, float, bytes],
            ex: Optional[int] = None, px: Optional[int] = None,
            exat: Optional[int] = None, pxat: Optional[int] = None,
            keepttl: bool = False, get: bool = False) -> Any:
        """Set a key-value pair with optional expiration.

        Args:
            key: Redis key
            value: Value to store
            ex: Expiration in seconds
            px: Expiration in milliseconds
            exat: Expire at Unix timestamp (seconds)
            pxat: Expire at Unix timestamp (milliseconds)
            keepttl: Keep existing TTL
            get: Return old value

        Returns:
            True if set, or old value if get=True
        """
        try:
            return self.client.set(key, value, ex=ex, px=px, exat=exat,
                                   pxat=pxat, keepttl=keepttl, get=get)
        except redis.RedisError as e:
            raise RedisAPIError(f"Set failed: {e}")

    def get(self, key: str) -> Optional[Any]:
        """Get a value by key.

        Args:
            key: Redis key

        Returns:
            Value or None if not found
        """
        try:
            return self.client.get(key)
        except redis.RedisError as e:
            raise RedisAPIError(f"Get failed: {e}")

    def delete(self, *keys: str) -> int:
        """Delete one or more keys.

        Args:
            *keys: Key names to delete

        Returns:
            Number of keys deleted
        """
        try:
            return self.client.delete(*keys)
        except redis.RedisError as e:
            raise RedisAPIError(f"Delete failed: {e}")

    def exists(self, *keys: str) -> int:
        """Check if keys exist.

        Args:
            *keys: Key names to check

        Returns:
            Number of keys that exist
        """
        try:
            return self.client.exists(*keys)
        except redis.RedisError as e:
            raise RedisAPIError(f"Exists failed: {e}")

    def expire(self, key: str, time: Union[int, timedelta]) -> bool:
        """Set expiration on a key.

        Args:
            key: Redis key
            time: Expiration time (seconds or timedelta)

        Returns:
            True if expiration was set
        """
        try:
            return self.client.expire(key, time)
        except redis.RedisError as e:
            raise RedisAPIError(f"Expire failed: {e}")

    def ttl(self, key: str) -> int:
        """Get remaining TTL for a key.

        Args:
            key: Redis key

        Returns:
            TTL in seconds, -1 if no TTL, -2 if key doesn't exist
        """
        try:
            return self.client.ttl(key)
        except redis.RedisError as e:
            raise RedisAPIError(f"TTL failed: {e}")

    def incr(self, key: str, amount: int = 1) -> int:
        """Increment a value.

        Args:
            key: Redis key
            amount: Amount to increment

        Returns:
            New value after increment
        """
        try:
            return self.client.incrby(key, amount)
        except redis.RedisError as e:
            raise RedisAPIError(f"Incr failed: {e}")

    def decr(self, key: str, amount: int = 1) -> int:
        """Decrement a value.

        Args:
            key: Redis key
            amount: Amount to decrement

        Returns:
            New value after decrement
        """
        try:
            return self.client.decrby(key, amount)
        except redis.RedisError as e:
            raise RedisAPIError(f"Decr failed: {e}")

    def hset(self, name: str, key: Optional[str] = None,
             value: Optional[Any] = None, mapping: Optional[dict] = None) -> int:
        """Set hash field(s).

        Args:
            name: Hash key name
            key: Single field name
            value: Single field value
            mapping: Dict of field-value pairs

        Returns:
            Number of fields set
        """
        try:
            if mapping:
                return self.client.hset(name, mapping=mapping)
            return self.client.hset(name, key, value)
        except redis.RedisError as e:
            raise RedisAPIError(f"HSet failed: {e}")

    def hget(self, name: str, key: str) -> Optional[Any]:
        """Get hash field value.

        Args:
            name: Hash key name
            key: Field name

        Returns:
            Field value or None
        """
        try:
            return self.client.hget(name, key)
        except redis.RedisError as e:
            raise RedisAPIError(f"HGet failed: {e}")

    def hgetall(self, name: str) -> dict:
        """Get all hash fields and values.

        Args:
            name: Hash key name

        Returns:
            Dict of all field-value pairs
        """
        try:
            return self.client.hgetall(name)
        except redis.RedisError as e:
            raise RedisAPIError(f"HGetAll failed: {e}")

    def hdel(self, name: str, *keys: str) -> int:
        """Delete hash fields.

        Args:
            name: Hash key name
            *keys: Field names to delete

        Returns:
            Number of fields deleted
        """
        try:
            return self.client.hdel(name, *keys)
        except redis.RedisError as e:
            raise RedisAPIError(f"HDel failed: {e}")

    def hlen(self, name: str) -> int:
        """Get number of fields in hash.

        Args:
            name: Hash key name

        Returns:
            Number of fields
        """
        try:
            return self.client.hlen(name)
        except redis.RedisError as e:
            raise RedisAPIError(f"HLen failed: {e}")

    def lpush(self, name: str, *values: Any) -> int:
        """Push values to list head.

        Args:
            name: List key name
            *values: Values to push

        Returns:
            List length after push
        """
        try:
            return self.client.lpush(name, *values)
        except redis.RedisError as e:
            raise RedisAPIError(f"LPush failed: {e}")

    def rpush(self, name: str, *values: Any) -> int:
        """Push values to list tail.

        Args:
            name: List key name
            *values: Values to push

        Returns:
            List length after push
        """
        try:
            return self.client.rpush(name, *values)
        except redis.RedisError as e:
            raise RedisAPIError(f"RPush failed: {e}")

    def lpop(self, name: str, count: int = 1) -> Any:
        """Pop values from list head.

        Args:
            name: List key name
            count: Number of values to pop

        Returns:
            Popped value(s) or None
        """
        try:
            if count == 1:
                return self.client.lpop(name)
            return self.client.lpop(name, count)
        except redis.RedisError as e:
            raise RedisAPIError(f"LPop failed: {e}")

    def rpop(self, name: str, count: int = 1) -> Any:
        """Pop values from list tail.

        Args:
            name: List key name
            count: Number of values to pop

        Returns:
            Popped value(s) or None
        """
        try:
            if count == 1:
                return self.client.rpop(name)
            return self.client.rpop(name, count)
        except redis.RedisError as e:
            raise RedisAPIError(f"RPop failed: {e}")

    def lrange(self, name: str, start: int = 0, end: int = -1) -> list:
        """Get list range.

        Args:
            name: List key name
            start: Start index
            end: End index (-1 for all)

        Returns:
            List of values
        """
        try:
            return self.client.lrange(name, start, end)
        except redis.RedisError as e:
            raise RedisAPIError(f"LRange failed: {e}")

    def llen(self, name: str) -> int:
        """Get list length.

        Args:
            name: List key name

        Returns:
            List length
        """
        try:
            return self.client.llen(name)
        except redis.RedisError as e:
            raise RedisAPIError(f"LLen failed: {e}")

    def sadd(self, name: str, *values: Any) -> int:
        """Add members to set.

        Args:
            name: Set key name
            *values: Values to add

        Returns:
            Number of members added
        """
        try:
            return self.client.sadd(name, *values)
        except redis.RedisError as e:
            raise RedisAPIError(f"SAdd failed: {e}")

    def smembers(self, name: str) -> set:
        """Get all set members.

        Args:
            name: Set key name

        Returns:
            Set of all members
        """
        try:
            return self.client.smembers(name)
        except redis.RedisError as e:
            raise RedisAPIError(f"SMembers failed: {e}")

    def sismember(self, name: str, value: Any) -> bool:
        """Check if value is member of set.

        Args:
            name: Set key name
            value: Value to check

        Returns:
            True if member
        """
        try:
            return self.client.sismember(name, value)
        except redis.RedisError as e:
            raise RedisAPIError(f"SIsMember failed: {e}")

    def srem(self, name: str, *values: Any) -> int:
        """Remove members from set.

        Args:
            name: Set key name
            *values: Values to remove

        Returns:
            Number of members removed
        """
        try:
            return self.client.srem(name, *values)
        except redis.RedisError as e:
            raise RedisAPIError(f"SRem failed: {e}")

    def zadd(self, name: str, mapping: dict, incr: bool = False) -> int:
        """Add members to sorted set with scores.

        Args:
            name: Sorted set key name
            mapping: Dict of member -> score
            incr: Increment existing scores

        Returns:
            Number of members added
        """
        try:
            return self.client.zadd(name, mapping, incr=incr)
        except redis.RedisError as e:
            raise RedisAPIError(f"ZAdd failed: {e}")

    def zrange(self, name: str, start: int = 0, end: int = -1,
               withscores: bool = False) -> list:
        """Get range of sorted set by index.

        Args:
            name: Sorted set key name
            start: Start index
            end: End index (-1 for all)
            withscores: Include scores in result

        Returns:
            List of members or (member, score) tuples
        """
        try:
            return self.client.zrange(name, start, end, withscores=withscores)
        except redis.RedisError as e:
            raise RedisAPIError(f"ZRange failed: {e}")

    def zrangebyscore(self, name: str, min_score: Union[str, float],
                      max_score: Union[str, float],
                      withscores: bool = False) -> list:
        """Get range by score.

        Args:
            name: Sorted set key name
            min_score: Minimum score
            max_score: Maximum score
            withscores: Include scores in result

        Returns:
            List of members or (member, score) tuples
        """
        try:
            return self.client.zrangebyscore(name, min_score, max_score,
                                             withscores=withscores)
        except redis.RedisError as e:
            raise RedisAPIError(f"ZRangeByScore failed: {e}")

    def zrank(self, name: str, member: Any) -> Optional[int]:
        """Get rank of member in sorted set.

        Args:
            name: Sorted set key name
            member: Member value

        Returns:
            Rank (0-indexed) or None if not found
        """
        try:
            return self.client.zrank(name, member)
        except redis.RedisError as e:
            raise RedisAPIError(f"ZRank failed: {e}")

    def zscore(self, name: str, member: Any) -> Optional[float]:
        """Get score of member in sorted set.

        Args:
            name: Sorted set key name
            member: Member value

        Returns:
            Score or None if not found
        """
        try:
            return self.client.zscore(name, member)
        except redis.RedisError as e:
            raise RedisAPIError(f"ZScore failed: {e}")

    def publish(self, channel: str, message: Any) -> int:
        """Publish message to channel.

        Args:
            channel: Channel name
            message: Message to publish

        Returns:
            Number of subscribers that received message
        """
        try:
            return self.client.publish(channel, message)
        except redis.RedisError as e:
            raise RedisAPIError(f"Publish failed: {e}")

    def cache_set(self, key: str, value: Any, ttl: int = 300) -> bool:
        """Set a cache value with expiration.

        Args:
            key: Cache key
            value: Value to cache (will be JSON serialized)
            ttl: Time to live in seconds

        Returns:
            True if successful
        """
        import json
        try:
            serialized = json.dumps(value)
            return self.set(key, serialized, ex=ttl)
        except (TypeError, ValueError) as e:
            raise RedisAPIError(f"Cache serialize failed: {e}")

    def cache_get(self, key: str) -> Optional[Any]:
        """Get a cached value.

        Args:
            key: Cache key

        Returns:
            Cached value or None
        """
        import json
        try:
            value = self.get(key)
            if value:
                return json.loads(value)
            return None
        except (TypeError, ValueError) as e:
            raise RedisAPIError(f"Cache deserialize failed: {e}")

    def cache_delete(self, key: str) -> bool:
        """Delete a cached value.

        Args:
            key: Cache key

        Returns:
            True if key existed
        """
        return self.delete(key) > 0

    def scan(self, match: Optional[str] = None, count: int = 100) -> list[str]:
        """Scan keys matching pattern.

        Args:
            match: Key pattern (e.g., 'user:*')
            count: Approximate number of keys per scan

        Returns:
            List of matching keys
        """
        try:
            keys = []
            cursor = 0
            while True:
                cursor, partial = self.client.scan(cursor, match=match, count=count)
                keys.extend(partial)
                if cursor == 0:
                    break
            return keys
        except redis.RedisError as e:
            raise RedisAPIError(f"Scan failed: {e}")
