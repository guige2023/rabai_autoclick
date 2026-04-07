"""
Redis operations actions.
"""
from __future__ import annotations

import redis
from typing import Dict, Any, Optional, List, Union


def create_redis_client(
    host: str = 'localhost',
    port: int = 6379,
    db: int = 0,
    password: Optional[str] = None,
    decode_responses: bool = True
) -> redis.Redis:
    """
    Create a Redis client.

    Args:
        host: Redis host.
        port: Redis port.
        db: Database number.
        password: Redis password.
        decode_responses: Decode responses to strings.

    Returns:
        Redis client.
    """
    return redis.Redis(
        host=host,
        port=port,
        db=db,
        password=password,
        decode_responses=decode_responses
    )


def ping_redis(client: redis.Redis) -> bool:
    """
    Ping Redis server.

    Args:
        client: Redis client.

    Returns:
        True if connected.
    """
    try:
        return client.ping()
    except redis.RedisError:
        return False


def get_value(client: redis.Redis, key: str) -> Optional[str]:
    """
    Get a value from Redis.

    Args:
        client: Redis client.
        key: Key name.

    Returns:
        Value or None.
    """
    return client.get(key)


def set_value(
    client: redis.Redis,
    key: str,
    value: str,
    expire: Optional[int] = None
) -> bool:
    """
    Set a value in Redis.

    Args:
        client: Redis client.
        key: Key name.
        value: Value to set.
        expire: Expiration in seconds.

    Returns:
        True if successful.
    """
    return client.set(key, value, ex=expire)


def delete_key(client: redis.Redis, key: str) -> bool:
    """
    Delete a key.

    Args:
        client: Redis client.
        key: Key to delete.

    Returns:
        True if key was deleted.
    """
    return bool(client.delete(key))


def key_exists(client: redis.Redis, key: str) -> bool:
    """
    Check if a key exists.

    Args:
        client: Redis client.
        key: Key to check.

    Returns:
        True if key exists.
    """
    return bool(client.exists(key))


def get_ttl(client: redis.Redis, key: str) -> int:
    """
    Get TTL of a key.

    Args:
        client: Redis client.
        key: Key name.

    Returns:
        TTL in seconds, -1 if no expiry, -2 if key doesn't exist.
    """
    return client.ttl(key)


def expire_key(client: redis.Redis, key: str, seconds: int) -> bool:
    """
    Set expiration on a key.

    Args:
        client: Redis client.
        key: Key name.
        seconds: Expiration in seconds.

    Returns:
        True if expiration was set.
    """
    return client.expire(key, seconds)


def increment(client: redis.Redis, key: str, amount: int = 1) -> int:
    """
    Increment a value.

    Args:
        client: Redis client.
        key: Key name.
        amount: Amount to increment.

    Returns:
        New value.
    """
    return client.incrby(key, amount)


def decrement(client: redis.Redis, key: str, amount: int = 1) -> int:
    """
    Decrement a value.

    Args:
        client: Redis client.
        key: Key name.
        amount: Amount to decrement.

    Returns:
        New value.
    """
    return client.decrby(key, amount)


def get_hash(client: redis.Redis, key: str) -> Dict[str, str]:
    """
    Get all fields and values of a hash.

    Args:
        client: Redis client.
        key: Hash key.

    Returns:
        Dictionary of field-value pairs.
    """
    return client.hgetall(key)


def set_hash_field(
    client: redis.Redis,
    key: str,
    field: str,
    value: str
) -> bool:
    """
    Set a field in a hash.

    Args:
        client: Redis client.
        key: Hash key.
        field: Field name.
        value: Field value.

    Returns:
        True if set.
    """
    return bool(client.hset(key, field, value))


def get_hash_field(client: redis.Redis, key: str, field: str) -> Optional[str]:
    """
    Get a field from a hash.

    Args:
        client: Redis client.
        key: Hash key.
        field: Field name.

    Returns:
        Field value or None.
    """
    return client.hget(key, field)


def delete_hash_fields(client: redis.Redis, key: str, fields: List[str]) -> int:
    """
    Delete fields from a hash.

    Args:
        client: Redis client.
        key: Hash key.
        fields: List of field names.

    Returns:
        Number of fields deleted.
    """
    return client.hdel(key, *fields)


def get_list(client: redis.Redis, key: str, start: int = 0, end: int = -1) -> List[str]:
    """
    Get list items.

    Args:
        client: Redis client.
        key: List key.
        start: Start index.
        end: End index (-1 for all).

    Returns:
        List of items.
    """
    return client.lrange(key, start, end)


def push_to_list(
    client: redis.Redis,
    key: str,
    value: str,
    left: bool = False
) -> int:
    """
    Push to a list.

    Args:
        client: Redis client.
        key: List key.
        value: Value to push.
        left: Push to left (front) or right (back).

    Returns:
        List length after push.
    """
    if left:
        return client.lpush(key, value)
    return client.rpush(key, value)


def pop_from_list(client: redis.Redis, key: str, left: bool = False) -> Optional[str]:
    """
    Pop from a list.

    Args:
        client: Redis client.
        key: List key.
        left: Pop from left (front) or right (back).

    Returns:
        Popped value or None.
    """
    if left:
        return client.lpop(key)
    return client.rpop(key)


def get_set(client: redis.Redis, key: str) -> set:
    """
    Get all members of a set.

    Args:
        client: Redis client.
        key: Set key.

    Returns:
        Set of members.
    """
    return client.smembers(key)


def add_to_set(client: redis.Redis, key: str, *values: str) -> int:
    """
    Add members to a set.

    Args:
        client: Redis client.
        key: Set key.
        values: Values to add.

    Returns:
        Number of members added.
    """
    return client.sadd(key, *values)


def is_set_member(client: redis.Redis, key: str, value: str) -> bool:
    """
    Check if value is a member of set.

    Args:
        client: Redis client.
        key: Set key.
        value: Value to check.

    Returns:
        True if member.
    """
    return client.sismember(key, value)


def remove_from_set(client: redis.Redis, key: str, *values: str) -> int:
    """
    Remove members from a set.

    Args:
        client: Redis client.
        key: Set key.
        values: Values to remove.

    Returns:
        Number of members removed.
    """
    return client.srem(key, *values)


def get_sorted_set(client: redis.Redis, key: str, start: int = 0, end: int = -1) -> List:
    """
    Get sorted set range with scores.

    Args:
        client: Redis client.
        key: Sorted set key.
        start: Start index.
        end: End index (-1 for all).

    Returns:
        List of (member, score) tuples.
    """
    return client.zrange(key, start, end, withscores=True)


def add_to_sorted_set(
    client: redis.Redis,
    key: str,
    mapping: Dict[str, float]
) -> int:
    """
    Add members to sorted set with scores.

    Args:
        client: Redis client.
        key: Sorted set key.
        mapping: Dictionary of member -> score.

    Returns:
        Number of members added.
    """
    return client.zadd(key, mapping)


def get_sorted_set_rank(client: redis.Redis, key: str, member: str) -> Optional[int]:
    """
    Get rank of member in sorted set (ascending).

    Args:
        client: Redis client.
        key: Sorted set key.
        member: Member name.

    Returns:
        Rank (0-indexed) or None.
    """
    return client.zrank(key, member)


def get_sorted_set_score(client: redis.Redis, key: str, member: str) -> Optional[float]:
    """
    Get score of member in sorted set.

    Args:
        client: Redis client.
        key: Sorted set key.
        member: Member name.

    Returns:
        Score or None.
    """
    return client.zscore(key, member)


def publish_message(client: redis.Redis, channel: str, message: str) -> int:
    """
    Publish a message to a channel.

    Args:
        client: Redis client.
        channel: Channel name.
        message: Message to publish.

    Returns:
        Number of subscribers received.
    """
    return client.publish(channel, message)


def get_all_keys(client: redis.Redis, pattern: str = '*') -> List[str]:
    """
    Get all keys matching pattern.

    Args:
        client: Redis client.
        pattern: Key pattern.

    Returns:
        List of keys.
    """
    return client.keys(pattern)


def get_database_info(client: redis.Redis) -> Dict[str, Any]:
    """
    Get Redis database info.

    Args:
        client: Redis client.

    Returns:
        Database info.
    """
    return client.info()


def flush_database(client: redis.Redis, db: int = 0) -> bool:
    """
    Flush a database.

    Args:
        client: Redis client.
        db: Database number.

    Returns:
        True if flushed.
    """
    return client.flushdb()


def test_connection(
    host: str = 'localhost',
    port: int = 6379,
    password: Optional[str] = None
) -> Dict[str, Any]:
    """
    Test Redis connection.

    Args:
        host: Redis host.
        port: Redis port.
        password: Redis password.

    Returns:
        Test result.
    """
    try:
        client = create_redis_client(
            host=host,
            port=port,
            password=password
        )

        if client.ping():
            info = client.info()
            return {
                'success': True,
                'host': host,
                'port': port,
                'version': info.get('redis_version'),
            }
        return {'success': False, 'error': 'Ping failed'}
    except Exception as e:
        return {'success': False, 'error': str(e)}


def cache_set(
    client: redis.Redis,
    key: str,
    value: str,
    ttl: int = 300
) -> bool:
    """
    Set a cached value with TTL.

    Args:
        client: Redis client.
        key: Cache key.
        value: Value to cache.
        ttl: Time to live in seconds.

    Returns:
        True if set.
    """
    return client.setex(key, ttl, value)


def cache_get(client: redis.Redis, key: str) -> Optional[str]:
    """
    Get a cached value.

    Args:
        client: Redis client.
        key: Cache key.

    Returns:
        Cached value or None.
    """
    return client.get(key)


def cache_delete(client: redis.Redis, key: str) -> bool:
    """
    Delete a cached value.

    Args:
        client: Redis client.
        key: Cache key.

    Returns:
        True if deleted.
    """
    return bool(client.delete(key))


def increment_counter(client: redis.Redis, key: str, ttl: Optional[int] = None) -> int:
    """
    Increment a counter, creating it if needed.

    Args:
        client: Redis client.
        key: Counter key.
        ttl: Optional TTL.

    Returns:
        New counter value.
    """
    pipe = client.pipeline()
    pipe.incr(key)
    if ttl:
        pipe.expire(key, ttl)
    results = pipe.execute()
    return results[0]


def get_or_set_lock(
    client: redis.Redis,
    lock_name: str,
    ttl: int = 10
) -> bool:
    """
    Acquire a distributed lock.

    Args:
        client: Redis client.
        lock_name: Lock name.
        ttl: Lock TTL in seconds.

    Returns:
        True if lock acquired.
    """
    return bool(client.set(f'lock:{lock_name}', '1', nx=True, ex=ttl))


def release_lock(client: redis.Redis, lock_name: str) -> bool:
    """
    Release a distributed lock.

    Args:
        client: Redis client.
        lock_name: Lock name.

    Returns:
        True if released.
    """
    return bool(client.delete(f'lock:{lock_name}'))
