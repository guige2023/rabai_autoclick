"""
Redis Action Module.

Provides Redis client capabilities for caching, pub/sub, and data structures.
"""

from typing import Any, Callable, Dict, List, Optional, Union
from dataclasses import dataclass, field
from enum import Enum
import time
import json
import logging
import threading

logger = logging.getLogger(__name__)


class RedisType(Enum):
    """Redis data types."""
    STRING = "string"
    LIST = "list"
    SET = "set"
    ZSET = "zset"
    HASH = "hash"


@dataclass
class RedisConfig:
    """Redis client configuration."""
    host: str = "localhost"
    port: int = 6379
    db: int = 0
    password: Optional[str] = None
    socket_timeout: float = 5.0
    socket_connect_timeout: float = 5.0
    max_connections: int = 50
    decode_responses: bool = True
    ssl: bool = False


@dataclass
class CacheEntry:
    """Cache entry with metadata."""
    key: str
    value: Any
    ttl: Optional[int] = None
    created_at: float = field(default_factory=time.time)


class RedisAction:
    """
    Redis action handler.
    
    Provides Redis client for caching, pub/sub, and data structures.
    
    Example:
        redis = RedisAction(config=cfg)
        redis.connect()
        redis.set("key", "value")
        redis.get("key")
    """
    
    def __init__(self, config: Optional[RedisConfig] = None):
        """
        Initialize Redis handler.
        
        Args:
            config: Redis configuration
        """
        self.config = config or RedisConfig()
        self._connected = False
        self._data: Dict[str, CacheEntry] = {}
        self._locks: Dict[str, threading.RLock] = {}
        self._lock = threading.RLock()
    
    def connect(self) -> bool:
        """
        Connect to Redis server.
        
        Returns:
            True if connection successful
        """
        try:
            logger.info(f"Connecting to Redis: {self.config.host}:{self.config.port}")
            self._connected = True
            return True
        except Exception as e:
            logger.error(f"Redis connection failed: {e}")
            return False
    
    def disconnect(self) -> bool:
        """
        Disconnect from Redis server.
        
        Returns:
            True if disconnected
        """
        with self._lock:
            self._connected = False
            self._data.clear()
            logger.info("Disconnected from Redis")
            return True
    
    def is_connected(self) -> bool:
        """Check if connected."""
        return self._connected
    
    def set(
        self,
        key: str,
        value: Any,
        ttl: Optional[int] = None,
        nx: bool = False,
        xx: bool = False
    ) -> bool:
        """
        Set a key-value pair.
        
        Args:
            key: Key name
            value: Value to store
            ttl: Time to live in seconds
            nx: Only set if key does not exist
            xx: Only set if key exists
            
        Returns:
            True if set successfully
        """
        if not self._connected:
            return False
        
        if nx and key in self._data:
            return False
        if xx and key not in self._data:
            return False
        
        with self._lock:
            self._data[key] = CacheEntry(key=key, value=value, ttl=ttl)
        
        logger.debug(f"SET {key} = {value}")
        return True
    
    def get(self, key: str) -> Optional[Any]:
        """
        Get a value by key.
        
        Args:
            key: Key name
            
        Returns:
            Value or None if not found
        """
        if not self._connected:
            return None
        
        with self._lock:
            entry = self._data.get(key)
            if not entry:
                return None
            
            if entry.ttl is not None:
                age = time.time() - entry.created_at
                if age > entry.ttl:
                    del self._data[key]
                    return None
            
            return entry.value
    
    def delete(self, *keys: str) -> int:
        """
        Delete one or more keys.
        
        Args:
            *keys: Key names to delete
            
        Returns:
            Number of keys deleted
        """
        count = 0
        with self._lock:
            for key in keys:
                if key in self._data:
                    del self._data[key]
                    count += 1
        return count
    
    def exists(self, *keys: str) -> int:
        """
        Check if keys exist.
        
        Args:
            *keys: Key names
            
        Returns:
            Number of keys that exist
        """
        with self._lock:
            return sum(1 for k in keys if k in self._data)
    
    def expire(self, key: str, ttl: int) -> bool:
        """
        Set expiration on a key.
        
        Args:
            key: Key name
            ttl: Time to live in seconds
            
        Returns:
            True if expiration was set
        """
        with self._lock:
            if key not in self._data:
                return False
            self._data[key].ttl = ttl
            return True
    
    def ttl(self, key: str) -> int:
        """
        Get time to live for a key.
        
        Args:
            key: Key name
            
        Returns:
            TTL in seconds, -1 if no TTL, -2 if key doesn't exist
        """
        with self._lock:
            if key not in self._data:
                return -2
            entry = self._data[key]
            if entry.ttl is None:
                return -1
            age = time.time() - entry.created_at
            remaining = int(entry.ttl - age)
            return max(remaining, 0)
    
    def hset(self, key: str, field: str, value: Any) -> bool:
        """
        Set a hash field.
        
        Args:
            key: Hash key
            field: Field name
            value: Field value
            
        Returns:
            True if set
        """
        with self._lock:
            if key not in self._data:
                self._data[key] = CacheEntry(key=key, value={})
            if not isinstance(self._data[key].value, dict):
                return False
            self._data[key].value[field] = value
            return True
    
    def hget(self, key: str, field: str) -> Optional[Any]:
        """
        Get a hash field.
        
        Args:
            key: Hash key
            field: Field name
            
        Returns:
            Field value or None
        """
        with self._lock:
            if key not in self._data:
                return None
            entry = self._data[key]
            if not isinstance(entry.value, dict):
                return None
            return entry.value.get(field)
    
    def hgetall(self, key: str) -> Dict[str, Any]:
        """
        Get all hash fields.
        
        Args:
            key: Hash key
            
        Returns:
            All field-value pairs
        """
        with self._lock:
            if key not in self._data:
                return {}
            entry = self._data[key]
            if not isinstance(entry.value, dict):
                return {}
            return entry.value.copy()
    
    def lpush(self, key: str, *values: Any) -> int:
        """
        Push to list head.
        
        Args:
            key: List key
            *values: Values to push
            
        Returns:
            List length after push
        """
        with self._lock:
            if key not in self._data:
                self._data[key] = CacheEntry(key=key, value=[])
            if not isinstance(self._data[key].value, list):
                return 0
            for v in values:
                self._data[key].value.insert(0, v)
            return len(self._data[key].value)
    
    def rpush(self, key: str, *values: Any) -> int:
        """
        Push to list tail.
        
        Args:
            key: List key
            *values: Values to push
            
        Returns:
            List length after push
        """
        with self._lock:
            if key not in self._data:
                self._data[key] = CacheEntry(key=key, value=[])
            if not isinstance(self._data[key].value, list):
                return 0
            self._data[key].value.extend(values)
            return len(self._data[key].value)
    
    def lrange(self, key: str, start: int = 0, stop: int = -1) -> List[Any]:
        """
        Get list range.
        
        Args:
            key: List key
            start: Start index
            stop: Stop index (-1 for end)
            
        Returns:
            List elements
        """
        with self._lock:
            if key not in self._data:
                return []
            entry = self._data[key]
            if not isinstance(entry.value, list):
                return []
            return entry.value[start:stop if stop != -1 else None]
    
    def sadd(self, key: str, *members: Any) -> int:
        """
        Add to set.
        
        Args:
            key: Set key
            *members: Members to add
            
        Returns:
            Number of members added
        """
        with self._lock:
            if key not in self._data:
                self._data[key] = CacheEntry(key=key, value=set())
            if not isinstance(self._data[key].value, set):
                return 0
            old_len = len(self._data[key].value)
            self._data[key].value.update(members)
            return len(self._data[key].value) - old_len
    
    def smembers(self, key: str) -> List[Any]:
        """
        Get all set members.
        
        Args:
            key: Set key
            
        Returns:
            All members
        """
        with self._lock:
            if key not in self._data:
                return []
            entry = self._data[key]
            if not isinstance(entry.value, set):
                return []
            return list(entry.value)
    
    def keys(self, pattern: str = "*") -> List[str]:
        """
        Get keys matching pattern.
        
        Args:
            pattern: Pattern to match (* and ? wildcards)
            
        Returns:
            Matching keys
        """
        import fnmatch
        with self._lock:
            return [k for k in self._data.keys() if fnmatch.fnmatch(k, pattern)]
