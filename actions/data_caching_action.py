"""
Data Caching Action Module.

Provides multi-level caching capabilities including in-memory cache,
LRU eviction, TTL support, and cache invalidation strategies.

Author: RabAI Team
"""

from typing import Any, Callable, Dict, Hashable, List, Optional, TypeVar, Generic
from dataclasses import dataclass, field
from enum import Enum
import threading
import time
import hashlib
import pickle
from collections import OrderedDict
from datetime import datetime, timedelta
from abc import ABC, abstractmethod


T = TypeVar('T')
K = TypeVar('K', bound=Hashable)
V = TypeVar('V')


class EvictionPolicy(Enum):
    """Cache eviction policies."""
    LRU = "lru"
    LFU = "lfu"
    FIFO = "fifo"
    LIFO = "lifo"
    TTL = "ttl"
    RANDOM = "random"


@dataclass
class CacheEntry(Generic[V]):
    """Represents a cache entry."""
    key: K
    value: V
    created_at: float
    last_accessed: float
    access_count: int = 0
    ttl: Optional[float] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def is_expired(self) -> bool:
        """Check if entry has expired."""
        if self.ttl is None:
            return False
        return time.monotonic() - self.created_at > self.ttl
    
    def touch(self):
        """Update last accessed time and count."""
        self.last_accessed = time.monotonic()
        self.access_count += 1


class CacheBackend(ABC):
    """Abstract base class for cache backends."""
    
    @abstractmethod
    def get(self, key: K) -> Optional[V]:
        pass
    
    @abstractmethod
    def set(self, key: K, value: V, ttl: Optional[float] = None) -> None:
        pass
    
    @abstractmethod
    def delete(self, key: K) -> bool:
        pass
    
    @abstractmethod
    def clear(self) -> None:
        pass
    
    @abstractmethod
    def keys(self) -> List[K]:
        pass


class InMemoryCache(CacheBackend):
    """
    In-memory cache with configurable eviction policy.
    
    Example:
        cache = InMemoryCache(max_size=100, policy=EvictionPolicy.LRU)
        cache.set("key1", "value1", ttl=60)
        value = cache.get("key1")
    """
    
    def __init__(
        self,
        max_size: int = 1000,
        policy: EvictionPolicy = EvictionPolicy.LRU,
        default_ttl: Optional[float] = None
    ):
        self.max_size = max_size
        self.policy = policy
        self.default_ttl = default_ttl
        
        self._cache: OrderedDict = OrderedDict()
        self._access_counts: Dict[K, int] = {}
        self._lock = threading.RLock()
    
    def get(self, key: K) -> Optional[V]:
        """Get value from cache."""
        with self._lock:
            if key not in self._cache:
                return None
            
            entry: CacheEntry = self._cache[key]
            
            if entry.is_expired():
                del self._cache[key]
                return None
            
            # Update access metadata based on policy
            if self.policy == EvictionPolicy.LRU:
                self._cache.move_to_end(key)
            elif self.policy == EvictionPolicy.LFU:
                self._access_counts[key] = entry.access_count + 1
            
            entry.touch()
            return entry.value
    
    def set(self, key: K, value: V, ttl: Optional[float] = None) -> None:
        """Set value in cache."""
        with self._lock:
            # Evict if necessary
            if key not in self._cache and len(self._cache) >= self.max_size:
                self._evict()
            
            now = time.monotonic()
            entry = CacheEntry(
                key=key,
                value=value,
                created_at=now,
                last_accessed=now,
                ttl=ttl or self.default_ttl
            )
            
            self._cache[key] = entry
            if self.policy == EvictionPolicy.LFU:
                self._access_counts[key] = 0
    
    def delete(self, key: K) -> bool:
        """Delete value from cache."""
        with self._lock:
            if key in self._cache:
                del self._cache[key]
                self._access_counts.pop(key, None)
                return True
            return False
    
    def clear(self) -> None:
        """Clear all entries."""
        with self._lock:
            self._cache.clear()
            self._access_counts.clear()
    
    def keys(self) -> List[K]:
        """Get all cache keys."""
        with self._lock:
            return list(self._cache.keys())
    
    def _evict(self):
        """Evict one entry based on policy."""
        if not self._cache:
            return
        
        if self.policy == EvictionPolicy.LRU:
            self._cache.popitem(last=False)
        elif self.policy == EvictionPolicy.LFU:
            min_key = min(self._access_counts, key=self._access_counts.get)
            del self._cache[min_key]
            del self._access_counts[min_key]
        elif self.policy == EvictionPolicy.FIFO:
            self._cache.popitem(last=False)
        elif self.policy == EvictionPolicy.LIFO:
            self._cache.popitem(last=True)
        elif self.policy == EvictionPolicy.RANDOM:
            key = next(iter(self._cache))
            del self._cache[key]
        elif self.policy == EvictionPolicy.TTL:
            # Evict oldest expired
            for key, entry in self._cache.items():
                if entry.is_expired():
                    del self._cache[key]
                    return


class TwoLevelCache(CacheBackend):
    """
    Two-level cache with L1 (memory) and L2 (persistent) layers.
    
    Example:
        cache = TwoLevelCache(l1_size=100, l2_size=1000)
        cache.set("key", "value")  # Writes to both levels
    """
    
    def __init__(
        self,
        l1_cache: InMemoryCache,
        l2_cache: Optional[CacheBackend] = None
    ):
        self.l1 = l1_cache
        self.l2 = l2_cache
        self._lock = threading.RLock()
    
    def get(self, key: K) -> Optional[V]:
        """Get from L1, fallback to L2."""
        with self._lock:
            value = self.l1.get(key)
            if value is not None:
                return value
            
            if self.l2:
                value = self.l2.get(key)
                if value is not None:
                    # Promote to L1
                    self.l1.set(key, value)
                return value
            
            return None
    
    def set(self, key: K, value: V, ttl: Optional[float] = None) -> None:
        """Set in both L1 and L2."""
        with self._lock:
            self.l1.set(key, value, ttl)
            if self.l2:
                self.l2.set(key, value, ttl)
    
    def delete(self, key: K) -> bool:
        """Delete from both levels."""
        with self._lock:
            l1_deleted = self.l1.delete(key)
            l2_deleted = self.l2.delete(key) if self.l2 else False
            return l1_deleted or l2_deleted
    
    def clear(self) -> None:
        """Clear both levels."""
        self.l1.clear()
        if self.l2:
            self.l2.clear()
    
    def keys(self) -> List[K]:
        """Get all keys from L1."""
        return self.l1.keys()


class CacheDecorator:
    """
    Decorator for adding caching to functions.
    
    Example:
        @CacheDecorator(cache)
        def expensive_function(x, y):
            return x + y
    """
    
    def __init__(self, cache: CacheBackend):
        self.cache = cache
    
    def __call__(self, func: Callable[..., V]) -> Callable[..., V]:
        def wrapper(*args, **kwargs) -> V:
            # Create cache key from function name and args
            key = self._make_key(func.__name__, args, kwargs)
            
            # Try cache
            result = self.cache.get(key)
            if result is not None:
                return result
            
            # Execute function
            result = func(*args, **kwargs)
            
            # Store in cache
            self.cache.set(key, result)
            
            return result
        
        return wrapper
    
    def _make_key(self, func_name: str, args: tuple, kwargs: dict) -> str:
        """Create cache key from function call."""
        key_data = {
            "func": func_name,
            "args": args,
            "kwargs": kwargs
        }
        key_str = pickle.dumps(key_data)
        return hashlib.md5(key_str).hexdigest()


class WriteBackCache:
    """
    Write-back cache that batches writes.
    
    Example:
        cache = WriteBackCache(max_size=100, flush_interval=5.0)
        cache.set("key", "value")
        cache.close()  # Flushes pending writes
    """
    
    def __init__(
        self,
        backend: CacheBackend,
        max_size: int = 1000,
        flush_interval: float = 60.0
    ):
        self.backend = backend
        self.max_size = max_size
        self.flush_interval = flush_interval
        
        self._write_buffer: Dict[K, V] = {}
        self._dirty_keys: Set[K] = set()
        self._lock = threading.RLock()
        
        self._flush_thread = threading.Thread(target=self._auto_flush, daemon=True)
        self._running = True
        self._flush_thread.start()
    
    def get(self, key: K) -> Optional[V]:
        """Get value, checking write buffer first."""
        with self._lock:
            if key in self._write_buffer:
                return self._write_buffer[key]
            return self.backend.get(key)
    
    def set(self, key: K, value: V) -> None:
        """Set value in write buffer."""
        with self._lock:
            self._write_buffer[key] = value
            self._dirty_keys.add(key)
            
            if len(self._write_buffer) >= self.max_size:
                self.flush()
    
    def flush(self):
        """Flush write buffer to backend."""
        with self._lock:
            for key in self._dirty_keys:
                if key in self._write_buffer:
                    self.backend.set(key, self._write_buffer[key])
            self._write_buffer.clear()
            self._dirty_keys.clear()
    
    def close(self):
        """Close cache and flush pending writes."""
        self._running = False
        self.flush()
    
    def _auto_flush(self):
        """Auto-flush thread."""
        while self._running:
            time.sleep(self.flush_interval)
            if self._running:
                self.flush()


class BaseAction:
    """Base class for all actions."""
    
    def execute(self, context: Dict[str, Any], params: Dict[str, Any]) -> Any:
        raise NotImplementedError


class DataCachingAction(BaseAction):
    """
    Data caching action for automation workflows.
    
    Parameters:
        operation: Operation type (set/get/delete/clear)
        key: Cache key
        value: Value to cache
        ttl: Time to live in seconds
        policy: Eviction policy (lru/lfu/fifo/ttl)
        max_size: Maximum cache size
    
    Example:
        action = DataCachingAction()
        result = action.execute({}, {
            "operation": "set",
            "key": "user_123",
            "value": {"name": "John"},
            "ttl": 300
        })
    """
    
    _cache: Optional[InMemoryCache] = None
    _lock = threading.Lock()
    
    def execute(self, context: Dict[str, Any], params: Dict[str, Any]) -> Dict[str, Any]:
        """Execute caching operation."""
        operation = params.get("operation", "get")
        key = params.get("key")
        value = params.get("value")
        ttl = params.get("ttl")
        policy_str = params.get("policy", "lru")
        max_size = params.get("max_size", 1000)
        
        policy = EvictionPolicy(policy_str)
        
        with self._lock:
            if self._cache is None:
                self._cache = InMemoryCache(max_size=max_size, policy=policy)
            elif operation == "clear":
                self._cache.clear()
                return {"success": True, "operation": "clear}
        
        if operation == "set":
            self._cache.set(key, value, ttl)
            return {
                "success": True,
                "operation": "set",
                "key": key,
                "cached_at": datetime.now().isoformat()
            }
        
        elif operation == "get":
            result = self._cache.get(key)
            return {
                "success": result is not None,
                "operation": "get",
                "key": key,
                "value": result,
                "found": result is not None
            }
        
        elif operation == "delete":
            deleted = self._cache.delete(key)
            return {
                "success": deleted,
                "operation": "delete",
                "key": key
            }
        
        elif operation == "clear":
            self._cache.clear()
            return {"success": True, "operation": "clear"}
        
        elif operation == "stats":
            return {
                "success": True,
                "operation": "stats",
                "size": len(self._cache.keys()),
                "max_size": max_size,
                "policy": policy_str
            }
        
        else:
            return {"success": False, "error": f"Unknown operation: {operation}"}
