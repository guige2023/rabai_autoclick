"""Cache action module for RabAI AutoClick.

Provides caching operations for improving performance of repeated operations,
including TTL-based cache, LRU cache, and cache invalidation.
"""

import time
import hashlib
import json
import pickle
import sys
import os
from typing import Any, Dict, List, Optional, Union, Tuple
from dataclasses import dataclass, field

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


@dataclass
class CacheEntry:
    """A single cache entry with value and expiration.
    
    Attributes:
        value: The cached value.
        expires_at: Unix timestamp when this entry expires.
        created_at: Unix timestamp when this entry was created.
        hits: Number of times this entry has been accessed.
    """
    value: Any
    expires_at: float
    created_at: float = field(default_factory=time.time)
    hits: int = 1


class InMemoryCache:
    """Thread-safe in-memory cache with TTL and LRU support.
    
    Provides a simple in-memory cache implementation with support for:
    - Time-to-live (TTL) expiration
    - Maximum cache size limits
    - Cache hit/miss statistics
    - Manual and automatic cache invalidation
    """
    
    def __init__(
        self,
        max_size: int = 1000,
        default_ttl: float = 3600.0,
        enable_stats: bool = True
    ) -> None:
        """Initialize the in-memory cache.
        
        Args:
            max_size: Maximum number of entries in cache.
            default_ttl: Default time-to-live in seconds.
            enable_stats: Whether to track hit/miss statistics.
        """
        self._cache: Dict[str, CacheEntry] = {}
        self._max_size = max_size
        self._default_ttl = default_ttl
        self._enable_stats = enable_stats
        self._stats: Dict[str, int] = {"hits": 0, "misses": 0, "evictions": 0}
        self._lock = __import__("threading").Lock()
    
    def get(self, key: str) -> Tuple[bool, Any]:
        """Get a value from cache.
        
        Args:
            key: Cache key to retrieve.
            
        Returns:
            Tuple of (found, value) where found indicates if key existed
            and value is the cached value or None.
        """
        with self._lock:
            if key not in self._cache:
                if self._enable_stats:
                    self._stats["misses"] += 1
                return False, None
            
            entry = self._cache[key]
            if entry.expires_at > 0 and time.time() > entry.expires_at:
                del self._cache[key]
                if self._enable_stats:
                    self._stats["misses"] += 1
                return False, None
            
            entry.hits += 1
            if self._enable_stats:
                self._stats["hits"] += 1
            return True, entry.value
    
    def set(
        self,
        key: str,
        value: Any,
        ttl: Optional[float] = None
    ) -> bool:
        """Set a value in cache.
        
        Args:
            key: Cache key to set.
            value: Value to cache.
            ttl: Optional TTL in seconds (uses default if not specified).
            
        Returns:
            True if successful, False otherwise.
        """
        ttl = ttl if ttl is not None else self._default_ttl
        expires_at = time.time() + ttl if ttl > 0 else 0
        
        with self._lock:
            if len(self._cache) >= self._max_size and key not in self._cache:
                self._evict_lru()
            
            self._cache[key] = CacheEntry(value=value, expires_at=expires_at)
            return True
    
    def delete(self, key: str) -> bool:
        """Delete a key from cache.
        
        Args:
            key: Cache key to delete.
            
        Returns:
            True if key was deleted, False if not found.
        """
        with self._lock:
            if key in self._cache:
                del self._cache[key]
                return True
            return False
    
    def clear(self) -> int:
        """Clear all entries from cache.
        
        Returns:
            Number of entries that were cleared.
        """
        with self._lock:
            count = len(self._cache)
            self._cache.clear()
            return count
    
    def _evict_lru(self) -> None:
        """Evict the least recently used entry."""
        if not self._cache:
            return
        
        lru_key = min(
            self._cache.keys(),
            key=lambda k: (self._cache[k].created_at, self._cache[k].hits)
        )
        del self._cache[lru_key]
        if self._enable_stats:
            self._stats["evictions"] += 1
    
    def get_stats(self) -> Dict[str, int]:
        """Get cache statistics.
        
        Returns:
            Dictionary with hits, misses, evictions, and size.
        """
        with self._lock:
            return {
                "hits": self._stats["hits"],
                "misses": self._stats["misses"],
                "evictions": self._stats["evictions"],
                "size": len(self._cache),
                "max_size": self._max_size,
                "hit_rate": (
                    self._stats["hits"] / (self._stats["hits"] + self._stats["misses"])
                    if (self._stats["hits"] + self._stats["misses"]) > 0
                    else 0.0
                )
            }
    
    def cleanup_expired(self) -> int:
        """Remove all expired entries.
        
        Returns:
            Number of entries that were removed.
        """
        with self._lock:
            now = time.time()
            expired_keys = [
                k for k, v in self._cache.items()
                if v.expires_at > 0 and now > v.expires_at
            ]
            for key in expired_keys:
                del self._cache[key]
            return len(expired_keys)


class CacheAction(BaseAction):
    """Cache action for storing and retrieving cached data.
    
    Supports in-memory caching with TTL, size limits, and statistics.
    """
    action_type: str = "cache"
    display_name: str = "缓存动作"
    description: str = "缓存数据以提高性能，支持TTL和LRU驱逐"
    
    def __init__(self) -> None:
        super().__init__()
        self._cache = InMemoryCache()
    
    def get_required_params(self) -> List[str]:
        """Return required parameters for this action."""
        return ["operation", "key"]
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute cache operation.
        
        Args:
            context: Execution context.
            params: Operation and parameters.
            
        Returns:
            ActionResult with operation outcome.
        """
        start_time = time.time()
        
        try:
            operation = params.get("operation", "get")
            key = str(params.get("key", ""))
            
            if not key and operation in ["get", "delete"]:
                return ActionResult(
                    success=False,
                    message="Cache key is required",
                    duration=time.time() - start_time
                )
            
            if operation == "get":
                found, value = self._cache.get(key)
                return ActionResult(
                    success=True,
                    message="Cache hit" if found else "Cache miss",
                    data={"found": found, "value": value},
                    duration=time.time() - start_time
                )
            
            elif operation == "set":
                value = params.get("value")
                ttl = params.get("ttl")
                self._cache.set(key, value, ttl)
                return ActionResult(
                    success=True,
                    message=f"Cached value for key: {key}",
                    duration=time.time() - start_time
                )
            
            elif operation == "delete":
                deleted = self._cache.delete(key)
                return ActionResult(
                    success=True,
                    message=f"Key deleted: {key}" if deleted else f"Key not found: {key}",
                    data={"deleted": deleted},
                    duration=time.time() - start_time
                )
            
            elif operation == "clear":
                count = self._cache.clear()
                return ActionResult(
                    success=True,
                    message=f"Cleared {count} entries",
                    data={"count": count},
                    duration=time.time() - start_time
                )
            
            elif operation == "stats":
                stats = self._cache.get_stats()
                return ActionResult(
                    success=True,
                    message="Cache statistics retrieved",
                    data=stats,
                    duration=time.time() - start_time
                )
            
            elif operation == "cleanup":
                count = self._cache.cleanup_expired()
                return ActionResult(
                    success=True,
                    message=f"Cleaned up {count} expired entries",
                    data={"count": count},
                    duration=time.time() - start_time
                )
            
            else:
                return ActionResult(
                    success=False,
                    message=f"Unknown operation: {operation}",
                    duration=time.time() - start_time
                )
        
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Cache operation failed: {str(e)}",
                duration=time.time() - start_time
            )
