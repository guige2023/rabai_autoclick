"""Data Cache Action Module.

Provides caching utilities: cache strategies, invalidation policies,
distributed caching helpers, and cache performance monitoring.

Example:
    result = execute(context, {"action": "set", "key": "user:123", "value": {...}})
"""
from typing import Any, Optional, Callable
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from collections import OrderedDict
import hashlib
import json


@dataclass
class CacheEntry:
    """A cache entry."""
    
    key: str
    value: Any
    created_at: datetime = field(default_factory=datetime.now)
    accessed_at: datetime = field(default_factory=datetime.now)
    access_count: int = 0
    ttl_seconds: Optional[float] = None
    tags: list[str] = field(default_factory=list)
    
    @property
    def is_expired(self) -> bool:
        """Check if entry is expired."""
        if self.ttl_seconds is None:
            return False
        age = (datetime.now() - self.created_at).total_seconds()
        return age > self.ttl_seconds
    
    def touch(self) -> None:
        """Update access time and count."""
        self.accessed_at = datetime.now()
        self.access_count += 1


class LRUCache:
    """Least Recently Used cache implementation."""
    
    def __init__(self, max_size: int = 1000, ttl_seconds: Optional[float] = None) -> None:
        """Initialize LRU cache.
        
        Args:
            max_size: Maximum number of entries
            ttl_seconds: Default TTL for entries
        """
        self.max_size = max_size
        self.ttl_seconds = ttl_seconds
        self._cache: OrderedDict[str, CacheEntry] = OrderedDict()
        self._hits = 0
        self._misses = 0
    
    def get(self, key: str) -> Optional[Any]:
        """Get value from cache.
        
        Args:
            key: Cache key
            
        Returns:
            Cached value or None
        """
        if key not in self._cache:
            self._misses += 1
            return None
        
        entry = self._cache[key]
        
        if entry.is_expired:
            del self._cache[key]
            self._misses += 1
            return None
        
        entry.touch()
        self._cache.move_to_end(key)
        self._hits += 1
        
        return entry.value
    
    def set(
        self,
        key: str,
        value: Any,
        ttl_seconds: Optional[float] = None,
        tags: Optional[list[str]] = None,
    ) -> None:
        """Set value in cache.
        
        Args:
            key: Cache key
            value: Value to cache
            ttl_seconds: TTL for this entry
            tags: Tags for this entry
        """
        if key in self._cache:
            del self._cache[key]
        elif len(self._cache) >= self.max_size:
            self._cache.popitem(last=False)
        
        entry = CacheEntry(
            key=key,
            value=value,
            ttl_seconds=ttl_seconds or self.ttl_seconds,
            tags=tags or [],
        )
        self._cache[key] = entry
    
    def delete(self, key: str) -> bool:
        """Delete entry from cache.
        
        Args:
            key: Cache key
            
        Returns:
            True if deleted
        """
        if key in self._cache:
            del self._cache[key]
            return True
        return False
    
    def clear(self) -> None:
        """Clear all cache entries."""
        self._cache.clear()
        self._hits = 0
        self._misses = 0
    
    def invalidate_by_tags(self, tags: list[str]) -> int:
        """Invalidate entries with any of the given tags.
        
        Args:
            tags: Tags to match
            
        Returns:
            Number of entries invalidated
        """
        to_remove = [
            key for key, entry in self._cache.items()
            if any(tag in entry.tags for tag in tags)
        ]
        
        for key in to_remove:
            del self._cache[key]
        
        return len(to_remove)
    
    def cleanup_expired(self) -> int:
        """Remove all expired entries.
        
        Returns:
            Number of entries removed
        """
        expired = [
            key for key, entry in self._cache.items()
            if entry.is_expired
        ]
        
        for key in expired:
            del self._cache[key]
        
        return len(expired)
    
    def get_stats(self) -> dict[str, Any]:
        """Get cache statistics."""
        total = self._hits + self._misses
        hit_rate = self._hits / total if total > 0 else 0.0
        
        return {
            "size": len(self._cache),
            "max_size": self.max_size,
            "hits": self._hits,
            "misses": self._misses,
            "hit_rate": hit_rate,
            "total_requests": total,
        }


class WriteThroughCache:
    """Write-through caching strategy."""
    
    def __init__(
        self,
        cache: LRUCache,
        store: Callable[[str, Any], None],
    ) -> None:
        """Initialize write-through cache.
        
        Args:
            cache: LRU cache instance
            store: Persistence function
        """
        self.cache = cache
        self.store = store
    
    def get(self, key: str) -> Optional[Any]:
        """Get value from cache or store."""
        value = self.cache.get(key)
        if value is not None:
            return value
        
        return None
    
    def set(self, key: str, value: Any) -> None:
        """Set value in cache and store."""
        self.cache.set(key, value)
        self.store(key, value)
    
    def delete(self, key: str) -> None:
        """Delete from cache and store."""
        self.cache.delete(key)


class WriteBehindCache:
    """Write-behind (lazy write) caching strategy."""
    
    def __init__(self, cache: LRUCache) -> None:
        """Initialize write-behind cache.
        
        Args:
            cache: LRU cache instance
        """
        self.cache = cache
        self._pending_writes: OrderedDict[str, Any] = OrderedDict()
    
    def get(self, key: str) -> Optional[Any]:
        """Get value from cache."""
        return self.cache.get(key)
    
    def set(self, key: str, value: Any) -> None:
        """Set value in cache, mark for async write."""
        self.cache.set(key, value)
        self._pending_writes[key] = value
    
    def flush_writes(self, writer: Callable[[str, Any], None]) -> int:
        """Flush pending writes to storage.
        
        Args:
            writer: Function to persist key-value
            
        Returns:
            Number of writes performed
        """
        count = 0
        while self._pending_writes:
            key, value = self._pending_writes.popitem()
            writer(key, value)
            count += 1
        
        return count


class CacheKeyBuilder:
    """Builds consistent cache keys."""
    
    def __init__(self, prefix: str = "") -> None:
        """Initialize key builder.
        
        Args:
            prefix: Key prefix
        """
        self.prefix = prefix
    
    def build(self, *parts: Any) -> str:
        """Build cache key from parts.
        
        Args:
            parts: Key components
            
        Returns:
            Cache key string
        """
        normalized = [str(p) for p in parts]
        key = ":".join(normalized)
        
        if self.prefix:
            key = f"{self.prefix}:{key}"
        
        return key
    
    def build_hash(self, *parts: Any, max_length: int = 32) -> str:
        """Build hashed cache key.
        
        Args:
            parts: Key components
            max_length: Maximum key length
            
        Returns:
            Hashed cache key
        """
        key = self.build(*parts)
        hash_value = hashlib.md5(key.encode()).hexdigest()[:max_length]
        return f"{self.prefix}:{hash_value}" if self.prefix else hash_value


class DistributedCache:
    """Distributed cache with consistent hashing."""
    
    def __init__(self, nodes: list[str]) -> None:
        """Initialize distributed cache.
        
        Args:
            nodes: List of cache node addresses
        """
        self.nodes = nodes
        self._ring: dict[int, str] = {}
        self._build_ring()
    
    def _build_ring(self) -> None:
        """Build consistent hashing ring."""
        for node in self.nodes:
            hash_value = int(hashlib.md5(node.encode()).hexdigest(), 16)
            self._ring[hash_value] = node
        
        self._sorted_keys = sorted(self._ring.keys())
    
    def _get_node(self, key: str) -> str:
        """Get node for a key using consistent hashing."""
        if not self._sorted_keys:
            return self.nodes[0] if self.nodes else ""
        
        hash_value = int(hashlib.md5(key.encode()).hexdigest(), 16)
        
        for ring_key in self._sorted_keys:
            if hash_value <= ring_key:
                return self._ring[ring_key]
        
        return self._ring[self._sorted_keys[0]]
    
    def get_node(self, key: str) -> str:
        """Get primary node for key."""
        return self._get_node(key)
    
    def get_replica_nodes(self, key: str, replica_count: int = 2) -> list[str]:
        """Get replica nodes for key."""
        primary = self._get_node(key)
        replicas = [primary]
        
        all_nodes = list(self.nodes)
        all_nodes.remove(primary)
        
        for _ in range(min(replica_count, len(all_nodes))):
            if all_nodes:
                replicas.append(all_nodes.pop(0))
        
        return replicas
    
    def add_node(self, node: str) -> None:
        """Add node to cache cluster."""
        if node not in self.nodes:
            self.nodes.append(node)
            self._build_ring()
    
    def remove_node(self, node: str) -> None:
        """Remove node from cache cluster."""
        if node in self.nodes:
            self.nodes.remove(node)
            self._build_ring()


def execute(context: dict[str, Any], params: dict[str, Any]) -> dict[str, Any]:
    """Execute data cache action.
    
    Args:
        context: Execution context
        params: Parameters including action type
        
    Returns:
        Result dictionary with status and data
    """
    action = params.get("action", "status")
    result: dict[str, Any] = {"status": "success"}
    
    if action == "set":
        cache = LRUCache(max_size=params.get("max_size", 1000))
        cache.set(
            params.get("key", ""),
            params.get("value"),
            params.get("ttl_seconds"),
        )
        result["data"] = {"set": True}
    
    elif action == "get":
        cache = LRUCache()
        value = cache.get(params.get("key", ""))
        result["data"] = {"value": value}
    
    elif action == "delete":
        cache = LRUCache()
        deleted = cache.delete(params.get("key", ""))
        result["data"] = {"deleted": deleted}
    
    elif action == "stats":
        cache = LRUCache()
        stats = cache.get_stats()
        result["data"] = stats
    
    elif action == "invalidate_tags":
        cache = LRUCache()
        count = cache.invalidate_by_tags(params.get("tags", []))
        result["data"] = {"invalidated": count}
    
    elif action == "cleanup":
        cache = LRUCache()
        count = cache.cleanup_expired()
        result["data"] = {"cleaned": count}
    
    elif action == "build_key":
        builder = CacheKeyBuilder(prefix=params.get("prefix", ""))
        key = builder.build(*params.get("parts", []))
        result["data"] = {"key": key}
    
    elif action == "build_hash_key":
        builder = CacheKeyBuilder(prefix=params.get("prefix", ""))
        key = builder.build_hash(*params.get("parts", []))
        result["data"] = {"key": key}
    
    elif action == "get_node":
        cache = DistributedCache(nodes=params.get("nodes", []))
        node = cache.get_node(params.get("key", ""))
        result["data"] = {"node": node}
    
    elif action == "clear":
        cache = LRUCache()
        cache.clear()
        result["data"] = {"cleared": True}
    
    else:
        result["status"] = "error"
        result["error"] = f"Unknown action: {action}"
    
    return result
