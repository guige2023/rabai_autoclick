"""Data Caching Action.

Multi-layer caching for data with TTL and invalidation support.
"""
from typing import Any, Callable, Dict, List, Optional, Tuple
from dataclasses import dataclass, field
import time
import hashlib
import json


@dataclass
class CacheEntry:
    key: str
    value: Any
    created_at: float
    expires_at: float
    hits: int = 0
    metadata: Dict[str, Any] = field(default_factory=dict)

    def is_expired(self) -> bool:
        return time.time() >= self.expires_at


class DataCachingAction:
    """Multi-layer cache with TTL and invalidation."""

    def __init__(
        self,
        default_ttl: float = 300.0,
        max_size: int = 1000,
        eviction_policy: str = "lru",
    ) -> None:
        self.default_ttl = default_ttl
        self.max_size = max_size
        self.eviction_policy = eviction_policy
        self.store: Dict[str, CacheEntry] = {}
        self._hits = 0
        self._misses = 0

    def _make_key(self, namespace: str, *args, **kwargs) -> str:
        raw = json.dumps({"args": args, "kwargs": kwargs}, sort_keys=True, default=str)
        return f"{namespace}:{hashlib.md5(raw.encode()).hexdigest()}"

    def get(
        self,
        key: str,
        namespace: str = "default",
    ) -> Tuple[Optional[Any], bool]:
        full_key = f"{namespace}:{key}" if ":" not in key else key
        entry = self.store.get(full_key)
        if entry is None:
            self._misses += 1
            return None, False
        if entry.is_expired():
            del self.store[full_key]
            self._misses += 1
            return None, False
        entry.hits += 1
        self._hits += 1
        return entry.value, True

    def set(
        self,
        key: str,
        value: Any,
        ttl: Optional[float] = None,
        namespace: str = "default",
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        full_key = f"{namespace}:{key}" if ":" not in key else key
        if len(self.store) >= self.max_size:
            self._evict()
        expires_at = time.time() + (ttl or self.default_ttl)
        entry = CacheEntry(
            key=full_key,
            value=value,
            created_at=time.time(),
            expires_at=expires_at,
            metadata=metadata or {},
        )
        self.store[full_key] = entry

    def delete(self, key: str, namespace: str = "default") -> bool:
        full_key = f"{namespace}:{key}" if ":" not in key else key
        if full_key in self.store:
            del self.store[full_key]
            return True
        return False

    def invalidate_namespace(self, namespace: str) -> int:
        prefix = f"{namespace}:"
        keys_to_delete = [k for k in self.store if k.startswith(prefix)]
        for k in keys_to_delete:
            del self.store[k]
        return len(keys_to_delete)

    def _evict(self) -> None:
        if not self.store:
            return
        if self.eviction_policy == "lru":
            lru_key = min(self.store.keys(), key=lambda k: self.store[k].hits)
            del self.store[lru_key]
        elif self.eviction_policy == "fifo":
            oldest_key = min(self.store.keys(), key=lambda k: self.store[k].created_at)
            del self.store[oldest_key]

    def clear(self) -> None:
        self.store.clear()
        self._hits = 0
        self._misses = 0

    def get_stats(self) -> Dict[str, Any]:
        total = self._hits + self._misses
        return {
            "size": len(self.store),
            "max_size": self.max_size,
            "hits": self._hits,
            "misses": self._misses,
            "hit_rate": self._hits / total if total > 0 else 0.0,
            "eviction_policy": self.eviction_policy,
        }

    def memoize(
        self,
        namespace: str,
        ttl: Optional[float] = None,
    ) -> Callable[[Callable], Callable]:
        def decorator(fn: Callable) -> Callable:
            def wrapper(*args, **kwargs):
                key = self._make_key(namespace, *args, **kwargs)
                cached, found = self.get(key, namespace)
                if found:
                    return cached
                result = fn(*args, **kwargs)
                self.set(key, result, ttl=ttl, namespace=namespace)
                return result
            return wrapper
        return decorator
