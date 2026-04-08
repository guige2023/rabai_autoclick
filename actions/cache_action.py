"""Cache Action Module.

Provides caching with TTL, eviction policies,
and cache invalidation support.
"""

import time
import threading
import hashlib
from typing import Any, Dict, Optional
from dataclasses import dataclass, field
from enum import Enum
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class EvictionPolicy(Enum):
    """Cache eviction policy."""
    LRU = "lru"
    LFU = "lfu"
    FIFO = "fifo"
    TTL = "ttl"


@dataclass
class CacheEntry:
    """Cache entry."""
    key: str
    value: Any
    created_at: float
    last_accessed: float
    access_count: int = 0
    ttl_seconds: Optional[float] = None


class CacheManager:
    """Manages caching operations."""

    def __init__(
        self,
        max_size: int = 1000,
        default_ttl: Optional[float] = None,
        policy: EvictionPolicy = EvictionPolicy.LRU
    ):
        self._cache: Dict[str, CacheEntry] = {}
        self._max_size = max_size
        self._default_ttl = default_ttl
        self._policy = policy
        self._lock = threading.Lock()

    def get(self, key: str) -> Optional[Any]:
        """Get value from cache."""
        entry = self._cache.get(key)
        if not entry:
            return None

        if self._is_expired(entry):
            self._evict(key)
            return None

        entry.last_accessed = time.time()
        entry.access_count += 1
        return entry.value

    def set(
        self,
        key: str,
        value: Any,
        ttl_seconds: Optional[float] = None
    ) -> None:
        """Set value in cache."""
        ttl = ttl_seconds or self._default_ttl

        with self._lock:
            if len(self._cache) >= self._max_size and key not in self._cache:
                self._evict_lru()

            self._cache[key] = CacheEntry(
                key=key,
                value=value,
                created_at=time.time(),
                last_accessed=time.time(),
                ttl_seconds=ttl
            )

    def delete(self, key: str) -> bool:
        """Delete from cache."""
        if key in self._cache:
            del self._cache[key]
            return True
        return False

    def clear(self) -> None:
        """Clear all cache."""
        with self._lock:
            self._cache.clear()

    def _is_expired(self, entry: CacheEntry) -> bool:
        """Check if entry is expired."""
        if entry.ttl_seconds is None:
            return False
        return time.time() - entry.created_at > entry.ttl_seconds

    def _evict(self, key: str) -> None:
        """Evict entry."""
        if key in self._cache:
            del self._cache[key]

    def _evict_lru(self) -> None:
        """Evict least recently used."""
        if not self._cache:
            return

        lru_key = min(
            self._cache.keys(),
            key=lambda k: self._cache[k].last_accessed
        )
        self._evict(lru_key)

    def stats(self) -> Dict:
        """Get cache statistics."""
        total = len(self._cache)
        expired = sum(1 for e in self._cache.values() if self._is_expired(e))

        return {
            "size": total,
            "max_size": self._max_size,
            "expired": expired,
            "policy": self._policy.value
        }


class CacheAction(BaseAction):
    """Action for cache operations."""

    def __init__(self):
        super().__init__("cache")
        self._cache = CacheManager()

    def execute(self, params: Dict) -> ActionResult:
        """Execute cache action."""
        try:
            operation = params.get("operation", "get")

            if operation == "get":
                return self._get(params)
            elif operation == "set":
                return self._set(params)
            elif operation == "delete":
                return self._delete(params)
            elif operation == "clear":
                return self._clear(params)
            elif operation == "stats":
                return self._stats(params)
            else:
                return ActionResult(success=False, message=f"Unknown: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=str(e))

    def _get(self, params: Dict) -> ActionResult:
        """Get from cache."""
        value = self._cache.get(params.get("key", ""))
        return ActionResult(success=value is not None, data={"value": value})

    def _set(self, params: Dict) -> ActionResult:
        """Set in cache."""
        self._cache.set(
            params.get("key", ""),
            params.get("value"),
            params.get("ttl")
        )
        return ActionResult(success=True)

    def _delete(self, params: Dict) -> ActionResult:
        """Delete from cache."""
        success = self._cache.delete(params.get("key", ""))
        return ActionResult(success=success)

    def _clear(self, params: Dict) -> ActionResult:
        """Clear cache."""
        self._cache.clear()
        return ActionResult(success=True)

    def _stats(self, params: Dict) -> ActionResult:
        """Get cache stats."""
        return ActionResult(success=True, data=self._cache.stats())
