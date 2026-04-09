"""
Data KV Store Action Module.

Provides a key-value store abstraction for data processing,
supporting TTL, versioning, and atomic operations.
"""

from typing import Any, Callable, Dict, List, Optional, Tuple
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from enum import Enum, auto
import hashlib
import logging
import threading

logger = logging.getLogger(__name__)


class EvictionPolicy(Enum):
    """Key eviction policies."""
    LRU = auto()
    LFU = auto()
    FIFO = auto()
    TTL = auto()


@dataclass
class KVEntry:
    """A key-value store entry."""
    key: str
    value: Any
    created_at: datetime
    updated_at: datetime
    version: int = 1
    hits: int = 0
    ttl_seconds: Optional[float] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    def is_expired(self) -> bool:
        """Check if entry has expired."""
        if self.ttl_seconds is None:
            return False
        age = (datetime.now(timezone.utc) - self.created_at).total_seconds()
        return age > self.ttl_seconds

    def age_seconds(self) -> float:
        """Get age of entry in seconds."""
        return (datetime.now(timezone.utc) - self.created_at).total_seconds()


@dataclass
class KVResult:
    """Result of a KV operation."""
    success: bool
    value: Any = None
    error: Optional[str] = None
    version: int = 0


class DataKVStoreAction:
    """
    Provides a key-value store for data processing.

    This action implements an in-memory key-value store with TTL support,
    versioning, atomic operations, and configurable eviction policies.

    Example:
        >>> store = DataKVStoreAction()
        >>> store.set("user:123", {"name": "Alice"}, ttl=3600)
        >>> result = store.get("user:123")
        >>> print(result.value["name"])
        Alice
    """

    def __init__(
        self,
        eviction_policy: EvictionPolicy = EvictionPolicy.LRU,
        max_size: int = 10000,
        default_ttl: Optional[float] = None,
    ):
        """
        Initialize the KV Store.

        Args:
            eviction_policy: Policy for evicting old entries.
            max_size: Maximum number of entries.
            default_ttl: Default TTL in seconds.
        """
        self.eviction_policy = eviction_policy
        self.max_size = max_size
        self.default_ttl = default_ttl
        self._store: Dict[str, KVEntry] = {}
        self._lock = threading.RLock()
        self._access_order: List[str] = []

    def set(
        self,
        key: str,
        value: Any,
        ttl: Optional[float] = None,
        metadata: Optional[Dict[str, Any]] = None,
        if_not_exists: bool = False,
        if_version: Optional[int] = None,
    ) -> KVResult:
        """
        Set a value in the store.

        Args:
            key: Key to set.
            value: Value to store.
            ttl: Optional TTL in seconds.
            metadata: Optional metadata.
            if_not_exists: Only set if key doesn't exist.
            if_version: Only set if version matches.

        Returns:
            KVResult indicating success.
        """
        with self._lock:
            existing = self._store.get(key)

            if if_not_exists and existing:
                return KVResult(success=False, error="Key already exists")

            if if_version is not None and existing:
                if existing.version != if_version:
                    return KVResult(
                        success=False,
                        error=f"Version mismatch: expected {if_version}, got {existing.version}",
                    )

            now = datetime.now(timezone.utc)
            new_entry = KVEntry(
                key=key,
                value=value,
                created_at=existing.created_at if existing else now,
                updated_at=now,
                version=(existing.version + 1) if existing else 1,
                ttl_seconds=ttl or self.default_ttl,
                metadata=metadata or (existing.metadata if existing else {}),
            )

            self._store[key] = new_entry
            self._update_access_order(key)

            if len(self._store) > self.max_size:
                self._evict()

            return KVResult(success=True, version=new_entry.version)

    def get(
        self,
        key: str,
        default: Any = None,
        update_hits: bool = True,
    ) -> KVResult:
        """
        Get a value from the store.

        Args:
            key: Key to retrieve.
            default: Default value if key not found.
            update_hits: Whether to update hit count.

        Returns:
            KVResult with value.
        """
        with self._lock:
            entry = self._store.get(key)

            if entry is None:
                return KVResult(success=False, value=default, error="Key not found")

            if entry.is_expired():
                del self._store[key]
                return KVResult(success=False, value=default, error="Key expired")

            if update_hits:
                entry.hits += 1
                self._update_access_order(key)

            return KVResult(success=True, value=entry.value, version=entry.version)

    def delete(self, key: str, if_version: Optional[int] = None) -> KVResult:
        """
        Delete a key from the store.

        Args:
            key: Key to delete.
            if_version: Only delete if version matches.

        Returns:
            KVResult indicating success.
        """
        with self._lock:
            entry = self._store.get(key)

            if entry is None:
                return KVResult(success=False, error="Key not found")

            if if_version is not None and entry.version != if_version:
                return KVResult(success=False, error="Version mismatch")

            del self._store[key]
            self._access_order = [k for k in self._access_order if k != key]

            return KVResult(success=True, version=entry.version)

    def get_many(
        self,
        keys: List[str],
        default: Any = None,
    ) -> Dict[str, Any]:
        """
        Get multiple values at once.

        Args:
            keys: List of keys to retrieve.
            default: Default value for missing keys.

        Returns:
            Dictionary of found key-value pairs.
        """
        with self._lock:
            results = {}
            for key in keys:
                entry = self._store.get(key)
                if entry and not entry.is_expired():
                    results[key] = entry.value
                else:
                    results[key] = default
            return results

    def set_many(
        self,
        items: Dict[str, Any],
        ttl: Optional[float] = None,
    ) -> int:
        """
        Set multiple values at once.

        Args:
            items: Dictionary of key-value pairs.
            ttl: Optional TTL for all items.

        Returns:
            Number of items successfully set.
        """
        count = 0
        for key, value in items.items():
            result = self.set(key, value, ttl=ttl)
            if result.success:
                count += 1
        return count

    def exists(self, key: str) -> bool:
        """Check if a key exists and is not expired."""
        with self._lock:
            entry = self._store.get(key)
            return entry is not None and not entry.is_expired()

    def expire(self, key: str, ttl_seconds: float) -> KVResult:
        """Set/reset TTL for a key."""
        with self._lock:
            entry = self._store.get(key)
            if entry is None:
                return KVResult(success=False, error="Key not found")

            entry.ttl_seconds = ttl_seconds
            return KVResult(success=True)

    def ttl(self, key: str) -> Optional[float]:
        """Get remaining TTL for a key."""
        with self._lock:
            entry = self._store.get(key)
            if entry is None or entry.ttl_seconds is None:
                return None

            remaining = entry.ttl_seconds - entry.age_seconds()
            return max(0, remaining)

    def increment(self, key: str, amount: float = 1) -> KVResult:
        """Atomically increment a numeric value."""
        with self._lock:
            entry = self._store.get(key)

            if entry is None:
                new_entry = KVEntry(
                    key=key,
                    value=amount,
                    created_at=datetime.now(timezone.utc),
                    updated_at=datetime.now(timezone.utc),
                )
                self._store[key] = new_entry
                return KVResult(success=True, value=amount)

            if not isinstance(entry.value, (int, float)):
                return KVResult(success=False, error="Value is not numeric")

            entry.value += amount
            entry.updated_at = datetime.now(timezone.utc)
            entry.version += 1
            return KVResult(success=True, value=entry.value, version=entry.version)

    def compare_and_set(
        self,
        key: str,
        expected: Any,
        new_value: Any,
    ) -> KVResult:
        """
        Atomic compare-and-set operation.

        Args:
            key: Key to update.
            expected: Expected current value.
            new_value: New value to set.

        Returns:
            KVResult indicating success.
        """
        with self._lock:
            entry = self._store.get(key)

            if entry is None:
                return KVResult(success=False, error="Key not found")

            if entry.value != expected:
                return KVResult(
                    success=False,
                    error="Value mismatch",
                    value=entry.value,
                )

            entry.value = new_value
            entry.updated_at = datetime.now(timezone.utc)
            entry.version += 1

            return KVResult(success=True, version=entry.version)

    def get_metadata(self, key: str) -> Optional[Dict[str, Any]]:
        """Get metadata for a key."""
        with self._lock:
            entry = self._store.get(key)
            if entry is None:
                return None

            return {
                "created_at": entry.created_at.isoformat(),
                "updated_at": entry.updated_at.isoformat(),
                "version": entry.version,
                "hits": entry.hits,
                "ttl_seconds": entry.ttl_seconds,
                "age_seconds": entry.age_seconds(),
                "metadata": entry.metadata,
            }

    def clear(self) -> int:
        """Clear all entries."""
        with self._lock:
            count = len(self._store)
            self._store.clear()
            self._access_order.clear()
            return count

    def _update_access_order(self, key: str) -> None:
        """Update access order for LRU/LFU tracking."""
        if key in self._access_order:
            self._access_order.remove(key)
        self._access_order.append(key)

    def _evict(self) -> None:
        """Evict entries based on policy."""
        if not self._store:
            return

        if self.eviction_policy == EvictionPolicy.FIFO:
            if self._access_order:
                oldest = self._access_order[0]
                del self._store[oldest]
                self._access_order.pop(0)

        elif self.eviction_policy == EvictionPolicy.LRU:
            if self._access_order:
                oldest = self._access_order[0]
                del self._store[oldest]
                self._access_order.pop(0)

        elif self.eviction_policy == EvictionPolicy.LFU:
            min_hits = min(e.hits for e in self._store.values())
            for key, entry in list(self._store.items()):
                if entry.hits == min_hits:
                    del self._store[key]
                    break

        elif self.eviction_policy == EvictionPolicy.TTL:
            expired = [k for k, e in self._store.items() if e.is_expired()]
            if expired:
                for key in expired[:1]:
                    del self._store[key]
            else:
                if self._access_order:
                    oldest = self._access_order[0]
                    del self._store[oldest]
                    self._access_order.pop(0)

    def keys(self, pattern: Optional[str] = None) -> List[str]:
        """Get all keys, optionally filtered by pattern."""
        with self._lock:
            keys = list(self._store.keys())
            if pattern:
                import fnmatch
                keys = [k for k in keys if fnmatch.fnmatch(k, pattern)]
            return keys

    def stats(self) -> Dict[str, Any]:
        """Get store statistics."""
        with self._lock:
            total_hits = sum(e.hits for e in self._store.values())
            return {
                "size": len(self._store),
                "max_size": self.max_size,
                "eviction_policy": self.eviction_policy.name,
                "total_hits": total_hits,
                "avg_hits": total_hits / len(self._store) if self._store else 0,
                "default_ttl": self.default_ttl,
            }


def create_kv_store(
    eviction_policy: EvictionPolicy = EvictionPolicy.LRU,
    **kwargs,
) -> DataKVStoreAction:
    """Factory function to create a DataKVStoreAction."""
    return DataKVStoreAction(eviction_policy=eviction_policy, **kwargs)
