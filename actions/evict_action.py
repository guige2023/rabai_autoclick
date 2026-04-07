"""evict_action module for rabai_autoclick.

Provides cache eviction policies: LRU, LFU, ARC, FIFO, Random,
and TTL-based eviction for bounded caches.
"""

from __future__ import annotations

import threading
import time
from collections import OrderedDict, deque
from dataclasses import dataclass
from typing import Any, Callable, Dict, Generic, Iterator, List, Optional, TypeVar

__all__ = [
    "LRUCache",
    "LFUCache",
    "LFUCache",
    "FIFOCache",
    "RandomCache",
    "TTLCache",
    "ARCCache",
    "TwoQueueCache",
    "SLRUCCache",
    "CacheStats",
    "EvictionPolicy",
    "BoundedCache",
]


T = TypeVar("T")
K = TypeVar("K")
V = TypeVar("V")


class EvictionPolicy:
    """Eviction policy types."""
    LRU = "lru"
    LFU = "lfu"
    FIFO = "fifo"
    RANDOM = "random"
    TTL = "ttl"
    ARC = "arc"


@dataclass
class CacheStats:
    """Cache statistics."""
    hits: int = 0
    misses: int = 0
    evictions: int = 0
    inserts: int = 0
    deletions: int = 0

    @property
    def total_requests(self) -> int:
        return self.hits + self.misses

    @property
    def hit_rate(self) -> float:
        if self.total_requests == 0:
            return 0.0
        return self.hits / self.total_requests


class BoundedCache(Generic[K, V]):
    """Base class for bounded caches."""

    def __init__(self, max_size: int) -> None:
        self.max_size = max_size
        self.stats = CacheStats()

    def get(self, key: K) -> Optional[V]:
        raise NotImplementedError

    def put(self, key: K, value: V) -> None:
        raise NotImplementedError

    def delete(self, key: K) -> bool:
        raise NotImplementedError

    def clear(self) -> None:
        raise NotImplementedError

    def __contains__(self, key: K) -> bool:
        return self.get(key) is not None

    def __len__(self) -> int:
        raise NotImplementedError


class LRUCache(BoundedCache[K, V]):
    """Least Recently Used cache."""

    def __init__(self, max_size: int) -> None:
        super().__init__(max_size)
        self._cache: OrderedDict[K, V] = OrderedDict()
        self._lock = threading.RLock()

    def get(self, key: K) -> Optional[V]:
        with self._lock:
            if key in self._cache:
                self._cache.move_to_end(key)
                self.stats.hits += 1
                return self._cache[key]
            self.stats.misses += 1
            return None

    def put(self, key: K, value: V) -> None:
        with self._lock:
            if key in self._cache:
                self._cache.move_to_end(key)
            else:
                self.stats.inserts += 1
                if len(self._cache) >= self.max_size:
                    evicted = next(iter(self._cache))
                    del self._cache[evicted]
                    self.stats.evictions += 1
            self._cache[key] = value

    def delete(self, key: K) -> bool:
        with self._lock:
            if key in self._cache:
                del self._cache[key]
                self.stats.deletions += 1
                return True
            return False

    def clear(self) -> None:
        with self._lock:
            self._cache.clear()

    def __len__(self) -> int:
        return len(self._cache)

    def keys(self) -> Iterator[K]:
        return iter(self._cache.keys())


class LFUCache(BoundedCache[K, V]):
    """Least Frequently Used cache."""

    def __init__(self, max_size: int) -> None:
        super().__init__(max_size)
        self._cache: Dict[K, V] = {}
        self._freq: Dict[K, int] = {}
        self._lock = threading.RLock()
        self._min_freq = 0

    def get(self, key: K) -> Optional[V]:
        with self._lock:
            if key in self._cache:
                self.stats.hits += 1
                self._freq[key] += 1
                self._update_min_freq()
                return self._cache[key]
            self.stats.misses += 1
            return None

    def _update_min_freq(self) -> None:
        if self._freq:
            self._min_freq = min(self._freq.values())

    def _evict(self) -> None:
        if not self._freq:
            return
        candidates = [k for k, f in self._freq.items() if f == self._min_freq]
        if candidates:
            key = candidates[0]
        else:
            key = next(iter(self._freq))
        del self._cache[key]
        del self._freq[key]
        self.stats.evictions += 1

    def put(self, key: K, value: V) -> None:
        with self._lock:
            if key in self._cache:
                self._cache[key] = value
                self._freq[key] += 1
            else:
                self.stats.inserts += 1
                if len(self._cache) >= self.max_size:
                    self._evict()
                self._cache[key] = value
                self._freq[key] = 1
                self._min_freq = 1

    def delete(self, key: K) -> bool:
        with self._lock:
            if key in self._cache:
                del self._cache[key]
                del self._freq[key]
                self.stats.deletions += 1
                return True
            return False

    def clear(self) -> None:
        with self._lock:
            self._cache.clear()
            self._freq.clear()
            self._min_freq = 0

    def __len__(self) -> int:
        return len(self._cache)


class FIFOCache(BoundedCache[K, V]):
    """First In First Out cache."""

    def __init__(self, max_size: int) -> None:
        super().__init__(max_size)
        self._cache: Dict[K, V] = {}
        self._queue: deque = deque()
        self._lock = threading.RLock()

    def get(self, key: K) -> Optional[V]:
        with self._lock:
            if key in self._cache:
                self.stats.hits += 1
                return self._cache[key]
            self.stats.misses += 1
            return None

    def put(self, key: K, value: V) -> None:
        with self._lock:
            if key not in self._cache:
                self.stats.inserts += 1
                if len(self._cache) >= self.max_size:
                    oldest = self._queue.popleft()
                    if oldest in self._cache:
                        del self._cache[oldest]
                        self.stats.evictions += 1
                self._queue.append(key)
            self._cache[key] = value

    def delete(self, key: K) -> bool:
        with self._lock:
            if key in self._cache:
                del self._cache[key]
                try:
                    self._queue.remove(key)
                except ValueError:
                    pass
                self.stats.deletions += 1
                return True
            return False

    def clear(self) -> None:
        with self._lock:
            self._cache.clear()
            self._queue.clear()

    def __len__(self) -> int:
        return len(self._cache)


class RandomCache(BoundedCache[K, V]):
    """Random eviction cache."""

    def __init__(self, max_size: int) -> None:
        import random
        super().__init__(max_size)
        self._cache: Dict[K, V] = {}
        self._lock = threading.Lock()

    def get(self, key: K) -> Optional[V]:
        with self._lock:
            if key in self._cache:
                self.stats.hits += 1
                return self._cache[key]
            self.stats.misses += 1
            return None

    def put(self, key: K, value: V) -> None:
        import random
        with self._lock:
            if key not in self._cache:
                self.stats.inserts += 1
                if len(self._cache) >= self.max_size:
                    random_key = random.choice(list(self._cache.keys()))
                    del self._cache[random_key]
                    self.stats.evictions += 1
            self._cache[key] = value

    def delete(self, key: K) -> bool:
        with self._lock:
            if key in self._cache:
                del self._cache[key]
                self.stats.deletions += 1
                return True
            return False

    def clear(self) -> None:
        with self._lock:
            self._cache.clear()

    def __len__(self) -> int:
        return len(self._cache)


class TTLCache(BoundedCache[K, V]):
    """Time-To-Live cache with automatic expiration."""

    def __init__(self, max_size: int, ttl_seconds: float) -> None:
        super().__init__(max_size)
        self.ttl_seconds = ttl_seconds
        self._cache: Dict[K, V] = {}
        self._timestamps: Dict[K, float] = {}
        self._lock = threading.RLock()

    def _is_expired(self, key: K) -> bool:
        if key not in self._timestamps:
            return True
        return (time.time() - self._timestamps[key]) > self.ttl_seconds

    def _cleanup(self) -> None:
        expired = [k for k in list(self._cache.keys()) if self._is_expired(k)]
        for k in expired:
            del self._cache[k]
            del self._timestamps[k]
            self.stats.evictions += 1

    def get(self, key: K) -> Optional[V]:
        with self._lock:
            if key in self._cache:
                if self._is_expired(key):
                    self.delete(key)
                    self.stats.misses += 1
                    return None
                self.stats.hits += 1
                return self._cache[key]
            self.stats.misses += 1
            return None

    def put(self, key: K, value: V) -> None:
        with self._lock:
            self._cleanup()
            if key not in self._cache and len(self._cache) >= self.max_size:
                oldest_key = min(self._timestamps, key=self._timestamps.get)
                self.delete(oldest_key)
                self.stats.evictions += 1
            self._cache[key] = value
            self._timestamps[key] = time.time()

    def delete(self, key: K) -> bool:
        with self._lock:
            if key in self._cache:
                del self._cache[key]
                if key in self._timestamps:
                    del self._timestamps[key]
                self.stats.deletions += 1
                return True
            return False

    def clear(self) -> None:
        with self._lock:
            self._cache.clear()
            self._timestamps.clear()

    def __len__(self) -> int:
        return len(self._cache)


class ARCCache(BoundedCache[K, V]):
    """Adaptive Replacement Cache combining LRU and LFU."""

    def __init__(self, max_size: int) -> None:
        super().__init__(max_size)
        self._t1: OrderedDict[K, V] = OrderedDict()
        self._t2: OrderedDict[K, V] = OrderedDict()
        self._b1: OrderedDict[K, V] = OrderedDict()
        self._b2: OrderedDict[K, V] = OrderedDict()
        self._p = 0
        self._target_size = max_size // 2
        self._lock = threading.RLock()

    def _replace(self, key: K, value: V) -> None:
        if len(self._t1) > 0 and (len(self._t1) > self._target_size or (key in self._b1 and len(self._t1) == self._target_size)):
            old = next(iter(self._t1))
            del self._t1[old]
            self._b1[old] = True
            self.stats.evictions += 1
        else:
            if len(self._t2) > 0 and (len(self._t2) > self._target_size or (key in self._b2 and len(self._t2) == self._target_size)):
                old = next(iter(self._t2))
                del self._t2[old]
                self._b2[old] = True
                self.stats.evictions += 1

        for old_key in list(self._b1.keys())[:len(self._b1) - self._target_size]:
            del self._b1[old_key]
        for old_key in list(self._b2.keys())[:len(self._b2) - self._target_size]:
            del self._b2[old_key]

    def get(self, key: K) -> Optional[V]:
        with self._lock:
            if key in self._t1:
                self._t1.move_to_end(key)
                self.stats.hits += 1
                return self._t1[key]
            if key in self._t2:
                self._t2.move_to_end(key)
                self.stats.hits += 1
                return self._t2[key]
            self.stats.misses += 1
            return None

    def put(self, key: K, value: V) -> None:
        with self._lock:
            if key in self._t1:
                self._t1.move_to_end(key)
                self._t1[key] = value
            elif key in self._t2:
                self._t2.move_to_end(key)
                self._t2[key] = value
            else:
                self.stats.inserts += 1
                if len(self._t1) + len(self._t2) >= self.max_size:
                    self._replace(key, value)
                if key in self._b1:
                    self._p = min(self.max_size, self._p + max(1, len(self._b2) // len(self._b1)))
                    del self._b1[key]
                    self._t2[key] = value
                elif key in self._b2:
                    self._p = max(0, self._p - max(1, len(self._b1) // len(self._b2)))
                    del self._b2[key]
                    self._t2[key] = value
                else:
                    if len(self._t1) + len(self._t2) >= self.max_size:
                        if len(self._t1) < self._target_size:
                            self.stats.evictions += 1
                            oldest = next(iter(self._b2))
                            del self._b2[oldest]
                        else:
                            self.stats.evictions += 1
                            oldest = next(iter(self._t1))
                            del self._t1[oldest]
                    self._t1[key] = value

    def delete(self, key: K) -> bool:
        with self._lock:
            for d in [self._t1, self._t2, self._b1, self._b2]:
                if key in d:
                    del d[key]
                    self.stats.deletions += 1
                    return True
            return False

    def clear(self) -> None:
        with self._lock:
            self._t1.clear()
            self._t2.clear()
            self._b1.clear()
            self._b2.clear()

    def __len__(self) -> int:
        return len(self._t1) + len(self._t2)


class TwoQueueCache(BoundedCache[K, V]):
    """Two-Queue (2Q) cache with protected and probationary sections."""

    def __init__(self, max_size: int) -> None:
        super().__init__(max_size)
        self._a1: OrderedDict[K, V] = OrderedDict()
        self._am: OrderedDict[K, V] = OrderedDict()
        self._a2: OrderedDict[K, V] = OrderedDict()
        self._a1_size = max_size // 2
        self._lock = threading.RLock()

    def get(self, key: K) -> Optional[V]:
        with self._lock:
            if key in self._am:
                self._am.move_to_end(key)
                self.stats.hits += 1
                return self._am[key]
            if key in self._a2:
                self._a2.move_to_end(key)
                self.stats.hits += 1
                return self._a2[key]
            if key in self._a1:
                self.stats.hits += 1
                val = self._a1.pop(key)
                self._am[key] = val
                return val
            self.stats.misses += 1
            return None

    def put(self, key: K, value: V) -> None:
        with self._lock:
            if key in self._am:
                self._am.move_to_end(key)
                self._am[key] = value
                return
            if key in self._a2:
                self._a2.move_to_end(key)
                self._a2[key] = value
                return
            if key in self._a1:
                self._a1.move_to_end(key)
                self._a1[key] = value
                return

            self.stats.inserts += 1
            if len(self._a1) >= self._a1_size:
                evicted_key, evicted_val = self._a1.popitem(last=False)
                self._a2[evicted_key] = evicted_val
                self.stats.evictions += 1

            self._a1[key] = value

    def delete(self, key: K) -> bool:
        with self._lock:
            for d in [self._a1, self._am, self._a2]:
                if key in d:
                    del d[key]
                    self.stats.deletions += 1
                    return True
            return False

    def clear(self) -> None:
        with self._lock:
            self._a1.clear()
            self._am.clear()
            self._a2.clear()

    def __len__(self) -> int:
        return len(self._a1) + len(self._am) + len(self._a2)


class SLRUCCache(BoundedCache[K, V]):
    """Segmented LRU cache with probationary and protected segments."""

    def __init__(self, max_size: int, protected_fraction: float = 0.5) -> None:
        super().__init__(max_size)
        self._probationary: OrderedDict[K, V] = OrderedDict()
        self._protected: OrderedDict[K, V] = OrderedDict()
        self._protected_size = int(max_size * protected_fraction)
        self._lock = threading.RLock()

    def get(self, key: K) -> Optional[V]:
        with self._lock:
            if key in self._protected:
                self._protected.move_to_end(key)
                self.stats.hits += 1
                return self._protected[key]
            if key in self._probationary:
                val = self._probationary.pop(key)
                self._protected[key] = val
                self.stats.hits += 1
                return val
            self.stats.misses += 1
            return None

    def put(self, key: K, value: V) -> None:
        with self._lock:
            if key in self._protected:
                self._protected.move_to_end(key)
                self._protected[key] = value
                return
            if key in self._probationary:
                self._probationary.move_to_end(key)
                self._probationary[key] = value
                return

            self.stats.inserts += 1
            if len(self._protected) >= self._protected_size:
                oldest = next(iter(self._protected))
                del self._protected[oldest]
                self._probationary[oldest] = value
                self.stats.evictions += 1
            else:
                if len(self._probationary) + len(self._protected) >= self.max_size:
                    oldest = next(iter(self._probationary))
                    del self._probationary[oldest]
                    self.stats.evictions += 1
                self._probationary[key] = value

    def delete(self, key: K) -> bool:
        with self._lock:
            for d in [self._probationary, self._protected]:
                if key in d:
                    del d[key]
                    self.stats.deletions += 1
                    return True
            return False

    def clear(self) -> None:
        with self._lock:
            self._probationary.clear()
            self._protected.clear()

    def __len__(self) -> int:
        return len(self._probationary) + len(self._protected)
