# Copyright (c) 2024. coded by claude
"""API Cache Manager Action Module.

Manages API response caching with support for multiple cache stores,
TTL policies, cache invalidation, and stale-while-revalidate patterns.
"""
from typing import Optional, Dict, Any, List, Callable
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
import asyncio
import hashlib
import logging

logger = logging.getLogger(__name__)


class CacheStrategy(Enum):
    LRU = "lru"
    LFU = "lfu"
    FIFO = "fifo"
    TTL = "ttl"


@dataclass
class CacheEntry:
    key: str
    value: Any
    created_at: datetime
    expires_at: Optional[datetime] = None
    hit_count: int = 0
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class CacheConfig:
    max_size: int = 1000
    default_ttl: Optional[int] = None
    strategy: CacheStrategy = CacheStrategy.LRU
    stale_while_revalidate: bool = False


@dataclass
class CacheResult:
    hit: bool
    value: Optional[Any]
    from_cache: bool
    age_seconds: Optional[float] = None


class CacheManager:
    def __init__(self, config: Optional[CacheConfig] = None):
        self.config = config or CacheConfig()
        self._cache: Dict[str, CacheEntry] = {}
        self._access_order: List[str] = []
        self._lock = asyncio.Lock()

    async def get(self, key: str) -> CacheResult:
        async with self._lock:
            entry = self._cache.get(key)
            if not entry:
                return CacheResult(hit=False, value=None, from_cache=False)
            if entry.expires_at and datetime.now() > entry.expires_at:
                del self._cache[key]
                return CacheResult(hit=False, value=None, from_cache=True)
            entry.hit_count += 1
            age = (datetime.now() - entry.created_at).total_seconds()
            return CacheResult(hit=True, value=entry.value, from_cache=True, age_seconds=age)

    async def set(self, key: str, value: Any, ttl: Optional[int] = None) -> None:
        async with self._lock:
            expires_at = None
            if ttl:
                expires_at = datetime.now() + timedelta(seconds=ttl)
            elif self.config.default_ttl:
                expires_at = datetime.now() + timedelta(seconds=self.config.default_ttl)
            entry = CacheEntry(
                key=key,
                value=value,
                created_at=datetime.now(),
                expires_at=expires_at,
            )
            self._cache[key] = entry
            self._update_access_order(key)
            await self._evict_if_needed()

    async def delete(self, key: str) -> bool:
        async with self._lock:
            if key in self._cache:
                del self._cache[key]
                if key in self._access_order:
                    self._access_order.remove(key)
                return True
            return False

    async def clear(self) -> None:
        async with self._lock:
            self._cache.clear()
            self._access_order.clear()

    async def invalidate_pattern(self, pattern: str) -> int:
        async with self._lock:
            keys_to_delete = [k for k in self._cache.keys() if pattern in k]
            for key in keys_to_delete:
                del self._cache[key]
            return len(keys_to_delete)

    def _update_access_order(self, key: str) -> None:
        if key in self._access_order:
            self._access_order.remove(key)
        self._access_order.append(key)

    async def _evict_if_needed(self) -> None:
        if len(self._cache) <= self.config.max_size:
            return
        if self.config.strategy == CacheStrategy.LRU:
            while len(self._cache) > self.config.max_size:
                oldest = self._access_order.pop(0)
                del self._cache[oldest]
        elif self.config.strategy == CacheStrategy.FIFO:
            while len(self._cache) > self.config.max_size:
                oldest_key = min(self._cache.keys(), key=lambda k: self._cache[k].created_at)
                del self._cache[oldest_key]
        elif self.config.strategy == CacheStrategy.LFU:
            while len(self._cache) > self.config.max_size:
                least_hit = min(self._cache.keys(), key=lambda k: self._cache[k].hit_count)
                del self._cache[least_hit]

    def generate_key(self, *args: Any) -> str:
        content = "|".join(str(arg) for arg in args)
        return hashlib.md5(content.encode()).hexdigest()

    def get_stats(self) -> Dict[str, Any]:
        total_hits = sum(e.hit_count for e in self._cache.values())
        return {
            "size": len(self._cache),
            "max_size": self.config.max_size,
            "total_hits": total_hits,
            "strategy": self.config.strategy.value,
        }
