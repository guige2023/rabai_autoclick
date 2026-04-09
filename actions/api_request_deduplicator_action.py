"""API Request Deduplicator Action Module.

Provides request deduplication for API clients to prevent duplicate
requests and reduce server load with configurable cache strategies.
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set

logger = logging.getLogger(__name__)


class DeduplicationStrategy(Enum):
    EXACT = "exact"
    NORMALIZED = "normalized"
    SEMANTIC = "semantic"


@dataclass
class CacheEntry:
    key: str
    value: Any
    created_at: float
    expires_at: Optional[float] = None
    hit_count: int = 0
    request_count: int = 1

    def is_expired(self) -> bool:
        if self.expires_at is None:
            return False
        return time.time() > self.expires_at

    def age_seconds(self) -> float:
        return time.time() - self.created_at


@dataclass
class DeduplicationConfig:
    strategy: DeduplicationStrategy = DeduplicationStrategy.EXACT
    ttl_seconds: float = 60.0
    max_cache_size: int = 10000
    normalize_whitespace: bool = True
    normalize_case: bool = True
    track_stats: bool = True


class RequestDeduplicator:
    def __init__(self, config: Optional[DeduplicationConfig] = None):
        self.config = config or DeduplicationConfig()
        self._cache: Dict[str, CacheEntry] = {}
        self._in_flight: Dict[str, asyncio.Task] = {}
        self._stats = {
            "total_requests": 0,
            "deduplicated": 0,
            "cache_hits": 0,
            "cache_misses": 0,
        }

    def _normalize(self, request_data: Any) -> str:
        import json
        text = json.dumps(request_data, sort_keys=True)

        if self.config.normalize_whitespace:
            import re
            text = re.sub(r'\s+', ' ', text)

        if self.config.normalize_case:
            text = text.lower()

        return text

    def _hash_key(self, request_data: Any) -> str:
        normalized = self._normalize(request_data)
        return hashlib.sha256(normalized.encode()).hexdigest()[:32]

    def _should_deduplicate(self, request_data: Any) -> bool:
        key = self._hash_key(request_data)
        if key not in self._cache:
            return False
        entry = self._cache[key]
        if entry.is_expired():
            del self._cache[key]
            return False
        return True

    async def execute(
        self,
        request_data: Any,
        fetch_fn: Callable[[], Any],
    ) -> Any:
        self._stats["total_requests"] += 1
        key = self._hash_key(request_data)

        if key in self._cache and not self._cache[key].is_expired():
            entry = self._cache[key]
            entry.hit_count += 1
            entry.request_count += 1
            self._stats["deduplicated"] += 1
            self._stats["cache_hits"] += 1
            logger.debug(f"Cache hit for key {key}, hit count: {entry.hit_count}")
            return entry.value

        if key in self._in_flight:
            self._stats["deduplicated"] += 1
            logger.debug(f"Waiting for in-flight request: {key}")
            return await self._in_flight[key]

        async def fetch_and_cache():
            try:
                result = await asyncio.to_thread(fetch_fn)
                entry = CacheEntry(
                    key=key,
                    value=result,
                    created_at=time.time(),
                    expires_at=time.time() + self.config.ttl_seconds if self.config.ttl_seconds > 0 else None,
                )
                self._cache[key] = entry
                return result
            finally:
                self._in_flight.pop(key, None)

        task = asyncio.create_task(fetch_and_cache())
        self._in_flight[key] = task
        self._stats["cache_misses"] += 1

        return await task

    def invalidate(self, request_data: Any) -> bool:
        key = self._hash_key(request_data)
        if key in self._cache:
            del self._cache[key]
            return True
        return False

    def invalidate_pattern(self, pattern: str) -> int:
        count = 0
        keys_to_delete = [k for k in self._cache if pattern in k]
        for key in keys_to_delete:
            del self._cache[key]
            count += 1
        return count

    def clear_expired(self) -> int:
        expired_keys = [k for k, v in self._cache.items() if v.is_expired()]
        for key in expired_keys:
            del self._cache[key]
        return len(expired_keys)

    def clear_all(self) -> int:
        count = len(self._cache)
        self._cache.clear()
        return count

    def get_stats(self) -> Dict[str, Any]:
        stats = dict(self._stats)
        stats["cache_size"] = len(self._cache)
        stats["in_flight"] = len(self._in_flight)
        stats["hit_rate"] = (
            stats["cache_hits"] / stats["total_requests"]
            if stats["total_requests"] > 0 else 0.0
        )
        return stats

    def get_cache_info(self) -> List[Dict[str, Any]]:
        return [
            {
                "key": k,
                "age_seconds": v.age_seconds(),
                "hit_count": v.hit_count,
                "request_count": v.request_count,
                "expires_in_seconds": v.expires_at - time.time() if v.expires_at else None,
            }
            for k, v in self._cache.items()
        ][:100]


def deduplicate_requests(
    requests: List[Any],
    fetch_fn: Callable[[Any], Any],
    config: Optional[DeduplicationConfig] = None,
) -> List[Any]:
    deduplicator = RequestDeduplicator(config)

    async def fetch_all():
        tasks = [deduplicator.execute(req, lambda r=req: fetch_fn(r)) for req in requests]
        return await asyncio.gather(*tasks)

    return asyncio.run(fetch_all())
