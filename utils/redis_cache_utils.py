"""Redis cache utilities with pub/sub, locks, and rate limiting."""

from __future__ import annotations

import json
import time
import uuid
from dataclasses import dataclass
from typing import Any, Callable

__all__ = ["RedisCache", "RateLimiter", "DistributedLock", "PubSubChannel"]


@dataclass
class CacheEntry:
    """A cache entry with optional TTL."""
    value: Any
    expires_at: float | None = None

    def is_expired(self) -> bool:
        if self.expires_at is None:
            return False
        return time.time() > self.expires_at


class RedisCache:
    """In-memory Redis-compatible cache with TTL, pub/sub, locks."""

    def __init__(self) -> None:
        self._store: dict[str, CacheEntry] = {}
        self._pubsub: dict[str, list[Callable[[Any], None]]] = {}
        self._locks: dict[str, DistributedLock] = {}
        self._hits = 0
        self._misses = 0

    def get(self, key: str, default: Any = None) -> Any:
        entry = self._store.get(key)
        if entry is None:
            self._misses += 1
            return default
        if entry.is_expired():
            del self._store[key]
            self._misses += 1
            return default
        self._hits += 1
        try:
            return json.loads(entry.value) if isinstance(entry.value, str) else entry.value
        except (json.JSONDecodeError, TypeError):
            return entry.value

    def set(self, key: str, value: Any, ttl: float | None = None) -> None:
        expires = time.time() + ttl if ttl else None
        if not isinstance(value, str):
            value = json.dumps(value)
        self._store[key] = CacheEntry(value, expires)

    def delete(self, key: str) -> bool:
        if key in self._store:
            del self._store[key]
            return True
        return False

    def exists(self, key: str) -> bool:
        entry = self._store.get(key)
        if entry and not entry.is_expired():
            return True
        return False

    def expire(self, key: str, ttl: float) -> bool:
        entry = self._store.get(key)
        if entry is None:
            return False
        entry.expires_at = time.time() + ttl
        return True

    def ttl(self, key: str) -> float:
        entry = self._store.get(key)
        if entry is None or entry.expires_at is None:
            return -1
        remaining = entry.expires_at - time.time()
        return remaining if remaining > 0 else -2

    def incr(self, key: str, amount: int = 1) -> int:
        current = self.get(key, 0)
        new_val = int(current) + amount
        self.set(key, str(new_val))
        return new_val

    def incr_by(self, key: str, amount: int, expire: float | None = None) -> int:
        new_val = self.incr(key, amount)
        if expire:
            self.expire(key, expire)
        return new_val

    def get_many(self, *keys: str) -> dict[str, Any]:
        return {k: self.get(k) for k in keys}

    def set_many(self, mapping: dict[str, Any], ttl: float | None = None) -> None:
        for k, v in mapping.items():
            self.set(k, v, ttl)

    def publish(self, channel: str, message: Any) -> int:
        message_str = json.dumps(message) if not isinstance(message, str) else message
        subscribers = self._pubsub.get(channel, [])
        for cb in subscribers:
            try:
                cb(message_str)
            except Exception:
                pass
        return len(subscribers)

    def subscribe(self, channel: str, callback: Callable[[Any], None]) -> None:
        if channel not in self._pubsub:
            self._pubsub[channel] = []
        self._pubsub[channel].append(callback)

    def unsubscribe(self, channel: str, callback: Callable[[Any], None]) -> None:
        if channel in self._pubsub:
            self._pubsub[channel] = [cb for cb in self._pubsub[channel] if cb != callback]

    def flush_pattern(self, pattern: str) -> int:
        import fnmatch
        keys = [k for k in self._store if fnmatch.fnmatch(k, pattern)]
        for k in keys:
            del self._store[k]
        return len(keys)

    def stats(self) -> dict[str, Any]:
        total = self._hits + self._misses
        return {
            "hits": self._hits,
            "misses": self._misses,
            "hit_rate": self._hits / total if total > 0 else 0.0,
            "keys": len(self._store),
        }


class DistributedLock:
    """Distributed lock implementation."""

    def __init__(self, cache: RedisCache, name: str, ttl: float = 10.0) -> None:
        self._cache = cache
        self._name = name
        self._ttl = ttl
        self._token = str(uuid.uuid4())
        self._acquired = False

    def acquire(self, timeout: float = 10.0) -> bool:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            existing = self._cache.get(self._name)
            if existing is None:
                self._cache.set(self._name, self._token, ttl=self._ttl)
                re_check = self._cache.get(self._name)
                if re_check == self._token:
                    self._acquired = True
                    return True
            time.sleep(0.01)
        return False

    def release(self) -> bool:
        if not self._acquired:
            return False
        current = self._cache.get(self._name)
        if current == self._token:
            self._cache.delete(self._name)
            self._acquired = False
            return True
        return False

    def extend(self, ttl: float | None = None) -> bool:
        if not self._acquired:
            return False
        current = self._cache.get(self._name)
        if current == self._token:
            self._cache.expire(self._name, ttl or self._ttl)
            return True
        return False

    def __enter__(self) -> "DistributedLock":
        if not self.acquire():
            raise TimeoutError(f"Failed to acquire lock: {self._name}")
        return self

    def __exit__(self, *args: Any) -> None:
        self.release()


class RateLimiter:
    """Token bucket rate limiter."""

    def __init__(self, cache: RedisCache, key: str, rate: float, capacity: int) -> None:
        self._cache = cache
        self._key = key
        self._rate = rate
        self._capacity = capacity

    def allow(self, tokens: int = 1) -> bool:
        bucket_key = f"{self._key}:tokens"
        count_key = f"{self._key}:last_refill"
        now = time.monotonic()

        tokens_val = self._cache.get(bucket_key, self._capacity)
        last_refill = self._cache.get(count_key, now)

        elapsed = now - last_refill
        refill = elapsed * self._rate
        tokens_val = min(self._capacity, float(tokens_val) + refill)

        if tokens_val >= tokens:
            self._cache.set(bucket_key, tokens_val - tokens)
            self._cache.set(count_key, now)
            return True
        return False

    def wait_and.allow(self, tokens: int = 1) -> None:
        while not self.allow(tokens):
            time.sleep(0.05)
