"""API Cache Action Module.

Provides intelligent caching for API responses with TTL,
storage backends, invalidation, and cache-aside patterns.
"""

from __future__ import annotations

import hashlib
import json
import logging
import sqlite3
import threading
import time
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


logger = logging.getLogger(__name__)


class CacheBackend(Enum):
    """Supported cache backends."""
    MEMORY = "memory"
    SQLITE = "sqlite"
    DISK = "disk"


class CacheStrategy(Enum):
    """Cache strategies."""
    CACHE_ASIDE = "cache_aside"    # Check cache, fallback to source
    WRITE_THROUGH = "write_through"  # Write to both
    WRITE_BACK = "write_back"      # Write to cache, sync later
    STALE_WHILE_REVALIDATE = "stale_while-revalidate"  # Return stale, revalidate async


@dataclass
class CacheEntry:
    """A cache entry with metadata."""
    key: str
    value: Any
    created_at: float
    expires_at: float
    hit_count: int = 0
    last_accessed: float = 0.0
    tags: List[str] = field(default_factory=list)

    def is_expired(self) -> bool:
        """Check if entry has expired."""
        return time.time() > self.expires_at

    def access(self) -> None:
        """Record an access."""
        self.hit_count += 1
        self.last_accessed = time.time()

    def to_dict(self) -> Dict[str, Any]:
        """Serialize to dictionary."""
        return {
            "key": self.key,
            "value": self.value,
            "created_at": self.created_at,
            "expires_at": self.expires_at,
            "hit_count": self.hit_count,
            "last_accessed": self.last_accessed,
            "tags": self.tags,
        }


@dataclass
class CacheStats:
    """Cache statistics."""
    hits: int = 0
    misses: int = 0
    evictions: int = 0
    writes: int = 0
    invalidations: int = 0
    total_size_bytes: int = 0


class MemoryCache:
    """In-memory LRU cache."""

    def __init__(self, max_size: int = 1000):
        self.max_size = max_size
        self._cache: Dict[str, CacheEntry] = {}
        self._lock = threading.RLock()
        self._stats = CacheStats()

    def get(self, key: str) -> Optional[Any]:
        """Get value from cache."""
        with self._lock:
            entry = self._cache.get(key)
            if entry is None:
                self._stats.misses += 1
                return None
            if entry.is_expired():
                del self._cache[key]
                self._stats.misses += 1
                self._stats.evictions += 1
                return None
            entry.access()
            self._stats.hits += 1
            return entry.value

    def set(self, key: str, value: Any, ttl: float = 300.0,
            tags: Optional[List[str]] = None) -> None:
        """Set value in cache."""
        with self._lock:
            if len(self._cache) >= self.max_size and key not in self._cache:
                self._evict_lru()
            entry = CacheEntry(
                key=key,
                value=value,
                created_at=time.time(),
                expires_at=time.time() + ttl,
                tags=tags or [],
            )
            self._cache[key] = entry
            self._stats.writes += 1

    def delete(self, key: str) -> bool:
        """Delete entry from cache."""
        with self._lock:
            if key in self._cache:
                del self._cache[key]
                self._stats.invalidations += 1
                return True
            return False

    def invalidate_by_tags(self, tags: List[str]) -> int:
        """Invalidate all entries with given tags."""
        with self._lock:
            count = 0
            keys_to_delete = [
                k for k, v in self._cache.items()
                if any(t in v.tags for t in tags)
            ]
            for key in keys_to_delete:
                del self._cache[key]
                count += 1
            self._stats.invalidations += count
            return count

    def clear(self) -> None:
        """Clear all entries."""
        with self._lock:
            self._cache.clear()

    def _evict_lru(self) -> None:
        """Evict least recently used entry."""
        if not self._cache:
            return
        lru_key = min(
            self._cache.items(),
            key=lambda x: x[1].last_accessed
        )[0]
        del self._cache[lru_key]
        self._stats.evictions += 1

    def get_stats(self) -> CacheStats:
        """Get cache statistics."""
        with self._lock:
            return CacheStats(
                hits=self._stats.hits,
                misses=self._stats.misses,
                evictions=self._stats.evictions,
                writes=self._stats.writes,
                invalidations=self._stats.invalidations,
            )


class SQLiteCache:
    """SQLite-based persistent cache."""

    def __init__(self, db_path: str = "/tmp/api_cache.db"):
        self.db_path = db_path
        self._lock = threading.RLock()
        self._init_db()

    def _init_db(self) -> None:
        """Initialize database schema."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS api_cache (
                    key TEXT PRIMARY KEY,
                    value BLOB NOT NULL,
                    created_at REAL NOT NULL,
                    expires_at REAL NOT NULL,
                    hit_count INTEGER DEFAULT 0,
                    last_accessed REAL,
                    tags TEXT DEFAULT ''
                )
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_expires_at ON api_cache(expires_at)
            """)

    def get(self, key: str) -> Optional[Any]:
        """Get value from cache."""
        with self._lock:
            try:
                with sqlite3.connect(self.db_path) as conn:
                    conn.row_factory = sqlite3.Row
                    cursor = conn.execute(
                        "SELECT * FROM api_cache WHERE key = ?",
                        (key,)
                    )
                    row = cursor.fetchone()
                    if row is None:
                        return None
                    if time.time() > row["expires_at"]:
                        conn.execute("DELETE FROM api_cache WHERE key = ?", (key,))
                        return None
                    # Update hit count
                    conn.execute(
                        "UPDATE api_cache SET hit_count = hit_count + 1, "
                        "last_accessed = ? WHERE key = ?",
                        (time.time(), key)
                    )
                    return json.loads(row["value"])
            except Exception as e:
                logger.error(f"Cache get error: {e}")
                return None

    def set(self, key: str, value: Any, ttl: float = 300.0,
            tags: Optional[List[str]] = None) -> None:
        """Set value in cache."""
        with self._lock:
            try:
                with sqlite3.connect(self.db_path) as conn:
                    conn.execute(
                        "INSERT OR REPLACE INTO api_cache "
                        "(key, value, created_at, expires_at, hit_count, "
                        "last_accessed, tags) VALUES (?, ?, ?, ?, 0, ?, ?)",
                        (
                            key,
                            json.dumps(value),
                            time.time(),
                            time.time() + ttl,
                            time.time(),
                            ",".join(tags or []),
                        )
                    )
            except Exception as e:
                logger.error(f"Cache set error: {e}")

    def delete(self, key: str) -> bool:
        """Delete entry from cache."""
        with self._lock:
            try:
                with sqlite3.connect(self.db_path) as conn:
                    cursor = conn.execute(
                        "DELETE FROM api_cache WHERE key = ?", (key,)
                    )
                    return cursor.rowcount > 0
            except Exception:
                return False

    def invalidate_by_tags(self, tags: List[str]) -> int:
        """Invalidate entries by tags."""
        with self._lock:
            try:
                with sqlite3.connect(self.db_path) as conn:
                    placeholders = ",".join("?" * len(tags))
                    cursor = conn.execute(
                        f"DELETE FROM api_cache WHERE tags IN ({placeholders})",
                        tags
                    )
                    return cursor.rowcount
            except Exception:
                return 0

    def clear(self) -> None:
        """Clear all entries."""
        with self._lock:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute("DELETE FROM api_cache")


class APICacheAction(BaseAction):
    """API Cache Action for response caching.

    Supports memory and SQLite backends with TTL, tagging,
    and various cache strategies.

    Examples:
        >>> action = APICacheAction()
        >>> result = action.execute(ctx, {
        ...     "url": "https://api.example.com/data",
        ...     "ttl": 300,
        ...     "backend": "memory",
        ... })
    """

    action_type = "api_cache"
    display_name = "API缓存"
    description = "API响应缓存，支持内存/SQLite后端，TTL和标签失效"

    _caches: Dict[str, Tuple[Any, CacheBackend]] = {}
    _cache_lock = threading.Lock()

    def __init__(self):
        super().__init__()

    @classmethod
    def get_cache(cls, name: str = "default",
                  backend: CacheBackend = CacheBackend.MEMORY,
                  **kwargs) -> Any:
        """Get or create a named cache."""
        with cls._cache_lock:
            if name not in cls._caches:
                if backend == CacheBackend.MEMORY:
                    cache = MemoryCache(max_size=kwargs.get("max_size", 1000))
                elif backend == CacheBackend.SQLITE:
                    cache = SQLiteCache(db_path=kwargs.get("db_path", "/tmp/api_cache.db"))
                else:
                    cache = MemoryCache()
                cls._caches[name] = (cache, backend)
            return cls._caches[name][0]

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute cached API call.

        Args:
            context: Execution context.
            params: Dict with keys:
                - url: Target API URL (used for cache key)
                - cache_name: Name of cache instance (default: 'default')
                - backend: 'memory' or 'sqlite' (default: 'memory')
                - ttl: Time to live in seconds (default: 300)
                - tags: List of tags for invalidation (optional)
                - force_refresh: Bypass cache (default: False)
                - max_size: Max entries for memory backend

        Returns:
            ActionResult with cached data or API response.
        """
        import urllib.request
        import urllib.error

        url = params.get("url")
        if not url:
            return ActionResult(success=False, message="Missing 'url' parameter")

        cache_name = params.get("cache_name", "default")
        backend = CacheBackend(params.get("backend", "memory"))
        ttl = params.get("ttl", 300)
        tags = params.get("tags", [])
        force_refresh = params.get("force_refresh", False)
        max_size = params.get("max_size", 1000)

        cache = self.get_cache(cache_name, backend, max_size=max_size)

        # Generate cache key
        cache_key = self._generate_key(url, params)

        # Try cache first (unless force_refresh)
        if not force_refresh:
            cached = cache.get(cache_key)
            if cached is not None:
                return ActionResult(
                    success=True,
                    message="Cache hit",
                    data={"cached": True, "data": cached}
                )

        # Cache miss or force refresh - call API
        try:
            headers = params.get("headers", {})
            method = params.get("method", "GET").upper()
            req = urllib.request.Request(url, headers=headers, method=method)

            timeout = params.get("timeout", 30.0)
            with urllib.request.urlopen(req, timeout=timeout) as response:
                content = response.read()
                try:
                    data = json.loads(content.decode())
                except (json.JSONDecodeError, UnicodeDecodeError):
                    data = content.decode(errors="replace")

                # Store in cache
                cache.set(cache_key, data, ttl=ttl, tags=tags)

                return ActionResult(
                    success=True,
                    message="API call succeeded, cached",
                    data={"cached": False, "data": data}
                )

        except urllib.error.HTTPError as e:
            return ActionResult(
                success=False,
                message=f"HTTP error {e.code}: {e.reason}",
                data={"status_code": e.code}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"API call failed: {str(e)}"
            )

    def _generate_key(self, url: str, params: Dict[str, Any]) -> str:
        """Generate cache key from URL and params."""
        key_parts = [url]
        for k in sorted(params.keys()):
            if k not in ("cache_name", "backend", "ttl", "tags",
                        "force_refresh", "max_size"):
                key_parts.append(f"{k}={params[k]}")
        key_str = "|".join(key_parts)
        return hashlib.sha256(key_str.encode()).hexdigest()

    def invalidate(self, cache_name: str = "default",
                   key: Optional[str] = None,
                   tags: Optional[List[str]] = None) -> int:
        """Invalidate cache entries."""
        with self._cache_lock:
            if cache_name not in self._caches:
                return 0
            cache = self._caches[cache_name][0]
            if key:
                return 1 if cache.delete(key) else 0
            elif tags:
                return cache.invalidate_by_tags(tags)
            return 0

    def get_stats(self, cache_name: str = "default") -> Optional[Dict[str, Any]]:
        """Get cache statistics."""
        with self._cache_lock:
            if cache_name not in self._caches:
                return None
            cache = self._caches[cache_name][0]
            stats = cache.get_stats()
            return {
                "hits": stats.hits,
                "misses": stats.misses,
                "hit_rate": (
                    stats.hits / (stats.hits + stats.misses)
                    if (stats.hits + stats.misses) > 0 else 0
                ),
                "evictions": stats.evictions,
                "writes": stats.writes,
                "invalidation": stats.invalidations,
            }

    def get_required_params(self) -> List[str]:
        return ["url"]

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            "cache_name": "default",
            "backend": "memory",
            "ttl": 300,
            "tags": [],
            "force_refresh": False,
            "max_size": 1000,
            "method": "GET",
            "headers": {},
            "timeout": 30.0,
        }
