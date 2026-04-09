"""
Request Deduplication Utilities.

Prevents duplicate request processing using various strategies including
content hashing, request signatures, and distributed locking.

Example:
    >>> dedup = RequestDeduplicator(ttl_seconds=300)
    >>> if dedup.is_new("request-123"):
    ...     process_request()
"""

from __future__ import annotations

import hashlib
import threading
import time
from collections import OrderedDict
from dataclasses import dataclass
from typing import Any, Callable, Optional


@dataclass
class DeduplicationConfig:
    """Configuration for request deduplication."""
    ttl_seconds: float = 300.0
    max_entries: int = 10000
    use_content_hash: bool = True


@dataclass
class DeduplicationStats:
    """Statistics for deduplication monitoring."""
    total_requests: int = 0
    unique_requests: int = 0
    duplicate_requests: int = 0
    cache_hits: int = 0


class RequestDeduplicator:
    """
    In-memory request deduplication with TTL support.

    Uses an LRU cache with time-based expiration to track
    seen requests and filter out duplicates.
    """

    def __init__(
        self,
        ttl_seconds: float = 300.0,
        max_entries: int = 10000,
        use_content_hash: bool = True
    ):
        """
        Initialize the deduplicator.

        Args:
            ttl_seconds: Time-to-live for each entry
            max_entries: Maximum number of entries to store
            use_content_hash: Use content hashing instead of exact match
        """
        self._ttl = ttl_seconds
        self._max_entries = max_entries
        self._use_content_hash = use_content_hash
        self._cache: OrderedDict[str, float] = OrderedDict()
        self._lock = threading.Lock()
        self._stats = DeduplicationStats()

    def _generate_key(self, request: Any) -> str:
        """Generate cache key from request."""
        if isinstance(request, str):
            content = request
        elif isinstance(request, bytes):
            content = request.decode('utf-8', errors='replace')
        else:
            content = str(request)

        if self._use_content_hash:
            return hashlib.sha256(content.encode()).hexdigest()[:32]
        return content

    def _cleanup_expired(self) -> None:
        """Remove expired entries from cache."""
        now = time.time()
        expired_keys = [
            key for key, timestamp in self._cache.items()
            if now - timestamp > self._ttl
        ]
        for key in expired_keys:
            self._cache.pop(key, None)

        while len(self._cache) > self._max_entries:
            self._cache.popitem(last=False)

    def is_new(self, request: Any) -> bool:
        """
        Check if request is new (not a duplicate).

        Args:
            request: Request identifier or content

        Returns:
            True if request is new, False if duplicate
        """
        with self._lock:
            self._stats.total_requests += 1
            self._cleanup_expired()

            key = self._generate_key(request)

            if key in self._cache:
                self._stats.duplicate_requests += 1
                self._stats.cache_hits += 1
                return False

            self._cache[key] = time.time()
            self._stats.unique_requests += 1
            return True

    def mark_processed(self, request: Any) -> None:
        """
        Explicitly mark a request as processed.

        Args:
            request: Request to mark
        """
        with self._lock:
            key = self._generate_key(request)
            self._cache[key] = time.time()
            self._cache.move_to_end(key)

    def is_duplicate(self, request: Any) -> bool:
        """
        Check if request is a duplicate.

        Args:
            request: Request to check

        Returns:
            True if duplicate, False otherwise
        """
        return not self.is_new(request)

    def get_stats(self) -> DeduplicationStats:
        """Get deduplication statistics."""
        with self._lock:
            return DeduplicationStats(
                total_requests=self._stats.total_requests,
                unique_requests=self._stats.unique_requests,
                duplicate_requests=self._stats.duplicate_requests,
                cache_hits=self._stats.cache_hits
            )

    def clear(self) -> None:
        """Clear all cached entries."""
        with self._lock:
            self._cache.clear()


class DistributedDeduplicator:
    """
    Distributed request deduplication using a shared backend.

    Supports Redis-based deduplication for multi-process/multi-machine
    deployments. Falls back to local deduplication if Redis unavailable.
    """

    def __init__(
        self,
        redis_client: Optional[Any] = None,
        ttl_seconds: float = 300.0,
        namespace: str = "dedup:"
    ):
        """
        Initialize distributed deduplicator.

        Args:
            redis_client: Redis client instance (optional)
            ttl_seconds: Time-to-live for keys
            namespace: Key namespace prefix
        """
        self._redis = redis_client
        self._ttl = ttl_seconds
        self._namespace = namespace
        self._local_dedup = RequestDeduplicator(ttl_seconds=ttl_seconds)

    def _generate_key(self, request: Any) -> str:
        """Generate Redis key from request."""
        if isinstance(request, str):
            content = request
        else:
            content = str(request)
        hash_val = hashlib.sha256(content.encode()).hexdigest()[:32]
        return f"{self._namespace}{hash_val}"

    def is_new(self, request: Any) -> bool:
        """
        Check if request is new using distributed lock.

        Args:
            request: Request to check

        Returns:
            True if new, False if duplicate
        """
        if self._redis is None:
            return self._local_dedup.is_new(request)

        key = self._generate_key(request)
        try:
            result = self._redis.set(key, "1", nx=True, ex=int(self._ttl))
            return result is not None
        except Exception:
            return self._local_dedup.is_new(request)

    def force_set(self, request: Any) -> None:
        """
        Force set a request as processed.

        Args:
            request: Request to mark
        """
        if self._redis is None:
            self._local_dedup.mark_processed(request)
            return

        key = self._generate_key(request)
        try:
            self._redis.set(key, "1", ex=int(self._ttl))
        except Exception:
            self._local_dedup.mark_processed(request)


class RequestSignatureBuilder:
    """
    Build request signatures for deduplication.

    Creates consistent signatures from request components including
    method, path, headers, and body.
    """

    @staticmethod
    def build(
        method: str,
        path: str,
        headers: Optional[dict[str, str]] = None,
        body: Optional[str] = None,
        exclude_headers: Optional[set[str]] = None
    ) -> str:
        """
        Build a signature for a request.

        Args:
            method: HTTP method
            path: Request path
            headers: Request headers
            body: Request body
            exclude_headers: Headers to exclude from signature

        Returns:
            Request signature string
        """
        parts = [method.upper(), path]

        if headers:
            exclude = exclude_headers or set()
            sorted_headers = sorted(
                (k.lower(), v) for k, v in headers.items()
                if k.lower() not in exclude
            )
            for k, v in sorted_headers:
                parts.append(f"{k}={v}")

        if body:
            parts.append(hashlib.sha256(body.encode()).hexdigest()[:16])

        return "|".join(parts)
