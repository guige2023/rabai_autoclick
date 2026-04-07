"""idempotent_action module for rabai_autoclick.

Provides idempotency utilities: deduplication, idempotent operations,
request tracing, and operation keys for safe retries.
"""

from __future__ import annotations

import hashlib
import threading
import time
from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional, Tuple

__all__ = [
    "IdempotencyKey",
    "IdempotencyStore",
    "InMemoryIdempotencyStore",
    "deduplicate",
    "idempotent",
    "IdempotentResult",
    "RequestTrace",
]


@dataclass
class IdempotentResult(Generic[T]):
    """Result of an idempotent operation."""
    value: T
    is_first: bool
    key: str
    timestamp: float


class IdempotencyKey:
    """Idempotency key generator and validator."""

    @staticmethod
    def generate(*parts: Any) -> str:
        """Generate idempotency key from parts.

        Args:
            *parts: Parts to include in key.

        Returns:
            Deterministic hash-based key.
        """
        content = "|".join(str(p) for p in parts)
        return hashlib.sha256(content.encode()).hexdigest()[:32]

    @staticmethod
    def from_request(
        method: str,
        path: str,
        body: Optional[str] = None,
        headers: Optional[Dict[str, str]] = None,
    ) -> str:
        """Generate key from HTTP request components.

        Args:
            method: HTTP method.
            path: Request path.
            body: Request body.
            headers: Request headers.

        Returns:
            Idempotency key.
        """
        parts = [method.upper(), path]
        if body:
            parts.append(body)
        if headers:
            for k in sorted(headers.keys()):
                if k.lower().startswith("x-idempotency"):
                    parts.append(f"{k}={headers[k]}")
        return IdempotencyKey.generate(*parts)


class IdempotencyStore:
    """Storage interface for idempotency keys."""

    def get(self, key: str) -> Optional[Any]:
        """Get stored result for key."""
        raise NotImplementedError

    def set(self, key: str, value: Any, ttl: Optional[float] = None) -> None:
        """Store result for key."""
        raise NotImplementedError

    def delete(self, key: str) -> bool:
        """Delete key from store."""
        raise NotImplementedError

    def exists(self, key: str) -> bool:
        """Check if key exists."""
        raise NotImplementedError


class InMemoryIdempotencyStore(IdempotencyStore):
    """In-memory idempotency store with TTL support."""

    def __init__(self, ttl_seconds: float = 3600.0) -> None:
        self.ttl_seconds = ttl_seconds
        self._store: Dict[str, Tuple[Any, float]] = {}
        self._lock = threading.RLock()
        self._cleanup_thread: Optional[threading.Thread] = None
        self._running = False

    def get(self, key: str) -> Optional[Any]:
        """Get stored result."""
        with self._lock:
            if key in self._store:
                value, expires = self._store[key]
                if time.time() < expires:
                    return value
                del self._store[key]
        return None

    def set(self, key: str, value: Any, ttl: Optional[float] = None) -> None:
        """Store result with TTL."""
        with self._lock:
            ttl_val = ttl if ttl is not None else self.ttl_seconds
            expires = time.time() + ttl_val
            self._store[key] = (value, expires)

    def delete(self, key: str) -> bool:
        """Delete key."""
        with self._lock:
            if key in self._store:
                del self._store[key]
                return True
        return False

    def exists(self, key: str) -> bool:
        """Check if key exists and is not expired."""
        return self.get(key) is not None

    def clear_expired(self) -> int:
        """Remove expired entries.

        Returns:
            Number of entries removed.
        """
        with self._lock:
            now = time.time()
            expired = [k for k, (_, exp) in self._store.items() if now >= exp]
            for k in expired:
                del self._store[k]
            return len(expired)

    def start_cleanup(self, interval: float = 60.0) -> None:
        """Start background cleanup thread."""
        if self._running:
            return
        self._running = True
        self._cleanup_thread = threading.Thread(
            target=self._cleanup_loop,
            args=(interval,),
            daemon=True,
        )
        self._cleanup_thread.start()

    def stop_cleanup(self) -> None:
        """Stop background cleanup thread."""
        self._running = False
        if self._cleanup_thread:
            self._cleanup_thread.join(timeout=2.0)

    def _cleanup_loop(self, interval: float) -> None:
        """Background cleanup loop."""
        while self._running:
            time.sleep(interval)
            self.clear_expired()


def deduplicate(
    store: IdempotencyStore,
    key: str,
    func: Callable,
    *args: Any,
    **kwargs: Any,
) -> IdempotentResult:
    """Execute function with deduplication.

    Args:
        store: Idempotency store.
        key: Idempotency key.
        func: Function to execute.
        *args: Positional args.
        **kwargs: Keyword args.

    Returns:
        IdempotentResult with value and metadata.
    """
    existing = store.get(key)
    if existing is not None:
        return IdempotentResult(
            value=existing,
            is_first=False,
            key=key,
            timestamp=time.time(),
        )

    result = func(*args, **kwargs)
    store.set(key, result)
    return IdempotentResult(
        value=result,
        is_first=True,
        key=key,
        timestamp=time.time(),
    )


def idempotent(
    store: Optional[IdempotencyStore] = None,
    key_func: Optional[Callable] = None,
    ttl: Optional[float] = None,
) -> Callable:
    """Decorator to make function idempotent.

    Args:
        store: Idempotency store (creates in-memory if None).
        key_func: Function to generate key from args.
        ttl: TTL for results.

    Returns:
        Decorated function.
    """
    if store is None:
        store = InMemoryIdempotencyStore()

    def decorator(func: Callable) -> Callable:
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            if key_func:
                key = key_func(*args, **kwargs)
            else:
                key = IdempotencyKey.generate(func.__name__, str(args), str(kwargs))

            result = deduplicate(store, key, func, *args, **kwargs)
            return result.value

        wrapper._idempotent_store = store
        wrapper._idempotent_key_func = key_func
        return wrapper
    return decorator


class RequestTrace:
    """Request tracing for distributed systems."""

    def __init__(self, trace_id: Optional[str] = None) -> None:
        import uuid
        self.trace_id = trace_id or uuid.uuid4().hex[:16]
        self.spans: List[Dict[str, Any]] = []
        self._current_span: Optional[Dict[str, Any]] = None
        self._lock = threading.Lock()

    def start_span(self, name: str, tags: Optional[Dict[str, str]] = None) -> None:
        """Start a new span.

        Args:
            name: Span name.
            tags: Optional tags.
        """
        with self._lock:
            self._current_span = {
                "name": name,
                "trace_id": self.trace_id,
                "start_time": time.time(),
                "tags": tags or {},
            }

    def end_span(self, result: Any = None, error: Optional[Exception] = None) -> None:
        """End current span.

        Args:
            result: Operation result.
            error: Any exception that occurred.
        """
        with self._lock:
            if self._current_span:
                self._current_span["end_time"] = time.time()
                self._current_span["duration_ms"] = (
                    self._current_span["end_time"] - self._current_span["start_time"]
                ) * 1000
                if result is not None:
                    self._current_span["result"] = result
                if error:
                    self._current_span["error"] = str(error)
                    self._current_span["tags"]["error"] = "true"
                self.spans.append(self._current_span)
                self._current_span = None

    def get_spans(self) -> List[Dict[str, Any]]:
        """Get all completed spans."""
        with self._lock:
            return list(self.spans)

    def total_duration_ms(self) -> float:
        """Get total duration across all spans."""
        return sum(s.get("duration_ms", 0) for s in self.spans)


from typing import Generic, TypeVar
T = TypeVar("T")
