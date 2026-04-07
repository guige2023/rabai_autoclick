"""
Proxy Pattern Implementation

Provides various proxy patterns: virtual, protection, remote, and logging proxies
with transparent access control and lazy loading capabilities.
"""

from __future__ import annotations

import copy
import time
from abc import ABC, abstractmethod
from collections.abc import Callable
from dataclasses import dataclass, field
from functools import wraps
from typing import Any, Generic, TypeVar

T = TypeVar("T")
R = TypeVar("R")


class Subject(ABC):
    """Abstract interface for real and proxy objects."""

    @abstractmethod
    def request(self, *args: Any, **kwargs: Any) -> Any:
        """Execute the request."""
        pass


class RealSubject(Subject):
    """The actual object that the proxy represents."""

    def __init__(self, name: str = "", expensive_init: bool = True):
        self.name = name
        if expensive_init:
            # Simulate expensive initialization
            time.sleep(0.01)
        self._initialized = True

    def request(self, *args: Any, **kwargs: Any) -> Any:
        return f"RealSubject '{self.name}' handling request with args={args}, kwargs={kwargs}"


@dataclass
class ProxyMetrics:
    """Metrics collected by a proxy."""
    access_count: int = 0
    cache_hits: int = 0
    cache_misses: int = 0
    total_latency_ms: float = 0.0
    last_access: float = field(default_factory=time.time)

    @property
    def avg_latency_ms(self) -> float:
        return self.total_latency_ms / self.access_count if self.access_count > 0 else 0.0

    @property
    def cache_hit_rate(self) -> float:
        total = self.cache_hits + self.cache_misses
        return self.cache_hits / total if total > 0 else 0.0


class VirtualProxy(Subject, Generic[T]):
    """
    Virtual Proxy - Lazy initialization.
    Creates the real object only when it's actually needed.
    """

    def __init__(self, factory: Callable[[], T], name: str = ""):
        self._factory = factory
        self._real_subject: T | None = None
        self._name = name
        self._metrics = ProxyMetrics()

    @property
    def real_subject(self) -> T:
        """Lazy initialization of the real subject."""
        if self._real_subject is None:
            self._metrics.cache_misses += 1
            self._real_subject = self._factory()
        else:
            self._metrics.cache_hits += 1
        return self._real_subject

    def request(self, *args: Any, **kwargs: Any) -> Any:
        """Delegate to the real subject, initializing it if needed."""
        start = time.time()
        self._metrics.access_count += 1
        self._metrics.last_access = time.time()

        result = self.real_subject.request(*args, **kwargs)  # type: ignore

        self._metrics.total_latency_ms += (time.time() - start) * 1000
        return result

    @property
    def metrics(self) -> ProxyMetrics:
        return self._metrics

    @property
    def is_initialized(self) -> bool:
        return self._real_subject is not None

    def force_initialize(self) -> T:
        """Force immediate initialization."""
        return self.real_subject


class ProtectionProxy(Subject):
    """
    Protection Proxy - Access control.
    Controls access to the real object based on permissions.
    """

    def __init__(
        self,
        real_subject: Subject,
        access_check: Callable[[str, dict[str, Any]], bool] | None = None,
    ):
        self._real_subject = real_subject
        self._access_check = access_check or self._default_access_check
        self._access_log: list[dict[str, Any]] = []

    def _default_access_check(self, method: str, kwargs: dict[str, Any]) -> bool:
        """Default: allow all access."""
        return True

    def request(self, *args: Any, **kwargs: Any) -> Any:
        """Check access before delegating."""
        method_name = kwargs.pop("_method_name", "request")
        user_id = kwargs.get("_user_id", "anonymous")

        log_entry = {
            "method": method_name,
            "user_id": user_id,
            "timestamp": time.time(),
            "allowed": False,
        }

        if not self._access_check(method_name, kwargs):
            log_entry["denied_reason"] = "access_check_failed"
            self._access_log.append(log_entry)
            raise PermissionError(f"Access denied for user '{user_id}' to '{method_name}'")

        log_entry["allowed"] = True
        self._access_log.append(log_entry)
        return self._real_subject.request(*args, **kwargs)

    def get_access_log(self) -> list[dict[str, Any]]:
        """Get the access log."""
        return list(self._access_log)

    def grant_access(self, user_id: str) -> None:
        """Grant access to a user (placeholder for permission system)."""
        pass


class LoggingProxy(Subject):
    """
    Logging Proxy - Logs all operations.
    Wraps a subject and logs all method calls with timing.
    """

    def __init__(
        self,
        real_subject: Subject,
        logger: Callable[[str, Any], None] | None = None,
        log_args: bool = True,
        log_result: bool = True,
    ):
        self._real_subject = real_subject
        self._logger = logger or self._default_logger
        self._log_args = log_args
        self._log_result = log_result
        self._call_log: list[dict[str, Any]] = []

    @staticmethod
    def _default_logger(level: str, msg: str) -> None:
        print(f"[{level.upper()}] {msg}")

    def request(self, *args: Any, **kwargs: Any) -> Any:
        """Log the request, execute, and log the result."""
        call_id = f"{time.time():.6f}"
        method_name = "request"

        self._logger("debug", f"[{call_id}] {method_name} called")
        if self._log_args:
            self._logger("debug", f"[{call_id}] args={args}, kwargs={kwargs}")

        start = time.time()
        try:
            result = self._real_subject.request(*args, **kwargs)
            elapsed = time.time() - start

            self._logger("info", f"[{call_id}] {method_name} completed in {elapsed*1000:.2f}ms")
            if self._log_result:
                self._logger("debug", f"[{call_id}] result={result}")

            self._call_log.append({
                "call_id": call_id,
                "method": method_name,
                "args": args,
                "kwargs": kwargs,
                "result": result,
                "elapsed_ms": elapsed * 1000,
                "success": True,
            })

            return result

        except Exception as e:
            elapsed = time.time() - start
            self._logger("error", f"[{call_id}] {method_name} failed after {elapsed*1000:.2f}ms: {e}")

            self._call_log.append({
                "call_id": call_id,
                "method": method_name,
                "args": args,
                "kwargs": kwargs,
                "error": str(e),
                "elapsed_ms": elapsed * 1000,
                "success": False,
            })
            raise

    def get_call_log(self) -> list[dict[str, Any]]:
        """Get the call log."""
        return list(self._call_log)

    def clear_log(self) -> None:
        """Clear the call log."""
        self._call_log.clear()


class CachingProxy(Subject, Generic[T]):
    """
    Caching Proxy - Cache results for repeated operations.
    """

    def __init__(
        self,
        real_subject: T,
        cache_ttl_seconds: float = 60.0,
        cache_by_args: bool = True,
    ):
        self._real_subject = real_subject
        self._cache: dict[str, tuple[Any, float]] = {}
        self._cache_ttl = cache_ttl_seconds
        self._cache_by_args = cache_by_args
        self._metrics = ProxyMetrics()

    def _make_cache_key(self, *args: Any, **kwargs: Any) -> str:
        """Create a cache key from arguments."""
        if not self._cache_by_args:
            return "__default__"
        import hashlib, json
        key_data = {"args": args, "kwargs": kwargs}
        key_str = json.dumps(key_data, sort_keys=True, default=str)
        return hashlib.md5(key_str.encode()).hexdigest()

    def _is_cache_valid(self, key: str) -> bool:
        """Check if a cached value is still valid."""
        if key not in self._cache:
            return False
        _, timestamp = self._cache[key]
        return (time.time() - timestamp) < self._cache_ttl

    def request(self, *args: Any, **kwargs: Any) -> Any:
        """Check cache, execute if needed, and cache the result."""
        start = time.time()
        self._metrics.access_count += 1
        self._metrics.last_access = time.time()

        key = self._make_cache_key(*args, **kwargs)

        if self._is_cache_valid(key):
            self._metrics.cache_hits += 1
            result, _ = self._cache[key]
            self._metrics.total_latency_ms += (time.time() - start) * 1000
            return result

        self._metrics.cache_misses += 1
        result = self._real_subject.request(*args, **kwargs)  # type: ignore
        self._cache[key] = (result, time.time())
        self._metrics.total_latency_ms += (time.time() - start) * 1000

        return result

    @property
    def metrics(self) -> ProxyMetrics:
        return self._metrics

    def invalidate_cache(self, *args: Any, **kwargs: Any) -> None:
        """Invalidate specific cache entry."""
        key = self._make_cache_key(*args, **kwargs)
        self._cache.pop(key, None)

    def clear_cache(self) -> None:
        """Clear all cached entries."""
        self._cache.clear()


def create_proxy(
    subject: Subject,
    proxy_type: str = "logging",
    **options: Any,
) -> Subject:
    """
    Factory function to create proxies by type.

    Args:
        subject: The real subject to wrap.
        proxy_type: Type of proxy ("virtual", "protection", "logging", "caching").
        **options: Additional options for the proxy.

    Returns:
        A proxy-wrapped subject.
    """
    proxies: dict[str, type[Subject]] = {
        "virtual": VirtualProxy,
        "protection": ProtectionProxy,
        "logging": LoggingProxy,
        "caching": CachingProxy,
    }

    if proxy_type not in proxies:
        raise ValueError(f"Unknown proxy type: {proxy_type}. Available: {list(proxies.keys())}")

    proxy_class = proxies[proxy_type]
    return proxy_class(subject, **options)  # type: ignore
