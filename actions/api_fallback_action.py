"""API fallback and degradation handling.

This module provides fallback strategies:
- Multiple fallback levels
- Degradation levels
- Cached fallback responses
- Circuit breaker integration

Example:
    >>> from actions.api_fallback_action import FallbackManager
    >>> manager = FallbackManager()
    >>> result = manager.execute_with_fallback(primary_func, [fallback1, fallback2])
"""

from __future__ import annotations

import time
import logging
import threading
from typing import Any, Callable, Optional
from dataclasses import dataclass, field
from enum import Enum

logger = logging.getLogger(__name__)


class DegradationLevel(Enum):
    """Service degradation levels."""
    HEALTHY = 0
    DEGRADED = 1
    PARTIAL = 2
    FULL = 3


@dataclass
class FallbackStrategy:
    """A fallback strategy definition."""
    name: str
    func: Callable[..., Any]
    timeout: float = 5.0
    condition: Optional[Callable[[Exception], bool]] = None
    cache_ttl: Optional[float] = None


@dataclass
class CachedFallback:
    """Cached fallback response."""
    data: Any
    cached_at: float
    ttl: float


class FallbackManager:
    """Manage fallback strategies for API calls.

    Example:
        >>> manager = FallbackManager()
        >>> manager.add_fallback("cache", self._get_cached)
        >>> result = manager.execute(primary_call, fallback_levels=["cache", "default"])
    """

    def __init__(self, default_timeout: float = 5.0) -> None:
        self.default_timeout = default_timeout
        self._fallbacks: dict[str, FallbackStrategy] = {}
        self._cache: dict[str, CachedFallback] = {}
        self._cache_lock = threading.RLock()
        self._degradation_level = DegradationLevel.HEALTHY
        logger.info("FallbackManager initialized")

    def register_fallback(
        self,
        name: str,
        func: Callable[..., Any],
        timeout: Optional[float] = None,
        condition: Optional[Callable[[Exception], bool]] = None,
        cache_ttl: Optional[float] = None,
    ) -> None:
        """Register a fallback strategy.

        Args:
            name: Strategy name.
            func: Fallback function.
            timeout: Timeout for this fallback.
            condition: Optional condition to trigger this fallback.
            cache_ttl: Optional TTL for caching fallback responses.
        """
        self._fallbacks[name] = FallbackStrategy(
            name=name,
            func=func,
            timeout=timeout or self.default_timeout,
            condition=condition,
            cache_ttl=cache_ttl,
        )
        logger.debug(f"Registered fallback: {name}")

    def execute_with_fallback(
        self,
        primary_func: Callable[..., Any],
        fallbacks: Optional[list[str]] = None,
        *args: Any,
        **kwargs: Any,
    ) -> Any:
        """Execute primary function with fallback support.

        Args:
            primary_func: Primary function to call.
            fallbacks: List of fallback names to try in order.
            *args: Arguments for the functions.
            **kwargs: Keyword arguments for the functions.

        Returns:
            Result from primary or fallback function.

        Raises:
            Exception: If all functions fail.
        """
        fallback_names = fallbacks or list(self._fallbacks.keys())
        last_error: Optional[Exception] = None
        for name in fallback_names:
            fallback = self._fallbacks.get(name)
            if not fallback:
                continue
            if fallback.condition:
                try:
                    result = primary_func(*args, **kwargs)
                    return result
                except Exception as e:
                    if not fallback.condition(e):
                        raise
                    last_error = e
            try:
                result = self._call_with_timeout(fallback, *args, **kwargs)
                if fallback.cache_ttl:
                    self._cache_fallback(name, result, fallback.cache_ttl)
                return result
            except Exception as e:
                last_error = e
                logger.warning(f"Fallback '{name}' failed: {e}")
        if last_error:
            raise last_error
        raise RuntimeError("No fallback available")

    def _call_with_timeout(
        self,
        fallback: FallbackStrategy,
        *args: Any,
        **kwargs: Any,
    ) -> Any:
        """Call fallback with timeout (simplified)."""
        return fallback.func(*args, **kwargs)

    def _cache_fallback(
        self,
        name: str,
        data: Any,
        ttl: float,
    ) -> None:
        """Cache a fallback response."""
        with self._cache_lock:
            self._cache[name] = CachedFallback(
                data=data,
                cached_at=time.time(),
                ttl=ttl,
            )

    def get_cached_fallback(self, name: str) -> Optional[Any]:
        """Get cached fallback if available and not expired."""
        with self._cache_lock:
            cached = self._cache.get(name)
            if not cached:
                return None
            if time.time() - cached.cached_at > cached.ttl:
                del self._cache[name]
                return None
            return cached.data

    def set_degradation_level(self, level: DegradationLevel) -> None:
        """Set the current degradation level."""
        self._degradation_level = level
        logger.info(f"Degradation level set to: {level.name}")

    def get_degradation_level(self) -> DegradationLevel:
        """Get current degradation level."""
        return self._degradation_level

    def clear_cache(self) -> None:
        """Clear all cached fallbacks."""
        with self._cache_lock:
            self._cache.clear()


class GracefulDegradation:
    """Context manager for graceful degradation based on degradation level."""

    def __init__(
        self,
        fallback_manager: FallbackManager,
        required_level: DegradationLevel = DegradationLevel.HEALTHY,
    ) -> None:
        self.fallback_manager = fallback_manager
        self.required_level = required_level

    def __enter__(self) -> bool:
        current_level = self.fallback_manager.get_degradation_level()
        if current_level.value > self.required_level.value:
            return False
        return True

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        pass


def create_circuit_breaker_fallback(
    func: Callable[..., Any],
    fallback: Callable[..., Any],
    failure_threshold: int = 5,
    timeout: float = 60.0,
) -> Callable[..., Any]:
    """Create a function with circuit breaker and fallback.

    Args:
        func: Primary function.
        fallback: Fallback function.
        failure_threshold: Number of failures before opening circuit.
        timeout: Timeout in seconds.

    Returns:
        Wrapped function with circuit breaker.
    """
    from actions.circuit_breaker_action import CircuitBreaker
    cb = CircuitBreaker(
        name=func.__name__,
        failure_threshold=failure_threshold,
        timeout=timeout,
    )

    def wrapper(*args: Any, **kwargs: Any) -> Any:
        return cb.call(func, *args, fallback=fallback, **kwargs)

    return wrapper
