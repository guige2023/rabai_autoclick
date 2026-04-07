"""Retry utilities with backoff strategies and exception handling."""

from __future__ import annotations

import asyncio
import random
import time
from dataclasses import dataclass
from enum import Enum
from functools import wraps
from typing import Any, Callable, TypeVar, Union

__all__ = [
    "BackoffStrategy",
    "RetryConfig",
    "retry",
    "retry_async",
    "RetryError",
    "CircuitBreaker",
]

T = TypeVar("T")


class BackoffStrategy(Enum):
    FIXED = "fixed"
    LINEAR = "linear"
    EXPONENTIAL = "exponential"
    FIBONACCI = "fibonacci"
    JITTER = "jitter"


@dataclass
class RetryConfig:
    """Configuration for retry behavior."""
    max_attempts: int = 3
    base_delay: float = 1.0
    max_delay: float = 60.0
    strategy: BackoffStrategy = BackoffStrategy.EXPONENTIAL
    jitter: bool = True
    retryable_exceptions: tuple[type[Exception], ...] = (Exception,)
    on_retry: Callable[[Exception, int], None] | None = None


class RetryError(Exception):
    """Raised when all retry attempts are exhausted."""

    def __init__(self, message: str, attempts: int, last_exception: Exception) -> None:
        super().__init__(message)
        self.attempts = attempts
        self.last_exception = last_exception


def _calculate_delay(
    attempt: int,
    config: RetryConfig,
) -> float:
    if config.strategy == BackoffStrategy.FIXED:
        delay = config.base_delay
    elif config.strategy == BackoffStrategy.LINEAR:
        delay = config.base_delay * attempt
    elif config.strategy == BackoffStrategy.EXPONENTIAL:
        delay = config.base_delay * (2 ** (attempt - 1))
    elif config.strategy == BackoffStrategy.FIBONACCI:
        a, b = 1, 1
        for _ in range(attempt - 1):
            a, b = b, a + b
        delay = config.base_delay * a
    else:
        delay = config.base_delay

    delay = min(delay, config.max_delay)

    if config.jitter:
        delay *= 0.5 + random.random()

    return delay


def retry(
    config: RetryConfig | None = None,
) -> Callable[[Callable[..., T]], Callable[..., T]]:
    """Decorator for retrying a function with backoff."""
    cfg = config or RetryConfig()

    def decorator(fn: Callable[..., T]) -> Callable[..., T]:
        @wraps(fn)
        def wrapper(*args: Any, **kwargs: Any) -> T:
            last_exception: Exception | None = None
            for attempt in range(1, cfg.max_attempts + 1):
                try:
                    return fn(*args, **kwargs)
                except cfg.retryable_exceptions as e:
                    last_exception = e
                    if attempt == cfg.max_attempts:
                        break
                    delay = _calculate_delay(attempt, cfg)
                    if cfg.on_retry:
                        cfg.on_retry(e, attempt)
                    time.sleep(delay)
            raise RetryError(
                f"Failed after {cfg.max_attempts} attempts",
                cfg.max_attempts,
                last_exception or RuntimeError("Unknown error"),
            )
        return wrapper
    return decorator


def retry_async(
    config: RetryConfig | None = None,
) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """Async version of retry decorator."""
    cfg = config or RetryConfig()

    def decorator(fn: Callable[..., Any]) -> Callable[..., Any]:
        @wraps(fn)
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            last_exception: Exception | None = None
            for attempt in range(1, cfg.max_attempts + 1):
                try:
                    return await fn(*args, **kwargs)
                except cfg.retryable_exceptions as e:
                    last_exception = e
                    if attempt == cfg.max_attempts:
                        break
                    delay = _calculate_delay(attempt, cfg)
                    if cfg.on_retry:
                        cfg.on_retry(e, attempt)
                    await asyncio.sleep(delay)
            raise RetryError(
                f"Failed after {cfg.max_attempts} attempts",
                cfg.max_attempts,
                last_exception or RuntimeError("Unknown error"),
            )
        return wrapper
    return decorator


class CircuitBreaker:
    """Circuit breaker pattern implementation."""

    def __init__(
        self,
        failure_threshold: int = 5,
        recovery_timeout: float = 60.0,
        expected_exception: type[Exception] = Exception,
    ) -> None:
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.expected_exception = expected_exception
        self._failure_count = 0
        self._last_failure_time: float | None = None
        self._state = "closed"

    @property
    def state(self) -> str:
        if self._state == "open":
            if self._last_failure_time:
                if time.time() - self._last_failure_time >= self.recovery_timeout:
                    self._state = "half-open"
        return self._state

    def call(self, fn: Callable[..., T], *args: Any, **kwargs: Any) -> T:
        if self.state == "open":
            raise RuntimeError("Circuit breaker is OPEN")

        try:
            result = fn(*args, **kwargs)
            if self._state == "half-open":
                self._state = "closed"
                self._failure_count = 0
            return result
        except self.expected_exception as e:
            self._failure_count += 1
            self._last_failure_time = time.time()
            if self._failure_count >= self.failure_threshold:
                self._state = "open"
            raise

    def reset(self) -> None:
        self._failure_count = 0
        self._last_failure_time = None
        self._state = "closed"
