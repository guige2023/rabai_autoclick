"""
Retry policy utilities for resilient operation execution.

Provides configurable retry strategies, backoff algorithms,
circuit breaker integration, and timeout management.
"""

from __future__ import annotations

import asyncio
import functools
import logging
import random
import time
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Callable, Coroutine, Optional

logger = logging.getLogger(__name__)


class BackoffStrategy(Enum):
    """Backoff strategies for retries."""
    FIXED = auto()
    LINEAR = auto()
    EXPONENTIAL = auto()
    FIBONACCI = auto()
    EXPONENTIAL_WITH_JITTER = auto()


@dataclass
class RetryConfig:
    """Configuration for retry behavior."""
    max_attempts: int = 3
    initial_delay: float = 0.1
    max_delay: float = 60.0
    backoff: BackoffStrategy = BackoffStrategy.EXPONENTIAL
    multiplier: float = 2.0
    jitter: float = 0.1
    retryable_exceptions: tuple = (Exception,)
    timeout: Optional[float] = None


@dataclass
class RetryStats:
    """Statistics for retry operations."""
    attempts: int = 0
    successes: int = 0
    failures: int = 0
    total_delay: float = 0.0
    last_attempt_at: float = 0.0


class RetryPolicy:
    """Configurable retry policy with backoff."""

    def __init__(self, config: Optional[RetryConfig] = None) -> None:
        self.config = config or RetryConfig()
        self.stats = RetryStats()
        self._fib_cache = [0, 1]

    def calculate_delay(self, attempt: int) -> float:
        """Calculate delay for a given attempt number."""
        if self.config.backoff == BackoffStrategy.FIXED:
            delay = self.config.initial_delay
        elif self.config.backoff == BackoffStrategy.LINEAR:
            delay = self.config.initial_delay * (attempt + 1)
        elif self.config.backoff == BackoffStrategy.EXPONENTIAL:
            delay = self.config.initial_delay * (self.config.multiplier ** attempt)
        elif self.config.backoff == BackoffStrategy.FIBONACCI:
            delay = self.config.initial_delay * self._fibonacci(attempt)
        elif self.config.backoff == BackoffStrategy.EXPONENTIAL_WITH_JITTER:
            base_delay = self.config.initial_delay * (self.config.multiplier ** attempt)
            jitter = random.uniform(-self.config.jitter, self.config.jitter) * base_delay
            delay = base_delay + jitter
        else:
            delay = self.config.initial_delay

        return min(delay, self.config.max_delay)

    def _fibonacci(self, n: int) -> int:
        """Get nth Fibonacci number."""
        while len(self._fib_cache) <= n:
            self._fib_cache.append(self._fib_cache[-1] + self._fib_cache[-2])
        return self._fib_cache[n]

    def is_retryable(self, exception: Exception) -> bool:
        """Check if an exception is retryable."""
        return isinstance(exception, self.config.retryable_exceptions)

    def execute(
        self,
        func: Callable[..., Any],
        *args: Any,
        **kwargs: Any,
    ) -> Any:
        """Execute a function with retry logic (sync)."""
        last_exception = None
        start_time = time.time()

        for attempt in range(self.config.max_attempts):
            self.stats.attempts += 1
            self.stats.last_attempt_at = time.time()

            try:
                result = func(*args, **kwargs)
                if attempt > 0:
                    self.stats.successes += 1
                return result
            except Exception as e:
                last_exception = e
                if not self.is_retryable(e):
                    self.stats.failures += 1
                    raise

                if attempt < self.config.max_attempts - 1:
                    delay = self.calculate_delay(attempt)
                    self.stats.total_delay += delay
                    logger.warning("Retry attempt %d after %.2fs: %s", attempt + 1, delay, e)
                    time.sleep(delay)

                    if self.config.timeout and (time.time() - start_time) >= self.config.timeout:
                        self.stats.failures += 1
                        raise

        self.stats.failures += 1
        raise last_exception

    async def execute_async(
        self,
        func: Callable[..., Coroutine[Any, Any, Any]],
        *args: Any,
        **kwargs: Any,
    ) -> Any:
        """Execute an async function with retry logic."""
        last_exception = None
        start_time = time.time()

        for attempt in range(self.config.max_attempts):
            self.stats.attempts += 1
            self.stats.last_attempt_at = time.time()

            try:
                if self.config.timeout:
                    result = await asyncio.wait_for(func(*args, **kwargs), timeout=self.config.timeout)
                else:
                    result = await func(*args, **kwargs)
                if attempt > 0:
                    self.stats.successes += 1
                return result
            except asyncio.TimeoutError:
                last_exception = Exception("Operation timed out")
                if attempt < self.config.max_attempts - 1:
                    delay = self.calculate_delay(attempt)
                    self.stats.total_delay += delay
                    logger.warning("Retry attempt %d after %.2fs (timeout)", attempt + 1, delay)
                    await asyncio.sleep(delay)
            except Exception as e:
                last_exception = e
                if not self.is_retryable(e):
                    self.stats.failures += 1
                    raise

                if attempt < self.config.max_attempts - 1:
                    delay = self.calculate_delay(attempt)
                    self.stats.total_delay += delay
                    logger.warning("Retry attempt %d after %.2fs: %s", attempt + 1, delay, e)
                    await asyncio.sleep(delay)

                    if self.config.timeout and (time.time() - start_time) >= self.config.timeout:
                        self.stats.failures += 1
                        raise

        self.stats.failures += 1
        raise last_exception


def with_retry(
    max_attempts: int = 3,
    initial_delay: float = 0.1,
    max_delay: float = 60.0,
    backoff: BackoffStrategy = BackoffStrategy.EXPONENTIAL,
    multiplier: float = 2.0,
    jitter: float = 0.1,
) -> Callable:
    """Decorator to add retry logic to a function."""
    def decorator(func: Callable) -> Callable:
        policy = RetryPolicy(RetryConfig(
            max_attempts=max_attempts,
            initial_delay=initial_delay,
            max_delay=max_delay,
            backoff=backoff,
            multiplier=multiplier,
            jitter=jitter,
        ))

        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            return policy.execute(func, *args, **kwargs)

        @functools.wraps(func)
        async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
            return await policy.execute_async(func, *args, **kwargs)

        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        return wrapper
    return decorator
