"""
Automation Retry Action Module.

Retry logic for automation tasks with backoff,
jitter, and recovery handlers.
"""

from __future__ import annotations

from typing import Any, Callable, Optional, TypeVar
from dataclasses import dataclass, field
from enum import Enum
import logging
import asyncio
import time
import random
from functools import wraps

logger = logging.getLogger(__name__)

T = TypeVar("T")


class RetryStrategy(Enum):
    """Retry strategy types."""
    FIXED = "fixed"
    LINEAR = "linear"
    EXPONENTIAL = "exponential"
    FIBONACCI = "fibonacci"


@dataclass
class RetryConfig:
    """Retry configuration for automation tasks."""
    max_attempts: int = 3
    base_delay: float = 1.0
    max_delay: float = 60.0
    strategy: RetryStrategy = RetryStrategy.EXPONENTIAL
    jitter: bool = True
    jitter_factor: float = 0.3
    retryable_exceptions: tuple = (Exception,)
    on_retry: Optional[Callable[[int, Exception], None]] = None
    on_failure: Optional[Callable[[Exception], None]] = None


class AutomationRetryAction:
    """
    Retry mechanism for automation task failures.

    Supports multiple backoff strategies, jitter,
    and hooks for retry/failure handling.

    Example:
        retry = AutomationRetryAction(max_attempts=5, base_delay=2.0)
        result = retry.run(flaky_automation_task)
    """

    def __init__(self, config: Optional[RetryConfig] = None, **kwargs: Any) -> None:
        self.config = config or RetryConfig(**kwargs)

    def run(
        self,
        func: Callable[..., T],
        *args: Any,
        **kwargs: Any,
    ) -> T:
        """Execute function with retry logic."""
        last_exception: Optional[Exception] = None

        for attempt in range(1, self.config.max_attempts + 1):
            try:
                return func(*args, **kwargs)

            except self.config.retryable_exceptions as e:
                last_exception = e

                if attempt >= self.config.max_attempts:
                    break

                delay = self._calculate_delay(attempt)

                if self.config.on_retry:
                    self.config.on_retry(attempt, e)

                logger.warning(
                    "Attempt %d/%d failed: %s. Retrying in %.2fs",
                    attempt, self.config.max_attempts, e, delay
                )

                time.sleep(delay)

        if self.config.on_failure and last_exception:
            self.config.on_failure(last_exception)

        raise last_exception or Exception("Retry exhausted")

    async def run_async(
        self,
        func: Callable[..., T],
        *args: Any,
        **kwargs: Any,
    ) -> T:
        """Execute async function with retry logic."""
        last_exception: Optional[Exception] = None

        for attempt in range(1, self.config.max_attempts + 1):
            try:
                if asyncio.iscoroutinefunction(func):
                    return await func(*args, **kwargs)
                else:
                    return func(*args, **kwargs)

            except self.config.retryable_exceptions as e:
                last_exception = e

                if attempt >= self.config.max_attempts:
                    break

                delay = self._calculate_delay(attempt)

                if self.config.on_retry:
                    self.config.on_retry(attempt, e)

                logger.warning(
                    "Async attempt %d/%d failed: %s. Retrying in %.2fs",
                    attempt, self.config.max_attempts, e, delay
                )

                await asyncio.sleep(delay)

        if self.config.on_failure and last_exception:
            self.config.on_failure(last_exception)

        raise last_exception or Exception("Async retry exhausted")

    def decorator(
        self,
        func: Optional[Callable[..., T]] = None,
        **kwargs: Any,
    ) -> Callable[[Callable[..., T]], Callable[..., T]]:
        """Decorator to add retry to any function."""
        _config = self.config.__class__(**{
            **vars(self.config),
            **kwargs
        })

        def decorator_inner(f: Callable[..., T]) -> Callable[..., T]:
            @wraps(f)
            def wrapper(*args: Any, **kwkwargs: Any) -> T:
                retry = AutomationRetryAction(config=_config)
                return retry.run(f, *args, **kwkwargs)

            @wraps(f)
            async def async_wrapper(*args: Any, **kwkwargs: Any) -> T:
                retry = AutomationRetryAction(config=_config)
                return await retry.run_async(f, *args, **kwkwargs)

            if asyncio.iscoroutinefunction(f):
                return async_wrapper
            return wrapper

        if func is not None:
            return decorator_inner(func)
        return decorator_inner

    def _calculate_delay(self, attempt: int) -> float:
        """Calculate delay for the given attempt number."""
        strategy = self.config.strategy
        base = self.config.base_delay

        if strategy == RetryStrategy.FIXED:
            delay = base

        elif strategy == RetryStrategy.LINEAR:
            delay = base * attempt

        elif strategy == RetryStrategy.EXPONENTIAL:
            delay = base * (2 ** (attempt - 1))

        elif strategy == RetryStrategy.FIBONACCI:
            delay = base * self._fibonacci(attempt)

        else:
            delay = base

        delay = min(delay, self.config.max_delay)

        if self.config.jitter:
            factor = self.config.jitter_factor
            jitter_range = delay * factor
            delay += random.uniform(-jitter_range, jitter_range)
            delay = max(0.1, delay)

        return delay

    @staticmethod
    def _fibonacci(n: int) -> int:
        """Calculate nth Fibonacci number."""
        if n <= 1:
            return 1
        a, b = 1, 1
        for _ in range(n - 1):
            a, b = b, a + b
        return b
