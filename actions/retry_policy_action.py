"""
Retry Policy Action Module

Configurable retry policies with backoff strategies,
jitter, and circuit breaker integration.

MIT License - Copyright (c) 2025 RabAi Research
"""

from __future__ import annotations

import asyncio
import logging
import random
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Optional, Set, TypeVar

logger = logging.getLogger(__name__)

T = TypeVar("T")


class BackoffStrategy(Enum):
    """Backoff strategies."""

    FIXED = "fixed"
    LINEAR = "linear"
    EXPONENTIAL = "exponential"
    FIBONACCI = "fibonacci"
    EXPONENTIAL_WITH_JITTER = "exponential_with_jitter"


@dataclass
class RetryPolicyConfig:
    """Configuration for retry policy."""

    max_attempts: int = 3
    base_delay: float = 1.0
    max_delay: float = 60.0
    exponential_base: float = 2.0
    jitter_range: float = 0.3
    strategy: BackoffStrategy = BackoffStrategy.EXPONENTIAL
    retryable_exceptions: Set[type] = field(default_factory=lambda: {Exception})
    non_retryable_exceptions: Set[type] = field(default_factory=set)


class RetryPolicy:
    """
    Configurable retry policy with various backoff strategies.
    """

    def __init__(self, config: Optional[RetryPolicyConfig] = None):
        self.config = config or RetryPolicyConfig()

    def calculate_delay(self, attempt: int) -> float:
        """Calculate delay for the given attempt number."""
        base_delay = self.config.base_delay

        if self.config.strategy == BackoffStrategy.FIXED:
            delay = base_delay

        elif self.config.strategy == BackoffStrategy.LINEAR:
            delay = base_delay * (attempt + 1)

        elif self.config.strategy == BackoffStrategy.EXPONENTIAL:
            delay = base_delay * (self.config.exponential_base ** attempt)

        elif self.config.strategy == BackoffStrategy.FIBONACCI:
            delay = base_delay * self._fibonacci(attempt + 2)

        elif self.config.strategy == BackoffStrategy.EXPONENTIAL_WITH_JITTER:
            delay = base_delay * (self.config.exponential_base ** attempt)
            jitter = delay * self.config.jitter_range
            delay += random.uniform(-jitter, jitter)

        else:
            delay = base_delay

        return min(delay, self.config.max_delay)

    @staticmethod
    def _fibonacci(n: int) -> int:
        """Calculate nth Fibonacci number."""
        if n <= 1:
            return n
        a, b = 0, 1
        for _ in range(n - 1):
            a, b = b, a + b
        return b

    def is_retryable(self, exception: Exception) -> bool:
        """Check if exception is retryable."""
        if type(exception) in self.config.non_retryable_exceptions:
            return False

        if type(exception) in self.config.retryable_exceptions:
            return True

        # Check inheritance
        for exc_type in self.config.retryable_exceptions:
            if isinstance(exception, exc_type):
                return True

        return False


@dataclass
class RetryStats:
    """Retry statistics."""

    total_attempts: int = 0
    successful_retries: int = 0
    failed_retries: int = 0
    total_delay_ms: float = 0.0


class RetryPolicyAction:
    """
    Main action class for retry policies.

    Features:
    - Multiple backoff strategies
    - Jitter support
    - Custom exception handling
    - Comprehensive statistics

    Usage:
        policy = RetryPolicyAction()
        result = await policy.execute_with_retry(
            some_operation,
            retry_on=[ValueError, ConnectionError],
        )
    """

    def __init__(self, config: Optional[RetryPolicyConfig] = None):
        self._policy = RetryPolicy(config)
        self._stats = RetryStats()

    async def execute_with_retry(
        self,
        func: Callable[..., T],
        *args: Any,
        **kwargs: Any,
    ) -> T:
        """Execute function with retry logic."""
        last_exception: Optional[Exception] = None

        for attempt in range(self._policy.config.max_attempts):
            self._stats.total_attempts += 1

            try:
                if asyncio.iscoroutinefunction(func):
                    result = await func(*args, **kwargs)
                else:
                    result = func(*args, **kwargs)

                if attempt > 0:
                    self._stats.successful_retries += 1

                return result

            except Exception as e:
                last_exception = e

                if not self._policy.is_retryable(e):
                    self._stats.failed_retries += 1
                    raise

                if attempt < self._policy.config.max_attempts - 1:
                    delay = self._policy.calculate_delay(attempt)
                    self._stats.total_delay_ms += delay * 1000
                    logger.warning(
                        f"Retry {attempt + 1}/{self._policy.config.max_attempts} "
                        f"after {delay:.2f}s: {e}"
                    )
                    await asyncio.sleep(delay)
                else:
                    self._stats.failed_retries += 1

        raise last_exception

    def get_stats(self) -> dict:
        """Get retry statistics."""
        return {
            "total_attempts": self._stats.total_attempts,
            "successful_retries": self._stats.successful_retries,
            "failed_retries": self._stats.failed_retries,
            "total_delay_ms": self._stats.total_delay_ms,
        }


async def demo_retry():
    """Demonstrate retry policy."""
    call_count = 0

    async def unreliable_operation():
        nonlocal call_count
        call_count += 1
        if call_count < 3:
            raise ConnectionError("Connection failed")
        return "Success"

    policy = RetryPolicyAction()
    result = await policy.execute_with_retry(unreliable_operation)
    print(f"Result: {result}")
    print(f"Stats: {policy.get_stats()}")


if __name__ == "__main__":
    asyncio.run(demo_retry())
