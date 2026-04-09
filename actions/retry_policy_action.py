"""Retry Policy Action Module.

Configurable retry policies with backoff strategies.
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, TypeVar

T = TypeVar("T")


class BackoffStrategy(Enum):
    """Backoff strategies."""
    FIXED = "fixed"
    LINEAR = "linear"
    EXPONENTIAL = "exponential"
    FIBONACCI = "fibonacci"
    EXPONENTIAL_WITH_JITTER = "exponential_with_jitter"
    FULL_JITTER = "full_jitter"


@dataclass
class RetryPolicy:
    """Retry policy configuration."""
    max_attempts: int = 3
    initial_delay: float = 1.0
    max_delay: float = 60.0
    backoff: BackoffStrategy = BackoffStrategy.EXPONENTIAL
    backoff_factor: float = 2.0
    jitter: bool = True
    jitter_factor: float = 0.1
    retriable_exceptions: tuple = (Exception,)
    non_retriable_exceptions: tuple = ()


@dataclass
class RetryAttempt:
    """Single retry attempt."""
    attempt_number: int
    started_at: float
    completed_at: float | None = None
    error: str | None = None
    succeeded: bool = False


@dataclass
class RetryStats:
    """Retry statistics."""
    total_attempts: int = 0
    successful_attempts: int = 0
    failed_attempts: int = 0
    total_delay: float = 0.0
    attempts: list[RetryAttempt] = field(default_factory=list)


class RetryExecutor:
    """Execute operations with retry policy."""

    def __init__(self, policy: RetryPolicy | None = None) -> None:
        self.policy = policy or RetryPolicy()
        self.stats = RetryStats()

    def calculate_delay(self, attempt: int) -> float:
        """Calculate delay for given attempt using backoff strategy."""
        if self.policy.backoff == BackoffStrategy.FIXED:
            delay = self.policy.initial_delay
        elif self.policy.backoff == BackoffStrategy.LINEAR:
            delay = self.policy.initial_delay * (attempt + 1)
        elif self.policy.backoff == BackoffStrategy.EXPONENTIAL:
            delay = self.policy.initial_delay * (self.policy.backoff_factor ** attempt)
        elif self.policy.backoff == BackoffStrategy.FIBONACCI:
            delay = self._fibonacci(attempt + 1) * self.policy.initial_delay
        else:
            delay = self.policy.initial_delay * (self.policy.backoff_factor ** attempt)
        delay = min(delay, self.policy.max_delay)
        if self.policy.jitter:
            jitter_range = delay * self.policy.jitter_factor
            delay += (time.time() % jitter_range) - (jitter_range / 2)
        return max(0, delay)

    def _fibonacci(self, n: int) -> float:
        """Calculate fibonacci number."""
        if n <= 1:
            return 1.0
        a, b = 1.0, 1.0
        for _ in range(n - 1):
            a, b = b, a + b
        return b

    def _is_retriable(self, exception: Exception) -> bool:
        """Check if exception is retriable."""
        if isinstance(exception, self.policy.non_retriable_exceptions):
            return False
        if isinstance(exception, self.policy.retriable_exceptions):
            return True
        return False

    async def execute(
        self,
        func: Callable[[], T | asyncio.coroutine],
        *args,
        **kwargs
    ) -> T:
        """Execute function with retry policy."""
        last_exception: Exception | None = None
        for attempt in range(self.policy.max_attempts):
            retry_attempt = RetryAttempt(attempt_number=attempt + 1, started_at=time.time())
            self.stats.total_attempts += 1
            try:
                result = func(*args, **kwargs)
                if asyncio.iscoroutine(result):
                    result = await result
                retry_attempt.succeeded = True
                retry_attempt.completed_at = time.time()
                self.stats.successful_attempts += 1
                self.stats.attempts.append(retry_attempt)
                return result
            except Exception as e:
                last_exception = e
                retry_attempt.error = str(e)
                retry_attempt.completed_at = time.time()
                self.stats.failed_attempts += 1
                self.stats.attempts.append(retry_attempt)
                if not self._is_retriable(e):
                    raise
                if attempt < self.policy.max_attempts - 1:
                    delay = self.calculate_delay(attempt)
                    self.stats.total_delay += delay
                    await asyncio.sleep(delay)
        raise last_exception

    def get_stats(self) -> RetryStats:
        """Get retry statistics."""
        return self.stats

    def reset_stats(self) -> None:
        """Reset retry statistics."""
        self.stats = RetryStats()


class CircuitBreakerRetryPolicy(RetryPolicy):
    """Retry policy with integrated circuit breaker."""

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.failure_threshold = 5
        self.success_threshold = 2
        self._failure_count = 0
        self._success_count = 0
        self._circuit_open = False
        self._last_failure_time: float | None = None

    def _check_circuit(self) -> bool:
        """Check if circuit breaker should open."""
        if self._circuit_open:
            if self._last_failure_time:
                if time.time() - self._last_failure_time > self.policy.max_delay:
                    self._circuit_open = False
                    self._failure_count = 0
                    return True
            return False
        return True

    def _record_success(self) -> None:
        """Record successful execution."""
        self._success_count += 1
        self._failure_count = 0
        if self._success_count >= self.success_threshold:
            self._circuit_open = False

    def _record_failure(self) -> None:
        """Record failed execution."""
        self._failure_count += 1
        self._success_count = 0
        self._last_failure_time = time.time()
        if self._failure_count >= self.failure_threshold:
            self._circuit_open = True
