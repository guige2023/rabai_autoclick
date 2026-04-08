"""
Retry Scheduler Utility

Implements configurable retry logic with backoff strategies.
Supports exponential backoff, jitter, and deadline tracking.

Example:
    >>> scheduler = RetryScheduler(max_attempts=3, base_delay=1.0)
    >>> result = scheduler.execute(maybe_failing_function)
    >>> print(f"Succeeded after {scheduler.attempt_count} attempts")
"""

from __future__ import annotations

import random
import threading
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Callable, Optional, TypeVar, Generic


T = TypeVar("T")


class BackoffStrategy(Enum):
    """Retry backoff strategies."""
    FIXED = "fixed"
    LINEAR = "linear"
    EXPONENTIAL = "exponential"
    FIBONACCI = "fibonacci"


@dataclass
class RetryResult:
    """Result of a retry operation."""
    success: bool
    attempt_count: int
    total_duration: float
    last_error: Optional[Exception] = None
    backoff_used: float = 0.0


@dataclass
class RetryConfig:
    """Configuration for retry behavior."""
    max_attempts: int = 3
    base_delay: float = 1.0
    max_delay: float = 60.0
    strategy: BackoffStrategy = BackoffStrategy.EXPONENTIAL
    jitter: bool = True
    jitter_range: tuple[float, float] = (0.5, 1.5)
    deadline: Optional[float] = None  # Total time budget for all retries


class RetryScheduler:
    """
    Configurable retry scheduler with backoff strategies.

    Args:
        config: Retry configuration.
    """

    def __init__(self, config: Optional[RetryConfig] = None) -> None:
        self.config = config or RetryConfig()
        self.attempt_count = 0
        self._last_backoff = 0.0
        self._lock = threading.Lock()

    def execute(
        self,
        func: Callable[[], T],
        should_retry: Optional[Callable[[Exception], bool]] = None,
    ) -> tuple[T, RetryResult]:
        """
        Execute a function with retry logic.

        Args:
            func: Function to execute.
            should_retry: Optional predicate to determine if retry should happen.

        Returns:
            Tuple of (result, RetryResult).
        """
        start_time = time.time()
        last_error: Optional[Exception] = None
        should_retry = should_retry or (lambda e: True)

        for attempt in range(1, self.config.max_attempts + 1):
            self.attempt_count = attempt

            try:
                result = func()
                duration = time.time() - start_time
                return result, RetryResult(
                    success=True,
                    attempt_count=attempt,
                    total_duration=duration,
                    backoff_used=self._last_backoff,
                )

            except Exception as e:
                last_error = e

                # Check deadline
                if self.config.deadline and (time.time() - start_time) >= self.config.deadline:
                    break

                # Check if we should retry
                if not should_retry(e):
                    break

                # Check if more attempts remain
                if attempt >= self.config.max_attempts:
                    break

                # Calculate backoff
                backoff = self._calculate_backoff(attempt)
                self._last_backoff = backoff

                time.sleep(backoff)

        duration = time.time() - start_time
        return None, RetryResult(
            success=False,
            attempt_count=self.attempt_count,
            total_duration=duration,
            last_error=last_error,
            backoff_used=self._last_backoff,
        )

    def _calculate_backoff(self, attempt: int) -> float:
        """Calculate backoff delay for given attempt."""
        base = self.config.base_delay

        if self.config.strategy == BackoffStrategy.FIXED:
            delay = base
        elif self.config.strategy == BackoffStrategy.LINEAR:
            delay = base * attempt
        elif self.config.strategy == BackoffStrategy.EXPONENTIAL:
            delay = base * (2 ** (attempt - 1))
        elif self.config.strategy == BackoffStrategy.FIBONACCI:
            delay = base * self._fibonacci(attempt)
        else:
            delay = base

        # Apply bounds
        delay = min(delay, self.config.max_delay)

        # Apply jitter
        if self.config.jitter:
            jitter_min, jitter_max = self.config.jitter_range
            jitter = random.uniform(jitter_min, jitter_max)
            delay *= jitter

        return delay

    def _fibonacci(self, n: int) -> int:
        """Calculate nth Fibonacci number."""
        if n <= 1:
            return 1
        a, b = 1, 1
        for _ in range(n - 1):
            a, b = b, a + b
        return b


def retry(
    max_attempts: int = 3,
    base_delay: float = 1.0,
    strategy: BackoffStrategy = BackoffStrategy.EXPONENTIAL,
    jitter: bool = True,
) -> Callable:
    """
    Decorator for adding retry logic to functions.

    Example:
        @retry(max_attempts=5, base_delay=0.5)
        def unstable_operation():
            ...
    """
    def decorator(func: Callable[[], T]) -> Callable[[], T]:
        config = RetryConfig(
            max_attempts=max_attempts,
            base_delay=base_delay,
            strategy=strategy,
            jitter=jitter,
        )
        scheduler = RetryScheduler(config)

        def wrapper() -> T:
            result, retry_result = scheduler.execute(func)
            if not retry_result.success and retry_result.last_error:
                raise retry_result.last_error
            return result

        return wrapper

    return decorator
