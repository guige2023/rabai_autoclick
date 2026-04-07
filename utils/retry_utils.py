"""
Retry and Backoff Utilities

Provides configurable retry logic with various backoff strategies
and error classification.
"""

from __future__ import annotations

import asyncio
import copy
import random
import time
from abc import ABC, abstractmethod
from collections.abc import Callable
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Generic, TypeVar

T = TypeVar("T")


class BackoffStrategy(Enum):
    """Available backoff strategies."""
    FIXED = auto()
    LINEAR = auto()
    EXPONENTIAL = auto()
    EXPONENTIAL_WITH_JITTER = auto()
    FIBONACCI = auto()


@dataclass
class RetryConfig:
    """Configuration for retry behavior."""
    max_attempts: int = 3
    initial_delay_ms: float = 100.0
    max_delay_ms: float = 30000.0
    backoff_strategy: BackoffStrategy = BackoffStrategy.EXPONENTIAL_WITH_JITTER
    retryable_exceptions: tuple[type[Exception], ...] = (Exception,)
    non_retryable_exceptions: tuple[type[Exception], ...] = ()
    timeout_seconds: float | None = None


@dataclass
class RetryResult(Generic[T]):
    """Result of a retry operation."""
    success: bool
    value: T | None = None
    error: Exception | None = None
    attempts: int = 0
    total_time_ms: float = 0.0
    backoff_ms: float = 0.0
    logs: list[dict[str, Any]] = field(default_factory=list)

    @property
    def failed(self) -> bool:
        return not self.success


class BackoffCalculator:
    """
    Calculates backoff delays based on strategy.
    """

    def __init__(self, strategy: BackoffStrategy, initial_delay_ms: float, max_delay_ms: float):
        self.strategy = strategy
        self.initial_delay_ms = initial_delay_ms
        self.max_delay_ms = max_delay_ms
        self._fib_prev = 0.0
        self._fib_curr = 1.0

    def calculate(self, attempt: int) -> float:
        """
        Calculate delay for the given attempt number.

        Args:
            attempt: The attempt number (0-indexed).

        Returns:
            Delay in milliseconds.
        """
        if self.strategy == BackoffStrategy.FIXED:
            delay = self.initial_delay_ms

        elif self.strategy == BackoffStrategy.LINEAR:
            delay = self.initial_delay_ms * (attempt + 1)

        elif self.strategy == BackoffStrategy.EXPONENTIAL:
            delay = self.initial_delay_ms * (2 ** attempt)

        elif self.strategy == BackoffStrategy.EXPONENTIAL_WITH_JITTER:
            base_delay = self.initial_delay_ms * (2 ** attempt)
            jitter = random.uniform(0, 0.3) * base_delay
            delay = base_delay + jitter

        elif self.strategy == BackoffStrategy.FIBONACCI:
            if attempt == 0:
                delay = self.initial_delay_ms
            else:
                fib = self._fib_prev + self._fib_curr
                self._fib_prev = self._fib_curr
                self._fib_curr = fib
                delay = self.initial_delay_ms * fib

        else:
            delay = self.initial_delay_ms

        return min(delay, self.max_delay_ms)

    def reset(self) -> None:
        """Reset the calculator state."""
        self._fib_prev = 0.0
        self._fib_curr = 1.0


def is_retryable(
    exception: Exception,
    retryable: tuple[type[Exception], ...],
    non_retryable: tuple[type[Exception], ...],
) -> bool:
    """
    Determine if an exception is retryable.

    Args:
        exception: The exception to check.
        retryable: Tuple of retryable exception types.
        non_retryable: Tuple of non-retryable exception types.

    Returns:
        True if the exception should be retried.
    """
    # Non-retryable takes precedence
    for exc_type in non_retryable:
        if isinstance(exception, exc_type):
            return False

    # Check if it's in the retryable list
    for exc_type in retryable:
        if isinstance(exception, exc_type):
            return True

    return False


def retry(
    func: Callable[[], T],
    config: RetryConfig | None = None,
) -> RetryResult[T]:
    """
    Execute a function with retry logic.

    Args:
        func: Function to execute.
        config: Retry configuration.

    Returns:
        RetryResult with execution details.
    """
    config = config or RetryConfig()
    calculator = BackoffCalculator(
        config.backoff_strategy,
        config.initial_delay_ms,
        config.max_delay_ms,
    )

    start_time = time.time()
    logs: list[dict[str, Any]] = []
    last_error: Exception | None = None

    for attempt in range(config.max_attempts):
        attempt_start = time.time()

        try:
            result = func()
            elapsed = (time.time() - start_time) * 1000

            return RetryResult(
                success=True,
                value=result,
                attempts=attempt + 1,
                total_time_ms=elapsed,
                backoff_ms=calculator.calculate(attempt - 1) if attempt > 0 else 0,
                logs=logs,
            )

        except Exception as e:
            attempt_elapsed = (time.time() - attempt_start) * 1000
            last_error = e

            # Check if retryable
            if not is_retryable(e, config.retryable_exceptions, config.non_retryable_exceptions):
                elapsed = (time.time() - start_time) * 1000
                return RetryResult(
                    success=False,
                    error=e,
                    attempts=attempt + 1,
                    total_time_ms=elapsed,
                    logs=logs,
                )

            logs.append({
                "attempt": attempt + 1,
                "error": str(e),
                "error_type": type(e).__name__,
                "duration_ms": attempt_elapsed,
                "timestamp": time.time(),
            })

            # Check timeout
            if config.timeout_seconds:
                elapsed = (time.time() - start_time) * 1000
                if elapsed / 1000 >= config.timeout_seconds:
                    return RetryResult(
                        success=False,
                        error=e,
                        attempts=attempt + 1,
                        total_time_ms=elapsed,
                        logs=logs,
                    )

            # Calculate backoff and sleep
            if attempt < config.max_attempts - 1:
                delay_ms = calculator.calculate(attempt)
                time.sleep(delay_ms / 1000)
                calculator.reset()

    elapsed = (time.time() - start_time) * 1000
    return RetryResult(
        success=False,
        error=last_error,
        attempts=config.max_attempts,
        total_time_ms=elapsed,
        logs=logs,
    )


async def retry_async(
    func: Callable[[], T],
    config: RetryConfig | None = None,
) -> RetryResult[T]:
    """
    Execute an async function with retry logic.

    Args:
        func: Async function to execute.
        config: Retry configuration.

    Returns:
        RetryResult with execution details.
    """
    config = config or RetryConfig()
    calculator = BackoffCalculator(
        config.backoff_strategy,
        config.initial_delay_ms,
        config.max_delay_ms,
    )

    start_time = time.time()
    logs: list[dict[str, Any]] = []
    last_error: Exception | None = None

    for attempt in range(config.max_attempts):
        attempt_start = time.time()

        try:
            result = await func()
            elapsed = (time.time() - start_time) * 1000

            return RetryResult(
                success=True,
                value=result,
                attempts=attempt + 1,
                total_time_ms=elapsed,
                backoff_ms=calculator.calculate(attempt - 1) if attempt > 0 else 0,
                logs=logs,
            )

        except Exception as e:
            attempt_elapsed = (time.time() - attempt_start) * 1000
            last_error = e

            if not is_retryable(e, config.retryable_exceptions, config.non_retryable_exceptions):
                elapsed = (time.time() - start_time) * 1000
                return RetryResult(
                    success=False,
                    error=e,
                    attempts=attempt + 1,
                    total_time_ms=elapsed,
                    logs=logs,
                )

            logs.append({
                "attempt": attempt + 1,
                "error": str(e),
                "error_type": type(e).__name__,
                "duration_ms": attempt_elapsed,
                "timestamp": time.time(),
            })

            if attempt < config.max_attempts - 1:
                delay_ms = calculator.calculate(attempt)
                await asyncio.sleep(delay_ms / 1000)
                calculator.reset()

    elapsed = (time.time() - start_time) * 1000
    return RetryResult(
        success=False,
        error=last_error,
        attempts=config.max_attempts,
        total_time_ms=elapsed,
        logs=logs,
    )


class RetryableOperation(Generic[T]):
    """
    Wrapper for making operations retryable.
    """

    def __init__(
        self,
        func: Callable[[], T],
        config: RetryConfig | None = None,
    ):
        self._func = func
        self._config = config or RetryConfig()

    def execute(self) -> RetryResult[T]:
        """Execute with retry."""
        return retry(self._func, self._config)

    async def execute_async(self) -> RetryResult[T]:
        """Execute async with retry."""
        return await retry_async(self._func, self._config)


def with_retry(
    config: RetryConfig | None = None,
) -> Callable[[Callable[[], T]], RetryableOperation[T]]:
    """
    Decorator to make a function retryable.

    Usage:
        @with_retry(RetryConfig(max_attempts=5))
        def my_function():
            ...
    """
    def decorator(func: Callable[[], T]) -> RetryableOperation[T]:
        return RetryableOperation(func, config)
    return decorator
