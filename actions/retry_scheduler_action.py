"""
Retry Scheduler Action Module

Provides retry scheduling with backoff strategies for UI automation workflows.
Supports exponential, linear, Fibonacci backoff and jitter for robust error recovery.

Author: AI Agent
Version: 1.0.0
"""

from __future__ import annotations

import asyncio
import logging
import random
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum, auto
from typing import Any, Callable, Optional, TypeVar

logger = logging.getLogger(__name__)

T = TypeVar("T")


class BackoffStrategy(Enum):
    """Backoff strategy types."""
    EXPONENTIAL = auto()
    LINEAR = auto()
    FIBONACCI = auto()
    FIXED = auto()
    EXPONENTIAL_WITH_JITTER = auto()


@dataclass
class RetryConfig:
    """Retry configuration."""
    max_attempts: int = 3
    initial_delay: float = 1.0
    max_delay: float = 60.0
    backoff_strategy: BackoffStrategy = BackoffStrategy.EXPONENTIAL
    multiplier: float = 2.0
    jitter: float = 0.1
    retryable_exceptions: tuple = (Exception,)
    timeout: Optional[float] = None


@dataclass
class RetryAttempt:
    """Single retry attempt."""
    attempt_number: int
    timestamp: float
    delay: float
    exception: Optional[Exception] = None
    success: bool = False
    result: Any = None


@dataclass
class RetryResult:
    """Retry operation result."""
    success: bool
    result: Any = None
    attempts: list[RetryAttempt] = field(default_factory=list)
    total_duration: float = 0.0
    error: Optional[str] = None


class BackoffCalculator:
    """
    Calculates backoff delays.

    Example:
        >>> calc = BackoffCalculator(BackoffStrategy.EXPONENTIAL, initial_delay=1.0)
        >>> delay = calc.calculate(attempt=3)
    """

    def __init__(
        self,
        strategy: BackoffStrategy,
        initial_delay: float = 1.0,
        max_delay: float = 60.0,
        multiplier: float = 2.0,
        jitter: float = 0.1,
    ) -> None:
        self.strategy = strategy
        self.initial_delay = initial_delay
        self.max_delay = max_delay
        self.multiplier = multiplier
        self.jitter = jitter
        self._fib_cache: dict[int, float] = {0: 1, 1: 1}

    def calculate(self, attempt: int) -> float:
        """Calculate delay for given attempt number."""
        if attempt <= 0:
            return self.initial_delay

        delay = self._calculate_base_delay(attempt)
        delay = min(delay, self.max_delay)

        if self.jitter > 0:
            delay = self._apply_jitter(delay)

        return delay

    def _calculate_base_delay(self, attempt: int) -> float:
        """Calculate base delay without jitter."""
        if self.strategy == BackoffStrategy.EXPONENTIAL:
            return self.initial_delay * (self.multiplier ** (attempt - 1))

        if self.strategy == BackoffStrategy.LINEAR:
            return self.initial_delay + (attempt - 1) * self.multiplier

        if self.strategy == BackoffStrategy.FIBONACCI:
            return self.initial_delay * self._fibonacci(attempt)

        if self.strategy == BackoffStrategy.FIXED:
            return self.initial_delay

        if self.strategy == BackoffStrategy.EXPONENTIAL_WITH_JITTER:
            return self.initial_delay * (self.multiplier ** (attempt - 1))

        return self.initial_delay

    def _fibonacci(self, n: int) -> float:
        """Calculate nth Fibonacci number."""
        if n in self._fib_cache:
            return self._fib_cache[n]

        result = self._fibonacci(n - 1) + self._fibonacci(n - 2)
        self._fib_cache[n] = result
        return result

    def _apply_jitter(self, delay: float) -> float:
        """Apply jitter to delay."""
        jitter_range = delay * self.jitter
        return delay + random.uniform(-jitter_range, jitter_range)


class RetryScheduler:
    """
    Retry scheduler with configurable backoff.

    Example:
        >>> scheduler = RetryScheduler(RetryConfig(max_attempts=3))
        >>> result = await scheduler.execute(my_async_function, arg1, arg2)
    """

    def __init__(self, config: Optional[RetryConfig] = None) -> None:
        self.config = config or RetryConfig()
        self._backoff = BackoffCalculator(
            strategy=self.config.backoff_strategy,
            initial_delay=self.config.initial_delay,
            max_delay=self.config.max_delay,
            multiplier=self.config.multiplier,
            jitter=self.config.jitter,
        )
        self._attempt_history: list[RetryAttempt] = []

    async def execute(
        self,
        func: Callable[..., T],
        *args: Any,
        **kwargs: Any,
    ) -> T:
        """Execute function with retry logic."""
        if asyncio.iscoroutinefunction(func):
            return await self._execute_async(func, *args, **kwargs)
        return self._execute_sync(func, *args, **kwargs)

    async def _execute_async(self, func: Callable[..., T], *args: Any, **kwargs: Any) -> T:
        """Execute async function with retry."""
        attempts: list[RetryAttempt] = []
        start_time = time.time()
        last_exception: Optional[Exception] = None

        for attempt_num in range(1, self.config.max_attempts + 1):
            delay = self._backoff.calculate(attempt_num)
            attempt = RetryAttempt(
                attempt_number=attempt_num,
                timestamp=time.time(),
                delay=delay,
            )

            try:
                if attempt_num > 1 and delay > 0:
                    await asyncio.sleep(delay)

                timeout = self.config.timeout
                if timeout:
                    result = await asyncio.wait_for(
                        func(*args, **kwargs),
                        timeout=timeout,
                    )
                else:
                    result = await func(*args, **kwargs)

                attempt.success = True
                attempt.result = result
                attempts.append(attempt)

                total_duration = time.time() - start_time
                return RetryResult(
                    success=True,
                    result=result,
                    attempts=attempts,
                    total_duration=total_duration,
                )

            except asyncio.TimeoutError:
                last_exception = asyncio.TimeoutError(f"Attempt {attempt_num} timed out after {self.config.timeout}s")
                attempt.exception = last_exception
                attempts.append(attempt)
                logger.warning(f"Attempt {attempt_num} timed out")

            except self.config.retryable_exceptions as e:
                last_exception = e
                attempt.exception = e
                attempts.append(attempt)
                logger.warning(f"Attempt {attempt_num} failed: {e}")

            except Exception as e:
                last_exception = e
                attempt.exception = e
                attempts.append(attempt)
                logger.error(f"Attempt {attempt_num} failed with non-retryable error: {e}")
                break

        total_duration = time.time() - start_time
        return RetryResult(
            success=False,
            attempts=attempts,
            total_duration=total_duration,
            error=str(last_exception),
        )

    def _execute_sync(self, func: Callable[..., T], *args: Any, **kwargs: Any) -> T:
        """Execute sync function with retry."""
        attempts: list[RetryAttempt] = []
        start_time = time.time()
        last_exception: Optional[Exception] = None

        for attempt_num in range(1, self.config.max_attempts + 1):
            delay = self._backoff.calculate(attempt_num)
            attempt = RetryAttempt(
                attempt_number=attempt_num,
                timestamp=time.time(),
                delay=delay,
            )

            try:
                if attempt_num > 1 and delay > 0:
                    time.sleep(delay)

                result = func(*args, **kwargs)
                attempt.success = True
                attempt.result = result
                attempts.append(attempt)

                total_duration = time.time() - start_time
                return RetryResult(
                    success=True,
                    result=result,
                    attempts=attempts,
                    total_duration=total_duration,
                )

            except self.config.retryable_exceptions as e:
                last_exception = e
                attempt.exception = e
                attempts.append(attempt)
                logger.warning(f"Attempt {attempt_num} failed: {e}")

            except Exception as e:
                last_exception = e
                attempt.exception = e
                attempts.append(attempt)
                logger.error(f"Attempt {attempt_num} failed with non-retryable error: {e}")
                break

        total_duration = time.time() - start_time
        return RetryResult(
            success=False,
            attempts=attempts,
            total_duration=total_duration,
            error=str(last_exception),
        )

    def get_history(self) -> list[RetryAttempt]:
        """Get retry attempt history."""
        return self._attempt_history.copy()

    def clear_history(self) -> None:
        """Clear retry history."""
        self._attempt_history.clear()


class RetryDecorator:
    """
    Decorator for adding retry logic to functions.

    Example:
        >>> @RetryDecorator(RetryConfig(max_attempts=3))
        >>> async def my_function():
        ...     pass
    """

    def __init__(self, config: Optional[RetryConfig] = None) -> None:
        self.config = config or RetryConfig()

    def __call__(self, func: Callable[..., T]) -> Callable[..., T]:
        """Apply decorator to function."""
        scheduler = RetryScheduler(self.config)

        if asyncio.iscoroutinefunction(func):
            async def async_wrapper(*args: Any, **kwargs: Any) -> T:
                result = await scheduler.execute(func, *args, **kwargs)
                if not result.success:
                    raise RetryError(f"Retry failed: {result.error}")
                return result.result

            return async_wrapper

        def sync_wrapper(*args: Any, **kwargs: Any) -> T:
            result = scheduler.execute(func, *args, **kwargs)
            if asyncio.iscoroutinefunction(func):
                return result
            if not result.success:
                raise RetryError(f"Retry failed: {result.error}")
            return result.result

        return sync_wrapper


class RetryError(Exception):
    """Retry operation error."""
    pass


class RetryPolicy:
    """
    Configurable retry policies.

    Example:
        >>> policy = RetryPolicy.quick_retry()
        >>> scheduler = RetryScheduler(policy.config)
    """

    @staticmethod
    def quick_retry() -> RetryScheduler:
        """Quick retry policy for transient failures."""
        return RetryScheduler(RetryConfig(
            max_attempts=3,
            initial_delay=0.5,
            max_delay=5.0,
            backoff_strategy=BackoffStrategy.EXPONENTIAL,
            multiplier=2.0,
        ))

    @staticmethod
    def aggressive_retry() -> RetryScheduler:
        """Aggressive retry policy for critical operations."""
        return RetryScheduler(RetryConfig(
            max_attempts=5,
            initial_delay=0.1,
            max_delay=10.0,
            backoff_strategy=BackoffStrategy.LINEAR,
            multiplier=0.5,
        ))

    @staticmethod
    def gentle_retry() -> RetryScheduler:
        """Gentle retry policy for external services."""
        return RetryScheduler(RetryConfig(
            max_attempts=3,
            initial_delay=2.0,
            max_delay=120.0,
            backoff_strategy=BackoffStrategy.EXPONENTIAL_WITH_JITTER,
            multiplier=3.0,
            jitter=0.2,
        ))

    @staticmethod
    def forever_retry() -> RetryScheduler:
        """Retry indefinitely with long delays."""
        return RetryScheduler(RetryConfig(
            max_attempts=1000,
            initial_delay=5.0,
            max_delay=300.0,
            backoff_strategy=BackoffStrategy.FIBONACCI,
            multiplier=1.5,
            jitter=0.05,
        ))
