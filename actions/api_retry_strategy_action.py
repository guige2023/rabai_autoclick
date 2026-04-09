"""API Retry Strategy Action module.

Provides configurable retry strategies with exponential backoff,
jitter, and condition-based retry logic for API calls.
"""

from __future__ import annotations

import asyncio
import random
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Optional, TypeVar

T = TypeVar("T")


class RetryStrategy(Enum):
    """Retry strategy types."""

    EXPONENTIAL = "exponential"
    LINEAR = "linear"
    FIXED = "fixed"
    FIBONACCI = "fibonacci"


@dataclass
class RetryConfig:
    """Configuration for retry behavior."""

    max_attempts: int = 3
    base_delay: float = 1.0
    max_delay: float = 60.0
    exponential_base: float = 2.0
    jitter: bool = True
    jitter_factor: float = 0.5
    retry_on: tuple = (Exception,)
    should_retry: Optional[Callable[[Exception], bool]] = None
    strategy: RetryStrategy = RetryStrategy.EXPONENTIAL


@dataclass
class RetryStats:
    """Statistics for retry operations."""

    total_attempts: int = 0
    successful_attempts: int = 0
    failed_attempts: int = 0
    total_retry_delay: float = 0.0
    max_attempts_used: int = 0

    def record_success(self, attempts: int, delay: float) -> None:
        """Record successful attempt."""
        self.total_attempts += attempts
        self.successful_attempts += 1
        self.total_retry_delay += delay
        self.max_attempts_used = max(self.max_attempts_used, attempts)

    def record_failure(self, attempts: int, delay: float) -> None:
        """Record failed attempt."""
        self.total_attempts += attempts
        self.failed_attempts += 1
        self.total_retry_delay += delay
        self.max_attempts_used = max(self.max_attempts_used, attempts)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "total_attempts": self.total_attempts,
            "successful_attempts": self.successful_attempts,
            "failed_attempts": self.failed_attempts,
            "total_retry_delay": self.total_retry_delay,
            "max_attempts_used": self.max_attempts_used,
            "success_rate": (
                self.successful_attempts / self.total_attempts
                if self.total_attempts > 0
                else 0
            ),
        }


def calculate_delay(
    attempt: int,
    config: RetryConfig,
) -> float:
    """Calculate delay for given attempt.

    Args:
        attempt: Attempt number (0-indexed)
        config: Retry configuration

    Returns:
        Delay in seconds
    """
    if config.strategy == RetryStrategy.EXPONENTIAL:
        delay = config.base_delay * (config.exponential_base**attempt)
    elif config.strategy == RetryStrategy.LINEAR:
        delay = config.base_delay * (attempt + 1)
    elif config.strategy == RetryStrategy.FIBONACCI:
        fib = [1, 1]
        for i in range(2, attempt + 2):
            fib.append(fib[-1] + fib[-2])
        delay = config.base_delay * fib[min(attempt, len(fib) - 1)]
    else:
        delay = config.base_delay

    delay = min(delay, config.max_delay)

    if config.jitter:
        jitter_range = delay * config.jitter_factor
        delay = delay + random.uniform(-jitter_range, jitter_range)
        delay = max(0, delay)

    return delay


async def retry_async(
    func: Callable[..., Any],
    config: Optional[RetryConfig] = None,
    stats: Optional[RetryStats] = None,
    *args: Any,
    **kwargs: Any,
) -> Any:
    """Retry an async function with backoff.

    Args:
        func: Async function to retry
        config: Retry configuration
        stats: Optional statistics tracker
        *args: Positional arguments
        **kwargs: Keyword arguments

    Returns:
        Function result

    Raises:
        Last exception if all retries exhausted
    """
    config = config or RetryConfig()
    last_exception: Optional[Exception] = None
    total_delay = 0.0

    for attempt in range(config.max_attempts):
        try:
            result = await func(*args, **kwargs)
            if stats and attempt > 0:
                stats.record_success(attempt + 1, total_delay)
            elif stats:
                stats.total_attempts += 1
                stats.successful_attempts += 1
            return result

        except Exception as e:
            last_exception = e

            should_retry = False

            if config.should_retry:
                should_retry = config.should_retry(e)
            else:
                should_retry = isinstance(e, config.retry_on)

            if not should_retry or attempt >= config.max_attempts - 1:
                if stats:
                    stats.record_failure(attempt + 1, total_delay)
                raise e

            delay = calculate_delay(attempt, config)
            total_delay += delay

            await asyncio.sleep(delay)

    if stats:
        stats.record_failure(config.max_attempts, total_delay)

    if last_exception:
        raise last_exception
    raise RuntimeError("Retry exhausted with no exception")


def retry_sync(
    func: Callable[..., Any],
    config: Optional[RetryConfig] = None,
    stats: Optional[RetryStats] = None,
    *args: Any,
    **kwargs: Any,
) -> Any:
    """Retry a sync function with backoff.

    Args:
        func: Function to retry
        config: Retry configuration
        stats: Optional statistics tracker
        *args: Positional arguments
        **kwargs: Keyword arguments

    Returns:
        Function result

    Raises:
        Last exception if all retries exhausted
    """
    config = config or RetryConfig()
    last_exception: Optional[Exception] = None
    total_delay = 0.0

    for attempt in range(config.max_attempts):
        try:
            result = func(*args, **kwargs)
            if stats and attempt > 0:
                stats.record_success(attempt + 1, total_delay)
            elif stats:
                stats.total_attempts += 1
                stats.successful_attempts += 1
            return result

        except Exception as e:
            last_exception = e

            should_retry = False

            if config.should_retry:
                should_retry = config.should_retry(e)
            else:
                should_retry = isinstance(e, config.retry_on)

            if not should_retry or attempt >= config.max_attempts - 1:
                if stats:
                    stats.record_failure(attempt + 1, total_delay)
                raise e

            delay = calculate_delay(attempt, config)
            total_delay += delay

            time.sleep(delay)

    if stats:
        stats.record_failure(config.max_attempts, total_delay)

    if last_exception:
        raise last_exception
    raise RuntimeError("Retry exhausted with no exception")


class RetryContext:
    """Context manager for retry operations."""

    def __init__(
        self,
        config: Optional[RetryConfig] = None,
        track_stats: bool = True,
    ):
        self.config = config or RetryConfig()
        self.stats = RetryStats() if track_stats else None
        self._last_result: Any = None

    async def execute_async(
        self,
        func: Callable[..., Any],
        *args: Any,
        **kwargs: Any,
    ) -> Any:
        """Execute async function with retry."""
        return await retry_async(func, self.config, self.stats, *args, **kwargs)

    def execute_sync(
        self,
        func: Callable[..., Any],
        *args: Any,
        **kwargs: Any,
    ) -> Any:
        """Execute sync function with retry."""
        return retry_sync(func, self.config, self.stats, *args, **kwargs)


class ConditionalRetry:
    """Retry with custom conditions."""

    def __init__(
        self,
        max_attempts: int = 3,
        base_delay: float = 1.0,
        max_delay: float = 60.0,
    ):
        self.max_attempts = max_attempts
        self.base_delay = base_delay
        self.max_delay = max_delay
        self._conditions: list[tuple[Callable[[Exception], bool], float]] = []

    def add_condition(
        self,
        condition: Callable[[Exception], bool],
        delay_multiplier: float = 1.0,
    ) -> "ConditionalRetry":
        """Add a retry condition.

        Args:
            condition: Function that returns True if should retry
            delay_multiplier: Multiply delay by this

        Returns:
            Self
        """
        self._conditions.append((condition, delay_multiplier))
        return self

    def when_status(self, *status_codes: int) -> "ConditionalRetry":
        """Retry on specific HTTP status codes.

        Args:
            *status_codes: Status codes that trigger retry

        Returns:
            Self
        """
        def condition(e: Exception) -> bool:
            if hasattr(e, "response") and hasattr(e.response, "status_code"):
                return e.response.status_code in status_codes
            return False

        return self.add_condition(condition)

    def when_timeout(self) -> "ConditionalRetry":
        """Retry on timeout errors.

        Returns:
            Self
        """
        def condition(e: Exception) -> bool:
            return isinstance(e, asyncio.TimeoutError) or "timeout" in str(e).lower()

        return self.add_condition(condition)

    def when_rate_limit(self) -> "ConditionalRetry":
        """Retry on rate limit errors.

        Returns:
            Self
        """
        def condition(e: Exception) -> bool:
            msg = str(e).lower()
            return "429" in msg or "rate limit" in msg or "too many requests" in msg

        return self.add_condition(condition)

    def get_delay(self, attempt: int, exception: Exception) -> float:
        """Get delay for attempt considering conditions."""
        multiplier = 1.0
        for condition, cond_multiplier in self._conditions:
            if condition(exception):
                multiplier = cond_multiplier
                break

        delay = self.base_delay * (2**attempt)
        delay = min(delay, self.max_delay)
        delay *= multiplier

        return delay
