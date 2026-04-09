"""Automation retry handler with backoff strategies."""

from __future__ import annotations

import asyncio
import random
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Coroutine, Optional


class BackoffStrategy(str, Enum):
    """Backoff strategy for retries."""

    FIXED = "fixed"
    LINEAR = "linear"
    EXPONENTIAL = "exponential"
    FIBONACCI = "fibonacci"
    EXPONENTIAL_WITH_JITTER = "exponential_with_jitter"
    FIBONACCI_WITH_JITTER = "fibonacci_with_jitter"


@dataclass
class RetryConfig:
    """Configuration for retry behavior."""

    max_attempts: int = 3
    initial_delay_seconds: float = 1.0
    max_delay_seconds: float = 60.0
    backoff_strategy: BackoffStrategy = BackoffStrategy.EXPONENTIAL
    base: float = 2.0
    jitter: bool = True
    jitter_factor: float = 0.1
    retryable_errors: Optional[list[str]] = None
    non_retryable_errors: Optional[list[str]] = None
    on_retry: Optional[Callable[[int, Exception], None]] = None


@dataclass
class RetryAttempt:
    """Record of a retry attempt."""

    attempt_number: int
    started_at: datetime
    completed_at: Optional[datetime] = None
    success: bool = False
    error: Optional[str] = None
    delay_used: float = 0


@dataclass
class RetryResult:
    """Result of retry operation."""

    success: bool
    total_attempts: int
    total_duration_seconds: float
    attempts: list[RetryAttempt]
    final_error: Optional[str] = None


class AutomationRetryHandlerAction:
    """Handles retry logic with configurable backoff."""

    def __init__(
        self,
        default_config: Optional[RetryConfig] = None,
    ):
        """Initialize retry handler.

        Args:
            default_config: Default retry configuration.
        """
        self._default_config = default_config or RetryConfig()
        self._retry_counters: dict[str, int] = {}

    def _calculate_delay(
        self,
        attempt: int,
        config: RetryConfig,
    ) -> float:
        """Calculate delay for given attempt."""
        if config.backoff_strategy == BackoffStrategy.FIXED:
            delay = config.initial_delay_seconds

        elif config.backoff_strategy == BackoffStrategy.LINEAR:
            delay = config.initial_delay_seconds * attempt

        elif config.backoff_strategy == BackoffStrategy.EXPONENTIAL:
            delay = config.initial_delay_seconds * (config.base ** (attempt - 1))

        elif config.backoff_strategy == BackoffStrategy.FIBONACCI:
            delay = self._fibonacci(attempt) * config.initial_delay_seconds

        elif config.backoff_strategy == BackoffStrategy.EXPONENTIAL_WITH_JITTER:
            base_delay = config.initial_delay_seconds * (config.base ** (attempt - 1))
            jitter = base_delay * config.jitter_factor * random.random()
            delay = base_delay + jitter

        elif config.backoff_strategy == BackoffStrategy.FIBONACCI_WITH_JITTER:
            base_delay = self._fibonacci(attempt) * config.initial_delay_seconds
            jitter = base_delay * config.jitter_factor * random.random()
            delay = base_delay + jitter

        else:
            delay = config.initial_delay_seconds

        return min(delay, config.max_delay_seconds)

    def _fibonacci(self, n: int) -> float:
        """Calculate nth Fibonacci number."""
        if n <= 1:
            return 1.0
        a, b = 1.0, 1.0
        for _ in range(n - 1):
            a, b = b, a + b
        return b

    def _is_retryable_error(self, error: Exception, config: RetryConfig) -> bool:
        """Check if error is retryable."""
        error_str = str(error)

        if config.non_retryable_errors:
            for pattern in config.non_retryable_errors:
                if pattern.lower() in error_str.lower():
                    return False

        if config.retryable_errors:
            for pattern in config.retryable_errors:
                if pattern.lower() in error_str.lower():
                    return True
            return False

        return True

    async def execute(
        self,
        func: Callable[..., Coroutine[Any, Any, Any]],
        *args: Any,
        config: Optional[RetryConfig] = None,
        **kwargs: Any,
    ) -> RetryResult:
        """Execute function with retry logic.

        Args:
            func: Async function to execute.
            *args: Positional arguments.
            config: Retry configuration.
            **kwargs: Keyword arguments.

        Returns:
            RetryResult with execution details.
        """
        config = config or self._default_config
        attempts: list[RetryAttempt] = []
        start_time = datetime.now()
        total_attempts = 0

        for attempt_num in range(1, config.max_attempts + 1):
            attempt = RetryAttempt(attempt_number=attempt_num, started_at=datetime.now())

            try:
                result = await func(*args, **kwargs)
                attempt.completed_at = datetime.now()
                attempt.success = True
                attempts.append(attempt)

                total_duration = (datetime.now() - start_time).total_seconds()
                return RetryResult(
                    success=True,
                    total_attempts=len(attempts),
                    total_duration_seconds=total_duration,
                    attempts=attempts,
                )

            except Exception as e:
                attempt.completed_at = datetime.now()
                attempt.error = str(e)
                attempt.success = False
                attempts.append(attempt)
                total_attempts = attempt_num

                if not self._is_retryable_error(e, config):
                    break

                if attempt_num < config.max_attempts:
                    delay = self._calculate_delay(attempt_num, config)
                    attempt.delay_used = delay

                    if config.on_retry:
                        config.on_retry(attempt_num, e)

                    await asyncio.sleep(delay)

        total_duration = (datetime.now() - start_time).total_seconds()
        final_error = attempts[-1].error if attempts else "Unknown error"

        return RetryResult(
            success=False,
            total_attempts=len(attempts),
            total_duration_seconds=total_duration,
            attempts=attempts,
            final_error=final_error,
        )

    def get_retry_count(self, key: str) -> int:
        """Get retry count for a key."""
        return self._retry_counters.get(key, 0)

    def increment_retry_count(self, key: str) -> int:
        """Increment and return retry count."""
        count = self._retry_counters.get(key, 0) + 1
        self._retry_counters[key] = count
        return count

    def reset_retry_count(self, key: str) -> None:
        """Reset retry count for a key."""
        self._retry_counters.pop(key, None)
