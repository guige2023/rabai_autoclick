"""API retry logic with backoff strategies.

This module provides retry functionality:
- Multiple backoff strategies
- Configurable retry conditions
- Jitter support
- Circuit breaker integration

Example:
    >>> from actions.api_retry_action import RetryPolicy, with_retry
    >>> policy = RetryPolicy(max_attempts=3, backoff="exponential")
    >>> result = with_retry(api_call, policy)
"""

from __future__ import annotations

import time
import random
import logging
from typing import Any, Callable, Optional
from dataclasses import dataclass
from enum import Enum

logger = logging.getLogger(__name__)


class BackoffStrategy(Enum):
    """Backoff strategies."""
    FIXED = "fixed"
    LINEAR = "linear"
    EXPONENTIAL = "exponential"
    FIBONACCI = "fibonacci"


@dataclass
class RetryPolicy:
    """Configuration for retry behavior."""
    max_attempts: int = 3
    backoff: BackoffStrategy = BackoffStrategy.EXPONENTIAL
    base_delay: float = 1.0
    max_delay: float = 60.0
    jitter: bool = True
    jitter_factor: float = 0.1
    retry_on_timeout: bool = True
    retry_on_errors: tuple[type, ...] = (Exception,)


def calculate_backoff(
    attempt: int,
    policy: RetryPolicy,
) -> float:
    """Calculate delay for the given attempt.

    Args:
        attempt: Current attempt number (0-indexed).
        policy: Retry policy.

    Returns:
        Delay in seconds.
    """
    if policy.backoff == BackoffStrategy.FIXED:
        delay = policy.base_delay
    elif policy.backoff == BackoffStrategy.LINEAR:
        delay = policy.base_delay * attempt
    elif policy.backoff == BackoffStrategy.EXPONENTIAL:
        delay = policy.base_delay * (2 ** attempt)
    elif policy.backoff == BackoffStrategy.FIBONACCI:
        delay = policy.base_delay * _fibonacci(attempt)
    else:
        delay = policy.base_delay
    delay = min(delay, policy.max_delay)
    if policy.jitter:
        jitter_range = delay * policy.jitter_factor
        delay += random.uniform(-jitter_range, jitter_range)
    return max(0, delay)


def _fibonacci(n: int) -> int:
    """Calculate nth Fibonacci number."""
    if n <= 1:
        return 1
    a, b = 1, 1
    for _ in range(n - 1):
        a, b = b, a + b
    return b


def with_retry(
    func: Callable[..., Any],
    policy: Optional[RetryPolicy] = None,
    *args: Any,
    **kwargs: Any,
) -> Any:
    """Execute function with retry logic.

    Args:
        func: Function to execute.
        policy: Retry policy.
        *args: Positional arguments.
        **kwargs: Keyword arguments.

    Returns:
        Function result.

    Raises:
        Last exception if all retries fail.
    """
    policy = policy or RetryPolicy()
    last_error: Optional[Exception] = None
    for attempt in range(policy.max_attempts):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            last_error = e
            if isinstance(e, TimeoutError) and not policy.retry_on_timeout:
                raise
            if not isinstance(e, policy.retry_on_errors):
                raise
            if attempt < policy.max_attempts - 1:
                delay = calculate_backoff(attempt, policy)
                logger.warning(f"Retry {attempt + 1}/{policy.max_attempts} after {delay:.2f}s: {e}")
                time.sleep(delay)
    if last_error:
        raise last_error


class RetryContext:
    """Context for tracking retry state."""

    def __init__(self, policy: RetryPolicy) -> None:
        self.policy = policy
        self.attempt = 0
        self.start_time = time.time()
        self.errors: list[Exception] = []

    @property
    def elapsed(self) -> float:
        """Time elapsed since first attempt."""
        return time.time() - self.start_time

    def should_retry(self) -> bool:
        """Check if another attempt should be made."""
        return self.attempt < self.policy.max_attempts

    def record_error(self, error: Exception) -> None:
        """Record an error for this attempt."""
        self.errors.append(error)
        self.attempt += 1


def retry_with_context(
    func: Callable[..., Any],
    policy: Optional[RetryPolicy] = None,
    on_retry: Optional[Callable[[RetryContext], None]] = None,
    *args: Any,
    **kwargs: Any,
) -> Any:
    """Execute with retry and context tracking.

    Args:
        func: Function to execute.
        policy: Retry policy.
        on_retry: Optional callback called before each retry.
        *args: Positional arguments.
        **kwargs: Keyword arguments.

    Returns:
        Function result.
    """
    policy = policy or RetryPolicy()
    ctx = RetryContext(policy)
    while ctx.should_retry():
        try:
            return func(*args, **kwargs)
        except Exception as e:
            ctx.record_error(e)
            if not isinstance(e, policy.retry_on_errors):
                raise
            if ctx.should_retry() and on_retry:
                on_retry(ctx)
            if ctx.should_retry():
                delay = calculate_backoff(ctx.attempt - 1, policy)
                logger.warning(f"Retry {ctx.attempt}/{policy.max_attempts} after {delay:.2f}s")
                time.sleep(delay)
    raise ctx.errors[-1]


class AsyncRetry:
    """Async retry support."""

    async def with_retry(
        self,
        func: Callable[..., Any],
        policy: Optional[RetryPolicy] = None,
        *args: Any,
        **kwargs: Any,
    ) -> Any:
        """Execute async function with retry."""
        import asyncio
        policy = policy or RetryPolicy()
        last_error: Optional[Exception] = None
        for attempt in range(policy.max_attempts):
            try:
                return await func(*args, **kwargs)
            except Exception as e:
                last_error = e
                if not isinstance(e, policy.retry_on_errors):
                    raise
                if attempt < policy.max_attempts - 1:
                    delay = calculate_backoff(attempt, policy)
                    logger.warning(f"Async retry {attempt + 1}/{policy.max_attempts} after {delay:.2f}s")
                    await asyncio.sleep(delay)
        if last_error:
            raise last_error
