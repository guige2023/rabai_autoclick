"""Auto-retry automation action module.

Provides automatic retry logic for any operation with configurable
backoff strategies, error categorization, and maximum attempt limits.
"""

from __future__ import annotations

import time
import random
import logging
from typing import Callable, TypeVar, Optional, Dict, Any
from dataclasses import dataclass
from enum import Enum

logger = logging.getLogger(__name__)

T = TypeVar("T")


class BackoffStrategy(Enum):
    """Supported backoff strategies."""
    FIXED = "fixed"
    LINEAR = "linear"
    EXPONENTIAL = "exponential"
    FIBONACCI = "fibonacci"
    JITTER = "jitter"
    FULL_JITTER = "full_jitter"


@dataclass
class RetryPolicy:
    """Configuration for retry behavior."""
    max_attempts: int = 3
    initial_delay: float = 1.0
    max_delay: float = 60.0
    multiplier: float = 2.0
    strategy: BackoffStrategy = BackoffStrategy.EXPONENTIAL
    retryable_exceptions: tuple = (Exception,)
    retryable_results: Optional[Callable[[Any], bool]] = None


class AutoRetryAction:
    """Automatic retry wrapper for any callable.

    Decorator-based retry with customizable backoff and error handling.

    Example:
        retry = AutoRetryAction(max_attempts=5, strategy=BackoffStrategy.EXPONENTIAL)

        @retry
        def fragile_operation():
            return may_fail()

        result = retry.execute(fragile_operation)
    """

    def __init__(
        self,
        max_attempts: int = 3,
        initial_delay: float = 1.0,
        max_delay: float = 60.0,
        multiplier: float = 2.0,
        strategy: BackoffStrategy = BackoffStrategy.EXPONENTIAL,
        retryable_exceptions: Optional[tuple] = None,
        retryable_results: Optional[Callable[[Any], bool]] = None,
    ) -> None:
        """Initialize auto-retry action.

        Args:
            max_attempts: Maximum number of attempts.
            initial_delay: Starting delay between retries (seconds).
            max_delay: Maximum delay cap (seconds).
            multiplier: Backoff multiplier.
            strategy: Backoff strategy.
            retryable_exceptions: Exceptions that trigger retry.
            retryable_results: Callable that returns True for retryable results.
        """
        self.policy = RetryPolicy(
            max_attempts=max_attempts,
            initial_delay=initial_delay,
            max_delay=max_delay,
            multiplier=multiplier,
            strategy=strategy,
            retryable_exceptions=retryable_exceptions or (Exception,),
            retryable_results=retryable_results,
        )

    def __call__(self, func: Callable[..., T]) -> Callable[..., T]:
        """Decorator syntax for auto-retry.

        Args:
            func: Function to wrap with retry logic.

        Returns:
            Wrapped function.
        """
        def wrapper(*args, **kwargs) -> T:
            return self.execute(func, *args, **kwargs)
        return wrapper

    def execute(
        self,
        func: Callable[..., T],
        *args,
        **kwargs,
    ) -> T:
        """Execute a function with automatic retry.

        Args:
            func: Callable to execute.
            *args: Positional arguments for func.
            **kwargs: Keyword arguments for func.

        Returns:
            Function result.

        Raises:
            The last exception if all attempts fail.
        """
        policy = self.policy
        last_exception: Optional[Exception] = None

        for attempt in range(1, policy.max_attempts + 1):
            try:
                result = func(*args, **kwargs)

                if policy.retryable_results and policy.retryable_results(result):
                    logger.debug("Result %s is retryable, attempt %d/%d", result, attempt, policy.max_attempts)
                    if attempt < policy.max_attempts:
                        delay = self._calculate_delay(attempt)
                        time.sleep(delay)
                    continue

                return result

            except policy.retryable_exceptions as e:
                last_exception = e
                logger.debug("Attempt %d/%d failed with %s: %s", attempt, policy.max_attempts, type(e).__name__, e)

                if attempt == policy.max_attempts:
                    break

                delay = self._calculate_delay(attempt)
                logger.debug("Retrying in %.2fs (attempt %d/%d)", delay, attempt + 1, policy.max_attempts)
                time.sleep(delay)

        if last_exception:
            raise last_exception

    def _calculate_delay(self, attempt: int) -> float:
        """Calculate delay in seconds for a given attempt."""
        policy = self.policy
        base_delay = min(
            policy.initial_delay * (policy.multiplier ** (attempt - 1)),
            policy.max_delay,
        )

        strategy = policy.strategy
        if strategy == BackoffStrategy.FIXED:
            return policy.initial_delay
        elif strategy == BackoffStrategy.LINEAR:
            return base_delay
        elif strategy == BackoffStrategy.EXPONENTIAL:
            return base_delay
        elif strategy == BackoffStrategy.FIBONACCI:
            fib = self._fibonacci(attempt)
            return min(fib * policy.initial_delay, policy.max_delay)
        elif strategy == BackoffStrategy.JITTER:
            jitter = random.uniform(0, base_delay * 0.1)
            return base_delay + jitter
        elif strategy == BackoffStrategy.FULL_JITTER:
            return random.uniform(0, base_delay)
        return base_delay

    def _fibonacci(self, n: int) -> int:
        """Calculate the nth Fibonacci number."""
        a, b = 1, 1
        for _ in range(n - 1):
            a, b = b, a + b
        return a


def retry(
    max_attempts: int = 3,
    initial_delay: float = 1.0,
    strategy: BackoffStrategy = BackoffStrategy.EXPONENTIAL,
) -> Callable[[Callable[..., T]], Callable[..., T]]:
    """Convenience decorator for auto-retry.

    Args:
        max_attempts: Maximum attempts.
        initial_delay: Starting delay.
        strategy: Backoff strategy.

    Returns:
        Decorator function.

    Example:
        @retry(max_attempts=5, initial_delay=0.5)
        def may_fail():
            return risky_call()
    """
    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        retry_action = AutoRetryAction(
            max_attempts=max_attempts,
            initial_delay=initial_delay,
            strategy=strategy,
        )
        def wrapper(*args, **kwargs) -> T:
            return retry_action.execute(func, *args, **kwargs)
        return wrapper
    return decorator
