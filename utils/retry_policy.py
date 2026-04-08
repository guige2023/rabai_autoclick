"""Retry policy framework for UI automation.

Provides configurable retry policies with backoff strategies
for handling transient automation failures.
"""

from __future__ import annotations

import random
import time
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Callable, Optional


class BackoffType(Enum):
    """Backoff strategies for retries."""
    FIXED = auto()       # Same delay between retries
    LINEAR = auto()      # Delay increases linearly
    EXPONENTIAL = auto() # Delay doubles each time
    FIBONACCI = auto()   # Delay follows Fibonacci sequence
    RANDOM = auto()      # Random delay within a range


@dataclass
class RetryPolicy:
    """Configuration for retry behavior.

    Attributes:
        max_attempts: Maximum number of retry attempts.
        initial_delay: Initial delay in seconds.
        max_delay: Maximum delay cap in seconds.
        backoff: Backoff strategy type.
        jitter: Whether to add random jitter to delays.
        jitter_range: Jitter as fraction of delay (0.0-1.0).
        retryable_check: Callable to determine if an error is retryable.
        on_retry: Callback called before each retry attempt.
    """
    max_attempts: int = 3
    initial_delay: float = 1.0
    max_delay: float = 30.0
    backoff: BackoffType = BackoffType.EXPONENTIAL
    jitter: bool = True
    jitter_range: float = 0.2
    retryable_check: Optional[Callable[[Exception], bool]] = None
    on_retry: Optional[Callable[[int, Exception], None]] = None
    retryable_exceptions: tuple[type, ...] = (Exception,)


def default_retryable_check(exc: Exception) -> bool:
    """Default check: retry most exceptions except KeyboardInterrupt, SystemExit."""
    return not isinstance(exc, (KeyboardInterrupt, SystemExit))


STANDARD_POLICY = RetryPolicy(
    max_attempts=3,
    initial_delay=0.5,
    max_delay=10.0,
    backoff=BackoffType.EXPONENTIAL,
    jitter=True,
)

AGGRESSIVE_POLICY = RetryPolicy(
    max_attempts=5,
    initial_delay=0.1,
    max_delay=5.0,
    backoff=BackoffType.LINEAR,
    jitter=False,
)

CONSERVATIVE_POLICY = RetryPolicy(
    max_attempts=3,
    initial_delay=2.0,
    max_delay=60.0,
    backoff=BackoffType.EXPONENTIAL,
    jitter=True,
    jitter_range=0.3,
)


class RetryExecutor:
    """Executes operations with retry policy."""

    def __init__(self, policy: Optional[RetryPolicy] = None) -> None:
        """Initialize with a retry policy."""
        self._policy = policy or STANDARD_POLICY

    def execute(
        self,
        operation: Callable[[], Any],
    ) -> Any:
        """Execute an operation with retries.

        Returns the operation result if successful.

        Raises the last exception if all retries fail.
        """
        policy = self._policy
        last_exc: Optional[Exception] = None

        for attempt in range(policy.max_attempts):
            try:
                return operation()
            except Exception as e:
                last_exc = e

                if policy.retryable_check and not policy.retryable_check(e):
                    raise

                is_last_attempt = attempt >= policy.max_attempts - 1
                if is_last_attempt:
                    break

                delay = self._compute_delay(attempt)

                if policy.on_retry:
                    policy.on_retry(attempt + 1, e)

                time.sleep(delay)

        if last_exc:
            raise last_exc
        raise RuntimeError("RetryExecutor: unexpected state")

    def execute_result(
        self,
        operation: Callable[[], Any],
    ) -> tuple[bool, Any]:
        """Execute and return (success, result_or_exception)."""
        try:
            result = self.execute(operation)
            return (True, result)
        except Exception as e:
            return (False, e)

    def _compute_delay(self, attempt: int) -> float:
        """Compute delay for a given attempt number."""
        policy = self._policy
        base_delay = policy.initial_delay

        if policy.backoff == BackoffType.FIXED:
            delay = base_delay
        elif policy.backoff == BackoffType.LINEAR:
            delay = base_delay * (attempt + 1)
        elif policy.backoff == BackoffType.EXPONENTIAL:
            delay = base_delay * (2 ** attempt)
        elif policy.backoff == BackoffType.FIBONACCI:
            fib = self._fibonacci(attempt + 2)
            delay = base_delay * fib
        elif policy.backoff == BackoffType.RANDOM:
            delay = base_delay
        else:
            delay = base_delay

        delay = min(delay, policy.max_delay)

        if policy.jitter:
            jitter_amount = delay * policy.jitter_range
            delay += random.uniform(-jitter_amount, jitter_amount)

        return max(0.0, delay)

    def _fibonacci(self, n: int) -> int:
        """Return nth Fibonacci number."""
        if n <= 1:
            return 1
        a, b = 1, 1
        for _ in range(n - 1):
            a, b = b, a + b
        return b


def retry(
    max_attempts: int = 3,
    initial_delay: float = 1.0,
    backoff: BackoffType = BackoffType.EXPONENTIAL,
) -> Callable[[Callable[[], Any]], Callable[[], Any]]:
    """Decorator to add retry behavior to a function."""
    def decorator(func: Callable[[], Any]) -> Callable[[], Any]:
        policy = RetryPolicy(
            max_attempts=max_attempts,
            initial_delay=initial_delay,
            backoff=backoff,
        )
        executor = RetryExecutor(policy)
        def wrapper() -> Any:
            return executor.execute(func)
        return wrapper
    return decorator
