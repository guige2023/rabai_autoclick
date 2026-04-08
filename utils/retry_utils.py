"""Retry utilities with backoff strategies for resilient automation.

Provides decorator-based retry with exponential backoff,
jitter, and conditional retry predicates for handling
transient failures in automation actions.

Example:
    >>> from utils.retry_utils import retry_with_backoff, ExponentialBackoff
    >>> @retry_with_backoff(max_attempts=3, backoff=ExponentialBackoff())
    ... def flaky_operation():
    ...     ...
"""

from __future__ import annotations

import random
import time
from typing import Callable, Optional, Tuple

__all__ = [
    "retry_with_backoff",
    "ExponentialBackoff",
    "LinearBackoff",
    "ConstantBackoff",
    "RetryExhausted",
]


class RetryExhausted(Exception):
    """Raised when all retry attempts have been exhausted."""
    pass


class BackoffStrategy:
    """Base class for backoff strategies."""

    def delay(self, attempt: int) -> float:
        """Return delay in seconds for a given attempt number."""
        raise NotImplementedError


class ExponentialBackoff(BackoffStrategy):
    """Exponential backoff: delay = base * factor^attempt."""

    def __init__(self, base: float = 1.0, factor: float = 2.0, max_delay: float = 60.0):
        self.base = base
        self.factor = factor
        self.max_delay = max_delay

    def delay(self, attempt: int) -> float:
        return min(self.base * (self.factor ** attempt), self.max_delay)


class LinearBackoff(BackoffStrategy):
    """Linear backoff: delay = base + attempt * step."""

    def __init__(self, base: float = 1.0, step: float = 1.0, max_delay: float = 30.0):
        self.base = base
        self.step = step
        self.max_delay = max_delay

    def delay(self, attempt: int) -> float:
        return min(self.base + attempt * self.step, self.max_delay)


class ConstantBackoff(BackoffStrategy):
    """Constant delay between retries."""

    def __init__(self, delay: float = 1.0):
        self.delay_value = delay

    def delay(self, attempt: int) -> float:
        return self.delay_value


def retry_with_backoff(
    func: Optional[Callable] = None,
    max_attempts: int = 3,
    backoff: Optional[BackoffStrategy] = None,
    exceptions: Tuple[type, ...] = (Exception,),
    jitter: float = 0.1,
    on_retry: Optional[Callable[[Exception, int], None]] = None,
) -> Callable:
    """Decorator for retrying functions with backoff.

    Args:
        max_attempts: Maximum number of attempts.
        backoff: Backoff strategy (defaults to ExponentialBackoff).
        exceptions: Tuple of exception types to catch.
        jitter: Random jitter fraction (0-1).
        on_retry: Optional callback on each retry (exception, attempt).

    Returns:
        Decorated function.

    Example:
        >>> @retry_with_backoff(max_attempts=5, backoff=ExponentialBackoff())
        ... def unreliable_call():
        ...     ...
    """
    if backoff is None:
        backoff = ExponentialBackoff()

    def decorator(fn: Callable) -> Callable:
        def wrapper(*args, **kwargs):
            last_error: Optional[Exception] = None

            for attempt in range(max_attempts):
                try:
                    return fn(*args, **kwargs)
                except exceptions as e:
                    last_error = e
                    if attempt == max_attempts - 1:
                        break

                    delay = backoff.delay(attempt)
                    if jitter > 0:
                        delay *= 1 + random.uniform(-jitter, jitter)

                    if on_retry:
                        try:
                            on_retry(e, attempt + 1)
                        except Exception:
                            pass

                    time.sleep(delay)

            if last_error:
                raise last_error

        return wrapper

    if func is not None:
        return decorator(func)
    return decorator


class RetryContext:
    """Stateful retry context for managing retries manually.

    Example:
        >>> ctx = RetryContext(max_attempts=3)
        >>> while ctx.attempt():
        ...     try:
        ...         do_action()
        ...         ctx.success()
        ...         break
        ...     except Exception as e:
        ...         ctx.failure(e)
    """

    def __init__(
        self,
        max_attempts: int = 3,
        backoff: Optional[BackoffStrategy] = None,
    ):
        self.max_attempts = max_attempts
        self.backoff = backoff or ExponentialBackoff()
        self.attempt_count = 0
        self._last_error: Optional[Exception] = None

    def attempt(self) -> bool:
        """Check if another attempt should be made.

        Returns:
            True if retries remain, False if exhausted.
        """
        return self.attempt_count < self.max_attempts

    def success(self) -> None:
        """Record a successful attempt."""
        self.attempt_count = 0
        self._last_error = None

    def failure(self, error: Exception) -> float:
        """Record a failed attempt and return the next delay.

        Returns:
            Delay in seconds before next retry.

        Raises:
            RetryExhausted: If all attempts are exhausted.
        """
        self.attempt_count += 1
        self._last_error = error

        if self.attempt_count >= self.max_attempts:
            raise RetryExhausted(f"Retry exhausted after {self.max_attempts} attempts: {error}")

        return self.backoff.delay(self.attempt_count - 1)

    @property
    def remaining_attempts(self) -> int:
        return max(0, self.max_attempts - self.attempt_count)
