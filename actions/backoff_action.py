"""backoff_action module for rabai_autoclick.

Provides backoff strategies: exponential, linear, fibonacci,
jittered, and adaptive backoff for retry operations.
"""

from __future__ import annotations

import random
import threading
import time
from dataclasses import dataclass
from typing import Any, Callable, Optional

__all__ = [
    "Backoff",
    "ExponentialBackoff",
    "LinearBackoff",
    "FibonacciBackoff",
    "JitteredBackoff",
    "DecorrelatedJitterBackoff",
    "AdaptiveBackoff",
    "BackoffContext",
    "retry",
    "retry_with_backoff",
]


@dataclass
class BackoffContext:
    """Context passed to backoff callbacks."""
    attempt: int
    max_attempts: int
    elapsed: float
    error: Optional[Exception] = None


class Backoff:
    """Base backoff strategy."""

    def __init__(self, base: float = 1.0, max_delay: float = 60.0) -> None:
        self.base = base
        self.max_delay = max_delay

    def get_delay(self, attempt: int) -> float:
        """Get delay for given attempt number."""
        raise NotImplementedError

    def reset(self) -> None:
        """Reset backoff state."""
        pass


class ExponentialBackoff(Backoff):
    """Exponential backoff: delay = base * 2^attempt."""

    def __init__(
        self,
        base: float = 1.0,
        max_delay: float = 60.0,
        multiplier: float = 2.0,
    ) -> None:
        super().__init__(base, max_delay)
        self.multiplier = multiplier

    def get_delay(self, attempt: int) -> float:
        delay = self.base * (self.multiplier ** attempt)
        return min(delay, self.max_delay)


class LinearBackoff(Backoff):
    """Linear backoff: delay = base + attempt * step."""

    def __init__(
        self,
        base: float = 1.0,
        max_delay: float = 60.0,
        step: float = 1.0,
    ) -> None:
        super().__init__(base, max_delay)
        self.step = step

    def get_delay(self, attempt: int) -> float:
        delay = self.base + attempt * self.step
        return min(delay, self.max_delay)


class FibonacciBackoff(Backoff):
    """Fibonacci backoff using fibonacci sequence."""

    def __init__(
        self,
        base: float = 1.0,
        max_delay: float = 60.0,
    ) -> None:
        super().__init__(base, max_delay)
        self._fib_cache: list = [0, 1]

    def _fib(self, n: int) -> int:
        """Get nth fibonacci number."""
        while len(self._fib_cache) <= n:
            self._fib_cache.append(self._fib_cache[-1] + self._fib_cache[-2])
        return self._fib_cache[n]

    def get_delay(self, attempt: int) -> float:
        delay = self.base * self._fib(attempt + 1)
        return min(delay, self.max_delay)


class JitteredBackoff(Backoff):
    """Exponential backoff with random jitter."""

    def __init__(
        self,
        base: float = 1.0,
        max_delay: float = 60.0,
        multiplier: float = 2.0,
        jitter: float = 1.0,
    ) -> None:
        super().__init__(base, max_delay)
        self.multiplier = multiplier
        self.jitter = jitter

    def get_delay(self, attempt: int) -> float:
        exp_delay = self.base * (self.multiplier ** attempt)
        capped = min(exp_delay, self.max_delay)
        if self.jitter > 0:
            capped *= (1.0 + random.uniform(-self.jitter, self.jitter))
        return max(0, capped)


class DecorrelatedJitterBackoff(Backoff):
    """Decorrelated jitter backoff (AWS style)."""

    def __init__(
        self,
        base: float = 1.0,
        max_delay: float = 60.0,
    ) -> None:
        super().__init__(base, max_delay)
        self._prev_delay = base

    def get_delay(self, attempt: int) -> float:
        delay = min(self._prev_delay * 3, self.max_delay)
        if attempt > 0:
            delay = random.uniform(self.base, delay)
        self._prev_delay = delay
        return delay


class AdaptiveBackoff(Backoff):
    """Adaptive backoff that adjusts based on success/failure."""

    def __init__(
        self,
        base: float = 1.0,
        max_delay: float = 60.0,
        success_factor: float = 0.5,
        failure_factor: float = 2.0,
    ) -> None:
        super().__init__(base, max_delay)
        self.success_factor = success_factor
        self.failure_factor = failure_factor
        self._current_delay = base
        self._lock = threading.Lock()

    def get_delay(self, attempt: int) -> float:
        with self._lock:
            return min(self._current_delay, self.max_delay)

    def record_success(self) -> None:
        """Decrease delay after success."""
        with self._lock:
            self._current_delay = max(
                self.base,
                self._current_delay * self.success_factor,
            )

    def record_failure(self) -> None:
        """Increase delay after failure."""
        with self._lock:
            self._current_delay = min(
                self.max_delay,
                self._current_delay * self.failure_factor,
            )

    def reset(self) -> None:
        """Reset to base delay."""
        with self._lock:
            self._current_delay = self.base


def retry(
    func: Callable,
    attempts: int = 3,
    backoff: Optional[Backoff] = None,
    exceptions: tuple = (Exception,),
    on_retry: Optional[Callable[[Exception, int], None]] = None,
) -> Any:
    """Retry function with backoff.

    Args:
        func: Function to retry.
        attempts: Maximum number of attempts.
        backoff: Backoff strategy.
        exceptions: Tuple of exceptions to catch.
        on_retry: Callback on each retry.

    Returns:
        Result of function.

    Raises:
        Last exception if all attempts fail.
    """
    if backoff is None:
        backoff = ExponentialBackoff()

    last_error: Optional[Exception] = None
    for attempt in range(attempts):
        try:
            return func()
        except exceptions as e:
            last_error = e
            if on_retry:
                on_retry(e, attempt)
            if attempt < attempts - 1:
                delay = backoff.get_delay(attempt)
                time.sleep(delay)
    if last_error:
        raise last_error


def retry_with_backoff(
    func: Callable,
    max_attempts: int = 5,
    base_delay: float = 1.0,
    backoff_type: str = "exponential",
    exceptions: tuple = (Exception,),
) -> Any:
    """Convenience function for retry with common backoff types.

    Args:
        func: Function to retry.
        max_attempts: Maximum attempts.
        base_delay: Base delay in seconds.
        backoff_type: "exponential", "linear", "fibonacci", "jittered".
        exceptions: Exceptions to catch.

    Returns:
        Function result.
    """
    if backoff_type == "exponential":
        backoff = ExponentialBackoff(base=base_delay)
    elif backoff_type == "linear":
        backoff = LinearBackoff(base=base_delay)
    elif backoff_type == "fibonacci":
        backoff = FibonacciBackoff(base=base_delay)
    elif backoff_type == "jittered":
        backoff = JitteredBackoff(base=base_delay)
    else:
        backoff = ExponentialBackoff(base=base_delay)

    return retry(func, attempts=max_attempts, backoff=backoff, exceptions=exceptions)
