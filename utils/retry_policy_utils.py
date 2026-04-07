"""Retry policy utilities: configurable retry with backoff, jitter, and error classification."""

from __future__ import annotations

import random
import time
from dataclasses import dataclass
from enum import Enum
from typing import Any, Callable

__all__ = [
    "RetryPolicy",
    "Backoff",
    "RetryResult",
    "retry_policy",
]


class Backoff(Enum):
    """Backoff calculation strategies."""

    CONSTANT = "constant"
    LINEAR = "linear"
    EXPONENTIAL = "exponential"
    FIBONACCI = "fibonacci"


@dataclass
class RetryPolicy:
    """Configurable retry policy."""

    max_attempts: int = 3
    base_delay: float = 1.0
    max_delay: float = 60.0
    backoff: Backoff = Backoff.EXPONENTIAL
    jitter: bool = True
    retriable: Callable[[Exception], bool] | None = None

    def calculate_delay(self, attempt: int) -> float:
        """Calculate delay for a given attempt number."""
        if self.backoff == Backoff.CONSTANT:
            delay = self.base_delay
        elif self.backoff == Backoff.LINEAR:
            delay = self.base_delay * attempt
        elif self.backoff == Backoff.EXPONENTIAL:
            delay = self.base_delay * (2 ** (attempt - 1))
        elif self.backoff == Backoff.FIBONACCI:
            a, b = 1, 1
            for _ in range(attempt - 1):
                a, b = b, a + b
            delay = self.base_delay * a
        else:
            delay = self.base_delay

        delay = min(delay, self.max_delay)

        if self.jitter:
            delay *= 0.5 + random.random() * 0.5

        return delay


@dataclass
class RetryResult:
    """Result of a retry operation."""

    success: bool
    attempts: int
    final_error: Exception | None = None
    total_time: float = 0.0

    @property
    def succeeded(self) -> bool:
        return self.success


def retry_policy(
    policy: RetryPolicy | None = None,
    func: Callable[[], Any] | None = None,
    on_retry: Callable[[Exception, int], None] | None = None,
) -> Callable[[Callable[[], Any]], Callable[[], Any]]:
    """Decorator to apply a retry policy to a function."""
    _policy = policy or RetryPolicy()

    def decorator(fn: Callable[[], Any]) -> Callable[[], Any]:
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            last_error: Exception | None = None
            start_time = time.time()

            for attempt in range(1, _policy.max_attempts + 1):
                try:
                    return fn(*args, **kwargs)
                except Exception as e:
                    last_error = e

                    if _policy.retriable and not _policy.retriable(e):
                        raise

                    if attempt >= _policy.max_attempts:
                        break

                    delay = _policy.calculate_delay(attempt)
                    if on_retry:
                        on_retry(e, attempt)
                    time.sleep(delay)

            raise last_error

        return wrapper

    return decorator if func is None else decorator(func)
