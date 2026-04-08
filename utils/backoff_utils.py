"""
Exponential backoff utilities with jitter.

Provides configurable backoff strategies for retry logic.
"""

from __future__ import annotations

import random
import time
from typing import Callable, Literal


class BackoffStrategy:
    """Base backoff strategy."""

    def get_delay(self, attempt: int) -> float:
        raise NotImplementedError


class ExponentialBackoff(BackoffStrategy):
    """Exponential backoff: base * 2^attempt."""

    def __init__(
        self,
        base: float = 1.0,
        max_delay: float = 60.0,
        multiplier: float = 2.0,
    ):
        self.base = base
        self.max_delay = max_delay
        self.multiplier = multiplier

    def get_delay(self, attempt: int) -> float:
        delay = self.base * (self.multiplier ** attempt)
        return min(delay, self.max_delay)


class ExponentialBackoffWithJitter(BackoffStrategy):
    """Exponential backoff with optional jitter."""

    def __init__(
        self,
        base: float = 1.0,
        max_delay: float = 60.0,
        multiplier: float = 2.0,
        jitter: float = 0.0,
    ):
        self.base = base
        self.max_delay = max_delay
        self.multiplier = multiplier
        self.jitter = jitter

    def get_delay(self, attempt: int) -> float:
        delay = self.base * (self.multiplier ** attempt)
        delay = min(delay, self.max_delay)
        if self.jitter > 0:
            jitter_range = delay * self.jitter
            delay += random.uniform(-jitter_range, jitter_range)
        return max(0, delay)


class LinearBackoff(BackoffStrategy):
    """Linear backoff: base + (attempt * step)."""

    def __init__(
        self,
        base: float = 1.0,
        step: float = 1.0,
        max_delay: float = 60.0,
    ):
        self.base = base
        self.step = step
        self.max_delay = max_delay

    def get_delay(self, attempt: int) -> float:
        return min(self.base + attempt * self.step, self.max_delay)


class ConstantBackoff(BackoffStrategy):
    """Constant delay backoff."""

    def __init__(self, delay: float = 1.0):
        self.delay = delay

    def get_delay(self, attempt: int) -> float:
        return self.delay


class FibonacciBackoff(BackoffStrategy):
    """Fibonacci backoff: base * fib(attempt)."""

    def __init__(
        self,
        base: float = 1.0,
        max_delay: float = 60.0,
    ):
        self.base = base
        self.max_delay = max_delay
        self._fib_cache: dict[int, float] = {0: 0, 1: 1}

    def _fib(self, n: int) -> float:
        if n in self._fib_cache:
            return self._fib_cache[n]
        result = self._fib(n - 1) + self._fib(n - 2)
        self._fib_cache[n] = result
        return result

    def get_delay(self, attempt: int) -> float:
        return min(self.base * self._fib(attempt + 1), self.max_delay)


def retry_with_backoff(
    func: Callable[[], Any],
    strategy: BackoffStrategy | None = None,
    max_attempts: int = 5,
    exceptions: tuple = (Exception,),
) -> Any:
    """
    Retry a function with backoff strategy.

    Args:
        func: Function to retry
        strategy: Backoff strategy (defaults to exponential)
        max_attempts: Maximum retry attempts
        exceptions: Exceptions to catch and retry

    Returns:
        Function result

    Raises:
        Last exception if all retries fail
    """
    if strategy is None:
        strategy = ExponentialBackoff()
    last_exception: Exception | None = None
    for attempt in range(max_attempts):
        try:
            return func()
        except exceptions as e:
            last_exception = e
            if attempt < max_attempts - 1:
                delay = strategy.get_delay(attempt)
                time.sleep(delay)
    if last_exception:
        raise last_exception


def get_backoff_json() -> str:
    """Return JSON config for common backoff strategies."""
    return """{
  "exponential": {
    "type": "exponential",
    "base": 1.0,
    "multiplier": 2.0,
    "max_delay": 60.0
  },
  "exponential_jitter": {
    "type": "exponential_jitter",
    "base": 1.0,
    "multiplier": 2.0,
    "max_delay": 60.0,
    "jitter": 0.2
  },
  "linear": {
    "type": "linear",
    "base": 1.0,
    "step": 1.0,
    "max_delay": 30.0
  }
}"""


def create_strategy(
    strategy_type: Literal["exponential", "exponential_jitter", "linear", "constant", "fibonacci"],
    **kwargs,
) -> BackoffStrategy:
    """Factory to create backoff strategy."""
    strategies = {
        "exponential": ExponentialBackoff,
        "exponential_jitter": ExponentialBackoffWithJitter,
        "linear": LinearBackoff,
        "constant": ConstantBackoff,
        "fibonacci": FibonacciBackoff,
    }
    cls = strategies.get(strategy_type, ExponentialBackoff)
    return cls(**kwargs)
