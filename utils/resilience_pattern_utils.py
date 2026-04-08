"""
Resilience pattern utilities.

Provides bulkhead, failover, and fallback patterns
for building resilient systems.
"""

from __future__ import annotations

import random
import threading
import time
from typing import Callable, TypeVar


T = TypeVar("T")


class Bulkhead:
    """
    Bulkhead pattern - isolate failures in separate thread pools.

    Limits concurrent executions to prevent resource exhaustion.
    """

    def __init__(self, max_concurrent: int = 10, max_queue: int = 0):
        self.max_concurrent = max_concurrent
        self.max_queue = max_queue
        self._semaphore = threading.Semaphore(max_concurrent)
        self._active = 0
        self._lock = threading.Lock()

    def execute(self, func: Callable[[], T], timeout: float | None = None) -> T:
        """
        Execute function through bulkhead.

        Args:
            func: Function to execute
            timeout: Max wait time for semaphore

        Returns:
            Function result

        Raises:
            BulkheadRejectedError: If rejected or timeout
        """
        acquired = self._semaphore.acquire(timeout=timeout if timeout else 0.1)
        if not acquired:
            raise BulkheadRejectedError("Bulkhead capacity exceeded")

        with self._lock:
            self._active += 1

        try:
            return func()
        finally:
            with self._lock:
                self._active -= 1
            self._semaphore.release()

    @property
    def available(self) -> int:
        """Available concurrent slots."""
        return self.max_concurrent - self._active


class BulkheadRejectedError(Exception):
    """Raised when bulkhead rejects execution."""
    pass


class Failover:
    """
    Failover pattern - try alternatives on failure.

    Tries multiple endpoints/functions in order until one succeeds.
    """

    def __init__(
        self,
        providers: list[Callable[[], T]],
        retry_timeout: float = 5.0,
    ):
        self.providers = providers
        self.retry_timeout = retry_timeout

    def call(self, *args: object, **kwargs: object) -> T:
        """
        Call providers in order until success.

        Args:
            *args: Arguments to pass to providers
            **kwargs: Keyword arguments to pass to providers

        Returns:
            First successful result

        Raises:
            Last exception if all providers fail
        """
        last_error: Exception | None = None
        deadline = time.time() + self.retry_timeout

        for provider in self.providers:
            if time.time() >= deadline:
                break
            try:
                return provider(*args, **kwargs)
            except Exception as e:
                last_error = e

        if last_error:
            raise last_error
        raise RuntimeError("All failover providers failed")


class Fallback:
    """
    Fallback pattern - return alternative on failure.

    Tries primary, falls back to alternative on failure.
    """

    def __init__(
        self,
        primary: Callable[[], T],
        fallback_value: T | None = None,
        fallback_func: Callable[[], T] | None = None,
    ):
        self.primary = primary
        self.fallback_value = fallback_value
        self.fallback_func = fallback_func

    def call(self) -> T | None:
        """
        Execute primary, fallback on failure.

        Returns:
            Primary result or fallback value/result
        """
        try:
            return self.primary()
        except Exception:
            if self.fallback_func:
                return self.fallback_func()
            return self.fallback_value


class CircuitBreaker2:
    """
    Simple circuit breaker implementation.

    Closes circuit after repeated failures, half-opens to test recovery.
    """

    def __init__(
        self,
        failure_threshold: int = 5,
        recovery_timeout: float = 30.0,
    ):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self._failures = 0
        self._last_failure_time: float | None = None
        self._state = "closed"
        self._lock = threading.Lock()

    def call(self, func: Callable[[], T]) -> T:
        """Execute through circuit breaker."""
        with self._lock:
            if self._state == "open":
                if self._should_attempt_reset():
                    self._state = "half_open"
                else:
                    raise CircuitOpenError("Circuit is open")

        try:
            result = func()
            self._on_success()
            return result
        except Exception as e:
            self._on_failure()
            raise e

    def _should_attempt_reset(self) -> bool:
        if self._last_failure_time is None:
            return True
        return time.time() - self._last_failure_time >= self.recovery_timeout

    def _on_success(self) -> None:
        with self._lock:
            self._failures = 0
            self._state = "closed"

    def _on_failure(self) -> None:
        with self._lock:
            self._failures += 1
            self._last_failure_time = time.time()
            if self._failures >= self.failure_threshold:
                self._state = "open"

    @property
    def state(self) -> str:
        return self._state


class CircuitOpenError(Exception):
    """Raised when circuit is open."""
    pass


class RetryWithFallback:
    """Retry with exponential backoff and fallback."""

    def __init__(
        self,
        func: Callable[[], T],
        fallback: Callable[[], T] | T | None = None,
        max_attempts: int = 3,
        base_delay: float = 1.0,
    ):
        self.func = func
        self.fallback = fallback
        self.max_attempts = max_attempts
        self.base_delay = base_delay

    def execute(self) -> T | None:
        """Execute with retry and fallback."""
        last_error: Exception | None = None
        for attempt in range(self.max_attempts):
            try:
                return self.func()
            except Exception as e:
                last_error = e
                if attempt < self.max_attempts - 1:
                    delay = self.base_delay * (2 ** attempt)
                    time.sleep(delay)

        if callable(self.fallback):
            return self.fallback()
        return self.fallback
