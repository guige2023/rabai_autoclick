"""Automation Retry and Backoff.

This module provides retry logic with backoff:
- Exponential backoff
- Jitter support
- Retry conditions
- Max attempt tracking

Example:
    >>> from actions.automation_retry_action import RetryHandler
    >>> handler = RetryHandler(max_attempts=3, backoff_base=2)
    >>> result = handler.execute_with_retry(failing_function)
"""

from __future__ import annotations

import time
import random
import logging
import threading
from typing import Any, Callable, Optional
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)


@dataclass
class RetryAttempt:
    """A single retry attempt."""
    attempt_number: int
    timestamp: float
    duration_ms: float
    success: bool
    error: Optional[str] = None


@dataclass
class RetryResult:
    """Result of a retry operation."""
    success: bool
    result: Any
    attempts: list[RetryAttempt]
    total_duration_ms: float
    final_error: Optional[str] = None


class RetryHandler:
    """Handles retry logic with backoff strategies."""

    def __init__(
        self,
        max_attempts: int = 3,
        backoff_base: float = 2.0,
        backoff_factor: float = 1.0,
        jitter: bool = True,
        max_backoff: float = 60.0,
    ) -> None:
        """Initialize the retry handler.

        Args:
            max_attempts: Maximum retry attempts.
            backoff_base: Base for exponential backoff.
            backoff_factor: Multiplier for backoff.
            jitter: Whether to add random jitter.
            max_backoff: Maximum backoff seconds.
        """
        self._max_attempts = max_attempts
        self._backoff_base = backoff_base
        self._backoff_factor = backoff_factor
        self._jitter = jitter
        self._max_backoff = max_backoff
        self._lock = threading.Lock()
        self._stats = {"retries": 0, "successes": 0, "failures": 0}

    def execute_with_retry(
        self,
        func: Callable[[], Any],
        should_retry: Optional[Callable[[Exception], bool]] = None,
        on_retry: Optional[Callable[[int, Exception], None]] = None,
    ) -> RetryResult:
        """Execute a function with retry logic.

        Args:
            func: Function to execute.
            should_retry: Function to determine if retry should happen. None = retry all.
            on_retry: Callback called before each retry.

        Returns:
            RetryResult with outcome and attempt history.
        """
        start_time = time.time()
        attempts = []
        last_error = None

        for attempt_num in range(1, self._max_attempts + 1):
            attempt_start = time.time()

            try:
                result = func()
                duration_ms = (time.time() - attempt_start) * 1000

                attempts.append(RetryAttempt(
                    attempt_number=attempt_num,
                    timestamp=attempt_start,
                    duration_ms=duration_ms,
                    success=True,
                ))

                with self._lock:
                    self._stats["successes"] += 1

                total_duration = (time.time() - start_time) * 1000
                return RetryResult(
                    success=True,
                    result=result,
                    attempts=attempts,
                    total_duration_ms=total_duration,
                )

            except Exception as e:
                duration_ms = (time.time() - attempt_start) * 1000
                last_error = str(e)

                attempts.append(RetryAttempt(
                    attempt_number=attempt_num,
                    timestamp=attempt_start,
                    duration_ms=duration_ms,
                    success=False,
                    error=last_error,
                ))

                if attempt_num >= self._max_attempts:
                    break

                if should_retry and not should_retry(e):
                    break

                backoff = self._calculate_backoff(attempt_num)

                if on_retry:
                    on_retry(attempt_num, e)

                logger.info("Retry %d/%d after %.1fs: %s", attempt_num, self._max_attempts, backoff, last_error)
                time.sleep(backoff)

        with self._lock:
            self._stats["retries"] += len(attempts) - 1
            self._stats["failures"] += 1

        total_duration = (time.time() - start_time) * 1000
        return RetryResult(
            success=False,
            result=None,
            attempts=attempts,
            total_duration_ms=total_duration,
            final_error=last_error,
        )

    def _calculate_backoff(self, attempt_num: int) -> float:
        """Calculate backoff delay.

        Args:
            attempt_num: Current attempt number.

        Returns:
            Backoff delay in seconds.
        """
        backoff = self._backoff_factor * (self._backoff_base ** (attempt_num - 1))
        backoff = min(backoff, self._max_backoff)

        if self._jitter:
            backoff = backoff * (0.5 + random.random())

        return backoff

    def retry_decorator(
        self,
        func: Optional[Callable] = None,
        max_attempts: Optional[int] = None,
        should_retry: Optional[Callable[[Exception], bool]] = None,
    ) -> Callable:
        """Decorator for adding retry logic to functions.

        Args:
            func: Function to decorate.
            max_attempts: Override max attempts.
            should_retry: Custom retry condition.

        Returns:
            Decorated function.
        """
        def decorator(fn: Callable) -> Callable:
            def wrapper(*args, **kwargs) -> Any:
                handler = RetryHandler(
                    max_attempts=max_attempts or self._max_attempts,
                    backoff_base=self._backoff_base,
                    backoff_factor=self._backoff_factor,
                    jitter=self._jitter,
                    max_backoff=self._max_backoff,
                )
                result = handler.execute_with_retry(
                    lambda: fn(*args, **kwargs),
                    should_retry=should_retry,
                )
                if not result.success:
                    raise result.final_error
                return result.result
            return wrapper

        if func:
            return decorator(func)
        return decorator

    def get_stats(self) -> dict[str, int]:
        """Get retry statistics."""
        with self._lock:
            return dict(self._stats)
