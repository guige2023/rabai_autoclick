"""
Automation Backoff Action Module.

Provides configurable retry mechanisms with exponential backoff,
jitter, and deadline support for resilient automation workflows.
"""

import asyncio
import random
import time
import threading
from typing import Optional, Callable, Any, List, Type, Union, TypeVar
from dataclasses import dataclass, field
from enum import Enum


class BackoffStrategy(Enum):
    """Backoff strategy types."""
    EXPONENTIAL = "exponential"
    LINEAR = "linear"
    FIXED = "fixed"
    FIBONACCI = "fibonacci"
    POLYNOMIAL = "polynomial"


class RetryError(Exception):
    """Exception raised when all retries are exhausted."""

    def __init__(self, message: str, attempts: int, last_error: Optional[Exception] = None):
        super().__init__(message)
        self.attempts = attempts
        self.last_error = last_error


@dataclass
class BackoffConfig:
    """Configuration for backoff retry behavior."""
    strategy: BackoffStrategy = BackoffStrategy.EXPONENTIAL
    initial_interval: float = 0.5  # seconds
    max_interval: float = 60.0  # seconds
    max_attempts: int = 3
    multiplier: float = 2.0
    jitter: float = 0.1  # fraction of interval to randomize
    deadline: Optional[float] = None  # max total time in seconds
    retryable_exceptions: tuple = (Exception,)


@dataclass
class RetryAttempt:
    """Record of a single retry attempt."""
    attempt_number: int
    start_time: float
    end_time: float
    duration: float
    success: bool
    error: Optional[str] = None
    backoff_used: float = 0.0


T = TypeVar("T")


class AutomationBackoffAction:
    """
    Retry action with configurable backoff strategies.

    Supports exponential, linear, fixed, fibonacci, and polynomial
    backoff strategies with optional jitter for distributed systems.
    """

    def __init__(self, config: Optional[BackoffConfig] = None):
        self.config = config or BackoffConfig()
        self._attempts: List[RetryAttempt] = []
        self._lock = threading.RLock()
        self._total_retries = 0
        self._total_successes = 0

    def _calculate_interval(self, attempt: int) -> float:
        """Calculate backoff interval for given attempt."""
        interval = self.config.initial_interval
        multiplier = self.config.multiplier

        if self.config.strategy == BackoffStrategy.EXPONENTIAL:
            interval = self.config.initial_interval * (multiplier ** attempt)

        elif self.config.strategy == BackoffStrategy.LINEAR:
            interval = self.config.initial_interval * (1 + attempt * multiplier)

        elif self.config.strategy == BackoffStrategy.FIXED:
            interval = self.config.initial_interval

        elif self.config.strategy == BackoffStrategy.FIBONACCI:
            a, b = 1, 1
            for _ in range(attempt):
                a, b = b, a + b
            interval = self.config.initial_interval * a

        elif self.config.strategy == BackoffStrategy.POLYNOMIAL:
            interval = self.config.initial_interval * ((attempt + 1) ** multiplier)

        # Cap at max interval
        interval = min(interval, self.config.max_interval)

        # Apply jitter
        if self.config.jitter > 0:
            jitter_amount = interval * self.config.jitter
            interval = interval + random.uniform(-jitter_amount, jitter_amount)

        return max(0, interval)

    def _is_retryable(self, exception: Exception) -> bool:
        """Check if exception is retryable."""
        return isinstance(exception, self.config.retryable_exceptions)

    def _check_deadline(self, elapsed: float) -> bool:
        """Check if deadline has been exceeded."""
        if self.config.deadline is not None:
            return elapsed >= self.config.deadline
        return False

    async def execute_async(
        self,
        func: Callable[..., Any],
        *args,
        **kwargs
    ) -> Any:
        """
        Execute function with retry and backoff.

        Args:
            func: Async function to execute
            *args: Positional arguments
            **kwargs: Keyword arguments

        Returns:
            Function result

        Raises:
            RetryError: If all retries exhausted
        """
        start_time = time.time()
        last_error: Optional[Exception] = None
        attempt = 0

        while True:
            attempt_start = time.time()

            try:
                if asyncio.iscoroutinefunction(func):
                    result = await func(*args, **kwargs)
                else:
                    result = func(*args, **kwargs)

                attempt_duration = time.time() - attempt_start
                self._attempts.append(RetryAttempt(
                    attempt_number=attempt + 1,
                    start_time=attempt_start,
                    end_time=time.time(),
                    duration=attempt_duration,
                    success=True,
                ))
                self._total_successes += 1
                return result

            except Exception as e:
                attempt_duration = time.time() - attempt_start
                elapsed = time.time() - start_time

                self._attempts.append(RetryAttempt(
                    attempt_number=attempt + 1,
                    start_time=attempt_start,
                    end_time=time.time(),
                    duration=attempt_duration,
                    success=False,
                    error=str(e),
                ))

                if not self._is_retryable(e):
                    raise RetryError(
                        f"Non-retryable exception: {e}",
                        attempts=attempt + 1,
                        last_error=e,
                    )

                # Check if we should retry
                if attempt >= self.config.max_attempts - 1:
                    raise RetryError(
                        f"All {self.config.max_attempts} retries exhausted",
                        attempts=attempt + 1,
                        last_error=e,
                    )

                if self._check_deadline(elapsed):
                    raise RetryError(
                        f"Deadline exceeded after {attempt + 1} attempts",
                        attempts=attempt + 1,
                        last_error=e,
                    )

                # Calculate backoff
                backoff = self._calculate_interval(attempt)
                self._attempts[-1].backoff_used = backoff

                # Wait before retry
                await asyncio.sleep(backoff)
                attempt += 1

    def execute(
        self,
        func: Callable[..., Any],
        *args,
        **kwargs
    ) -> Any:
        """Execute function with retry (sync version)."""
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                future = asyncio.run_coroutine_threadsafe(
                    self.execute_async(func, *args, **kwargs), loop
                )
                return future.result(
                    timeout=self.config.deadline + 60 if self.config.deadline else 60
                )
            return asyncio.run(self.execute_async(func, *args, **kwargs))
        except RetryError:
            raise
        except Exception as e:
            raise RetryError(str(e), attempts=1, last_error=e)

    def retry(
        self,
        func: Callable[..., T],
        *args,
        **kwargs
    ) -> T:
        """Decorator-style retry. Alias for execute."""
        return self.execute(func, *args, **kwargs)

    def get_attempts(self) -> List[RetryAttempt]:
        """Get all retry attempts."""
        with self._lock:
            return list(self._attempts)

    def get_stats(self) -> dict:
        """Get retry statistics."""
        with self._lock:
            successful = [a for a in self._attempts if a.success]
            failed = [a for a in self._attempts if not a.success]
            return {
                "total_attempts": len(self._attempts),
                "successful": len(successful),
                "failed": len(failed),
                "current_streak": len(successful) if successful else 0,
                "total_retries": self._total_retries,
                "total_successes": self._total_successes,
            }

    def reset(self) -> None:
        """Reset retry statistics."""
        with self._lock:
            self._attempts.clear()


class RetryContext:
    """Context manager for retry operations."""

    def __init__(
        self,
        config: Optional[BackoffConfig] = None,
    ):
        self.config = config or BackoffConfig()
        self.backoff = AutomationBackoffAction(self.config)
        self._entered = False

    async def __aenter__(self):
        self._entered = True
        return self.backoff

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        self._entered = False
        return False

    def __enter__(self):
        self._entered = True
        return self.backoff

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._entered = False
        return False
