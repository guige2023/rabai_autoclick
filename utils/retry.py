"""Retry utilities for RabAI AutoClick.

Provides decorators and utilities for:
- Exponential backoff retry
- Circuit breaker pattern
- Rate limiting
"""

import asyncio
import functools
import time
import threading
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Optional, Type, TypeVar, Union


T = TypeVar("T")


class RetryError(Exception):
    """Raised when all retry attempts fail."""

    def __init__(self, message: str, attempts: int, last_exception: Exception) -> None:
        self.attempts = attempts
        self.last_exception = last_exception
        super().__init__(message)


def retry(
    max_attempts: int = 3,
    delay: float = 1.0,
    backoff: float = 2.0,
    exceptions: Union[Type[Exception], tuple] = Exception,
    on_retry: Optional[Callable[[Exception, int], None]] = None,
) -> Callable[[Callable[..., T]], Callable[..., T]]:
    """Decorator to retry a function on failure with exponential backoff.

    Args:
        max_attempts: Maximum number of retry attempts.
        delay: Initial delay between retries in seconds.
        backoff: Multiplier for delay after each retry.
        exceptions: Exception types to catch and retry.
        on_retry: Optional callback called on each retry (exception, attempt).

    Returns:
        Decorated function.
    """
    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> T:
            current_delay = delay
            last_exc: Exception = Exception("No exception occurred")

            for attempt in range(1, max_attempts + 1):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    last_exc = e
                    if attempt == max_attempts:
                        raise RetryError(
                            f"Failed after {max_attempts} attempts: {e}",
                            attempts=max_attempts,
                            last_exception=e,
                        ) from e

                    if on_retry:
                        on_retry(e, attempt)

                    time.sleep(current_delay)
                    current_delay *= backoff

            raise last_exc

        return wrapper
    return decorator


def retry_async(
    max_attempts: int = 3,
    delay: float = 1.0,
    backoff: float = 2.0,
    exceptions: Union[Type[Exception], tuple] = Exception,
    on_retry: Optional[Callable[[Exception, int], None]] = None,
) -> Callable[[Callable[..., T]], Callable[..., T]]:
    """Async version of retry decorator.

    Args:
        max_attempts: Maximum number of retry attempts.
        delay: Initial delay between retries in seconds.
        backoff: Multiplier for delay after each retry.
        exceptions: Exception types to catch and retry.
        on_retry: Optional callback called on each retry.

    Returns:
        Decorated async function.
    """
    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @functools.wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> T:
            current_delay = delay
            last_exc: Exception = Exception("No exception occurred")

            for attempt in range(1, max_attempts + 1):
                try:
                    return await func(*args, **kwargs)
                except exceptions as e:
                    last_exc = e
                    if attempt == max_attempts:
                        raise RetryError(
                            f"Failed after {max_attempts} attempts: {e}",
                            attempts=max_attempts,
                            last_exception=e,
                        ) from e

                    if on_retry:
                        on_retry(e, attempt)

                    await asyncio.sleep(current_delay)
                    current_delay *= backoff

            raise last_exc

        return wrapper
    return decorator


class CircuitState(Enum):
    """Circuit breaker states."""
    CLOSED = "closed"      # Normal operation
    OPEN = "open"          # Failing, reject requests
    HALF_OPEN = "half_open"  # Testing if service recovered


@dataclass
class CircuitBreakerConfig:
    """Configuration for circuit breaker."""
    failure_threshold: int = 5
    recovery_timeout: float = 60.0
    half_open_max_calls: int = 3
    expected_exceptions: tuple = (Exception,)


@dataclass
class CircuitBreaker:
    """Circuit breaker to prevent cascading failures.

    Attributes:
        failure_threshold: Number of failures before opening circuit.
        recovery_timeout: Seconds before attempting recovery.
        half_open_max_calls: Max calls allowed in half-open state.
    """
    failure_threshold: int = 5
    recovery_timeout: float = 60.0
    half_open_max_calls: int = 3
    expected_exceptions: tuple = (Exception,)

    _state: CircuitState = field(default=CircuitState.CLOSED, init=False)
    _failure_count: int = field(default=0, init=False)
    _last_failure_time: float = field(default=0.0, init=False)
    _half_open_calls: int = field(default=0, init=False)
    _lock: threading.Lock = field(default_factory=threading.Lock, init=False)

    def __post_init__(self) -> None:
        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._last_failure_time = 0.0
        self._half_open_calls = 0
        self._lock = threading.Lock()

    @property
    def state(self) -> CircuitState:
        """Get current circuit state."""
        with self._lock:
            if self._state == CircuitState.OPEN:
                if time.time() - self._last_failure_time >= self.recovery_timeout:
                    self._state = CircuitState.HALF_OPEN
                    self._half_open_calls = 0
            return self._state

    def record_success(self) -> None:
        """Record a successful call."""
        with self._lock:
            if self._state == CircuitState.HALF_OPEN:
                self._half_open_calls += 1
                if self._half_open_calls >= self.half_open_max_calls:
                    self._state = CircuitState.CLOSED
                    self._failure_count = 0
            elif self._state == CircuitState.CLOSED:
                self._failure_count = max(0, self._failure_count - 1)

    def record_failure(self) -> None:
        """Record a failed call."""
        with self._lock:
            self._failure_count += 1
            self._last_failure_time = time.time()

            if self._state == CircuitState.HALF_OPEN:
                self._state = CircuitState.OPEN
            elif self._failure_count >= self.failure_threshold:
                self._state = CircuitState.OPEN

    def call(self, func: Callable[..., T], *args: Any, **kwargs: Any) -> T:
        """Execute a function through the circuit breaker.

        Args:
            func: Function to call.
            *args: Positional arguments for func.
            **kwargs: Keyword arguments for func.

        Returns:
            Result of func.

        Raises:
            RuntimeError: If circuit is open.
        """
        if self.state == CircuitState.OPEN:
            raise RuntimeError("Circuit breaker is OPEN")

        try:
            result = func(*args, **kwargs)
            self.record_success()
            return result
        except self.expected_exceptions as e:
            self.record_failure()
            raise e

    def reset(self) -> None:
        """Reset the circuit breaker to closed state."""
        with self._lock:
            self._state = CircuitState.CLOSED
            self._failure_count = 0
            self._half_open_calls = 0


class RateLimiter:
    """Token bucket rate limiter.

    Attributes:
        rate: Tokens per second.
        capacity: Maximum token capacity.
    """

    def __init__(self, rate: float, capacity: Optional[int] = None) -> None:
        """Initialize rate limiter.

        Args:
            rate: Tokens added per second.
            capacity: Maximum tokens (defaults to rate).
        """
        self.rate = rate
        self.capacity = capacity or int(rate)
        self._tokens = float(self.capacity)
        self._last_update = time.time()
        self._lock = threading.Lock()

    def acquire(self, tokens: int = 1, blocking: bool = True, timeout: Optional[float] = None) -> bool:
        """Acquire tokens from the bucket.

        Args:
            tokens: Number of tokens to acquire.
            blocking: If True, wait until tokens available.
            timeout: Maximum time to wait in seconds.

        Returns:
            True if tokens acquired, False if timeout.
        """
        start_time = time.time()

        while True:
            with self._lock:
                self._refill()

                if self._tokens >= tokens:
                    self._tokens -= tokens
                    return True

                if not blocking:
                    return False

                wait_time = (tokens - self._tokens) / self.rate
                if timeout is not None:
                    elapsed = time.time() - start_time
                    if elapsed + wait_time > timeout:
                        return False

            sleep_time = min(wait_time, 0.1)
            time.sleep(sleep_time)

            if timeout is not None:
                elapsed = time.time() - start_time
                if elapsed >= timeout:
                    return False

    def _refill(self) -> None:
        """Refill tokens based on elapsed time."""
        now = time.time()
        elapsed = now - self._last_update
        self._tokens = min(self.capacity, self._tokens + elapsed * self.rate)
        self._last_update = now

    @property
    def available_tokens(self) -> float:
        """Get current available tokens."""
        with self._lock:
            self._refill()
            return self._tokens


def rate_limit(rate: float, capacity: Optional[int] = None) -> Callable[[Callable[..., T]], Callable[..., T]]:
    """Decorator to rate limit a function.

    Args:
        rate: Calls per second.
        capacity: Maximum burst capacity.

    Returns:
        Decorated function.
    """
    limiter = RateLimiter(rate, capacity)

    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> T:
            if not limiter.acquire():
                raise RuntimeError(f"Rate limit exceeded for {func.__name__}")
            return func(*args, **kwargs)
        return wrapper
    return decorator