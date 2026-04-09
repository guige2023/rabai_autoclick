"""API retry utilities with exponential backoff and jitter.

Supports configurable retry policies, timeout handling, and circuit breaker.
"""

from __future__ import annotations

import asyncio
import functools
import logging
import random
import time
from dataclasses import dataclass, field
from typing import Any, Callable, TypeVar

logger = logging.getLogger(__name__)

T = TypeVar("T")


class RetryError(Exception):
    """Raised when all retry attempts are exhausted."""

    def __init__(self, message: str, attempts: int, last_exception: Exception | None = None) -> None:
        self.attempts = attempts
        self.last_exception = last_exception
        super().__init__(message)


class CircuitOpenError(Exception):
    """Raised when circuit breaker is open."""

    pass


@dataclass
class RetryConfig:
    """Configuration for retry behavior."""

    max_attempts: int = 3
    initial_delay: float = 1.0
    max_delay: float = 60.0
    multiplier: float = 2.0
    jitter: bool = True
    jitter_factor: float = 0.2
    timeout: float | None = None
    retry_on: tuple[type[Exception], ...] = (Exception,)
    retry_on_result: Callable[[Any], bool] | None = None


@dataclass
class CircuitBreakerConfig:
    """Configuration for circuit breaker."""

    failure_threshold: int = 5
    success_threshold: int = 2
    timeout: float = 60.0
    half_open_max_calls: int = 3


class CircuitState(Enum):
    """Circuit breaker states."""

    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


@dataclass
class CircuitBreaker:
    """Circuit breaker for fault tolerance.

    Attributes:
        failure_threshold: Failures before opening circuit.
        success_threshold: Successes in half-open before closing.
        timeout: Seconds before attempting half-open.
    """

    failure_threshold: int = 5
    success_threshold: int = 2
    timeout: float = 60.0
    half_open_max_calls: int = 3

    _state: CircuitState = field(default=CircuitState.CLOSED, init=False)
    _failure_count: int = field(default=0, init=False)
    _success_count: int = field(default=0, init=False)
    _last_failure_time: float = field(default=0.0, init=False)
    _half_open_calls: int = field(default=0, init=False)

    @property
    def state(self) -> CircuitState:
        """Get current circuit state."""
        if self._state == CircuitState.OPEN:
            if time.monotonic() - self._last_failure_time >= self.timeout:
                self._state = CircuitState.HALF_OPEN
                self._half_open_calls = 0
        return self._state

    def record_success(self) -> None:
        """Record a successful call."""
        if self._state == CircuitState.HALF_OPEN:
            self._success_count += 1
            if self._success_count >= self.success_threshold:
                self._state = CircuitState.CLOSED
                self._failure_count = 0
                self._success_count = 0
                logger.info("Circuit breaker closed")
        elif self._state == CircuitState.CLOSED:
            self._failure_count = 0

    def record_failure(self) -> None:
        """Record a failed call."""
        self._failure_count += 1
        self._last_failure_time = time.monotonic()

        if self._state == CircuitState.HALF_OPEN:
            self._state = CircuitState.OPEN
            logger.warning("Circuit breaker re-opened")
        elif self._failure_count >= self.failure_threshold:
            self._state = CircuitState.OPEN
            logger.warning("Circuit breaker opened after %d failures", self._failure_count)

    def can_execute(self) -> bool:
        """Check if execution is allowed."""
        if self.state == CircuitState.CLOSED:
            return True
        if self.state == CircuitState.HALF_OPEN:
            return self._half_open_calls < self.half_open_max_calls
        return False

    def on_execute(self) -> None:
        """Called when execution starts."""
        if self.state == CircuitState.HALF_OPEN:
            self._half_open_calls += 1

    def reset(self) -> None:
        """Reset circuit breaker to initial state."""
        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._success_count = 0
        self._half_open_calls = 0


class BackoffStrategy:
    """Exponential backoff with jitter calculation."""

    def __init__(
        self,
        initial_delay: float = 1.0,
        max_delay: float = 60.0,
        multiplier: float = 2.0,
        jitter_factor: float = 0.2,
    ) -> None:
        self.initial_delay = initial_delay
        self.max_delay = max_delay
        self.multiplier = multiplier
        self.jitter_factor = jitter_factor

    def calculate(self, attempt: int) -> float:
        """Calculate delay for given attempt number."""
        delay = min(self.initial_delay * (self.multiplier ** attempt), self.max_delay)
        jitter_range = delay * self.jitter_factor
        jitter = random.uniform(-jitter_range, jitter_range)
        return max(0, delay + jitter)


async def retry_async(
    func: Callable[..., T],
    config: RetryConfig | None = None,
    *args,
    **kwargs,
) -> T:
    """Retry an async function with exponential backoff.

    Args:
        func: Async function to retry.
        config: Retry configuration.
        *args: Positional arguments for func.
        **kwargs: Keyword arguments for func.

    Returns:
        Result of func.

    Raises:
        RetryError: When all attempts are exhausted.
    """
    config = config or RetryConfig()
    backoff = BackoffStrategy(
        initial_delay=config.initial_delay,
        max_delay=config.max_delay,
        multiplier=config.multiplier,
        jitter_factor=config.jitter_factor,
    )

    last_error: Exception | None = None

    for attempt in range(config.max_attempts):
        try:
            if config.timeout:
                result = await asyncio.wait_for(func(*args, **kwargs), timeout=config.timeout)
            else:
                result = await func(*args, **kwargs)

            if config.retry_on_result and config.retry_on_result(result):
                if attempt < config.max_attempts - 1:
                    delay = backoff.calculate(attempt)
                    logger.warning("Result triggered retry, attempt %d/%d", attempt + 1, config.max_attempts)
                    await asyncio.sleep(delay)
                    continue

            return result

        except config.retry_on as e:
            last_error = e
            if attempt < config.max_attempts - 1:
                delay = backoff.calculate(attempt)
                logger.warning("Retry attempt %d/%d after %.2fs: %s", attempt + 1, config.max_attempts, delay, e)
                await asyncio.sleep(delay)
            else:
                logger.error("All retry attempts exhausted: %s", e)

    raise RetryError(f"All {config.max_attempts} attempts failed", config.max_attempts, last_error)


def retry_sync(
    func: Callable[..., T],
    config: RetryConfig | None = None,
    *args,
    **kwargs,
) -> T:
    """Retry a sync function with exponential backoff.

    Args:
        func: Synchronous function to retry.
        config: Retry configuration.
        *args: Positional arguments for func.
        **kwargs: Keyword arguments for func.

    Returns:
        Result of func.

    Raises:
        RetryError: When all attempts are exhausted.
    """
    config = config or RetryConfig()
    backoff = BackoffStrategy(
        initial_delay=config.initial_delay,
        max_delay=config.max_delay,
        multiplier=config.multiplier,
        jitter_factor=config.jitter_factor,
    )

    last_error: Exception | None = None

    for attempt in range(config.max_attempts):
        try:
            start = time.monotonic()
            result = func(*args, **kwargs)
            elapsed = time.monotonic() - start

            if config.timeout and elapsed > config.timeout:
                raise TimeoutError(f"Function took {elapsed:.2f}s, exceeded timeout {config.timeout}s")

            if config.retry_on_result and config.retry_on_result(result):
                if attempt < config.max_attempts - 1:
                    delay = backoff.calculate(attempt)
                    logger.warning("Result triggered retry, attempt %d/%d", attempt + 1, config.max_attempts)
                    time.sleep(delay)
                    continue

            return result

        except config.retry_on as e:
            last_error = e
            if attempt < config.max_attempts - 1:
                delay = backoff.calculate(attempt)
                logger.warning("Retry attempt %d/%d after %.2fs: %s", attempt + 1, config.max_attempts, delay, e)
                time.sleep(delay)
            else:
                logger.error("All retry attempts exhausted: %s", e)

    raise RetryError(f"All {config.max_attempts} attempts failed", config.max_attempts, last_error)


def with_retry(config: RetryConfig | None = None):
    """Decorator to add retry logic to a function."""

    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        if asyncio.iscoroutinefunction(func):

            @functools.wraps(func)
            async def async_wrapper(*args: Any, **kwargs: Any) -> T:
                return await retry_async(func, config, *args, **kwargs)

            return async_wrapper
        else:

            @functools.wraps(func)
            def sync_wrapper(*args: Any, **kwargs: Any) -> T:
                return retry_sync(func, config, *args, **kwargs)

            return sync_wrapper

    return decorator


def with_circuit_breaker(circuit_breaker: CircuitBreaker):
    """Decorator to add circuit breaker to a function."""

    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        if asyncio.iscoroutinefunction(func):

            @functools.wraps(func)
            async def async_wrapper(*args: Any, **kwargs: Any) -> T:
                if not circuit_breaker.can_execute():
                    raise CircuitOpenError("Circuit breaker is open")

                circuit_breaker.on_execute()
                try:
                    result = await func(*args, **kwargs)
                    circuit_breaker.record_success()
                    return result
                except Exception as e:
                    circuit_breaker.record_failure()
                    raise

            return async_wrapper
        else:

            @functools.wraps(func)
            def sync_wrapper(*args: Any, **kwargs: Any) -> T:
                if not circuit_breaker.can_execute():
                    raise CircuitOpenError("Circuit breaker is open")

                circuit_breaker.on_execute()
                try:
                    result = func(*args, **kwargs)
                    circuit_breaker.record_success()
                    return result
                except Exception as e:
                    circuit_breaker.record_failure()
                    raise

            return sync_wrapper

    return decorator


@dataclass
class RetryContext:
    """Context object tracking retry state."""

    attempt: int = 0
    total_delay: float = 0.0
    errors: list[Exception] = field(default_factory=list)

    def record_attempt(self, error: Exception | None = None) -> None:
        """Record an attempt."""
        self.attempt += 1
        if error:
            self.errors.append(error)


async def retry_with_context(
    func: Callable[..., T],
    config: RetryConfig | None = None,
) -> tuple[T, RetryContext]:
    """Retry with detailed context tracking.

    Returns:
        Tuple of (result, context).
    """
    config = config or RetryConfig()
    ctx = RetryContext()
    backoff = BackoffStrategy(
        initial_delay=config.initial_delay,
        max_delay=config.max_delay,
        multiplier=config.multiplier,
        jitter_factor=config.jitter_factor,
    )

    for attempt in range(config.max_attempts):
        ctx.attempt = attempt + 1
        try:
            if config.timeout:
                result = await asyncio.wait_for(func(), timeout=config.timeout)
            else:
                result = await func()

            if config.retry_on_result and config.retry_on_result(result):
                if attempt < config.max_attempts - 1:
                    delay = backoff.calculate(attempt)
                    ctx.total_delay += delay
                    await asyncio.sleep(delay)
                    continue

            return result, ctx

        except config.retry_on as e:
            ctx.errors.append(e)
            if attempt < config.max_attempts - 1:
                delay = backoff.calculate(attempt)
                ctx.total_delay += delay
                logger.warning("Retry %d/%d after %.2fs", attempt + 1, config.max_attempts, delay)
                await asyncio.sleep(delay)

    raise RetryError(f"All {config.max_attempts} attempts failed", config.max_attempts, ctx.errors[-1] if ctx.errors else None)
