"""
API Retry Action Module.

Provides configurable retry logic with exponential backoff, circuit breaker,
and jitter for resilient API calls.
"""

import asyncio
import random
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Optional, TypeVar

T = TypeVar("T")


class RetryStrategy(Enum):
    """Retry strategy types."""
    EXPONENTIAL = "exponential"
    LINEAR = "linear"
    FIBONACCI = "fibonacci"


class CircuitState(Enum):
    """Circuit breaker states."""
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


@dataclass
class RetryConfig:
    """Configuration for retry behavior."""
    max_attempts: int = 3
    initial_delay: float = 0.5
    max_delay: float = 60.0
    multiplier: float = 2.0
    jitter: bool = True
    jitter_range: float = 0.3
    strategy: RetryStrategy = RetryStrategy.EXPONENTIAL
    retriable_exceptions: tuple = (Exception,)
    non_retriable: tuple = ()


@dataclass
class CircuitBreakerConfig:
    """Configuration for circuit breaker."""
    failure_threshold: int = 5
    recovery_timeout: float = 60.0
    half_open_attempts: int = 3
    success_threshold: int = 2


class CircuitBreaker:
    """Circuit breaker to prevent cascading failures."""

    def __init__(self, config: Optional[CircuitBreakerConfig] = None):
        self.config = config or CircuitBreakerConfig()
        self.state = CircuitState.CLOSED
        self.failure_count = 0
        self.success_count = 0
        self.last_failure_time: Optional[float] = None
        self._lock = asyncio.Lock()

    async def can_execute(self) -> bool:
        """Check if execution is allowed."""
        async with self._lock:
            if self.state == CircuitState.CLOSED:
                return True
            if self.state == CircuitState.OPEN:
                if self._should_attempt_reset():
                    self.state = CircuitState.HALF_OPEN
                    return True
                return False
            return True

    def _should_attempt_reset(self) -> bool:
        """Check if enough time has passed to attempt reset."""
        if self.last_failure_time is None:
            return True
        return (time.time() - self.last_failure_time) >= self.config.recovery_timeout

    async def record_success(self) -> None:
        """Record successful execution."""
        async with self._lock:
            self.failure_count = 0
            if self.state == CircuitState.HALF_OPEN:
                self.success_count += 1
                if self.success_count >= self.config.half_open_attempts:
                    self.state = CircuitState.CLOSED
                    self.success_count = 0

    async def record_failure(self) -> None:
        """Record failed execution."""
        async with self._lock:
            self.failure_count += 1
            self.last_failure_time = time.time()
            if self.state == CircuitState.HALF_OPEN:
                self.state = CircuitState.OPEN
            elif self.failure_count >= self.config.failure_threshold:
                self.state = CircuitState.OPEN


def _calculate_delay(
    attempt: int,
    config: RetryConfig
) -> float:
    """Calculate delay for the given attempt."""
    if config.strategy == RetryStrategy.EXPONENTIAL:
        delay = config.initial_delay * (config.multiplier ** (attempt - 1))
    elif config.strategy == RetryStrategy.LINEAR:
        delay = config.initial_delay * attempt
    elif config.strategy == RetryStrategy.FIBONACCI:
        a, b = 1, 1
        for _ in range(attempt - 1):
            a, b = b, a + b
        delay = config.initial_delay * a
    else:
        delay = config.initial_delay

    delay = min(delay, config.max_delay)

    if config.jitter:
        jitter_range = delay * config.jitter_range
        delay += random.uniform(-jitter_range, jitter_range)

    return max(0, delay)


class APIRetryAction:
    """
    Retry action with exponential backoff and circuit breaker.

    Example:
        action = APIRetryAction(
            max_attempts=5,
            initial_delay=1.0,
            multiplier=2.0
        )
        result = await action.execute(
            lambda: api.call(),
            retriable_exceptions=(TimeoutError, ConnectionError)
        )
    """

    def __init__(
        self,
        config: Optional[RetryConfig] = None,
        circuit_config: Optional[CircuitBreakerConfig] = None
    ):
        self.config = config or RetryConfig()
        self.circuit_breaker = CircuitBreaker(circuit_config)

    async def execute(
        self,
        func: Callable[[], T],
        *args: Any,
        **kwargs: Any
    ) -> T:
        """Execute function with retry logic."""
        last_exception: Optional[Exception] = None

        for attempt in range(1, self.config.max_attempts + 1):
            try:
                if not await self.circuit_breaker.can_execute():
                    raise CircuitOpenError(
                        "Circuit breaker is open"
                    )

                result = func(*args, **kwargs)
                await self.circuit_breaker.record_success()
                return result

            except self.config.non_retriable as e:
                raise

            except self.config.retriable_exceptions as e:
                last_exception = e
                if attempt < self.config.max_attempts:
                    delay = _calculate_delay(attempt, self.config)
                    await asyncio.sleep(delay)
                await self.circuit_breaker.record_failure()

        raise MaxRetriesExceededError(
            f"Max retries ({self.config.max_attempts}) exceeded",
            last_exception
        )

    async def execute_async(
        self,
        func: Callable[..., T],
        *args: Any,
        **kwargs: Any
    ) -> T:
        """Execute async function with retry logic."""
        last_exception: Optional[Exception] = None

        for attempt in range(1, self.config.max_attempts + 1):
            try:
                if not await self.circuit_breaker.can_execute():
                    raise CircuitOpenError(
                        "Circuit breaker is open"
                    )

                result = await func(*args, **kwargs)
                await self.circuit_breaker.record_success()
                return result

            except self.config.non_retriable as e:
                raise

            except self.config.retriable_exceptions as e:
                last_exception = e
                if attempt < self.config.max_attempts:
                    delay = _calculate_delay(attempt, self.config)
                    await asyncio.sleep(delay)
                await self.circuit_breaker.record_failure()

        raise MaxRetriesExceededError(
            f"Max retries ({self.config.max_attempts}) exceeded",
            last_exception
        )


class CircuitOpenError(Exception):
    """Raised when circuit breaker is open."""
    pass


class MaxRetriesExceededError(Exception):
    """Raised when max retries are exceeded."""

    def __init__(self, message: str, last_exception: Optional[Exception] = None):
        super().__init__(message)
        self.last_exception = last_exception
