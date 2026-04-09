"""
Automation Retry Action Module

Provides robust retry mechanisms for automation workflows
with configurable backoff strategies and error handling.

Author: RabAi Team
"""

from __future__ import annotations

import asyncio
import enum
import functools
import random
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Dict, List, Optional, TypeVar, Union

import logging

logger = logging.getLogger(__name__)

T = TypeVar("T")
FuncType = Callable[..., Awaitable[T]]


class RetryStrategy(enum.Enum):
    """Available retry backoff strategies."""

    FIXED = "fixed"
    LINEAR = "linear"
    EXPONENTIAL = "exponential"
    FIBONACCI = "fibonacci"
    EXPONENTIAL_WITH_JITTER = "exponential_with_jitter"
    FIBONACCI_WITH_JITTER = "fibonacci_with_jitter"


class RetryError(Exception):
    """Exception raised when all retry attempts are exhausted."""

    def __init__(self, message: str, attempts: int, last_error: Optional[Exception] = None) -> None:
        super().__init__(message)
        self.attempts = attempts
        self.last_error = last_error


@dataclass
class RetryConfig:
    """Configuration for retry behavior."""

    max_attempts: int = 3
    initial_delay: float = 1.0
    max_delay: float = 60.0
    strategy: RetryStrategy = RetryStrategy.EXPONENTIAL_WITH_JITTER
    retryable_exceptions: tuple = (Exception,)
    non_retryable_exceptions: tuple = ()
    on_retry: Optional[Callable[[int, Exception], None]] = None
    jitter_factor: float = 0.1


@dataclass
class RetryAttempt:
    """Record of a single retry attempt."""

    attempt_number: int
    timestamp: float
    delay_used: float
    success: bool
    error: Optional[str] = None
    duration_ms: float = 0.0


@dataclass
class RetryContext:
    """Context tracking retry state across attempts."""

    retry_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    started_at: float = field(default_factory=time.time)
    attempts: List[RetryAttempt] = field(default_factory=list)
    total_delay: float = 0.0
    current_attempt: int = 0


class BackoffCalculator:
    """Calculate delays for different backoff strategies."""

    @staticmethod
    def calculate(
        attempt: int,
        config: RetryConfig,
    ) -> float:
        """
        Calculate delay for the given attempt number.

        Args:
            attempt: Current attempt number (0-indexed)
            config: Retry configuration

        Returns:
            Delay in seconds before next retry
        """
        base_delay = config.initial_delay

        if config.strategy == RetryStrategy.FIXED:
            delay = base_delay

        elif config.strategy == RetryStrategy.LINEAR:
            delay = base_delay * (attempt + 1)

        elif config.strategy == RetryStrategy.EXPONENTIAL:
            delay = base_delay * (2**attempt)

        elif config.strategy == RetryStrategy.FIBONACCI:
            delay = base_delay * BackoffCalculator._fibonacci(attempt + 1)

        elif config.strategy in (
            RetryStrategy.EXPONENTIAL_WITH_JITTER,
            RetryStrategy.FIBONACCI_WITH_JITTER,
        ):
            if config.strategy == RetryStrategy.EXPONENTIAL_WITH_JITTER:
                base = base_delay * (2**attempt)
            else:
                base = base_delay * BackoffCalculator._fibonacci(attempt + 1)

            jitter_range = base * config.jitter_factor
            jitter = random.uniform(-jitter_range, jitter_range)
            delay = max(0, base + jitter)

        else:
            delay = base_delay

        return min(delay, config.max_delay)

    @staticmethod
    def _fibonacci(n: int) -> int:
        """Calculate nth Fibonacci number."""
        if n <= 0:
            return 0
        if n == 1:
            return 1

        a, b = 0, 1
        for _ in range(2, n + 1):
            a, b = b, a + b
        return b


class RetryPolicy:
    """
    Policy-based retry with condition evaluation.

    Allows specifying which exceptions to retry and
    custom conditions for continuing retries.
    """

    def __init__(self, config: RetryConfig) -> None:
        self._config = config
        self._conditions: List[Callable[[int, Exception], bool]] = []

    def add_condition(
        self, condition: Callable[[int, Exception], bool]
    ) -> RetryPolicy:
        """
        Add a retry continuation condition.

        Args:
            condition: Function that receives attempt number and exception,
                      returns True to continue retry

        Returns:
            Self for chaining
        """
        self._conditions.append(condition)
        return self

    def should_retry(self, attempt: int, exception: Exception) -> bool:
        """
        Determine if the exception warrants a retry.

        Args:
            attempt: Current attempt number
            exception: The exception that occurred

        Returns:
            True if should retry, False otherwise
        """
        # Check non-retryable exceptions first
        if isinstance(exception, self._config.non_retryable_exceptions):
            return False

        # Check retryable exceptions
        if isinstance(exception, self._config.retryable_exceptions):
            # Check custom conditions
            for condition in self._conditions:
                if not condition(attempt, exception):
                    return False
            return True

        return False

    @property
    def config(self) -> RetryConfig:
        """Get retry configuration."""
        return self._config


class RetryExecutor:
    """
    Executes operations with automatic retry handling.

    Tracks retry state, calculates backoff delays,
    and invokes callbacks at appropriate points.
    """

    def __init__(
        self,
        policy: RetryPolicy,
    ) -> None:
        self._policy = policy
        self._context: Optional[RetryContext] = None

    async def execute(
        self,
        func: FuncType,
        *args: Any,
        **kwargs: Any,
    ) -> T:
        """
        Execute function with retry logic.

        Args:
            func: Async function to execute
            *args: Positional arguments for func
            **kwargs: Keyword arguments for func

        Returns:
            Result of successful function execution

        Raises:
            RetryError: When all retry attempts are exhausted
        """
        self._context = RetryContext()
        config = self._policy.config
        last_error: Optional[Exception] = None

        for attempt in range(config.max_attempts):
            self._context.current_attempt = attempt + 1
            start_time = time.time()

            try:
                result = await func(*args, **kwargs)
                duration_ms = (time.time() - start_time) * 1000

                self._context.attempts.append(
                    RetryAttempt(
                        attempt_number=attempt + 1,
                        timestamp=time.time(),
                        delay_used=0.0,
                        success=True,
                        duration_ms=duration_ms,
                    )
                )

                return result

            except Exception as e:
                duration_ms = (time.time() - start_time) * 1000
                last_error = e

                # Check if we should retry
                if attempt < config.max_attempts - 1 and self._policy.should_retry(
                    attempt, e
                ):
                    delay = BackoffCalculator.calculate(attempt, config)
                    self._context.total_delay += delay
                    self._context.attempts.append(
                        RetryAttempt(
                            attempt_number=attempt + 1,
                            timestamp=time.time(),
                            delay_used=delay,
                            success=False,
                            error=str(e),
                            duration_ms=duration_ms,
                        )
                    )

                    logger.warning(
                        f"Attempt {attempt + 1}/{config.max_attempts} failed: {e}. "
                        f"Retrying in {delay:.2f}s"
                    )

                    if config.on_retry:
                        config.on_retry(attempt + 1, e)

                    await asyncio.sleep(delay)
                else:
                    # No more retries
                    self._context.attempts.append(
                        RetryAttempt(
                            attempt_number=attempt + 1,
                            timestamp=time.time(),
                            delay_used=0.0,
                            success=False,
                            error=str(e),
                            duration_ms=duration_ms,
                        )
                    )
                    break

        # All attempts exhausted
        raise RetryError(
            f"All {config.max_attempts} attempts failed. Last error: {last_error}",
            attempts=config.max_attempts,
            last_error=last_error,
        )

    @property
    def context(self) -> Optional[RetryContext]:
        """Get retry execution context."""
        return self._context


def with_retry(
    config: Optional[RetryConfig] = None,
    strategy: RetryStrategy = RetryStrategy.EXPONENTIAL_WITH_JITTER,
    max_attempts: int = 3,
    **kwargs: Any,
) -> Callable[[FuncType], FuncType]:
    """
    Decorator to add retry behavior to async functions.

    Args:
        config: Optional RetryConfig instance
        strategy: Default retry strategy
        max_attempts: Maximum retry attempts
        **kwargs: Additional RetryConfig options

    Returns:
        Decorated function with retry behavior

    Example:
        @with_retry(max_attempts=5, strategy=RetryStrategy.EXPONENTIAL)
        async def fetch_data(url: str) -> dict:
            ...
    """
    if config is None:
        config = RetryConfig(
            strategy=strategy,
            max_attempts=max_attempts,
            **kwargs,
        )

    policy = RetryPolicy(config)
    executor = RetryExecutor(policy)

    def decorator(func: FuncType) -> FuncType:
        @functools.wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> T:
            return await executor.execute(func, *args, **kwargs)

        # Attach retry metadata to function
        wrapper.retry_policy = policy  # type: ignore
        wrapper.retry_config = config  # type: ignore

        return wrapper  # type: ignore

    return decorator


class CircuitBreaker:
    """
    Circuit breaker pattern to prevent cascading failures.

    States:
    - CLOSED: Normal operation, requests pass through
    - OPEN: Failures exceeded threshold, requests fail fast
    - HALF_OPEN: Testing if service recovered
    """

    class State(enum.Enum):
        CLOSED = "closed"
        OPEN = "open"
        HALF_OPEN = "half_open"

    def __init__(
        self,
        failure_threshold: int = 5,
        recovery_timeout: float = 60.0,
        half_open_max_calls: int = 3,
    ) -> None:
        self._failure_threshold = failure_threshold
        self._recovery_timeout = recovery_timeout
        self._half_open_max_calls = half_open_max_calls

        self._state = CircuitBreaker.State.CLOSED
        self._failure_count = 0
        self._success_count = 0
        self._last_failure_time: Optional[float] = None
        self._half_open_calls = 0
        self._lock = asyncio.Lock()

    async def call(
        self,
        func: FuncType,
        *args: Any,
        **kwargs: Any,
    ) -> T:
        """
        Execute function through circuit breaker.

        Args:
            func: Function to execute
            *args: Positional arguments
            **kwargs: Keyword arguments

        Returns:
            Function result

        Raises:
            RetryError: Circuit is open
        """
        async with self._lock:
            if self._state == CircuitBreaker.State.OPEN:
                if (
                    self._last_failure_time
                    and time.time() - self._last_failure_time >= self._recovery_timeout
                ):
                    self._state = CircuitBreaker.State.HALF_OPEN
                    self._half_open_calls = 0
                    logger.info("Circuit breaker entering HALF_OPEN state")
                else:
                    raise RetryError(
                        "Circuit breaker is OPEN",
                        attempts=0,
                    )

            if self._state == CircuitBreaker.State.HALF_OPEN:
                if self._half_open_calls >= self._half_open_max_calls:
                    raise RetryError(
                        "Circuit breaker HALF_OPEN max calls exceeded",
                        attempts=0,
                    )
                self._half_open_calls += 1

        try:
            result = await func(*args, **kwargs)
            await self._on_success()
            return result
        except Exception as e:
            await self._on_failure(e)
            raise

    async def _on_success(self) -> None:
        """Handle successful call."""
        async with self._lock:
            if self._state == CircuitBreaker.State.HALF_OPEN:
                self._success_count += 1
                if self._success_count >= self._half_open_max_calls:
                    self._state = CircuitBreaker.State.CLOSED
                    self._failure_count = 0
                    self._success_count = 0
                    logger.info("Circuit breaker CLOSED after recovery")
            else:
                self._failure_count = max(0, self._failure_count - 1)

    async def _on_failure(self, error: Exception) -> None:
        """Handle failed call."""
        async with self._lock:
            self._failure_count += 1
            self._last_failure_time = time.time()

            if self._state == CircuitBreaker.State.HALF_OPEN:
                self._state = CircuitBreaker.State.OPEN
                logger.warning("Circuit breaker OPEN after half_open failure")
            elif self._failure_count >= self._failure_threshold:
                self._state = CircuitBreaker.State.OPEN
                logger.warning(
                    f"Circuit breaker OPEN after {self._failure_count} failures"
                )

    @property
    def state(self) -> State:
        """Get current circuit breaker state."""
        return self._state

    @property
    def stats(self) -> Dict[str, Any]:
        """Get circuit breaker statistics."""
        return {
            "state": self._state.value,
            "failure_count": self._failure_count,
            "success_count": self._success_count,
            "last_failure_time": self._last_failure_time,
        }
