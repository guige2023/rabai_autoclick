"""Resilience and fault-tolerance utilities.

Provides retry logic, circuit breakers, and
bulkhead patterns for robust automation.
"""

import random
import time
from dataclasses import dataclass
from typing import Any, Callable, Optional, TypeVar, Union


T = TypeVar("T")


@dataclass
class RetryResult:
    """Result of a retry operation."""
    success: bool
    attempts: int
    elapsed_seconds: float
    last_error: Optional[Exception] = None


class RetryStrategy:
    """Base retry strategy."""

    def get_delay(self, attempt: int, exception: Exception) -> float:
        """Get delay in seconds before next retry."""
        raise NotImplementedError


class FixedDelay(RetryStrategy):
    """Fixed delay between retries.

    Example:
        strategy = FixedDelay(seconds=2.0)
    """

    def __init__(self, seconds: float) -> None:
        self.seconds = seconds

    def get_delay(self, attempt: int, exception: Exception) -> float:
        return self.seconds


class ExponentialBackoff(RetryStrategy):
    """Exponential backoff delay.

    Example:
        strategy = ExponentialBackoff(base=1.0, max_delay=60.0, jitter=True)
    """

    def __init__(
        self,
        base: float = 1.0,
        multiplier: float = 2.0,
        max_delay: float = 60.0,
        jitter: bool = True,
    ) -> None:
        self.base = base
        self.multiplier = multiplier
        self.max_delay = max_delay
        self.jitter = jitter

    def get_delay(self, attempt: int, exception: Exception) -> float:
        delay = min(self.base * (self.multiplier ** attempt), self.max_delay)
        if self.jitter:
            delay = delay * (0.5 + random.random() * 0.5)
        return delay


class LinearBackoff(RetryStrategy):
    """Linear increasing delay.

    Example:
        strategy = LinearBackoff(base=1.0, increment=2.0, max_delay=30.0)
    """

    def __init__(
        self,
        base: float = 1.0,
        increment: float = 1.0,
        max_delay: float = 30.0,
    ) -> None:
        self.base = base
        self.increment = increment
        self.max_delay = max_delay

    def get_delay(self, attempt: int, exception: Exception) -> float:
        return min(self.base + (attempt * self.increment), self.max_delay)


class RetryContext:
    """Context for retryable operations.

    Example:
        ctx = RetryContext(max_attempts=3)
        result = ctx.run(risky_function)
    """

    def __init__(
        self,
        strategy: RetryStrategy,
        max_attempts: int = 3,
        retryable_exceptions: Optional[tuple] = None,
        on_retry: Optional[Callable[[int, Exception], None]] = None,
    ) -> None:
        self.strategy = strategy
        self.max_attempts = max_attempts
        self.retryable_exceptions = retryable_exceptions or (Exception,)
        self.on_retry = on_retry

    def run(
        self,
        func: Callable[[], T],
        *args: Any,
        **kwargs: Any,
    ) -> T:
        """Execute function with retries.

        Raises:
            Last exception if all retries fail.
        """
        last_error: Optional[Exception] = None

        for attempt in range(self.max_attempts):
            try:
                return func(*args, **kwargs)
            except self.retryable_exceptions as e:
                last_error = e
                if attempt < self.max_attempts - 1:
                    delay = self.strategy.get_delay(attempt, e)
                    if self.on_retry:
                        self.on_retry(attempt + 1, e)
                    time.sleep(delay)

        raise last_error


def retry(
    func: Callable[..., T],
    strategy: RetryStrategy,
    max_attempts: int = 3,
    retryable_exceptions: Optional[tuple] = None,
) -> Callable[..., T]:
    """Decorator to add retry logic to function.

    Example:
        @retry(strategy=ExponentialBackoff(), max_attempts=5)
        def call_api():
            return requests.get(url)
    """
    def wrapper(*args: Any, **kwargs: Any) -> T:
        ctx = RetryContext(
            strategy=strategy,
            max_attempts=max_attempts,
            retryable_exceptions=retryable_exceptions,
        )
        return ctx.run(func, *args, **kwargs)

    return wrapper


class CircuitState:
    """Circuit breaker states."""
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class CircuitBreaker:
    """Circuit breaker for fault tolerance.

    Example:
        cb = CircuitBreaker(failure_threshold=5, recovery_timeout=60)
        result = cb.call(risky_function)
    """

    def __init__(
        self,
        failure_threshold: int = 5,
        recovery_timeout: float = 60.0,
        half_open_attempts: int = 3,
    ) -> None:
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.half_open_attempts = half_open_attempts

        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._last_failure_time: Optional[float] = None
        self._half_open_successes = 0

    @property
    def state(self) -> str:
        """Get current circuit state."""
        if self._state == CircuitState.OPEN:
            if self._should_attempt_reset():
                self._state = CircuitState.HALF_OPEN
        return self._state

    def _should_attempt_reset(self) -> bool:
        """Check if should transition from OPEN to HALF_OPEN."""
        if self._last_failure_time is None:
            return True
        return (time.time() - self._last_failure_time) >= self.recovery_timeout

    def call(
        self,
        func: Callable[[], T],
        *args: Any,
        **kwargs: Any,
    ) -> T:
        """Execute function through circuit breaker.

        Raises:
            CircuitOpenError: If circuit is OPEN.
        """
        if self.state == CircuitState.OPEN:
            raise CircuitOpenError("Circuit breaker is OPEN")

        try:
            result = func(*args, **kwargs)
            self._on_success()
            return result
        except Exception as e:
            self._on_failure()
            raise

    def _on_success(self) -> None:
        """Handle successful call."""
        if self._state == CircuitState.HALF_OPEN:
            self._half_open_successes += 1
            if self._half_open_successes >= self.half_open_attempts:
                self._state = CircuitState.CLOSED
                self._failure_count = 0
                self._half_open_successes = 0
        else:
            self._failure_count = 0

    def _on_failure(self) -> None:
        """Handle failed call."""
        self._failure_count += 1
        self._last_failure_time = time.time()

        if self._state == CircuitState.HALF_OPEN:
            self._state = CircuitState.OPEN
            self._half_open_successes = 0
        elif self._failure_count >= self.failure_threshold:
            self._state = CircuitState.OPEN

    def reset(self) -> None:
        """Manually reset circuit breaker."""
        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._half_open_successes = 0
        self._last_failure_time = None

    def get_stats(self) -> dict:
        """Get circuit breaker statistics."""
        return {
            "state": self.state,
            "failure_count": self._failure_count,
            "last_failure_time": self._last_failure_time,
            "half_open_successes": self._half_open_successes,
        }


class CircuitOpenError(Exception):
    """Raised when circuit breaker is OPEN."""
    pass


class Bulkhead:
    """Bulkhead pattern for resource isolation.

    Example:
        bulkhead = Bulkhead(max_concurrent=5)
        with bulkhead:
            process_task()
    """

    def __init__(self, max_concurrent: int = 5) -> None:
        self.max_concurrent = max_concurrent
        self._semaphore: Any = None

    def __enter__(self) -> "Bulkhead":
        import threading
        if self._semaphore is None:
            self._semaphore = threading.Semaphore(self.max_concurrent)
        self._semaphore.acquire()
        return self

    def __exit__(self, *args: Any) -> None:
        if self._semaphore:
            self._semaphore.release()


class Timeout:
    """Execute function with timeout.

    Example:
        result = Timeout(5.0).run(long_running_function)
    """

    def __init__(self, seconds: float) -> None:
        self.seconds = seconds

    def run(self, func: Callable[[], T], *args: Any, **kwargs: Any) -> T:
        """Execute function with timeout.

        Raises:
            TimeoutError: If execution exceeds timeout.
        """
        import signal

        def timeout_handler(signum: Any, frame: Any) -> None:
            raise TimeoutError(f"Execution exceeded {self.seconds} seconds")

        old_handler = signal.signal(signal.SIGALRM, timeout_handler)
        signal.alarm(int(self.seconds))

        try:
            result = func(*args, **kwargs)
        finally:
            signal.alarm(0)
            signal.signal(signal.SIGALRM, old_handler)

        return result


class TimeoutError(Exception):
    """Raised when operation times out."""
    pass


def with_timeout(seconds: float) -> Callable:
    """Decorator to add timeout to function.

    Example:
        @with_timeout(10.0)
        def long_running_task():
            ...
    """
    def decorator(func: Callable) -> Callable:
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            return Timeout(seconds).run(func, *args, **kwargs)
        return wrapper
    return decorator
