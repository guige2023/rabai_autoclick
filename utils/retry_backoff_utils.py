"""Retry and backoff strategy utilities.

Provides configurable retry mechanisms with various
backoff strategies for resilient operations.
"""

from __future__ import annotations

from typing import Callable, TypeVar, Generic, Optional, Tuple, Any, List
from dataclasses import dataclass, field
from enum import Enum, auto
import time
import random
import threading


T = TypeVar('T')


class BackoffType(Enum):
    """Backoff strategy types."""
    FIXED = auto()
    LINEAR = auto()
    EXPONENTIAL = auto()
    EXPONENTIAL_WITH_JITTER = auto()
    FIBONACCI = auto()
    POLYNOMIAL = auto()


@dataclass
class RetryConfig:
    """Configuration for retry behavior."""
    max_attempts: int = 3
    base_delay: float = 1.0
    max_delay: float = 60.0
    backoff_type: BackoffType = BackoffType.EXPONENTIAL
    jitter: float = 0.1
    retry_on: Tuple[type, ...] = (Exception,)
    timeout_seconds: Optional[float] = None


@dataclass
class RetryResult(Generic[T]):
    """Result of a retry operation."""
    success: bool
    value: Optional[T] = None
    error: Optional[Exception] = None
    attempts: int = 0
    total_duration: float = 0.0
    delays: List[float] = field(default_factory=list)

    @property
    def succeeded(self) -> bool:
        return self.success

    @property
    def failed(self) -> bool:
        return not self.success


class BackoffCalculator:
    """Calculates delay intervals for backoff strategies.

    Example:
        calc = BackoffCalculator(BackoffType.EXPONENTIAL, base_delay=1.0)
        delay = calc.get_delay(attempt=3)  # ~8 seconds
    """

    def __init__(
        self,
        backoff_type: BackoffType = BackoffType.EXPONENTIAL,
        base_delay: float = 1.0,
        max_delay: float = 60.0,
        jitter: float = 0.1,
    ) -> None:
        self.backoff_type = backoff_type
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.jitter = jitter

    def get_delay(self, attempt: int) -> float:
        """Get delay for given attempt number (1-indexed)."""
        if attempt < 1:
            attempt = 1
        raw = self._calculate_raw(attempt)
        clamped = min(raw, self.max_delay)
        return self._apply_jitter(clamped)

    def _calculate_raw(self, attempt: int) -> float:
        """Calculate raw delay without jitter."""
        if self.backoff_type == BackoffType.FIXED:
            return self.base_delay
        elif self.backoff_type == BackoffType.LINEAR:
            return self.base_delay * attempt
        elif self.backoff_type == BackoffType.EXPONENTIAL:
            return self.base_delay * (2 ** (attempt - 1))
        elif self.backoff_type == BackoffType.EXPONENTIAL_WITH_JITTER:
            return self.base_delay * (2 ** (attempt - 1))
        elif self.backoff_type == BackoffType.FIBONACCI:
            return self.base_delay * self._fibonacci(attempt)
        elif self.backoff_type == BackoffType.POLYNOMIAL:
            return self.base_delay * (attempt ** 2)
        return self.base_delay

    def _apply_jitter(self, delay: float) -> float:
        """Apply jitter to delay."""
        if self.jitter <= 0:
            return delay
        factor = 1.0 + random.uniform(-self.jitter, self.jitter)
        return delay * factor

    @staticmethod
    def _fibonacci(n: int) -> int:
        """Calculate nth Fibonacci number (1-indexed)."""
        if n <= 0:
            return 0
        if n == 1:
            return 1
        a, b = 1, 1
        for _ in range(n - 2):
            a, b = b, a + b
        return b


def retry(
    func: Callable[..., T],
    *args: Any,
    config: Optional[RetryConfig] = None,
    **kwargs: Any
) -> RetryResult[T]:
    """Execute function with retry logic.

    Example:
        def unreliable_operation():
            if random.random() < 0.5:
                raise ValueError("Failed")
            return "Success"

        result = retry(unreliable_operation, RetryConfig(max_attempts=5))
        print(result.succeeded, result.attempts)
    """
    config = config or RetryConfig()
    backoff = BackoffCalculator(
        backoff_type=config.backoff_type,
        base_delay=config.base_delay,
        max_delay=config.max_delay,
        jitter=config.jitter,
    )
    delays: List[float] = []
    start_time = time.time()
    last_error: Optional[Exception] = None

    for attempt in range(1, config.max_attempts + 1):
        try:
            if config.timeout_seconds:
                elapsed = time.time() - start_time
                remaining = config.timeout_seconds - elapsed
                if remaining <= 0:
                    break
            value = func(*args, **kwargs)
            return RetryResult(
                success=True,
                value=value,
                attempts=attempt,
                total_duration=time.time() - start_time,
                delays=delays,
            )
        except config.retry_on as e:  # type: ignore
            last_error = e
            if attempt < config.max_attempts:
                delay = backoff.get_delay(attempt)
                delays.append(delay)
                time.sleep(delay)
            else:
                break

    return RetryResult(
        success=False,
        error=last_error,
        attempts=config.max_attempts,
        total_duration=time.time() - start_time,
        delays=delays,
    )


class RetryContext(Generic[T]):
    """Context manager for retry operations.

    Example:
        with RetryContext[None]() as ctx:
            ctx.execute(unreliable_operation)
            if ctx.result.failed:
                print("Failed after", ctx.result.attempts, "attempts")
    """

    def __init__(
        self,
        config: Optional[RetryConfig] = None,
    ) -> None:
        self.config = config or RetryConfig()
        self._backoff = BackoffCalculator(
            backoff_type=self.config.backoff_type,
            base_delay=self.config.base_delay,
            max_delay=self.config.max_delay,
            jitter=self.config.jitter,
        )
        self.result: Optional[RetryResult[T]] = None
        self._attempt = 0
        self._delays: List[float] = []
        self._start_time = 0.0

    def __enter__(self) -> RetryContext[T]:
        self._start_time = time.time()
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> bool:
        return False

    def execute(self, func: Callable[..., T], *args: Any, **kwargs: Any) -> None:
        """Execute function within retry context."""
        self._attempt = 0
        self._delays = []
        self._start_time = time.time()
        last_error: Optional[Exception] = None

        while self._attempt < self.config.max_attempts:
            self._attempt += 1
            try:
                value = func(*args, **kwargs)
                self.result = RetryResult(
                    success=True,
                    value=value,
                    attempts=self._attempt,
                    total_duration=time.time() - self._start_time,
                    delays=self._delays,
                )
                return
            except self.config.retry_on as e:  # type: ignore
                last_error = e
                if self._attempt < self.config.max_attempts:
                    delay = self._backoff.get_delay(self._attempt)
                    self._delays.append(delay)
                    time.sleep(delay)
                else:
                    break

        self.result = RetryResult(
            success=False,
            error=last_error,
            attempts=self._attempt,
            total_duration=time.time() - self._start_time,
            delays=self._delays,
        )


def with_retry(
    config: Optional[RetryConfig] = None,
) -> Callable[[Callable[..., T]], Callable[..., RetryResult[T]]]:
    """Decorator for adding retry logic to functions.

    Example:
        @with_retry(RetryConfig(max_attempts=5, backoff_type=BackoffType.LINEAR))
        def unreliable_operation():
            pass
    """
    def decorator(func: Callable[..., T]) -> Callable[..., RetryResult[T]]:
        def wrapper(*args: Any, **kwargs: Any) -> RetryResult[T]:
            return retry(func, *args, config=config, **kwargs)
        return wrapper
    return decorator


@dataclass
class CircuitBreakerConfig:
    """Configuration for circuit breaker pattern."""
    failure_threshold: int = 5
    success_threshold: int = 2
    timeout_seconds: float = 30.0
    half_open_max_calls: int = 3


class CircuitState(Enum):
    """Circuit breaker states."""
    CLOSED = auto()
    OPEN = auto()
    HALF_OPEN = auto()


class CircuitBreaker:
    """Circuit breaker for failing fast on persistent errors.

    Example:
        cb = CircuitBreaker(failure_threshold=3, timeout_seconds=30)
        for i in range(10):
            result = cb.call(flaky_operation)
            print(cb.state)
    """

    def __init__(self, config: Optional[CircuitBreakerConfig] = None) -> None:
        self.config = config or CircuitBreakerConfig()
        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._success_count = 0
        self._last_failure_time: Optional[float] = None
        self._lock = threading.RLock()

    @property
    def state(self) -> CircuitState:
        """Get current circuit state."""
        with self._lock:
            if self._state == CircuitState.OPEN:
                if self._should_attempt_reset():
                    self._state = CircuitState.HALF_OPEN
            return self._state

    def _should_attempt_reset(self) -> bool:
        """Check if enough time has passed to attempt reset."""
        if self._last_failure_time is None:
            return True
        return (time.time() - self._last_failure_time) >= self.config.timeout_seconds

    def call(self, func: Callable[..., T], *args: Any, **kwargs: Any) -> T:
        """Call function through circuit breaker."""
        with self._lock:
            if self.state == CircuitState.OPEN:
                raise CircuitOpenError("Circuit breaker is OPEN")
            if self.state == CircuitState.HALF_OPEN:
                if self._success_count >= self.config.half_open_max_calls:
                    self._state = CircuitState.CLOSED
                    self._failure_count = 0
                    self._success_count = 0

        try:
            result = func(*args, **kwargs)
            self._on_success()
            return result
        except Exception as e:
            self._on_failure()
            raise e

    def _on_success(self) -> None:
        """Handle successful call."""
        with self._lock:
            if self._state == CircuitState.HALF_OPEN:
                self._success_count += 1
                if self._success_count >= self.config.success_threshold:
                    self._state = CircuitState.CLOSED
                    self._failure_count = 0
            elif self._state == CircuitState.CLOSED:
                self._failure_count = 0

    def _on_failure(self) -> None:
        """Handle failed call."""
        with self._lock:
            self._failure_count += 1
            self._last_failure_time = time.time()
            if self._failure_count >= self.config.failure_threshold:
                self._state = CircuitState.OPEN
            elif self._state == CircuitState.HALF_OPEN:
                self._state = CircuitState.OPEN

    def reset(self) -> None:
        """Manually reset circuit breaker."""
        with self._lock:
            self._state = CircuitState.CLOSED
            self._failure_count = 0
            self._success_count = 0
            self._last_failure_time = None


class CircuitOpenError(Exception):
    """Raised when circuit breaker is open."""
    pass
