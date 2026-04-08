"""Error recovery and retry utilities for resilient automation workflows.

Provides automatic retry with backoff, circuit breaker pattern,
fallback action chains, and error categorization for building
robust automation that handles transient failures gracefully.

Example:
    >>> from utils.error_recovery_utils import retry, circuit_breaker, fallback
    >>> @retry(max_attempts=3, backoff='exponential')
    ... def click_with_retry():
    ...     click(100, 200)
    >>> @circuit_breaker(failure_threshold=5)
    ... def fragile_action():
    ...     ...
"""

from __future__ import annotations

import time
import threading
from typing import Any, Callable, Optional

__all__ = [
    "retry",
    "RetryPolicy",
    "circuit_breaker",
    "CircuitBreaker",
    "FallbackChain",
    "ErrorCategorizer",
    "TransientError",
    "PermanentError",
]


class TransientError(Exception):
    """An error that may succeed on retry (network timeout, etc.)."""
    pass


class PermanentError(Exception):
    """An error that will not succeed on retry (invalid input, etc.)."""
    pass


class RetryPolicy:
    """Defines retry behavior for failed operations.

    Attributes:
        max_attempts: Maximum number of retry attempts.
        initial_delay: Initial delay between retries in seconds.
        backoff_factor: Multiplier for delay after each retry.
        max_delay: Maximum delay between retries.
        jitter: Random jitter to add to delays.
    """

    def __init__(
        self,
        max_attempts: int = 3,
        initial_delay: float = 0.1,
        backoff_factor: float = 2.0,
        max_delay: float = 10.0,
        jitter: float = 0.1,
    ):
        self.max_attempts = max_attempts
        self.initial_delay = initial_delay
        self.backoff_factor = backoff_factor
        self.max_delay = max_delay
        self.jitter = jitter

    def delay_for_attempt(self, attempt: int) -> float:
        """Calculate the delay before a specific attempt number."""
        import random

        delay = min(self.initial_delay * (self.backoff_factor ** attempt), self.max_delay)
        if self.jitter > 0:
            delay += random.uniform(0, self.jitter * delay)
        return delay


def retry(
    func: Optional[Callable] = None,
    policy: Optional[RetryPolicy] = None,
    exceptions: tuple = (Exception,),
    on_retry: Optional[Callable[[Exception, int], None]] = None,
) -> Callable:
    """Decorator for retrying functions that raise recoverable errors.

    Args:
        policy: RetryPolicy instance (uses default if None).
        exceptions: Tuple of exception types to retry on.
        on_retry: Optional callback(exception, attempt) on each retry.

    Returns:
        Decorated function.

    Example:
        >>> @retry(policy=RetryPolicy(max_attempts=5))
        ... def unreliable_network_call():
        ...     ...
    """
    if policy is None:
        policy = RetryPolicy()

    def decorator(fn: Callable) -> Callable:
        def wrapper(*args, **kwargs):
            last_error: Optional[Exception] = None
            for attempt in range(policy.max_attempts):
                try:
                    return fn(*args, **kwargs)
                except exceptions as e:
                    last_error = e
                    # Don't retry permanent errors
                    if isinstance(e, PermanentError):
                        raise
                    if attempt < policy.max_attempts - 1:
                        delay = policy.delay_for_attempt(attempt)
                        if on_retry:
                            try:
                                on_retry(e, attempt + 1)
                            except Exception:
                                pass
                        time.sleep(delay)
                    else:
                        break
            if last_error:
                raise last_error

        return wrapper

    if func is not None:
        return decorator(func)
    return decorator


class CircuitState:
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class CircuitBreaker:
    """Circuit breaker to prevent cascading failures.

    When failures exceed a threshold, the circuit "opens" and
    immediately fails fast. After a timeout, it enters "half-open"
    state to test recovery.
    """

    def __init__(
        self,
        failure_threshold: int = 5,
        recovery_timeout: float = 60.0,
        expected_exception: type = Exception,
    ):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.expected_exception = expected_exception
        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._last_failure_time: Optional[float] = None
        self._lock = threading.Lock()

    @property
    def state(self) -> str:
        with self._lock:
            self._update_state()
            return self._state

    def _update_state(self) -> None:
        if self._state == CircuitState.OPEN:
            if self._last_failure_time is not None:
                if time.time() - self._last_failure_time > self.recovery_timeout:
                    self._state = CircuitState.HALF_OPEN

    def call(self, func: Callable, *args, **kwargs) -> Any:
        """Execute a function through the circuit breaker."""
        with self._lock:
            self._update_state()
            if self._state == CircuitState.OPEN:
                raise Exception("Circuit breaker is OPEN")

        try:
            result = func(*args, **kwargs)
            with self._lock:
                self._failure_count = 0
                self._state = CircuitState.CLOSED
            return result
        except self.expected_exception as e:
            with self._lock:
                self._failure_count += 1
                self._last_failure_time = time.time()
                if self._failure_count >= self.failure_threshold:
                    self._state = CircuitState.OPEN
            raise

    def record_success(self) -> None:
        """Record a successful call."""
        with self._lock:
            self._failure_count = max(0, self._failure_count - 1)

    def record_failure(self) -> None:
        """Record a failed call."""
        with self._lock:
            self._failure_count += 1
            self._last_failure_time = time.time()
            if self._failure_count >= self.failure_threshold:
                self._state = CircuitState.OPEN

    def reset(self) -> None:
        """Manually reset the circuit breaker."""
        with self._lock:
            self._state = CircuitState.CLOSED
            self._failure_count = 0


def circuit_breaker(
    failure_threshold: int = 5,
    recovery_timeout: float = 60.0,
) -> Callable:
    """Decorator to apply a circuit breaker to a function.

    Args:
        failure_threshold: Number of failures before opening.
        recovery_timeout: Seconds before attempting recovery.

    Returns:
        Decorated function with circuit breaker.
    """
    breaker = CircuitBreaker(failure_threshold, recovery_timeout)

    def decorator(fn: Callable) -> Callable:
        def wrapper(*args, **kwargs):
            return breaker.call(fn, *args, **kwargs)
        return wrapper
    return decorator


class FallbackChain:
    """A chain of fallback actions tried in sequence.

    Example:
        >>> chain = FallbackChain()
        >>> chain.add(primary_action)
        >>> chain.add(backup_action)
        >>> chain.add(default_action)
        >>> result = chain.execute()
    """

    def __init__(self):
        self._handlers: list[tuple[Callable, tuple]] = []

    def add(self, func: Callable, *args, **kwargs) -> "FallbackChain":
        """Add a fallback handler to the chain."""
        self._handlers.append((func, (args, kwargs)))
        return self

    def execute(self, *args, **kwargs) -> Any:
        """Execute handlers in order until one succeeds.

        Returns:
            Result of the first succeeding handler.
        """
        for func, (fixed_args, fixed_kwargs) in self._handlers:
            merged_args = args or fixed_args
            merged_kwargs = {**fixed_kwargs, **kwargs}
            try:
                result = func(*merged_args, **merged_kwargs)
                return result
            except Exception:
                continue
        raise Exception("All fallback handlers in the chain failed")


class ErrorCategorizer:
    """Categorizes errors into transient vs permanent.

    Example:
        >>> categorizer = ErrorCategorizer()
        >>> categorizer.register(TransientError, 'network_timeout')
        >>> is_transient = categorizer.is_transient(error)
    """

    def __init__(self):
        self._categories: dict[type, str] = {}
        self._patterns: list[tuple[str, type]] = []

    def register(self, exception_type: type, category: str) -> None:
        """Register an exception type to a category."""
        self._categories[exception_type] = category

    def is_transient(self, error: Exception) -> bool:
        """Check if an error is likely transient."""
        if isinstance(error, TransientError):
            return True
        if isinstance(error, PermanentError):
            return False
        return self._categories.get(type(error)) == "transient"

    def categorize(self, error: Exception) -> str:
        """Get the category of an error."""
        return self._categories.get(type(error), "unknown")
