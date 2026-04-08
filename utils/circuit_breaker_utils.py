"""Circuit breaker pattern implementation for fault tolerance."""

from typing import Callable, TypeVar, Optional, Any
from enum import Enum
import time
import threading


T = TypeVar("T")


class CircuitState(Enum):
    """Circuit breaker states."""
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class CircuitBreaker:
    """Circuit breaker for preventing cascading failures."""

    def __init__(
        self,
        failure_threshold: int = 5,
        recovery_timeout: float = 60.0,
        expected_exception: type = Exception,
        name: Optional[str] = None
    ):
        """Initialize circuit breaker.
        
        Args:
            failure_threshold: Number of failures before opening circuit.
            recovery_timeout: Seconds before attempting recovery.
            expected_exception: Exception type to count as failure.
            name: Optional name for logging.
        """
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.expected_exception = expected_exception
        self.name = name or "CircuitBreaker"
        self._failure_count = 0
        self._last_failure_time: Optional[float] = None
        self._state = CircuitState.CLOSED
        self._lock = threading.RLock()

    @property
    def state(self) -> CircuitState:
        """Get current circuit state."""
        with self._lock:
            if self._state == CircuitState.OPEN:
                if self._last_failure_time and \
                   time.time() - self._last_failure_time >= self.recovery_timeout:
                    self._state = CircuitState.HALF_OPEN
            return self._state

    def call(self, func: Callable[[], T], *args: Any, **kwargs: Any) -> T:
        """Execute function through circuit breaker.
        
        Args:
            func: Function to execute.
            *args: Positional arguments.
            **kwargs: Keyword arguments.
        
        Returns:
            Function result.
        
        Raises:
            CircuitBreakerOpen: If circuit is open.
            Exception: Original exception from function.
        """
        if self.state == CircuitState.OPEN:
            raise CircuitBreakerOpen(f"Circuit {self.name} is OPEN")

        try:
            result = func(*args, **kwargs)
            self._on_success()
            return result
        except self.expected_exception as e:
            self._on_failure()
            raise e

    def _on_success(self) -> None:
        """Handle successful call."""
        with self._lock:
            self._failure_count = 0
            self._state = CircuitState.CLOSED

    def _on_failure(self) -> None:
        """Handle failed call."""
        with self._lock:
            self._failure_count += 1
            self._last_failure_time = time.time()
            if self._failure_count >= self.failure_threshold:
                self._state = CircuitState.OPEN

    def reset(self) -> None:
        """Manually reset the circuit breaker."""
        with self._lock:
            self._failure_count = 0
            self._last_failure_time = None
            self._state = CircuitState.CLOSED


class CircuitBreakerOpen(Exception):
    """Raised when circuit breaker is open."""
    pass


def circuit_breaker(
    failure_threshold: int = 5,
    recovery_timeout: float = 60.0
) -> Callable[[Callable[[], T]], Callable[[], T]]:
    """Decorator to add circuit breaker to a function.
    
    Args:
        failure_threshold: Failures before opening circuit.
        recovery_timeout: Recovery attempt timeout.
    
    Returns:
        Decorated function with circuit breaker.
    """
    breaker = CircuitBreaker(failure_threshold, recovery_timeout)

    def decorator(func: Callable[[], T]) -> Callable[[], T]:
        def wrapper(*args: Any, **kwargs: Any) -> T:
            return breaker.call(func, *args, **kwargs)
        return wrapper
    return decorator
