"""resilient_action module for rabai_autoclick.

Provides resilient execution utilities: circuit breaker pattern,
bulkhead isolation, retry with backoff, timeout handling, and fallback execution.
"""

from __future__ import annotations

import threading
import time
from collections import deque
from dataclasses import dataclass
from enum import Enum, auto
from typing import Any, Callable, Optional, TypeVar

__all__ = [
    "CircuitState",
    "CircuitBreaker",
    "Bulkhead",
    "Timeout",
    "Fallback",
    "ResilientExecutor",
    "execute_with_timeout",
    "execute_with_fallback",
    "execute_with_circuit_breaker",
]


class CircuitState(Enum):
    """Circuit breaker states."""
    CLOSED = auto()
    OPEN = auto()
    HALF_OPEN = auto()


@dataclass
class CircuitBreaker:
    """Circuit breaker for fault tolerance.

    Prevents cascading failures by stopping requests to a failing service.
    """

    name: str
    failure_threshold: int = 5
    recovery_timeout: float = 60.0
    half_open_max_calls: int = 3

    _state: CircuitState = CircuitState.CLOSED
    _failure_count: int = 0
    _success_count: int = 0
    _last_failure_time: float = 0.0
    _lock: threading.Lock = None

    def __post_init__(self) -> None:
        self._lock = threading.Lock()

    @property
    def state(self) -> CircuitState:
        """Get current circuit state."""
        with self._lock:
            if self._state == CircuitState.OPEN:
                if time.time() - self._last_failure_time >= self.recovery_timeout:
                    self._state = CircuitState.HALF_OPEN
                    self._success_count = 0
            return self._state

    def record_success(self) -> None:
        """Record successful call."""
        with self._lock:
            self._failure_count = 0
            if self._state == CircuitState.HALF_OPEN:
                self._success_count += 1
                if self._success_count >= self.half_open_max_calls:
                    self._state = CircuitState.CLOSED

    def record_failure(self) -> None:
        """Record failed call."""
        with self._lock:
            self._failure_count += 1
            self._last_failure_time = time.time()
            if self._failure_count >= self.failure_threshold:
                self._state = CircuitState.OPEN

    def allow_request(self) -> bool:
        """Check if request is allowed."""
        return self.state != CircuitState.OPEN

    def execute(self, func: Callable, *args: Any, **kwargs: Any) -> Any:
        """Execute function with circuit breaker protection."""
        if not self.allow_request():
            raise CircuitOpenError(f"Circuit {self.name} is open")
        try:
            result = func(*args, **kwargs)
            self.record_success()
            return result
        except Exception:
            self.record_failure()
            raise

    def reset(self) -> None:
        """Reset circuit breaker."""
        with self._lock:
            self._state = CircuitState.CLOSED
            self._failure_count = 0
            self._success_count = 0
            self._last_failure_time = 0.0


class CircuitOpenError(Exception):
    """Raised when circuit is open."""
    pass


@dataclass
class Bulkhead:
    """Bulkhead pattern for resource isolation.

    Limits concurrent executions to prevent resource exhaustion.
    """

    max_concurrent: int = 10
    max_queue: int = 0

    _semaphore: threading.Semaphore = None
    _active_count: int = 0
    _lock: threading.Lock = None
    _queue: deque = None

    def __post_init__(self) -> None:
        import threading
        self._semaphore = threading.Semaphore(self.max_concurrent)
        self._lock = threading.Lock()
        self._queue = deque()

    def acquire(self, timeout: Optional[float] = None) -> bool:
        """Acquire bulkhead slot.

        Returns:
            True if acquired, False if timeout.
        """
        if self.max_queue > 0:
            with self._lock:
                if len(self._queue) < self.max_queue:
                    self._queue.append(threading.current_thread())
                else:
                    return False
        return self._semaphore.acquire(blocking=True, timeout=timeout)

    def release(self) -> None:
        """Release bulkhead slot."""
        if self.max_queue > 0:
            with self._lock:
                try:
                    self._queue.popleft()
                except IndexError:
                    pass
        self._semaphore.release()

    def execute(self, func: Callable, *args: Any, **kwargs: Any) -> Any:
        """Execute function with bulkhead protection."""
        if not self.acquire(timeout=0):
            raise BulkheadFullError("Bulkhead capacity exceeded")
        try:
            return func(*args, **kwargs)
        finally:
            self.release()

    @property
    def available(self) -> int:
        """Number of available slots."""
        return self.max_concurrent - self._semaphore._value


class BulkheadFullError(Exception):
    """Raised when bulkhead is full."""
    pass


@dataclass
class Timeout:
    """Timeout wrapper for function execution."""

    seconds: float

    def execute(self, func: Callable, *args: Any, **kwargs: Any) -> Any:
        """Execute function with timeout.

        Raises:
            TimeoutError: If execution exceeds timeout.
        """
        import time
        result = None
        finished = threading.Event()
        exception: Optional[Exception] = None

        def worker() -> None:
            nonlocal result, exception
            try:
                result = func(*args, **kwargs)
            except Exception as e:
                exception = e
            finally:
                finished.set()

        thread = threading.Thread(target=worker)
        thread.daemon = True
        thread.start()
        if not finished.wait(timeout=self.seconds):
            raise TimeoutError(f"Execution exceeded {self.seconds}s")
        thread.join(timeout=1.0)
        if exception:
            raise exception
        return result


class TimeoutError(Exception):
    """Raised when operation times out."""
    pass


class Fallback:
    """Fallback execution on primary failure."""

    def __init__(self, fallback_func: Callable) -> None:
        self._fallback = fallback_func

    def execute(self, func: Callable, *args: Any, **kwargs: Any) -> Any:
        """Execute primary, fall back on failure."""
        try:
            return func(*args, **kwargs)
        except Exception:
            return self._fallback(*args, **kwargs)


class ResilientExecutor:
    """Combined resilient execution with all patterns."""

    def __init__(
        self,
        circuit_breaker: Optional[CircuitBreaker] = None,
        bulkhead: Optional[Bulkhead] = None,
        timeout: Optional[float] = None,
        fallback: Optional[Callable] = None,
    ) -> None:
        self.circuit_breaker = circuit_breaker
        self.bulkhead = bulkhead
        self.timeout = timeout
        self.fallback = fallback

    def execute(self, func: Callable, *args: Any, **kwargs: Any) -> Any:
        """Execute with all resilience patterns."""
        def do_execute() -> Any:
            if self.circuit_breaker:
                return self.circuit_breaker.execute(func, *args, **kwargs)
            return func(*args, **kwargs)

        wrapper = do_execute
        if self.bulkhead:
            def bulkhead_wrapper() -> Any:
                return self.bulkhead.execute(do_execute)
            wrapper = bulkhead_wrapper

        if self.timeout:
            timeout_obj = Timeout(seconds=self.timeout)
            def timeout_wrapper() -> Any:
                return timeout_obj.execute(wrapper)
            wrapper = timeout_wrapper

        try:
            return wrapper()
        except Exception:
            if self.fallback:
                return self.fallback(*args, **kwargs)
            raise


def execute_with_timeout(
    func: Callable,
    timeout: float,
    *args: Any,
    **kwargs: Any,
) -> Any:
    """Execute function with timeout.

    Args:
        func: Function to execute.
        timeout: Timeout in seconds.
        *args: Positional args.
        **kwargs: Keyword args.

    Returns:
        Function result.

    Raises:
        TimeoutError: If timeout exceeded.
    """
    return Timeout(seconds=timeout).execute(func, *args, **kwargs)


def execute_with_fallback(
    func: Callable,
    fallback: Callable,
    *args: Any,
    **kwargs: Any,
) -> Any:
    """Execute with fallback on failure.

    Args:
        func: Primary function.
        fallback: Fallback function.
        *args: Positional args.
        **kwargs: Keyword args.

    Returns:
        Result from primary or fallback.
    """
    return Fallback(fallback=fallback).execute(func, *args, **kwargs)


def execute_with_circuit_breaker(
    breaker: CircuitBreaker,
    func: Callable,
    *args: Any,
    **kwargs: Any,
) -> Any:
    """Execute with circuit breaker protection.

    Args:
        breaker: Circuit breaker.
        func: Function to execute.
        *args: Positional args.
        **kwargs: Keyword args.

    Returns:
        Function result.
    """
    return breaker.execute(func, *args, **kwargs)
