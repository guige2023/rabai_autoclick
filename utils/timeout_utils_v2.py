"""
Timeout utilities for function execution.

Provides decorators and context managers for adding
timeout support to functions and code blocks.

Example:
    >>> from utils.timeout_utils_v2 import timeout, with_timeout
    >>> @timeout(5.0)
    ... def long_running():
    ...     pass
"""

from __future__ import annotations

import functools
import signal
import threading
import time
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeoutError
from typing import Any, Callable, Optional, TypeVar

T = TypeVar("T")


class TimeoutError(Exception):
    """Raised when a timeout occurs."""
    pass


def timeout(
    seconds: float,
    use_signal: bool = False,
    error_message: Optional[str] = None,
) -> Callable:
    """
    Decorator that adds a timeout to a function.

    Args:
        seconds: Timeout in seconds.
        use_signal: Use signals (only works on main thread).
        error_message: Custom error message.

    Returns:
        Decorated function.
    """
    def decorator(func: Callable) -> Callable:
        if use_signal:

            @functools.wraps(func)
            def wrapper(*args: Any, **kwargs: Any) -> Any:
                def handler(signum: Any, frame: Any) -> None:
                    raise TimeoutError(error_message or f"Function {func.__name__} timed out")

                old_handler = signal.signal(signal.SIGALRM, handler)
                signal.alarm(int(seconds))
                try:
                    result = func(*args, **kwargs)
                finally:
                    signal.alarm(0)
                    signal.signal(signal.SIGALRM, old_handler)
                return result

            return wrapper
        else:

            @functools.wraps(func)
            def wrapper(*args: Any, **kwargs: Any) -> Any:
                with ThreadPoolExecutor(max_workers=1) as executor:
                    future = executor.submit(func, *args, **kwargs)
                    try:
                        return future.result(timeout=seconds)
                    except FutureTimeoutError:
                        raise TimeoutError(
                            error_message or f"Function {func.__name__} timed out after {seconds}s"
                        )

            return wrapper
    return decorator


def with_timeout(
    seconds: float,
    default: Optional[T] = None,
) -> Callable:
    """
    Decorator that returns default on timeout.

    Args:
        seconds: Timeout in seconds.
        default: Default value to return on timeout.

    Returns:
        Decorated function.
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            with ThreadPoolExecutor(max_workers=1) as executor:
                future = executor.submit(func, *args, **kwargs)
                try:
                    return future.result(timeout=seconds)
                except FutureTimeoutError:
                    return default
        return wrapper
    return decorator


class TimeoutContext:
    """
    Context manager for timeout blocks.

    Example:
        >>> with TimeoutContext(5.0) as ctx:
        ...     do_work()
        ...     if ctx.expired:
        ...         handle_timeout()
    """

    def __init__(
        self,
        seconds: float,
        on_timeout: Optional[Callable] = None,
    ) -> None:
        """
        Initialize the timeout context.

        Args:
            seconds: Timeout in seconds.
            on_timeout: Optional callback when timeout occurs.
        """
        self.seconds = seconds
        self.on_timeout = on_timeout
        self._start: Optional[float] = None
        self._expired = False

    def __enter__(self) -> "TimeoutContext":
        """Start the timeout timer."""
        self._start = time.monotonic()
        return self

    def __exit__(self, *args: Any) -> None:
        """Check if timeout occurred on exit."""
        elapsed = time.monotonic() - (self._start or 0)
        if elapsed >= self.seconds:
            self._expired = True
            if self.on_timeout:
                self.on_timeout()

    @property
    def expired(self) -> bool:
        """Check if the timeout has expired."""
        if self._start is None:
            return False
        return time.monotonic() - self._start >= self.seconds

    @property
    def remaining(self) -> float:
        """Get remaining time in seconds."""
        if self._start is None:
            return self.seconds
        return max(0, self.seconds - (time.monotonic() - self._start))


def run_with_timeout(
    func: Callable[[T], Any],
    args: tuple = (),
    kwargs: dict = None,
    timeout_seconds: float = 30.0,
    default: Optional[T] = None,
) -> Optional[T]:
    """
    Run a function with a timeout.

    Args:
        func: Function to run.
        args: Positional arguments.
        kwargs: Keyword arguments.
        timeout_seconds: Timeout in seconds.
        default: Default value on timeout.

    Returns:
        Function result or default.
    """
    kwargs = kwargs or {}

    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(func, *args, **kwargs)
        try:
            return future.result(timeout=timeout_seconds)
        except FutureTimeoutError:
            return default


class AsyncTimeout:
    """
    Async timeout context manager.
    """

    def __init__(
        self,
        seconds: float,
        error_message: Optional[str] = None,
    ) -> None:
        """
        Initialize the async timeout.

        Args:
            seconds: Timeout in seconds.
            error_message: Custom error message.
        """
        self.seconds = seconds
        self.error_message = error_message
        self._task: Optional[asyncio.Task] = None

    async def __aenter__(self) -> "AsyncTimeout":
        """Enter the async context."""
        return self

    async def __aexit__(self, *args: Any) -> None:
        """Exit and check timeout."""
        pass

    async def wait(self, coro: Any) -> Any:
        """
        Wait for a coroutine with timeout.

        Args:
            coro: Coroutine to execute.

        Returns:
            Result of coroutine.

        Raises:
            asyncio.TimeoutError: If timeout occurs.
        """
        return await asyncio.wait_for(coro, timeout=self.seconds)


def async_timeout(seconds: float) -> Callable:
    """
    Async decorator for timeout.

    Args:
        seconds: Timeout in seconds.

    Returns:
        Decorator function.
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            return await asyncio.wait_for(func(*args, **kwargs), timeout=seconds)
        return wrapper
    return decorator


def retry_with_timeout(
    func: Callable,
    timeout_seconds: float = 30.0,
    max_retries: int = 3,
    retry_delay: float = 1.0,
) -> Any:
    """
    Run a function with timeout and retries.

    Args:
        func: Function to run.
        timeout_seconds: Timeout per attempt.
        max_retries: Maximum number of attempts.
        retry_delay: Delay between retries.

    Returns:
        Function result.

    Raises:
        TimeoutError: If all retries timeout.
        Exception: If function raises an exception.
    """
    last_error = None

    for attempt in range(max_retries):
        try:
            return run_with_timeout(
                func,
                timeout_seconds=timeout_seconds,
            )
        except TimeoutError as e:
            last_error = e
            if attempt < max_retries - 1:
                time.sleep(retry_delay)

    raise TimeoutError(f"All {max_retries} attempts timed out") from last_error
