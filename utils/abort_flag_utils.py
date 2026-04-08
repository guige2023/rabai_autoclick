"""Abort flag and cancellation utilities.

Provides cooperative cancellation mechanism for
long-running automation tasks and workflows.
"""

import threading
import time
from typing import Callable, Optional


class AbortFlag:
    """Thread-safe abort flag for cancellation.

    Example:
        abort = AbortFlag()
        for item in items:
            if abort.is_set:
                break
            process(item)
    """

    def __init__(self) -> None:
        self._flag = False
        self._lock = threading.Lock()
        self._callbacks: list = []

    def set(self) -> None:
        """Set the abort flag."""
        with self._lock:
            self._flag = True
            for callback in self._callbacks:
                try:
                    callback()
                except Exception:
                    pass

    def clear(self) -> None:
        """Clear the abort flag."""
        with self._lock:
            self._flag = False

    def is_set(self) -> bool:
        """Check if abort flag is set."""
        with self._lock:
            return self._flag

    def on_abort(self, callback: Callable[[], None]) -> None:
        """Register callback to be called when abort is set.

        Args:
            callback: Function to call on abort.
        """
        with self._lock:
            self._callbacks.append(callback)

    def check(self) -> None:
        """Check flag and raise if set.

        Raises:
            AbortError: If flag is set.
        """
        if self.is_set():
            raise AbortError()

    def __bool__(self) -> bool:
        return self.is_set()


class AbortError(Exception):
    """Raised when operation is aborted."""
    pass


class AbortScope:
    """Scope for managing cancellable operations.

    Example:
        with AbortScope() as scope:
            scope.run(heavy_task)
            if need_to_cancel:
                scope.abort()
    """

    def __init__(self) -> None:
        self._flag = AbortFlag()
        self._running = False

    def __enter__(self) -> "AbortScope":
        self._running = True
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self._running = False

    @property
    def abort_flag(self) -> AbortFlag:
        """Get the abort flag."""
        return self._flag

    @property
    def is_aborted(self) -> bool:
        """Check if scope is aborted."""
        return self._flag.is_set()

    def abort(self) -> None:
        """Abort all operations in this scope."""
        self._flag.set()

    def run(self, func: Callable[[], T], *args: Any, **kwargs: T) -> Optional[T]:
        """Run function if not aborted.

        Args:
            func: Function to run.
            *args: Positional arguments.
            **kwargs: Keyword arguments.

        Returns:
            Function result or None if aborted.
        """
        if self._flag.is_set():
            return None
        result = func(*args, **kwargs)
        return result

    def run_with_check(self, func: Callable[[], T], *args: Any, **kwargs: Any) -> T:
        """Run function with periodic abort checks.

        Args:
            func: Function to run.
            *args: Positional arguments.
            **kwargs: Keyword arguments.

        Returns:
            Function result.

        Raises:
            AbortError: If aborted during execution.
        """
        self._flag.check()
        result = func(*args, **kwargs)
        self._flag.check()
        return result


def with_abort(
    func: Callable[..., T],
    abort_flag: Optional[AbortFlag] = None,
) -> Callable[..., Optional[T]]:
    """Decorator to add abort checking to function.

    Args:
        func: Function to wrap.
        abort_flag: Abort flag to use. Uses global if None.

    Returns:
        Wrapped function that checks abort flag.
    """
    def wrapper(*args: Any, **kwargs: Any) -> Optional[T]:
        if abort_flag and abort_flag.is_set():
            return None
        return func(*args, **kwargs)
    return wrapper


class RetryContext:
    """Context for retryable operations with abort support.

    Example:
        ctx = RetryContext(max_attempts=3)
        while ctx.attempt < ctx.max_attempts:
            try:
                return do_work()
            except Exception as e:
                if not ctx.should_retry(e):
                    raise
                ctx.backoff()
    """

    def __init__(
        self,
        max_attempts: int = 3,
        base_delay: float = 1.0,
        max_delay: float = 60.0,
    ) -> None:
        self.max_attempts = max_attempts
        self.base_delay = base_delay
        self.max_delay = max_delay
        self._attempt = 0
        self._abort = AbortFlag()

    @property
    def attempt(self) -> int:
        """Current attempt number (1-based)."""
        return self._attempt

    @property
    def abort_flag(self) -> AbortFlag:
        """Get abort flag."""
        return self._abort

    def should_retry(self, exc: Exception) -> bool:
        """Check if should retry after exception."""
        if self._attempt >= self.max_attempts:
            return False
        if self._abort.is_set():
            return False
        return True

    def proceed(self) -> bool:
        """Start next attempt.

        Returns:
            True if should proceed, False if exhausted.
        """
        self._attempt += 1
        return self._attempt <= self.max_attempts and not self._abort.is_set()

    def backoff(self) -> None:
        """Wait before next retry using exponential backoff."""
        delay = min(self.base_delay * (2 ** (self._attempt - 1)), self.max_delay)
        time.sleep(delay)


from typing import TypeVar
T = TypeVar("T")
