"""Context manager utilities for RabAI AutoClick.

Provides:
- Reusable context manager decorators and classes
- Nested context support
- Context variable helpers
"""

import contextlib
import functools
import threading
from contextvars import ContextVar, copy_context
from typing import (
    Any,
    Callable,
    ContextManager,
    Generator,
    Generic,
    Optional,
    TypeVar,
)

T = TypeVar("T")


class ContextStack(Generic[T]):
    """Thread-safe context stack.

    Useful for managing nested contexts like scopes or levels.
    """

    def __init__(self, initial: Optional[T] = None) -> None:
        self._stack: list[T] = [initial] if initial is not None else []
        self._lock = threading.Lock()

    def push(self, value: T) -> None:
        """Push a value onto the stack."""
        with self._lock:
            self._stack.append(value)

    def pop(self) -> Optional[T]:
        """Pop the top value from the stack."""
        with self._lock:
            return self._stack.pop() if self._stack else None

    def peek(self) -> Optional[T]:
        """Peek at the top value without popping."""
        with self._lock:
            return self._stack[-1] if self._stack else None

    def get(self) -> Optional[T]:
        """Get current (top) value."""
        return self.peek()

    def set(self, value: T) -> None:
        """Set the current (top) value or push if empty."""
        with self._lock:
            if self._stack:
                self._stack[-1] = value
            else:
                self._stack.append(value)

    def is_empty(self) -> bool:
        """Check if stack is empty."""
        with self._lock:
            return len(self._stack) == 0

    def __len__(self) -> int:
        with self._lock:
            return len(self._stack)


class NestedContext:
    """A context manager that supports nested entries with callbacks.

    Args:
        enter_callback: Called when entering the context.
        exit_callback: Called when exiting the context.
    """

    def __init__(
        self,
        enter_callback: Optional[Callable[[], Any]] = None,
        exit_callback: Optional[Callable[[], Any]] = None,
    ) -> None:
        self._enter_cb = enter_callback
        self._exit_cb = exit_callback
        self._depth = 0

    def __enter__(self) -> "NestedContext":
        if self._enter_cb and self._depth == 0:
            self._enter_cb()
        self._depth += 1
        return self

    def __exit__(self, *args: Any) -> None:
        self._depth -= 1
        if self._exit_cb and self._depth == 0:
            self._exit_cb()


@contextlib.contextmanager
def temp_contextvars(**vars: Any) -> Generator[None, None, None]:
    """Temporarily set context variables.

    Args:
        **vars: ContextVar names and values to set.

    Example:
        with temp_contextvars(my_var="value"):
            # my_var is set here
            pass
    """
    # We need to use ContextVar for this pattern
    # This creates a copy of current context, applies temp changes
    ctx = copy_context()
    for key, value in vars.items():
        if isinstance(key, ContextVar):
            key.set(value)
        else:
            raise TypeError(f"Expected ContextVar, got {type(key)}")
    yield


@contextlib.contextmanager
def sandbox_context(
    **overrides: Any,
) -> Generator[None, None, None]:
    """Create a sandboxed context with overrides.

    Restores original values on exit.

    Args:
        **overrides: Variable overrides.
    """
    original_values: dict = {}
    for key, value in overrides.items():
        try:
            original_values[key] = key.get()
        except LookupError:
            original_values[key] = None

    saved: dict = {}
    for key in overrides:
        try:
            saved[key] = key.set(overrides[key])
        except Exception:
            pass

    try:
        yield
    finally:
        for key in saved:
            try:
                key.set(original_values[key])
            except Exception:
                pass


@contextlib.contextmanager
def timed_context() -> Generator[dict[str, float], None, None]:
    """Context manager that tracks entry/exit time.

    Yields:
        Dict with 'enter_time' and 'exit_time' keys.

    Example:
        with timed_context() as timing:
            do_work()
            print(f"Elapsed: {timing['exit_time'] - timing['enter_time']}")
    """
    import time
    timing: dict[str, float] = {}
    timing["enter_time"] = time.perf_counter()
    try:
        yield timing
    finally:
        timing["exit_time"] = time.perf_counter()


class CallbackContext:
    """A context manager that invokes registered callbacks.

    Useful for implementing lifecycle hooks.
    """

    def __init__(self) -> None:
        self._on_enter: list[Callable[[], Any]] = []
        self._on_exit: list[Callable[[], Any]] = []
        self._on_error: list[Callable[[Exception], Any]] = []

    def on_enter(self, cb: Callable[[], Any]) -> "CallbackContext":
        """Register a callback to run on context entry."""
        self._on_enter.append(cb)
        return self

    def on_exit(self, cb: Callable[[], Any]) -> "CallbackContext":
        """Register a callback to run on context exit."""
        self._on_exit.append(cb)
        return self

    def on_error(self, cb: Callable[[Exception], Any]) -> "CallbackContext":
        """Register a callback to run on context error."""
        self._on_error.append(cb)
        return self

    def __enter__(self) -> "CallbackContext":
        for cb in self._on_enter:
            cb()
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> bool:
        for cb in self._on_exit:
            cb()
        if exc_type is not None and exc_val is not None:
            for cb in self._on_error:
                cb(exc_val)
        return False


@contextlib.contextmanager
def suppress_errors(
    *exceptions: type,
    log_errors: bool = True,
) -> Generator[None, None, None]:
    """Context manager that suppresses specified exceptions.

    Args:
        *exceptions: Exception types to suppress.
        log_errors: Whether to log suppressed errors.
    """
    try:
        yield
    except exceptions as e:
        if log_errors:
            import logging
            logging.getLogger(__name__).debug(f"Suppressed error: {e}")
        pass


@contextlib.contextmanager
def retry_context(
    max_attempts: int = 3,
    delay: float = 0.0,
    backoff: float = 1.0,
    exceptions: tuple = (Exception,),
) -> Generator[None, None, None]:
    """Context manager that retries the block on failure.

    Args:
        max_attempts: Maximum number of attempts.
        delay: Initial delay between retries (seconds).
        backoff: Multiplier for delay after each retry.
        exceptions: Tuple of exceptions to catch.
    """
    import time
    attempt = 0
    current_delay = delay
    while True:
        try:
            yield
            return
        except exceptions:
            attempt += 1
            if attempt >= max_attempts:
                raise
            time.sleep(current_delay)
            current_delay *= backoff


def contextmanager_decorator(
    func: Callable[..., Generator[Any, None, None]],
) -> Callable[..., Callable[..., Any]]:
    """Convert a generator function to a context manager decorator.

    Args:
        func: Generator function to convert.

    Returns:
        Decorated function.
    """
    @functools.wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        return contextlib.contextmanager(func(*args, **kwargs))
    return wrapper


class ContextVar:
    """Typed wrapper around contextvars.ContextVar."""

    def __init__(self, name: str, default: Optional[T] = None) -> None:
        self._var = ContextVar(name, default=default)
        self._name = name

    def get(self, default: Optional[T] = None) -> Optional[T]:
        """Get the current value."""
        try:
            return self._var.get()
        except LookupError:
            return default if default is not None else self._var.default

    def set(self, value: T) -> None:
        """Set the value."""
        self._var.set(value)

    def reset(self, token: Any) -> None:
        """Reset to a previous value."""
        self._var.reset(token)

    @property
    def name(self) -> str:
        """Context variable name."""
        return self._name
