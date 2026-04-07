"""contextvars action extensions for rabai_autoclick.

Provides utilities for context variable management, async context
handling, and context-local state management.
"""

from __future__ import annotations

import contextvars
import threading
from typing import Any, Callable, TypeVar, Generic
from contextlib import contextmanager
import copy

__all__ = [
    "ContextVar",
    "copy_context",
    "get_context",
    "run_in_context",
    "Token",
    "ContextLocal",
    "ContextLocalStack",
    "context_var",
    "get_current_context",
    "create_context",
    "merge_context",
    "context_collector",
    "ContextManager",
    "ContextScope",
    "thread_local_context",
    "async_context",
    "context_callback",
    "context_middleware",
    "ContextMiddleware",
]


T = TypeVar("T")


class ContextVar(contextvars.ContextVar[T]):
    """A typed context variable.

    Provides thread-safe, async-safe storage of values
    that are local to a context.
    """

    def __init__(
        self,
        name: str,
        *,
        default: T | None = None,
    ) -> None:
        super().__init__(name, default=default)

    def get_or_default(self, default: T | None = None) -> T:
        """Get value or return default if not set.

        Args:
            default: Default value.

        Returns:
            Current value or default.
        """
        try:
            value = self.get()
            return value if value is not None else default  # type: ignore
        except LookupError:
            return default  # type: ignore

    def set_safe(self, value: T) -> contextvars.Token:
        """Set value safely, handling errors.

        Args:
            value: Value to set.

        Returns:
            Token for later reset.
        """
        try:
            return self.set(value)
        except Exception:
            pass
            return self.set(value)

    def update(self, value: T | Callable[[T], T]) -> T:
        """Update value using a function or direct value.

        Args:
            value: New value or function to transform current value.

        Returns:
            New value.
        """
        if callable(value) and not isinstance(value, type):
            try:
                current = self.get()
                new_value = value(current)
            except Exception:
                new_value = value(None)  # type: ignore
        else:
            new_value = value
        self.set(new_value)
        return new_value


def context_var(
    name: str,
    default: T | None = None,
) -> ContextVar[T]:
    """Create a new context variable.

    Args:
        name: Variable name.
        default: Default value.

    Returns:
        New ContextVar instance.
    """
    return ContextVar(name, default=default)  # type: ignore


class ContextLocal(Generic[T]):
    """Thread-safe context-local storage.

    Provides per-context storage that works correctly
    with async code and contextvars.
    """

    def __init__(self, default: T | None = None) -> None:
        self._var = ContextVar[T]("context_local", default=default)
        self._default = default

    def get(self) -> T:
        """Get current value.

        Returns:
            Current value or default.
        """
        return self._var.get_or_default(self._default)

    def set(self, value: T) -> None:
        """Set current value.

        Args:
            value: Value to set.
        """
        self._var.set(value)

    def reset(self) -> None:
        """Reset to default value."""
        self._var.set(self._default)  # type: ignore

    @property
    def value(self) -> T:
        """Get current value (property form)."""
        return self.get()

    @value.setter
    def value(self, val: T) -> None:
        """Set current value (property form)."""
        self.set(val)

    def __call__(self) -> T:
        """Get value when called directly."""
        return self.get()


class ContextLocalStack(Generic[T]):
    """Stack-based context-local storage.

    Allows pushing/popping values while preserving
    the previous state.
    """

    def __init__(self, default: T | None = None) -> None:
        self._var = ContextVar[T]("context_local_stack", default=None)
        self._default = default

    def get(self) -> T | None:
        """Get current top of stack.

        Returns:
            Top value or default.
        """
        return self._var.get_or_default(self._default)

    def push(self, value: T) -> contextvars.Token:
        """Push a value onto the stack.

        Args:
            value: Value to push.

        Returns:
            Token for popping.
        """
        current = self._var.get(None)
        if isinstance(current, list):
            new_stack = current + [value]
        else:
            new_stack = [value]
        return self._var.set(new_stack)

    def pop(self) -> T | None:
        """Pop the top value from stack.

        Returns:
            Popped value or default.
        """
        current = self._var.get([])
        if not current:
            return self._default
        new_stack = current[:-1]
        if new_stack:
            self._var.set(new_stack)
        else:
            self._var.set(self._default)  # type: ignore
        return current[-1]

    @property
    def top(self) -> T | None:
        """Get top of stack without popping."""
        return self.get()


class ContextManager(Generic[T]):
    """Context manager for context variable operations.

    Example:
        var = ContextVar("request_id")

        with ContextManager(var) as ctx:
            ctx.set("123")
            # var == "123" within this context
        # var restored outside context
    """

    def __init__(self, var: ContextVar[T]) -> None:
        self._var = var
        self._token: contextvars.Token | None = None
        self._saved: T | None = None

    def __enter__(self) -> ContextManager[T]:
        """Enter context, saving current value."""
        try:
            self._saved = self._var.get()
        except LookupError:
            self._saved = None
        return self

    def __exit__(self, *args: Any) -> None:
        """Exit context, restoring previous value."""
        if self._saved is not None:
            self._var.set(self._saved)
        else:
            self._var.reset()

    def set(self, value: T) -> None:
        """Set value within this context."""
        self._token = self._var.set(value)


class ContextScope:
    """Scope for temporarily setting multiple context variables."""

    def __init__(self, **vars: Any) -> None:
        self._vars = vars
        self._tokens: dict[str, contextvars.Token] = {}
        self._saved: dict[str, Any] = {}

    def __enter__(self) -> ContextScope:
        """Enter scope, saving and setting variables."""
        for name, value in self._vars.items():
            var = self._vars.get(name)
            if isinstance(var, ContextVar):
                try:
                    self._saved[name] = var.get()
                except LookupError:
                    self._saved[name] = None
                self._tokens[name] = var.set(value)
        return self

    def __exit__(self, *args: Any) -> None:
        """Exit scope, restoring previous values."""
        for name, token in self._tokens.items():
            var = self._vars.get(name)
            if isinstance(var, ContextVar):
                if self._saved.get(name) is not None:
                    var.set(self._saved[name])
                else:
                    var.reset()


_thread_local = threading.local()


def thread_local_context() -> dict[str, Any]:
    """Get thread-local context dict.

    Returns:
        Dict unique to current thread.
    """
    if not hasattr(_thread_local, "context"):
        _thread_local.context = {}
    return _thread_local.context


@contextmanager
def async_context(var: ContextVar[T], value: T) -> Any:
    """Context manager for async context variable setting.

    Args:
        var: Context variable to set.
        value: Value to set within context.

    Yields:
        Token for the set operation.
    """
    token = var.set(value)
    try:
        yield token
    finally:
        try:
            var.reset(token)
        except ValueError:
            pass


def context_callback(
    func: Callable[..., T],
) -> Callable[..., Callable[[], T]]:
    """Decorator to cache function result in context.

    Args:
        func: Function to wrap.

    Returns:
        Wrapped function that caches result.
    """
    cache_var: ContextVar[dict[tuple, T]] = ContextVar("callback_cache", default={})

    def wrapper(*args: Any, **kwargs: Any) -> T:
        cache = cache_var.get({})
        key = (args, tuple(sorted(kwargs.items())))
        if key in cache:
            return cache[key]
        result = func(*args, **kwargs)
        cache[key] = result
        cache_var.set(cache)
        return result

    return wrapper


class ContextMiddleware:
    """Middleware pattern for context variable management.

    Allows chaining middleware that processes context.
    """

    def __init__(self) -> None:
        self._middlewares: list[Callable[[Any], Any]] = []

    def use(self, middleware: Callable[[Any], Any]) -> None:
        """Add a middleware function.

        Args:
            middleware: Function to add to chain.
        """
        self._middlewares.append(middleware)

    def process(self, data: Any) -> Any:
        """Process data through middleware chain.

        Args:
            data: Data to process.

        Returns:
            Processed data.
        """
        result = data
        for middleware in self._middlewares:
            result = middleware(result)
        return result

    def clear(self) -> None:
        """Clear all middleware."""
        self._middlewares.clear()


def get_current_context() -> contextvars.Context:
    """Get the current context.

    Returns:
        Current Context object.
    """
    return contextvars.copy_context()


def create_context(
    **vars: Any,
) -> contextvars.Context:
    """Create a new context with variables.

    Args:
        **vars: Context variables to set.

    Returns:
        New Context object.
    """
    ctx = contextvars.copy_context()
    for name, value in vars.items():
        if isinstance(value, ContextVar):
            value.set(value)  # type: ignore
    return ctx


def merge_context(
    *contexts: contextvars.Context,
) -> dict[int, Any]:
    """Merge multiple contexts.

    Args:
        *contexts: Contexts to merge.

    Returns:
        Merged context as dict.
    """
    result: dict[int, Any] = {}
    for ctx in contexts:
        result.update(ctx)
    return result


def context_collector(
    **vars: ContextVar,
) -> Callable[[Callable], Callable]:
    """Decorator to collect context variables into a dict.

    Args:
        **vars: Context variables to collect.

    Returns:
        Decorator function.
    """
    def decorator(func: Callable) -> Callable:
        def wrapper() -> dict[str, Any]:
            return {name: var.get() for name, var in vars.items()}

        wrapper.collect = wrapper  # type: ignore
        return wrapper

    return decorator
