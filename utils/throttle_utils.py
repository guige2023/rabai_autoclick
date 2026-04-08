"""
Throttle and debounce utilities for rate limiting function calls.

Provides throttling (limit execution frequency) and debouncing
(delay until idle) decorators with configurable timing.

Example:
    >>> from utils.throttle_utils import debounce, throttle
    >>> @debounce(0.5)
    ... def on_resize():
    ...     pass
    >>> @throttle(1.0)
    ... def on_click():
    ...     pass
"""

from __future__ import annotations

import asyncio
import functools
import threading
import time
from typing import Any, Callable, Optional


def debounce(
    wait: float,
    leading: bool = False,
    trailing: bool = True,
) -> Callable:
    """
    Decorator that delays execution until after `wait` seconds
    of inactivity.

    Args:
        wait: Seconds to wait after last call.
        leading: Execute on leading edge (first call).
        trailing: Execute on trailing edge (after wait).

    Returns:
        Decorated function.
    """
    def decorator(func: Callable) -> Callable:
        timer: List[Optional[float]] = [None]
        args_store: list = []
        kwargs_store: list = []

        def wrapper(*args: Any, **kwargs: Any) -> Any:
            args_store.append(args)
            kwargs_store.append(kwargs)

            def call_it() -> None:
                if trailing and args_store:
                    args = args_store.pop(0)
                    kwargs = kwargs_store.pop(0) if kwargs_store else {}
                    func(*args, **kwargs)

            if leading and timer[0] is None:
                func(*args, **kwargs)

            if timer[0] is not None:
                threading_timer = threading.Timer(wait, call_it)
                timer[0] = threading_timer
                threading_timer.start()

        return wrapper
    return decorator


def throttle(
    rate: float,
    leading: bool = True,
    trailing: bool = True,
) -> Callable:
    """
    Decorator that limits execution to at most once per `rate` seconds.

    Args:
        rate: Minimum seconds between executions.
        leading: Execute on leading edge.
        trailing: Execute on trailing edge.

    Returns:
        Decorated function.
    """
    def decorator(func: Callable) -> Callable:
        last_call = [0.0]
        pending = [False]

        def wrapper(*args: Any, **kwargs: Any) -> Any:
            now = time.monotonic()
            time_since_last = now - last_call[0]

            if time_since_last >= rate:
                if leading:
                    last_call[0] = now
                    return func(*args, **kwargs)
                elif trailing:
                    last_call[0] = now
                    return func(*args, **kwargs)
            else:
                if trailing and not pending[0]:
                    pending[0] = True
                    delay = rate - time_since_last
                    threading_timer = threading.Timer(delay, func, args, kwargs)
                    threading_timer.start()
                    threading_timer.join()

            return None

        return wrapper
    return decorator


class AsyncDebounce:
    """
    Async debouncer for async functions.

    Delays execution until after wait seconds of inactivity.
    """

    def __init__(
        self,
        wait: float,
        leading: bool = False,
        trailing: bool = True,
    ) -> None:
        """
        Initialize the async debouncer.

        Args:
            wait: Seconds to wait after last call.
            leading: Execute on leading edge.
            trailing: Execute on trailing edge.
        """
        self.wait = wait
        self.leading = leading
        self.trailing = trailing
        self._timer: Optional[asyncio.Task] = None
        self._pending_args: Optional[tuple] = None
        self._pending_kwargs: Optional[dict] = None

    async def call(
        self,
        coro: Callable,
        *args: Any,
        **kwargs: Any
    ) -> Any:
        """
        Call the debounced function.

        Args:
            coro: Async coroutine to execute.
            *args: Positional arguments.
            **kwargs: Keyword arguments.

        Returns:
            Result of coroutine execution.
        """
        self._pending_args = args
        self._pending_kwargs = kwargs

        if self.leading and self._timer is None:
            return await coro(*args, **kwargs)

        if self._timer is not None:
            self._timer.cancel()

        self._timer = asyncio.create_task(self._execute(coro))

    async def _execute(self, coro: Callable) -> Any:
        """Execute after wait period."""
        await asyncio.sleep(self.wait)

        if self.trailing and self._pending_args is not None:
            result = await coro(*self._pending_args, **self._pending_kwargs)
            self._pending_args = None
            self._pending_kwargs = None
            return result

        return None


def async_debounce(
    wait: float,
    leading: bool = False,
    trailing: bool = True,
) -> Callable:
    """
    Async decorator for debouncing.

    Args:
        wait: Seconds to wait.
        leading: Execute on leading edge.
        trailing: Execute on trailing edge.

    Returns:
        Decorated async function.
    """
    debouncer = AsyncDebounce(wait, leading, trailing)

    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            return await debouncer.call(func, *args, **kwargs)
        return wrapper
    return decorator


class ThrottledFunction:
    """
    Throttled function with explicit control.

    Useful when you need to throttle without a decorator.
    """

    def __init__(
        self,
        func: Callable,
        rate: float,
        leading: bool = True,
        trailing: bool = True,
    ) -> None:
        """
        Initialize the throttled function.

        Args:
            func: Function to throttle.
            rate: Minimum seconds between executions.
            leading: Execute on leading edge.
            trailing: Execute on trailing edge.
        """
        self.func = func
        self.rate = rate
        self.leading = leading
        self.trailing = trailing
        self._last_call = 0.0
        self._pending_timer: Optional[threading.Timer] = None

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        """Call the throttled function."""
        now = time.monotonic()
        time_since_last = now - self._last_call

        if time_since_last >= self.rate:
            if self.leading:
                self._last_call = now
                return self.func(*args, **kwargs)
        else:
            if self.trailing and self._pending_timer is None:
                delay = self.rate - time_since_last
                self._pending_timer = threading.Timer(
                    delay, self.func, args, kwargs
                )
                self._pending_timer.start()

        return None

    def cancel(self) -> None:
        """Cancel any pending trailing execution."""
        if self._pending_timer is not None:
            self._pending_timer.cancel()
            self._pending_timer = None


class DebouncedFunction:
    """
    Debounced function with explicit control.
    """

    def __init__(
        self,
        func: Callable,
        wait: float,
        leading: bool = False,
        trailing: bool = True,
    ) -> None:
        """
        Initialize the debounced function.

        Args:
            func: Function to debounce.
            wait: Seconds to wait after last call.
            leading: Execute on leading edge.
            trailing: Execute on trailing edge.
        """
        self.func = func
        self.wait = wait
        self.leading = leading
        self.trailing = trailing
        self._timer: Optional[threading.Timer] = None
        self._last_args: Optional[tuple] = None
        self._last_kwargs: Optional[dict] = None

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        """Call the debounced function."""
        self._last_args = args
        self._last_kwargs = kwargs

        if self.leading and self._timer is None:
            return self.func(*args, **kwargs)

        if self._timer is not None:
            self._timer.cancel()

        def call_trailing() -> Any:
            if self.trailing and self._last_args is not None:
                return self.func(*self._last_args, **self._last_kwargs)

        self._timer = threading.Timer(self.wait, call_trailing)
        self._timer.start()

        return None

    def flush(self) -> Any:
        """Execute immediately if there's a pending call."""
        if self._timer is not None:
            self._timer.cancel()
            self._timer = None

        if self._last_args is not None and self.trailing:
            return self.func(*self._last_args, **self._last_kwargs)

        return None

    def cancel(self) -> None:
        """Cancel any pending execution."""
        if self._timer is not None:
            self._timer.cancel()
            self._timer = None


def create_throttle(
    func: Callable,
    rate: float,
    **kwargs
) -> ThrottledFunction:
    """
    Factory to create a throttled function.

    Args:
        func: Function to throttle.
        rate: Minimum seconds between executions.
        **kwargs: Additional arguments.

    Returns:
        ThrottledFunction instance.
    """
    return ThrottledFunction(func, rate, **kwargs)


def create_debounce(
    func: Callable,
    wait: float,
    **kwargs
) -> DebouncedFunction:
    """
    Factory to create a debounced function.

    Args:
        func: Function to debounce.
        wait: Seconds to wait.
        **kwargs: Additional arguments.

    Returns:
        DebouncedFunction instance.
    """
    return DebouncedFunction(func, wait, **kwargs)
