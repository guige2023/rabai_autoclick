"""Context manager utilities for RabAI AutoClick.

Provides:
- Context manager helpers
- Resource management
"""

import contextlib
import threading
from typing import Any, Callable, Generator, Optional, TypeVar


T = TypeVar("T")


@contextlib.contextmanager
def lock(lock_obj: threading.Lock, timeout: Optional[float] = None) -> Generator:
    """Context manager for acquiring a lock.

    Args:
        lock_obj: Lock to acquire.
        timeout: Optional timeout in seconds.

    Yields:
        Lock object if acquired.
    """
    acquired = lock_obj.acquire(timeout=timeout if timeout else -1)
    if not acquired:
        raise TimeoutError("Failed to acquire lock")
    try:
        yield lock_obj
    finally:
        lock_obj.release()


@contextlib.contextmanager
def temp_override(obj: Any, attr: str, value: Any) -> Generator:
    """Temporarily override an attribute.

    Args:
        obj: Object with attribute.
        attr: Attribute name.
        value: Temporary value.

    Yields:
        The temporary value.
    """
    original = getattr(obj, attr)
    setattr(obj, attr, value)
    try:
        yield value
    finally:
        setattr(obj, attr, original)


@contextlib.contextmanager
def working_directory(path: str) -> Generator:
    """Temporarily change working directory.

    Args:
        path: New working directory.

    Yields:
        New working directory path.
    """
    import os
    old_cwd = os.getcwd()
    try:
        os.chdir(path)
        yield path
    finally:
        os.chdir(old_cwd)


@contextlib.contextmanager
def suppress(*exceptions: type) -> Generator:
    """Suppress specified exceptions.

    Args:
        *exceptions: Exception types to suppress.

    Yields:
        None.
    """
    try:
        yield
    except exceptions:
        pass


@contextlib.contextmanager
def ignored(*exceptions: type) -> Generator:
    """Ignore specified exceptions (alias for suppress).

    Args:
        *exceptions: Exception types to ignore.

    Yields:
        None.
    """
    with suppress(*exceptions):
        yield


class ContextStack:
    """Stack of context managers.

    Allows pushing and popping context managers.

    Usage:
        stack = ContextStack()
        stack.push(open_file("a.txt"))
        stack.push(open_file("b.txt"))
        # Use files...
        stack.pop_all()
    """

    def __init__(self) -> None:
        self._stack: list = []

    def push(self, ctx: contextlib.AbstractContextManager) -> None:
        """Push a context manager onto the stack.

        Args:
            ctx: Context manager to push.
        """
        ctx.__enter__()
        self._stack.append(ctx)

    def pop(self) -> None:
        """Pop the top context manager."""
        if self._stack:
            ctx = self._stack.pop()
            ctx.__exit__(None, None, None)

    def pop_all(self) -> None:
        """Pop all context managers."""
        while self._stack:
            self.pop()

    def __len__(self) -> int:
        """Get stack depth."""
        return len(self._stack)


@contextlib.contextmanager
def nested(*managers: contextlib.AbstractContextManager) -> Generator:
    """Enter multiple context managers simultaneously.

    Args:
        *managers: Context managers to enter.

    Yields:
        Tuple of entered values.
    """
    if not managers:
        yield
        return

    # Enter all managers
    values = []
    try:
        for mgr in managers:
            values.append(mgr.__enter__())
        yield tuple(values)
    except Exception:
        # Exit in reverse order
        for mgr in reversed(managers):
            mgr.__exit__(*sys.exc_info())
        raise
    finally:
        # Exit any that were entered
        for mgr in reversed(managers[:len(values)]):
            if sys.exc_info()[0] is None:
                mgr.__exit__(None, None, None)


import sys


@contextlib.contextmanager
def redirect_stdout(new_target: Any) -> Generator:
    """Redirect stdout temporarily.

    Args:
        new_target: New stdout target.

    Yields:
        New stdout target.
    """
    old_target = sys.stdout
    sys.stdout = new_target
    try:
        yield new_target
    finally:
        sys.stdout = old_target


@contextlib.contextmanager
def redirect_stderr(new_target: Any) -> Generator:
    """Redirect stderr temporarily.

    Args:
        new_target: New stderr target.

    Yields:
        New stderr target.
    """
    old_target = sys.stderr
    sys.stderr = new_target
    try:
        yield new_target
    finally:
        sys.stderr = old_target


@contextlib.contextmanager
def capture_stdout() -> Generator:
    """Capture stdout to StringIO.

    Yields:
        StringIO object with captured output.
    """
    import io
    buffer = io.StringIO()
    with redirect_stdout(buffer):
        yield buffer


@contextlib.contextmanager
def capture_stderr() -> Generator:
    """Capture stderr to StringIO.

    Yields:
        StringIO object with captured output.
    """
    import io
    buffer = io.StringIO()
    with redirect_stderr(buffer):
        yield buffer


class ExitStack:
    """Manages multiple context managers.

    Alternative to contextlib.ExitStack with a cleaner interface.
    """

    def __init__(self) -> None:
        self._stack: list = []

    def enter(self, ctx: contextlib.AbstractContextManager) -> Any:
        """Enter and track a context manager.

        Args:
            ctx: Context manager.

        Returns:
            Value from __enter__.
        """
        result = ctx.__enter__()
        self._stack.append(ctx)
        return result

    def callback(self, func: Callable, *args: Any, **kwargs: Any) -> None:
        """Register a callback to be called on exit.

        Args:
            func: Function to call.
            *args: Positional arguments.
            **kwargs: Keyword arguments.
        """
        @contextlib.contextmanager
        def _ctx():
            yield
            func(*args, **kwargs)
        self._stack.append(_ctx())

    def pop(self) -> None:
        """Pop and exit top context manager."""
        if self._stack:
            ctx = self._stack.pop()
            ctx.__exit__(None, None, None)

    def __enter__(self) -> 'ExitStack':
        return self

    def __exit__(self, *args: Any) -> None:
        while self._stack:
            self.pop()


def contextmanager(func: Callable[..., Generator]) -> Callable[..., contextlib.AbstractContextManager]:
    """Decorator to convert generator to context manager.

    Args:
        func: Generator function.

    Returns:
        Context manager function.
    """
    return contextlib.contextmanager(func)