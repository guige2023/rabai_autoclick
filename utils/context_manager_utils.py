"""Context manager utilities for RabAI AutoClick.

Provides:
- Async context managers
- Reusable context managers
- Context manager factories
- Nested context managers
"""

from __future__ import annotations

import asyncio
import contextlib
from typing import (
    Any,
    AsyncIterator,
    Callable,
    Generator,
    Iterator,
    Optional,
    TypeVar,
)


T = TypeVar("T")


class Timer(contextlib.ContextDecorator):
    """Context manager that measures execution time.

    Example:
        with Timer() as t:
            do_work()

        print(f"Took {t.elapsed:.2f}s")
    """

    def __init__(self) -> None:
        self.start: float = 0
        self.end: float = 0
        self.elapsed: float = 0

    def __enter__(self) -> Timer:
        import time
        self.start = time.perf_counter()
        return self

    def __exit__(self, *args: Any) -> None:
        import time
        self.end = time.perf_counter()
        self.elapsed = self.end - self.start


class AsyncTimer(contextlib.AbstractAsyncContextManager):
    """Async context manager for measuring execution time."""

    def __init__(self) -> None:
        self.start: float = 0
        self.end: float = 0
        self.elapsed: float = 0

    async def __aenter__(self) -> AsyncTimer:
        self.start = asyncio.get_event_loop().time()
        return self

    async def __aexit__(self, *args: Any) -> None:
        self.end = asyncio.get_event_loop().time()
        self.elapsed = self.end - self.start


class ResourceTracker(contextlib.ContextDecorator):
    """Track resources acquired within a context.

    Example:
        with ResourceTracker() as tracker:
            f = open("file.txt")
            tracker.add_resource(lambda: f.close(), f)

        # All tracked resources are cleaned up on exit
    """

    def __init__(self) -> None:
        self._resources: List[tuple[Callable[[], None], Any]] = []
        self._entered = False

    def add_resource(
        self,
        cleanup: Callable[[], None],
        resource: Any = None,
    ) -> None:
        if not self._entered:
            raise RuntimeError("Cannot add resource before entering context")
        self._resources.append((cleanup, resource))

    def __enter__(self) -> ResourceTracker:
        self._entered = True
        return self

    def __exit__(self, *args: Any) -> None:
        for cleanup, _ in reversed(self._resources):
            try:
                cleanup()
            except Exception:
                pass
        self._resources.clear()
        self._entered = False


@contextlib.contextmanager
def temp_override(
    target: dict,
    updates: dict,
) -> Generator[None, None, None]:
    """Temporarily override dictionary values.

    Example:
        config = {"debug": False}
        with temp_override(config, {"debug": True}):
            # config["debug"] is True here
            pass
        # config["debug"] is False again
    """
    original = {}
    for key, value in updates.items():
        original[key] = target.get(key)
        target[key] = value

    try:
        yield
    finally:
        for key, original_value in original.items():
            if original_value is None:
                target.pop(key, None)
            else:
                target[key] = original_value


@contextlib.contextmanager
def temp_cwd(
    path: str,
) -> Generator[None, None, None]:
    """Temporarily change working directory.

    Example:
        with temp_cwd("/tmp"):
            # Work in /tmp
            pass
    """
    import os
    original = os.getcwd()
    os.chdir(path)
    try:
        yield
    finally:
        os.chdir(original)


@contextlib.contextmanager
def swallow_exceptions(
    *exception_types: type,
) -> Generator[bool, None, None]:
    """Context manager that suppresses specified exceptions.

    Example:
        with swallow_exceptions(ValueError, TypeError) as suppressed:
            risky_operation()

        if suppressed:
            print("Exception was suppressed")
    """
    suppressed = False
    try:
        yield suppressed
    except exception_types:  # type: ignore
        suppressed = True

    if suppressed:
        suppressed = True
    return suppress


@contextlib.contextmanager
def timed_block(
    name: str,
    logger: Optional[Callable[[str], None]] = None,
) -> Generator[None, None, None]:
    """Context manager that logs entry and exit of a block.

    Example:
        with timed_block("database_query", print):
            query_database()
    """
    import time
    if logger:
        logger(f"Entering: {name}")
    start = time.perf_counter()
    try:
        yield
    finally:
        elapsed = time.perf_counter() - start
        if logger:
            logger(f"Exiting: {name} ({elapsed:.4f}s)")


class BoundedSemaphore(contextlib.ContextDecorator):
    """Semaphore with bounded concurrency limit."""

    def __init__(self, limit: int) -> None:
        self._semaphore: Optional[asyncio.Semaphore] = None
        self._limit = limit

    def __enter__(self) -> BoundedSemaphore:
        self._semaphore = asyncio.Semaphore(self._limit)
        return self._semaphore.__enter__()

    def __exit__(self, *args: Any) -> None:
        if self._semaphore:
            self._semaphore.__exit__(*args)


async def asynccontextmanager(
    func: Callable[[], AsyncIterator[T]],
) -> Callable[[], AsyncIterator[T]]:
    """Decorator to convert async generator to async context manager."""
    @functools.wraps(func)
    async def wrapper() -> AsyncIterator[T]:
        async for item in func():
            yield item
    return wrapper


import functools


@contextlib.contextmanager
def closing(
    resource: Any,
) -> Generator[Any, None, None]:
    """Context manager that closes resource on exit."""
    try:
        yield resource
    finally:
        if hasattr(resource, "close"):
            resource.close()
