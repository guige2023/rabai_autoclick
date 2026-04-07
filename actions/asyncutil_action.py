"""asyncutil_action module for rabai_autoclick.

Provides async utilities: async context managers, Future wrappers,
concurrency helpers, and async iteration utilities.
"""

from __future__ import annotations

import asyncio
import threading
import time
from collections import deque
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Generic, Iterator, List, Optional, TypeVar, Union

__all__ = [
    "async_timeout",
    "async_retry",
    "async_first_completed",
    "async_wait_all",
    "async_gather",
    "AsyncBatch",
    "FuturePool",
    "AsyncIterator",
    "await_result",
    "run_in_executor",
]


T = TypeVar("T")
U = TypeVar("U")


async def async_timeout(coro: Awaitable[T], seconds: float) -> T:
    """Execute coroutine with timeout.

    Args:
        coro: Coroutine to execute.
        seconds: Timeout in seconds.

    Returns:
        Coroutine result.

    Raises:
        asyncio.TimeoutError: If timeout exceeded.
    """
    return await asyncio.wait_for(coro, timeout=seconds)


async def async_retry(
    coro_func: Callable[..., Awaitable[T]],
    *args: Any,
    max_attempts: int = 3,
    delay: float = 1.0,
    backoff: float = 2.0,
    exceptions: tuple = (Exception,),
    **kwargs: Any,
) -> T:
    """Retry async function on failure.

    Args:
        coro_func: Async function to retry.
        *args: Positional args for function.
        max_attempts: Maximum retry attempts.
        delay: Initial delay between retries.
        backoff: Backoff multiplier for delay.
        exceptions: Tuple of exceptions to catch.
        **kwargs: Keyword args for function.

    Returns:
        Function result.

    Raises:
        Last exception if all attempts fail.
    """
    last_error: Optional[Exception] = None
    current_delay = delay
    for attempt in range(max_attempts):
        try:
            return await coro_func(*args, **kwargs)
        except exceptions as e:
            last_error = e
            if attempt < max_attempts - 1:
                await asyncio.sleep(current_delay)
                current_delay *= backoff
    if last_error:
        raise last_error


async def async_first_completed(
    *aws: Awaitable[T],
    timeout: Optional[float] = None,
) -> T:
    """Wait for first coroutine to complete.

    Args:
        *aws: Coroutines to wait on.
        timeout: Optional timeout.

    Returns:
        Result from first completed coroutine.
    """
    done, _ = await asyncio.wait(aws, timeout=timeout, return_when=asyncio.FIRST_COMPLETED)
    for d in done:
        return d.result()
    raise asyncio.TimeoutError()


async def async_wait_all(
    *aws: Awaitable[T],
    timeout: Optional[float] = None,
) -> List[T]:
    """Wait for all coroutines to complete.

    Args:
        *aws: Coroutines to wait on.
        timeout: Optional timeout.

    Returns:
        List of results.
    """
    done, _ = await asyncio.wait(aws, timeout=timeout)
    return [d.result() for d in done]


async def async_gather(
    *aws: Awaitable[T],
    return_exceptions: bool = False,
) -> List[T]:
    """Gather results from multiple coroutines.

    Args:
        *aws: Coroutines to gather.
        return_exceptions: Return exceptions instead of raising.

    Returns:
        List of results in order.
    """
    return await asyncio.gather(*aws, return_exceptions=return_exceptions)


class AsyncBatch(Generic[T]):
    """Batch async operations for efficiency."""

    def __init__(
        self,
        batch_size: int = 10,
        max_wait: float = 1.0,
    ) -> None:
        self.batch_size = batch_size
        self.max_wait = max_wait
        self._buffer: List[T] = []
        self._results: deque = deque()
        self._lock = asyncio.Lock()
        self._not_empty = asyncio.Event()
        self._closed = False

    async def add(self, item: T) -> None:
        """Add item to batch."""
        async with self._lock:
            self._buffer.append(item)
            if len(self._buffer) >= self.batch_size:
                self._not_empty.set()

    async def get_batch(self) -> List[T]:
        """Get next batch of items."""
        async with self._lock:
            while not self._buffer and not self._closed:
                await asyncio.wait_for(
                    asyncio.sleep(0.1),
                    timeout=self.max_wait,
                )
            batch = list(self._buffer)
            self._buffer.clear()
            self._not_empty.clear()
            return batch

    def close(self) -> None:
        """Close batch."""
        self._closed = True
        self._not_empty.set()


class FuturePool:
    """Pool for managing concurrent futures."""

    def __init__(self, max_workers: int = 4) -> None:
        self.max_workers = max_workers
        self._executor = ThreadPoolExecutor(max_workers=max_workers)
        self._futures: List[Future] = []
        self._lock = threading.Lock()

    def submit(
        self,
        func: Callable,
        *args: Any,
        **kwargs: Any,
    ) -> Future:
        """Submit function for execution.

        Args:
            func: Function to execute.
            *args: Positional args.
            **kwargs: Keyword args.

        Returns:
            Future representing the execution.
        """
        future = self._executor.submit(func, *args, **kwargs)
        with self._lock:
            self._futures.append(future)
        return future

    def wait_all(self, timeout: Optional[float] = None) -> List[Any]:
        """Wait for all futures to complete.

        Args:
            timeout: Optional timeout.

        Returns:
            List of results.
        """
        results = []
        with self._lock:
            futures = list(self._futures)
        for f in futures:
            try:
                results.append(f.result(timeout=timeout))
            except Exception as e:
                results.append(e)
        return results

    def cancel_all(self) -> int:
        """Cancel all pending futures.

        Returns:
            Number of futures cancelled.
        """
        cancelled = 0
        with self._lock:
            for f in self._futures:
                if f.cancel():
                    cancelled += 1
        return cancelled

    def shutdown(self, wait: bool = True) -> None:
        """Shutdown the executor."""
        self._executor.shutdown(wait=wait)

    def __enter__(self) -> "FuturePool":
        return self

    def __exit__(self, *args: Any) -> None:
        self.shutdown()


class AsyncIterator(Generic[T]):
    """Async iterator wrapper for sync iterables."""

    def __init__(self, iterable: Iterator[T]) -> None:
        self._iterable = iterable

    def __aiter__(self) -> "AsyncIterator[T]":
        return self

    async def __anext__(self) -> T:
        """Get next item asynchronously."""
        while True:
            try:
                item = next(self._iterable)
                return item
            except StopIteration:
                raise StopAsyncIteration


async def await_result(
    future_or_coro: Union[Future, Awaitable[T]],
) -> T:
    """Await a future or coroutine uniformly.

    Args:
        future_or_coro: Future or coroutine to await.

    Returns:
        Result of the future/coroutine.
    """
    if asyncio.isfuture(future_or_coro):
        return future_or_coro.result()
    return await future_or_coro


def run_in_executor(
    func: Callable,
    *args: Any,
    executor: Optional[Any] = None,
) -> asyncio.Future:
    """Run sync function in executor.

    Args:
        func: Function to run.
        *args: Positional args.
        executor: Optional executor.

    Returns:
        Future representing the execution.
    """
    loop = asyncio.get_event_loop()
    return loop.run_in_executor(executor, func, *args)
