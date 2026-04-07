"""Async utilities for RabAI AutoClick.

Provides:
- Async context managers and decorators
- Task grouping and coordination
- Timeout and cancellation utilities
- Async iterator helpers
"""

import asyncio
import functools
from typing import (
    Any,
    Awaitable,
    Callable,
    Coroutine,
    Iterable,
    List,
    Optional,
    Set,
    TypeVar,
    Union,
)

T = TypeVar("T")
T_co = TypeVar("T_co", covariant=True)


class AsyncTimeoutError(asyncio.TimeoutError):
    """Raised when an async operation times out."""
    pass


async def with_timeout(
    coro: Coroutine[Any, Any, T],
    timeout: float,
    *,
    cancel_on_timeout: bool = True,
) -> T:
    """Execute a coroutine with a timeout.

    Args:
        coro: The coroutine to execute.
        timeout: Timeout in seconds.
        cancel_on_timeout: If True, cancel the coroutine on timeout.

    Returns:
        The result of the coroutine.

    Raises:
        AsyncTimeoutError: If the operation times out.
    """
    try:
        return await asyncio.wait_for(coro, timeout=timeout)
    except asyncio.TimeoutError:
        if cancel_on_timeout:
            coro.close()
        raise AsyncTimeoutError(f"Operation timed out after {timeout}s")


async def gather_with_concurrency(
    *coros: Awaitable[T],
    max_concurrency: int = 10,
    return_exceptions: bool = False,
) -> List[T]:
    """Gather coroutines with limited concurrency.

    Args:
        *coros: Coroutines to execute.
        max_concurrency: Maximum number of concurrent tasks.
        return_exceptions: If True, return exceptions as results.

    Returns:
        List of results in the same order as input coroutines.
    """
    semaphore = asyncio.Semaphore(max_concurrency)

    async def _run(coro: Awaitable[T]) -> T:
        async with semaphore:
            return await coro

    wrapped = [_run(c) for c in coros]
    return await asyncio.gather(*wrapped, return_exceptions=return_exceptions)


async def gather_with_progress(
    coros: List[Awaitable[T]],
    *,
    progress_callback: Optional[Callable[[int, int], None]] = None,
) -> List[T]:
    """Gather coroutines with optional progress reporting.

    Args:
        coros: List of coroutines to execute.
        progress_callback: Called with (completed, total) on each completion.

    Returns:
        List of results.
    """
    results: List[T] = []
    total = len(coros)

    async def _run_with_report(coro: Awaitable[T], idx: int) -> T:
        result = await coro
        if progress_callback:
            progress_callback(idx + 1, total)
        return result

    wrapped = [_run_with_report(c, i) for i, c in enumerate(coros)]
    results = await asyncio.gather(*wrapped)
    return results


class AsyncSemaphore:
    """Async semaphore with acquire context manager."""

    def __init__(self, value: int = 1) -> None:
        self._sem = asyncio.Semaphore(value)

    async def __aenter__(self) -> "AsyncSemaphore":
        await self._sem.acquire()
        return self

    async def __aexit__(self, *args: Any) -> None:
        self._sem.release()

    async def acquire(self) -> None:
        await self._sem.acquire()

    def release(self) -> None:
        self._sem.release()


class AsyncLock:
    """Async lock with context manager support."""

    def __init__(self) -> None:
        self._lock = asyncio.Lock()

    async def __aenter__(self) -> "AsyncLock":
        await self._lock.acquire()
        return self

    async def __aexit__(self, *args: Any) -> None:
        self._lock.release()

    async def acquire(self) -> None:
        await self._lock.acquire()

    def release(self) -> None:
        self._lock.release()


class AsyncEvent:
    """Async event for signaling between tasks."""

    def __init__(self) -> None:
        self._event = asyncio.Event()

    async def wait(self) -> None:
        await self._event.wait()

    def is_set(self) -> bool:
        return self._event.is_set()

    def set(self) -> None:
        self._event.set()

    def clear(self) -> None:
        self._event.clear()


class AsyncBarrier:
    """Async barrier for synchronizing tasks."""

    def __init__(self, parties: int) -> None:
        self._barrier = asyncio.Barrier(parties)

    async def wait(self) -> int:
        return await self._barrier.wait()


async def retry_async(
    coro_fn: Callable[..., Coroutine[Any, Any, T]],
    *args: Any,
    retries: int = 3,
    delay: float = 1.0,
    backoff: float = 2.0,
    exceptions: tuple = (Exception,),
    **kwargs: Any,
) -> T:
    """Retry an async function with exponential backoff.

    Args:
        coro_fn: Async function to retry.
        *args: Positional arguments for the function.
        retries: Number of retry attempts.
        delay: Initial delay between retries (seconds).
        backoff: Multiplier for delay after each retry.
        exceptions: Tuple of exceptions to catch.
        **kwargs: Keyword arguments for the function.

    Returns:
        Result of the function call.

    Raises:
        Last exception if all retries fail.
    """
    last_exc: Optional[Exception] = None
    current_delay = delay

    for attempt in range(retries + 1):
        try:
            return await coro_fn(*args, **kwargs)
        except exceptions as e:
            last_exc = e
            if attempt < retries:
                await asyncio.sleep(current_delay)
                current_delay *= backoff
            else:
                raise last_exc


def async_to_sync(coro: Coroutine[Any, Any, T]) -> T:
    """Run a coroutine in the default event loop synchronously.

    Args:
        coro: The coroutine to run.

    Returns:
        The result of the coroutine.
    """
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(coro)

    future = asyncio.ensure_future(coro)
    return asyncio.run(future)


async def run_in_executor(
    func: Callable[..., T],
    *args: Any,
) -> T:
    """Run a blocking function in a thread pool executor.

    Args:
        func: Blocking function to run.
        *args: Arguments for the function.

    Returns:
        Result of the function call.
    """
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, func, *args)


async def sleep_with_jitter(
    base_delay: float,
    *,
    jitter: float = 0.1,
) -> None:
    """Sleep with random jitter.

    Args:
        base_delay: Base sleep duration in seconds.
        jitter: Maximum random jitter to add (as fraction of base_delay).
    """
    import random
    jitter_amount = base_delay * jitter * random.random()
    await asyncio.sleep(base_delay + jitter_amount)


async def wait_for_any(
    *tasks: Union[Awaitable[T], asyncio.Task[T]],
    timeout: Optional[float] = None,
) -> tuple[int, T]:
    """Wait for any one task to complete.

    Args:
        *tasks: Tasks to wait on.
        timeout: Optional timeout in seconds.
        return_when: When to return (FIRST_COMPLETED, FIRST_EXCEPTION, etc.)

    Returns:
        Tuple of (index, result) of the completed task.
    """
    if not tasks:
        raise ValueError("At least one task required")

    if len(tasks) == 1:
        result = await tasks[0]
        return (0, result)

    done, _ = await asyncio.wait(
        [asyncio.ensure_future(t) for t in tasks],
        timeout=timeout,
        return_when=asyncio.FIRST_COMPLETED,
    )

    for i, task in enumerate(tasks):
        if task.done():
            return (i, task.result())

    raise asyncio.TimeoutError("No task completed within timeout")


async def cancel_tasks(tasks: List[asyncio.Task[T]]) -> None:
    """Cancel a list of tasks gracefully.

    Args:
        tasks: List of tasks to cancel.
    """
    for task in tasks:
        if not task.done():
            task.cancel()

    await asyncio.gather(*tasks, return_exceptions=True)


def create_task(
    coro: Coroutine[Any, Any, T],
    *,
    name: Optional[str] = None,
    early_cancel: Optional[asyncio.Event] = None,
) -> asyncio.Task[T]:
    """Create a task with optional early cancellation.

    Args:
        coro: The coroutine to wrap in a task.
        name: Optional task name.
        early_cancel: Optional event that, if set, cancels the task early.

    Returns:
        The created task.
    """
    async def _watch_cancel() -> T:
        result = await coro
        if early_cancel is not None:
            early_cancel.set()
        return result

    return asyncio.create_task(_watch_cancel(), name=name)
