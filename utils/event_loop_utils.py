"""
Async event loop utilities and helpers.

Provides event loop management, scheduling, and coordination utilities.
"""

from __future__ import annotations

import asyncio
import threading
import time
from typing import Any, Callable, Coroutine


def get_running_loop() -> asyncio.AbstractEventLoop | None:
    """Get the currently running event loop."""
    try:
        return asyncio.get_running_loop()
    except RuntimeError:
        return None


def get_or_create_event_loop() -> asyncio.AbstractEventLoop:
    """Get or create an event loop for current thread."""
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    return loop


class AsyncScheduler:
    """Async task scheduler with interval support."""

    def __init__(self, loop: asyncio.AbstractEventLoop | None = None):
        self.loop = loop or get_or_create_event_loop()
        self._tasks: set[asyncio.Task] = set()
        self._running = False

    async def schedule_interval(
        self,
        coro: Coroutine,
        interval: float,
        name: str | None = None,
    ) -> asyncio.Task:
        """
        Schedule a coroutine to run at fixed intervals.

        Args:
            coro: Coroutine to run
            interval: Interval in seconds
            name: Optional task name

        Returns:
            asyncio.Task
        """
        async def wrapper() -> None:
            while True:
                await coro
                await asyncio.sleep(interval)

        task = asyncio.create_task(wrapper(), name=name)
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)
        return task

    async def schedule_delay(
        self,
        coro: Coroutine,
        delay: float,
        name: str | None = None,
    ) -> Any:
        """
        Schedule a coroutine to run after a delay.

        Args:
            coro: Coroutine to run
            delay: Delay in seconds
            name: Optional task name

        Returns:
            Task result
        """
        await asyncio.sleep(delay)
        return await coro

    def cancel_all(self) -> None:
        """Cancel all scheduled tasks."""
        for task in self._tasks:
            task.cancel()


class AsyncBatcher:
    """Batch async operations for efficiency."""

    def __init__(
        self,
        batch_size: int = 10,
        max_wait: float = 0.1,
    ):
        self.batch_size = batch_size
        self.max_wait = max_wait
        self._queue: list[tuple[asyncio.Future, Callable[[], Coroutine]]] = []
        self._lock = asyncio.Lock()
        self._processing = False

    async def submit(
        self,
        coro_factory: Callable[[], Coroutine],
    ) -> Any:
        """
        Submit a coroutine for batched execution.

        Args:
            coro_factory: Factory that produces the coroutine

        Returns:
            Result of the coroutine
        """
        future = asyncio.get_event_loop().create_future()
        async with self._lock:
            self._queue.append((future, coro_factory))
            if len(self._queue) >= self.batch_size:
                await self._flush()
        return await future

    async def _flush(self) -> None:
        if not self._queue or self._processing:
            return
        self._processing = True
        try:
            batch = self._queue[:self.batch_size]
            self._queue[:self.batch_size] = []
            for future, factory in batch:
                try:
                    result = await factory()
                    future.set_result(result)
                except Exception as e:
                    future.set_exception(e)
        finally:
            self._processing = False


def run_async_from_thread(
    coro: Coroutine,
    loop: asyncio.AbstractEventLoop | None = None,
) -> Any:
    """
    Run coroutine from a synchronous thread.

    Args:
        coro: Coroutine to run
        loop: Target event loop (uses get_or_create_event_loop if None)

    Returns:
        Coroutine result
    """
    loop = loop or get_or_create_event_loop()
    return loop.run_until_complete(coro)


class EventLoopPool:
    """Pool of event loops for multi-threaded async."""

    def __init__(self, size: int = 4):
        self.size = size
        self._loops: list[asyncio.AbstractEventLoop] = []
        self._threads: list[threading.Thread] = []
        self._round_robin = 0
        self._lock = threading.Lock()
        self._started = False

    def start(self) -> None:
        """Start the event loop pool."""
        with self._lock:
            if self._started:
                return
            for i in range(self.size):
                loop = asyncio.new_event_loop()
                thread = threading.Thread(target=loop.run_forever, daemon=True)
                self._loops.append(loop)
                self._threads.append(thread)
                thread.start()
            self._started = True

    def get_loop(self) -> asyncio.AbstractEventLoop:
        """Get next event loop in round-robin."""
        with self._lock:
            idx = self._round_robin % self.size
            self._round_robin += 1
            return self._loops[idx]

    def stop(self) -> None:
        """Stop all event loops."""
        for loop in self._loops:
            loop.call_soon_threadsafe(loop.stop)


async def gather_with_concurrency(
    max_concurrent: int,
    *coros: Coroutine,
) -> list[Any]:
    """
    Gather coroutines with concurrency limit.

    Args:
        max_concurrent: Maximum concurrent executions
        *coros: Coroutines to execute

    Returns:
        List of results
    """
    semaphore = asyncio.Semaphore(max_concurrent)

    async def sem_coro(coro: Coroutine) -> Any:
        async with semaphore:
            return await coro

    return await asyncio.gather(*(sem_coro(c) for c in coros))
