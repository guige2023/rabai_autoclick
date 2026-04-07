"""Concurrency utilities: async helpers, thread pools, futures, and synchronization primitives."""

from __future__ import annotations

import asyncio
import concurrent.futures
import threading
from collections.abc import Awaitable
from concurrent.futures import Future
from dataclasses import dataclass
from typing import Any, Callable, TypeVar

__all__ = [
    "AsyncHelper",
    "ThreadPool",
    "FutureGroup",
    "run_in_executor",
    "gather_with_concurrency",
]

T = TypeVar("T")


@dataclass
class FutureGroup:
    """Manages a group of futures and collects their results."""
    futures: list[Future[Any]] = None
    _results: list[Any] = None
    _exceptions: list[Exception] = None

    def __post_init__(self) -> None:
        self.futures = []
        self._results = []
        self._exceptions = []

    def add(self, future: Future[Any]) -> None:
        self.futures.append(future)

    def wait(self, timeout: float | None = None) -> tuple[list[Any], list[Exception]]:
        concurrent.futures.wait(self.futures, timeout=timeout)
        results: list[Any] = []
        exceptions: list[Exception] = []
        for f in self.futures:
            if f.exception():
                exceptions.append(f.exception())  # type: ignore
            else:
                try:
                    results.append(f.result())
                except Exception as e:
                    exceptions.append(e)
        self._results = results
        self._exceptions = exceptions
        return results, exceptions

    @property
    def results(self) -> list[Any]:
        return self._results

    @property
    def exceptions(self) -> list[Exception]:
        return self._exceptions


class ThreadPool:
    """Thread pool executor with convenient interface."""

    def __init__(
        self,
        max_workers: int | None = None,
        thread_name_prefix: str = "",
    ) -> None:
        self._pool = concurrent.futures.ThreadPoolExecutor(
            max_workers=max_workers,
            thread_name_prefix=thread_name_prefix,
        )
        self._futures: list[Future[Any]] = []

    def submit(
        self,
        fn: Callable[..., T],
        *args: Any,
        **kwargs: Any,
    ) -> Future[T]:
        future = self._pool.submit(fn, *args, **kwargs)
        self._futures.append(future)
        return future

    def map(
        self,
        fn: Callable[[Any], T],
        *iterables: Any,
        timeout: float | None = None,
    ) -> list[T]:
        return list(self._pool.map(fn, *iterables))

    def shutdown(self, wait: bool = True) -> None:
        self._pool.shutdown(wait=wait)

    def __enter__(self) -> "ThreadPool":
        return self

    def __exit__(self, *args: Any) -> None:
        self.shutdown()


class AsyncHelper:
    """Async/await utility helpers."""

    @staticmethod
    async def sleep(seconds: float) -> None:
        await asyncio.sleep(seconds)

    @staticmethod
    async def gather(*tasks: Awaitable[T], return_exceptions: bool = False) -> list[T]:
        return await asyncio.gather(*tasks, return_exceptions=return_exceptions)

    @staticmethod
    async def wait_for(
        coro: Awaitable[T],
        timeout: float | None = None,
    ) -> T:
        return await asyncio.wait_for(coro, timeout=timeout)

    @staticmethod
    async def create_task(coro: Awaitable[T]) -> asyncio.Task[T]:
        return asyncio.create_task(coro)

    @staticmethod
    async def run_in_thread(fn: Callable[..., T], *args: Any) -> T:
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, fn, *args)


async def gather_with_concurrency(
    n: int,
    *tasks: Awaitable[T],
) -> list[T]:
    """Run tasks with a concurrency limit."""
    semaphore = asyncio.Semaphore(n)

    async def sem_task(task: Awaitable[T]) -> T:
        async with semaphore:
            return await task

    return await asyncio.gather(*(sem_task(t) for t in tasks))


def run_in_executor(
    fn: Callable[..., T],
    *args: Any,
    executor: concurrent.futures.Executor | None = None,
) -> Future[T]:
    """Run a synchronous function in an executor."""
    loop = asyncio.new_event_loop()
    return loop.run_in_executor(executor, fn, *args)


class RWLock:
    """Read-Write lock implementation."""

    def __init__(self) -> None:
        self._read_ready = threading.Condition(threading.Lock())
        self._readers = 0
        self._writers_waiting = 0
        self._writer_active = False

    def acquire_read(self) -> None:
        with self._read_ready:
            while self._writer_active or self._writers_waiting > 0:
                self._read_ready.wait()
            self._readers += 1

    def release_read(self) -> None:
        with self._read_ready:
            self._readers -= 1
            if self._readers == 0:
                self._read_ready.notify_all()

    def acquire_write(self) -> None:
        with self._read_ready:
            self._writers_waiting += 1
            while self._readers > 0 or self._writer_active:
                self._read_ready.wait()
            self._writers_waiting -= 1
            self._writer_active = True

    def release_write(self) -> None:
        with self._read_ready:
            self._writer_active = False
            self._read_ready.notify_all()
