"""Async task utilities: asyncio-based task management, batching, and result handling."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable

__all__ = [
    "AsyncTask",
    "AsyncTaskManager",
    "gather_with_concurrency",
    "run_async",
]


@dataclass
class AsyncTask:
    """An async task with metadata."""

    id: str
    coro: Awaitable[Any]
    created_at: float = 0.0
    completed_at: float | None = None
    result: Any = None
    error: Exception | None = None


class AsyncTaskManager:
    """Manage multiple async tasks with timeout and cancellation support."""

    def __init__(self) -> None:
        self._tasks: dict[str, asyncio.Task] = {}
        self._results: dict[str, Any] = {}

    async def run(
        self,
        coro: Awaitable[Any],
        task_id: str = "",
        timeout: float | None = None,
    ) -> Any:
        """Run an async coroutine with optional timeout."""
        import time
        task_id = task_id or str(time.time_ns())

        try:
            if timeout:
                result = await asyncio.wait_for(coro, timeout=timeout)
            else:
                result = await coro
            self._results[task_id] = result
            return result
        except asyncio.TimeoutError:
            raise TimeoutError(f"Task {task_id} timed out after {timeout}s")
        except Exception as e:
            self._results[task_id] = None
            raise

    async def gather(
        self,
        *coros: Awaitable[Any],
        timeout: float | None = None,
    ) -> list[Any]:
        """Run multiple coroutines concurrently."""
        if timeout:
            return await asyncio.wait_for(
                asyncio.gather(*coros, return_exceptions=True),
                timeout=timeout,
            )
        return await asyncio.gather(*coros, return_exceptions=True)


async def gather_with_concurrency(
    max_concurrent: int,
    *coros: Awaitable[Any],
) -> list[Any]:
    """Gather with controlled concurrency using semaphore."""
    semaphore = asyncio.Semaphore(max_concurrent)

    async def bounded_coro(coro: Awaitable[Any]) -> Any:
        async with semaphore:
            return await coro

    return await asyncio.gather(*(bounded_coro(c) for c in coros), return_exceptions=True)


def run_async(coro: Awaitable[Any], timeout: float | None = None) -> Any:
    """Run an async coroutine from synchronous code."""
    if timeout:
        return asyncio.run(asyncio.wait_for(coro, timeout=timeout))
    return asyncio.run(coro)
