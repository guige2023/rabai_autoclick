"""Async queue utilities: async/await based queues, producers, and consumers."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Any, Callable

__all__ = [
    "AsyncQueue",
    "AsyncPriorityQueue",
    "AsyncBoundedQueue",
]


@dataclass
class AsyncQueue:
    """Async queue for use with asyncio."""

    def __init__(self) -> None:
        self._queue: asyncio.Queue = asyncio.Queue()

    async def put(self, item: Any) -> None:
        await self._queue.put(item)

    async def get(self) -> Any:
        return await self._queue.get()

    def qsize(self) -> int:
        return self._queue.qsize()

    def empty(self) -> bool:
        return self._queue.empty()

    async def join(self) -> None:
        await self._queue.join()


class AsyncPriorityQueue:
    """Async priority queue."""

    def __init__(self) -> None:
        self._queue: asyncio.PriorityQueue = asyncio.PriorityQueue()

    async def put(self, item: tuple[int, Any]) -> None:
        await self._queue.put(item)

    async def get(self) -> Any:
        _, item = await self._queue.get()
        return item

    def qsize(self) -> int:
        return self._queue.qsize()


class AsyncBoundedQueue:
    """Async queue with maximum size (backpressure support)."""

    def __init__(self, maxsize: int = 0) -> None:
        self._queue: asyncio.Queue = asyncio.Queue(maxsize=maxsize)

    async def put(self, item: Any) -> None:
        await self._queue.put(item)

    async def get(self) -> Any:
        return await self._queue.get()

    @property
    def maxsize(self) -> int:
        return self._queue.maxsize

    def full(self) -> bool:
        return self._queue.full()

    def qsize(self) -> int:
        return self._queue.qsize()
