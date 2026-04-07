"""Lock and synchronization utilities for RabAI AutoClick.

Provides:
- Reentrant lock
- Read-write lock
- Semaphore
- Condition variables
- Async locks
"""

from __future__ import annotations

import asyncio
import threading
from typing import Optional


class ReentrantLock:
    """Reentrant lock that allows the same thread to acquire multiple times."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._owner: Optional[int] = None
        self._count = 0

    def acquire(self) -> bool:
        me = threading.current_thread().ident
        if self._owner == me:
            self._count += 1
            return True
        if self._lock.acquire():
            self._owner = me
            self._count = 1
            return True
        return False

    def release(self) -> None:
        me = threading.current_thread().ident
        if self._owner != me:
            raise RuntimeError("Cannot release lock not owned by this thread")
        self._count -= 1
        if self._count == 0:
            self._owner = None
            self._lock.release()

    def __enter__(self) -> ReentrantLock:
        self.acquire()
        return self

    def __exit__(self, *args) -> None:
        self.release()


class ReadWriteLock:
    """Read-write lock allowing multiple readers or one writer.

    Example:
        rwlock = ReadWriteLock()

        with rwlock.read_lock():
            # Multiple readers allowed
            data = self._data

        with rwlock.write_lock():
            # Exclusive access
            self._data = new_data
    """

    def __init__(self) -> None:
        self._read_ready = threading.Condition(threading.Lock())
        self._readers = 0

    def acquire_read(self) -> bool:
        with self._read_ready:
            self._readers += 1
        return True

    def release_read(self) -> None:
        with self._read_ready:
            self._readers -= 1
            if self._readers == 0:
                self._read_ready.notify_all()

    def acquire_write(self) -> bool:
        self._read_ready.acquire()
        while self._readers > 0:
            self._read_ready.wait()
        return True

    def release_write(self) -> None:
        self._read_ready.release()

    def read_lock(self) -> ReadLock:
        return ReadLock(self)

    def write_lock(self) -> WriteLock:
        return WriteLock(self)


class ReadLock:
    def __init__(self, rwlock: ReadWriteLock) -> None:
        self._rwlock = rwlock

    def __enter__(self) -> ReadLock:
        self._rwlock.acquire_read()
        return self

    def __exit__(self, *args) -> None:
        self._rwlock.release_read()


class WriteLock:
    def __init__(self, rwlock: ReadWriteLock) -> None:
        self._rwlock = rwlock

    def __enter__(self) -> WriteLock:
        self._rwlock.acquire_write()
        return self

    def __exit__(self, *args) -> None:
        self._rwlock.release_write()


class AsyncReadWriteLock:
    """Async read-write lock."""

    def __init__(self) -> None:
        self._read_ready = asyncio.Condition()
        self._readers = 0

    async def acquire_read(self) -> None:
        async with self._read_ready:
            self._readers += 1

    async def release_read(self) -> None:
        async with self._read_ready:
            self._readers -= 1
            if self._readers == 0:
                self._read_ready.notify_all()

    async def acquire_write(self) -> None:
        await self._read_ready.acquire()
        while self._readers > 0:
            await self._read_ready.wait()

    async def release_write(self) -> None:
        self._read_ready.release()

    def read_lock(self) -> AsyncReadLock:
        return AsyncReadLock(self)

    def write_lock(self) -> AsyncWriteLock:
        return AsyncWriteLock(self)


class AsyncReadLock:
    def __init__(self, rwlock: AsyncReadWriteLock) -> None:
        self._rwlock = rwlock

    async def __aenter__(self) -> AsyncReadLock:
        await self._rwlock.acquire_read()
        return self

    async def __aexit__(self, *args) -> None:
        await self._rwlock.release_read()


class AsyncWriteLock:
    def __init__(self, rwlock: AsyncReadWriteLock) -> None:
        self._rwlock = rwlock

    async def __aenter__(self) -> AsyncWriteLock:
        await self._rwlock.acquire_write()
        return self

    async def __aexit__(self, *args) -> None:
        await self._rwlock.release_write()


class CountingSemaphore:
    """Semaphore with a counter."""

    def __init__(self, initial: int = 1) -> None:
        self._semaphore = threading.Semaphore(initial)

    def acquire(self, blocking: bool = True, timeout: Optional[float] = None) -> bool:
        return self._semaphore.acquire(blocking, timeout)

    def release(self) -> None:
        self._semaphore.release()

    def __enter__(self) -> CountingSemaphore:
        self.acquire()
        return self

    def __exit__(self, *args) -> None:
        self.release()
