"""
API Resilience Action Module.

Provides bulkhead isolation, timeouts, and
resilience patterns for API calls.
"""

import asyncio
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Optional, TypeVar

T = TypeVar("T")


class ResiliencePattern(Enum):
    """Resilience patterns."""
    BULKHEAD = "bulkhead"
    TIMEOUT = "timeout"
    RATE_LIMIT = "rate_limit"
    CIRCUIT_BREAKER = "circuit_breaker"
    BULKHEAD_TIMEOUT = "bulkhead_timeout"


@dataclass
class BulkheadConfig:
    """Bulkhead isolation configuration."""
    max_concurrent: int = 10
    max_queued: int = 100
    timeout: float = 30.0


@dataclass
class ResilienceConfig:
    """Resilience configuration."""
    pattern: ResiliencePattern = ResiliencePattern.TIMEOUT
    timeout: float = 10.0
    max_retries: int = 3
    retry_delay: float = 1.0


class SemaphoreBulkhead:
    """Semaphore-based bulkhead."""

    def __init__(self, max_concurrent: int, max_queued: int):
        self._semaphore = asyncio.Semaphore(max_concurrent)
        self._queue: asyncio.Queue = asyncio.Queue(maxsize=max_queued)
        self._max_concurrent = max_concurrent
        self._max_queued = max_queued
        self._active = 0
        self._rejected = 0

    async def acquire(self) -> bool:
        """Acquire bulkhead slot."""
        if self._queue.full():
            self._rejected += 1
            return False

        await self._queue.put(True)
        try:
            await self._semaphore.acquire()
            self._active += 1
            return True
        except:
            self._queue.get_nowait()
            return False

    def release(self) -> None:
        """Release bulkhead slot."""
        self._semaphore.release()
        self._active -= 1
        try:
            self._queue.get_nowait()
        except asyncio.QueueEmpty:
            pass

    @property
    def stats(self) -> dict:
        return {
            "active": self._active,
            "queued": self._queue.qsize(),
            "rejected": self._rejected
        }


class APIResilienceAction:
    """
    API resilience patterns.

    Example:
        resilience = APIResilienceAction(
            pattern=ResiliencePattern.BULKHEAD_TIMEOUT,
            timeout=5.0
        )

        result = await resilience.execute(
            lambda: api.call(),
            bulkhead_config=BulkheadConfig(max_concurrent=5)
        )
    """

    def __init__(
        self,
        pattern: ResiliencePattern = ResiliencePattern.TIMEOUT,
        timeout: float = 10.0,
        max_retries: int = 3
    ):
        self.pattern = pattern
        self.config = ResilienceConfig(
            pattern=pattern,
            timeout=timeout,
            max_retries=max_retries
        )
        self._bulkhead: Optional[SemaphoreBulkhead] = None

    def set_bulkhead(self, max_concurrent: int = 10, max_queued: int = 100) -> None:
        """Set bulkhead configuration."""
        self._bulkhead = SemaphoreBulkhead(max_concurrent, max_queued)

    async def execute(
        self,
        func: Callable[[], T],
        *args: Any,
        **kwargs: Any
    ) -> T:
        """Execute with resilience pattern."""
        if self.pattern in (ResiliencePattern.BULKHEAD, ResiliencePattern.BULKHEAD_TIMEOUT):
            return await self._execute_bulkhead(func, *args, **kwargs)
        elif self.pattern == ResiliencePattern.TIMEOUT:
            return await self._execute_timeout(func, *args, **kwargs)
        elif self.pattern == ResiliencePattern.CIRCUIT_BREAKER:
            return await self._execute_circuit_breaker(func, *args, **kwargs)
        else:
            return await self._execute_timeout(func, *args, **kwargs)

    async def _execute_bulkhead(
        self,
        func: Callable[[], T],
        *args: Any,
        **kwargs: Any
    ) -> T:
        """Execute with bulkhead isolation."""
        if not self._bulkhead:
            self._bulkhead = SemaphoreBulkhead(10, 100)

        acquired = await self._bulkhead.acquire()
        if not acquired:
            raise Exception("Bulkhead rejected: too many concurrent requests")

        try:
            if self.pattern == ResiliencePattern.BULKHEAD_TIMEOUT:
                return await asyncio.wait_for(
                    func(*args, **kwargs),
                    timeout=self.config.timeout
                )
            return await func(*args, **kwargs)
        finally:
            self._bulkhead.release()

    async def _execute_timeout(
        self,
        func: Callable[[], T],
        *args: Any,
        **kwargs: Any
    ) -> T:
        """Execute with timeout."""
        if asyncio.iscoroutinefunction(func):
            return await asyncio.wait_for(
                func(*args, **kwargs),
                timeout=self.config.timeout
            )
        return await asyncio.wait_for(
            asyncio.to_thread(func, *args, **kwargs),
            timeout=self.config.timeout
        )

    async def _execute_circuit_breaker(
        self,
        func: Callable[[], T],
        *args: Any,
        **kwargs: Any
    ) -> T:
        """Execute with circuit breaker."""
        for attempt in range(self.config.max_retries + 1):
            try:
                return await asyncio.wait_for(
                    func(*args, **kwargs),
                    timeout=self.config.timeout
                )
            except asyncio.TimeoutError:
                if attempt >= self.config.max_retries:
                    raise
                await asyncio.sleep(self.config.retry_delay * (attempt + 1))
            except Exception as e:
                if attempt >= self.config.max_retries:
                    raise
                await asyncio.sleep(self.config.retry_delay * (attempt + 1))

    def get_bulkhead_stats(self) -> Optional[dict]:
        """Get bulkhead statistics."""
        if self._bulkhead:
            return self._bulkhead.stats
        return None
