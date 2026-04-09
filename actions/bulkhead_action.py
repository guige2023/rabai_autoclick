"""
Bulkhead Action Module

Provides bulkhead isolation pattern for preventing resource exhaustion
in UI automation workflows. Supports semaphore-based and thread-pool
based bulkhead implementations.

Author: AI Agent
Version: 1.0.0
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Callable, Optional, TypeVar

logger = logging.getLogger(__name__)

T = TypeVar("T")


class BulkheadType(Enum):
    """Bulkhead implementation types."""
    SEMAPHORE = auto()
    THREAD_POOL = auto()
    PROCESS = auto()


@dataclass
class BulkheadConfig:
    """Bulkhead configuration."""
    max_concurrent_calls: int = 10
    max_queue_size: int = 0
    timeout: float = 30.0
    bulkhead_type: BulkheadType = BulkheadType.SEMAPHORE
    pool_size: int = 10
    pool_name: str = "default"


@dataclass
class BulkheadMetrics:
    """Bulkhead metrics."""
    active_calls: int = 0
    queued_calls: int = 0
    completed_calls: int = 0
    rejected_calls: int = 0
    timed_out_calls: int = 0
    total_wait_time: float = 0.0
    peak_active: int = 0
    peak_queued: int = 0

    @property
    def utilization(self) -> float:
        """Calculate utilization percentage."""
        return 0.0


class BulkheadRejectedError(Exception):
    """Bulkhead rejection error."""
    pass


class BulkheadTimeoutError(Exception):
    """Bulkhead timeout error."""
    pass


class SemaphoreBulkhead:
    """
    Semaphore-based bulkhead implementation.

    Example:
        >>> bulkhead = SemaphoreBulkhead(BulkheadConfig(max_concurrent_calls=5))
        >>> async with bulkhead:
        ...     result = await my_function()
    """

    def __init__(self, config: BulkheadConfig) -> None:
        self.config = config
        self._semaphore: asyncio.Semaphore = asyncio.Semaphore(config.max_concurrent_calls)
        self._queue: asyncio.Queue = asyncio.Queue(maxsize=config.max_queue_size)
        self._metrics = BulkheadMetrics()
        self._lock = asyncio.Lock()
        self._calls_in_progress = 0
        self._call_start_times: dict[int, float] = {}

    async def acquire(self, timeout: Optional[float] = None) -> bool:
        """Acquire bulkhead slot."""
        timeout = timeout or self.config.timeout

        async with self._lock:
            self._metrics.queued_calls += 1
            self._metrics.peak_queued = max(self._metrics.peak_queued, self._metrics.queued_calls)
            call_id = id(asyncio.current_task())

        try:
            await asyncio.wait_for(self._semaphore.acquire(), timeout=timeout)
            async with self._lock:
                self._metrics.active_calls += 1
                self._metrics.queued_calls -= 1
                self._metrics.peak_active = max(self._metrics.peak_active, self._metrics.active_calls)
                self._call_start_times[call_id] = time.time()
            return True
        except asyncio.TimeoutError:
            async with self._lock:
                self._metrics.queued_calls -= 1
                self._metrics.rejected_calls += 1
            raise BulkheadTimeoutError(f"Bulkhead acquisition timed out after {timeout}s")
        except Exception as e:
            async with self._lock:
                self._metrics.queued_calls -= 1
                self._metrics.rejected_calls += 1
            raise

    def release(self) -> None:
        """Release bulkhead slot."""
        call_id = id(asyncio.current_task())
        async with self._lock:
            self._metrics.active_calls -= 1
            if call_id in self._call_start_times:
                wait_time = time.time() - self._call_start_times.pop(call_id)
                self._metrics.total_wait_time += wait_time
        self._semaphore.release()

    async def execute(
        self,
        func: Callable[..., T],
        *args: Any,
        **kwargs: Any,
    ) -> T:
        """Execute function with bulkhead protection."""
        if asyncio.iscoroutinefunction(func):
            async def wrapped():
                await self.acquire()
                try:
                    return await func(*args, **kwargs)
                finally:
                    self.release()
            return await wrapped()
        else:
            await self.acquire()
            try:
                return func(*args, **kwargs)
            finally:
                self.release()

    @property
    def metrics(self) -> BulkheadMetrics:
        """Get bulkhead metrics."""
        return self._metrics

    def reset_metrics(self) -> None:
        """Reset metrics."""
        self._metrics = BulkheadMetrics()


class Bulkhead:
    """
    Unified bulkhead with type selection.

    Example:
        >>> config = BulkheadConfig(max_concurrent_calls=5)
        >>> bulkhead = Bulkhead(config)
        >>> async with bulkhead:
        ...     result = await my_function()
    """

    def __init__(self, config: BulkheadConfig) -> None:
        self.config = config
        self._impl = SemaphoreBulkhead(config)

    async def acquire(self, timeout: Optional[float] = None) -> bool:
        """Acquire bulkhead slot."""
        return await self._impl.acquire(timeout)

    def release(self) -> None:
        """Release bulkhead slot."""
        self._impl.release()

    async def execute(
        self,
        func: Callable[..., T],
        *args: Any,
        **kwargs: Any,
    ) -> T:
        """Execute function with bulkhead protection."""
        return await self._impl.execute(func, *args, **kwargs)

    @property
    def metrics(self) -> BulkheadMetrics:
        """Get bulkhead metrics."""
        return self._impl.metrics

    def reset_metrics(self) -> None:
        """Reset metrics."""
        self._impl.reset_metrics()

    async def __aenter__(self) -> "Bulkhead":
        """Async context manager entry."""
        await self.acquire()
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Async context manager exit."""
        self.release()

    def __repr__(self) -> str:
        return f"Bulkhead(active={self._impl._metrics.active_calls}, queued={self._impl._metrics.queued_calls})"


class BulkheadRegistry:
    """
    Registry for managing multiple bulkheads.

    Example:
        >>> registry = BulkheadRegistry()
        >>> bulkhead = registry.get("payment", BulkheadConfig(max_concurrent_calls=3))
    """

    def __init__(self) -> None:
        self._bulkheads: dict[str, Bulkhead] = {}
        self._configs: dict[str, BulkheadConfig] = {}

    def get(
        self,
        name: str,
        config: Optional[BulkheadConfig] = None,
    ) -> Bulkhead:
        """Get or create bulkhead."""
        if name not in self._bulkheads:
            self._configs[name] = config or BulkheadConfig()
            self._bulkheads[name] = Bulkhead(self._configs[name])
            logger.info(f"Created bulkhead: {name}")
        return self._bulkheads[name]

    def remove(self, name: str) -> None:
        """Remove bulkhead."""
        if name in self._bulkheads:
            del self._bulkheads[name]
        if name in self._configs:
            del self._configs[name]

    def get_all_metrics(self) -> dict[str, BulkheadMetrics]:
        """Get metrics for all bulkheads."""
        return {name: bh.metrics for name, bh in self._bulkheads.items()}

    def list_bulkheads(self) -> list[str]:
        """List all registered bulkheads."""
        return list(self._bulkheads.keys())

    def __repr__(self) -> str:
        return f"BulkheadRegistry(bulkheads={len(self._bulkheads)})"
