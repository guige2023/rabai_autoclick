"""Cache Warmup Action Module.

Warm up caches with prioritized data loading strategies.
"""

from __future__ import annotations

import asyncio
import time
from collections import deque
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Generic, TypeVar

T = TypeVar("T")


class WarmupPriority(Enum):
    """Warmup priority levels."""
    LOW = 0
    NORMAL = 1
    HIGH = 2
    CRITICAL = 3


@dataclass
class WarmupTask(Generic[T]):
    """Cache warmup task."""
    task_id: str
    key: str
    fetcher: Callable[[], T | asyncio.coroutine]
    priority: WarmupPriority = WarmupPriority.NORMAL
    ttl: float | None = None
    dependencies: list[str] = field(default_factory=list)
    created_at: float = field(default_factory=time.time)


@dataclass
class WarmupStats:
    """Warmup statistics."""
    total_tasks: int = 0
    completed: int = 0
    failed: int = 0
    skipped: int = 0
    total_time: float = 0.0
    errors: dict[str, str] = field(default_factory=dict)


class CacheWarmupStrategy(ABC):
    """Abstract warmup strategy."""

    @abstractmethod
    async def execute(
        self,
        tasks: list[WarmupTask],
        cache_setter: Callable[[str, Any], Any]
    ) -> WarmupStats:
        """Execute warmup strategy."""
        pass


class SequentialWarmup(CacheWarmupStrategy):
    """Sequential warmup - one at a time."""

    async def execute(
        self,
        tasks: list[WarmupTask],
        cache_setter: Callable[[str, Any], Any]
    ) -> WarmupStats:
        stats = WarmupStats(total_tasks=len(tasks))
        start = time.time()
        for task in tasks:
            try:
                result = task.fetcher()
                if asyncio.iscoroutine(result):
                    result = await result
                await asyncio.to_thread(cache_setter, task.key, result)
                stats.completed += 1
            except Exception as e:
                stats.failed += 1
                stats.errors[task.key] = str(e)
        stats.total_time = time.time() - start
        return stats


class ParallelWarmup(CacheWarmupStrategy):
    """Parallel warmup - all at once with concurrency limit."""

    def __init__(self, max_concurrency: int = 10) -> None:
        self.max_concurrency = max_concurrency

    async def execute(
        self,
        tasks: list[WarmupTask],
        cache_setter: Callable[[str, Any], Any]
    ) -> WarmupStats:
        stats = WarmupStats(total_tasks=len(tasks))
        start = time.time()
        semaphore = asyncio.Semaphore(self.max_concurrency)
        async def warm_task(task: WarmupTask) -> None:
            async with semaphore:
                try:
                    result = task.fetcher()
                    if asyncio.iscoroutine(result):
                        result = await result
                    await asyncio.to_thread(cache_setter, task.key, result)
                    stats.completed += 1
                except Exception as e:
                    stats.failed += 1
                    stats.errors[task.key] = str(e)
        await asyncio.gather(*[warm_task(t) for t in tasks])
        stats.total_time = time.time() - start
        return stats


class PriorityWarmup(CacheWarmupStrategy):
    """Priority-based warmup with dependency ordering."""

    async def execute(
        self,
        tasks: list[WarmupTask],
        cache_setter: Callable[[str, Any], Any]
    ) -> WarmupStats:
        stats = WarmupStats(total_tasks=len(tasks))
        start = time.time()
        completed_keys: set = set()
        pending = sorted(tasks, key=lambda t: t.priority.value, reverse=True)
        while pending:
            ready = [t for t in pending if all(d in completed_keys for d in t.dependencies)]
            if not ready:
                stats.skipped += len(pending)
                break
            for task in ready:
                try:
                    result = task.fetcher()
                    if asyncio.iscoroutine(result):
                        result = await result
                    await asyncio.to_thread(cache_setter, task.key, result)
                    completed_keys.add(task.key)
                    stats.completed += 1
                except Exception as e:
                    stats.failed += 1
                    stats.errors[task.key] = str(e)
                pending.remove(task)
        stats.total_time = time.time() - start
        return stats


class CacheWarmupManager:
    """Manage cache warmup operations."""

    def __init__(self, strategy: CacheWarmupStrategy | None = None) -> None:
        self.strategy = strategy or ParallelWarmup()
        self._tasks: dict[str, WarmupTask] = {}

    def add_task(
        self,
        key: str,
        fetcher: Callable[[], T | asyncio.coroutine],
        priority: WarmupPriority = WarmupPriority.NORMAL,
        dependencies: list[str] | None = None,
        ttl: float | None = None
    ) -> str:
        """Add a warmup task."""
        import uuid
        task_id = str(uuid.uuid4())
        task = WarmupTask(
            task_id=task_id,
            key=key,
            fetcher=fetcher,
            priority=priority,
            dependencies=dependencies or [],
            ttl=ttl
        )
        self._tasks[task_id] = task
        return task_id

    async def warmup(
        self,
        cache_setter: Callable[[str, Any], Any]
    ) -> WarmupStats:
        """Execute warmup strategy."""
        return await self.strategy.execute(list(self._tasks.values()), cache_setter)

    def clear_tasks(self) -> None:
        """Clear all warmup tasks."""
        self._tasks.clear()

    def get_stats(self) -> dict[str, int]:
        """Get warmup statistics."""
        return {
            "total_tasks": len(self._tasks),
            "by_priority": {
                p.name: sum(1 for t in self._tasks.values() if t.priority == p)
                for p in WarmupPriority
            }
        }
