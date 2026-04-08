"""
Automation Semaphore Action Module.

Semaphore-based concurrency control for automation tasks,
supports acquire/release with timeout and priority queuing.
"""

from __future__ import annotations

from typing import Any, Optional, Callable
from dataclasses import dataclass, field
import logging
import asyncio
import time

logger = logging.getLogger(__name__)


@dataclass
class SemaphoreStats:
    """Semaphore statistics."""
    total_slots: int
    available: int
    acquired: int
    total_acquires: int
    total_releases: int


class AutomationSemaphoreAction:
    """
    Semaphore for controlling concurrent automation task execution.

    Limits the number of concurrent operations,
    supports timeout, and tracks acquisition patterns.

    Example:
        sem = AutomationSemaphoreAction(max_concurrent=5)
        async with sem.acquire():
            await run_automation_task()
    """

    def __init__(
        self,
        max_concurrent: int = 5,
        name: str = "default",
    ) -> None:
        self.max_concurrent = max_concurrent
        self.name = name
        self._semaphore: asyncio.Semaphore = asyncio.Semaphore(max_concurrent)
        self._available = max_concurrent
        self._acquired_count = 0
        self._total_acquires = 0
        self._total_releases = 0
        self._waiting: list[str] = []

    async def acquire(
        self,
        timeout: Optional[float] = None,
        task_id: Optional[str] = None,
    ) -> bool:
        """Acquire a semaphore slot."""
        task_id = task_id or f"task_{time.time()}"

        try:
            if timeout:
                await asyncio.wait_for(
                    self._semaphore.acquire(),
                    timeout=timeout,
                )
            else:
                await self._semaphore.acquire()

            self._acquired_count += 1
            self._total_acquires += 1
            self._available = self.max_concurrent - self._acquired_count

            logger.debug(
                "Semaphore '%s' acquired by %s (available=%d)",
                self.name, task_id, self._available
            )
            return True

        except asyncio.TimeoutError:
            logger.warning(
                "Semaphore '%s' acquire timeout for %s",
                self.name, task_id
            )
            return False

    def release(self) -> None:
        """Release a semaphore slot."""
        self._semaphore.release()
        self._acquired_count = max(0, self._acquired_count - 1)
        self._total_releases += 1
        self._available = self.max_concurrent - self._acquired_count

        logger.debug(
            "Semaphore '%s' released (available=%d)",
            self.name, self._available
        )

    async def run_with_semaphore(
        self,
        func: Callable[..., Any],
        *args: Any,
        timeout: Optional[float] = None,
        **kwargs: Any,
    ) -> Any:
        """Run a function with semaphore protection."""
        acquired = await self.acquire(timeout=timeout)
        if not acquired:
            raise TimeoutError(f"Failed to acquire semaphore '{self.name}'")

        try:
            if asyncio.iscoroutinefunction(func):
                return await func(*args, **kwargs)
            else:
                return func(*args, **kwargs)
        finally:
            self.release()

    @property
    def stats(self) -> SemaphoreStats:
        """Get current semaphore statistics."""
        return SemaphoreStats(
            total_slots=self.max_concurrent,
            available=self._available,
            acquired=self._acquired_count,
            total_acquires=self._total_acquires,
            total_releases=self._total_releases,
        )

    def is_available(self) -> bool:
        """Check if any slots are available."""
        return self._available > 0

    def reset(self) -> None:
        """Reset semaphore to initial state."""
        while self._acquired_count > 0:
            self.release()
