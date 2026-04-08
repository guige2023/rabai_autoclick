"""Automation Throttler Action Module.

Provides throttling for operations with burst handling,
priority queues, and adaptive rate limiting.
"""
from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, TypeVar
from collections import deque
import logging

logger = logging.getLogger(__name__)

T = TypeVar("T")


class ThrottleStrategy(Enum):
    """Throttle strategy."""
    TOKEN_BUCKET = "token_bucket"
    LEAKY_BUCKET = "leaky_bucket"
    DELAY_QUEUE = "delay_queue"


@dataclass
class ThrottleConfig:
    """Throttle configuration."""
    rate: float
    burst_size: float = 1.0
    strategy: ThrottleStrategy = ThrottleStrategy.TOKEN_BUCKET
    priority_enabled: bool = False


@dataclass
class ThrottledTask:
    """Task in throttle queue."""
    task_id: str
    func: Callable
    args: tuple = field(default_factory=tuple)
    kwargs: Dict = field(default_factory=dict)
    priority: int = 0
    enqueued_at: float = field(default_factory=time.time)


class AutomationThrottlerAction:
    """Operation throttler with rate limiting.

    Example:
        throttler = AutomationThrottlerAction(
            ThrottleConfig(rate=10.0, burst_size=5.0)
        )

        await throttler.throttle("task1", expensive_operation)
    """

    def __init__(self, config: Optional[ThrottleConfig] = None) -> None:
        self.config = config or ThrottleConfig(rate=1.0)
        self._tokens = self.config.burst_size
        self._last_refill = time.time()
        self._queue: deque = deque()
        self._processing = False
        self._lock = asyncio.Lock()
        self._task_count = 0

    async def throttle(
        self,
        task_id: str,
        func: Callable[..., T],
        *args: Any,
        priority: int = 0,
        **kwargs: Any,
    ) -> T:
        """Throttle and execute task.

        Args:
            task_id: Unique task identifier
            func: Function to execute
            *args: Positional arguments for func
            priority: Task priority (higher = sooner)
            **kwargs: Keyword arguments for func

        Returns:
            Result from func
        """
        await self._acquire_token()

        try:
            if asyncio.iscoroutinefunction(func):
                return await func(*args, **kwargs)
            else:
                return func(*args, **kwargs)
        finally:
            self._task_count += 1

    async def throttle_many(
        self,
        tasks: List[ThrottledTask],
    ) -> List[Any]:
        """Throttle and execute multiple tasks.

        Args:
            tasks: List of ThrottledTask objects

        Returns:
            List of results
        """
        if self.config.priority_enabled:
            tasks = sorted(tasks, key=lambda t: -t.priority)

        results = []
        for task in tasks:
            result = await self.throttle(
                task.task_id,
                task.func,
                *task.args,
                priority=task.priority,
                **task.kwargs
            )
            results.append(result)

        return results

    async def _acquire_token(self) -> None:
        """Acquire token based on throttle strategy."""
        if self.config.strategy == ThrottleStrategy.TOKEN_BUCKET:
            await self._acquire_token_bucket()
        elif self.config.strategy == ThrottleStrategy.LEAKY_BUCKET:
            await self._acquire_leaky_bucket()
        else:
            await self._acquire_delay_queue()

    async def _acquire_token_bucket(self) -> None:
        """Token bucket acquisition."""
        async with self._lock:
            now = time.time()
            elapsed = now - self._last_refill

            tokens_to_add = elapsed * self.config.rate
            self._tokens = min(
                self.config.burst_size,
                self._tokens + tokens_to_add
            )
            self._last_refill = now

            if self._tokens < 1.0:
                wait_time = (1.0 - self._tokens) / self.config.rate
                await asyncio.sleep(wait_time)
                self._tokens = 0.0
            else:
                self._tokens -= 1.0

    async def _acquire_leaky_bucket(self) -> None:
        """Leaky bucket acquisition."""
        async with self._lock:
            now = time.time()
            elapsed = now - self._last_refill

            leak_amount = elapsed * self.config.rate
            self._tokens = max(0.0, self._tokens - leak_amount)
            self._last_refill = now

            if self._tokens >= self.config.burst_size:
                wait_time = (self._tokens - self.config.burst_size + 1) / self.config.rate
                await asyncio.sleep(wait_time)
                self._tokens = 0.0
            else:
                self._tokens += 1.0

    async def _acquire_delay_queue(self) -> None:
        """Delay queue acquisition."""
        async with self._lock:
            min_interval = 1.0 / self.config.rate
            elapsed = time.time() - self._last_refill

            if elapsed < min_interval:
                await asyncio.sleep(min_interval - elapsed)

            self._last_refill = time.time()

    def get_stats(self) -> Dict[str, Any]:
        """Get throttle statistics."""
        return {
            "rate": self.config.rate,
            "burst_size": self.config.burst_size,
            "strategy": self.config.strategy.value,
            "tokens_available": self._tokens,
            "tasks_processed": self._task_count,
        }
