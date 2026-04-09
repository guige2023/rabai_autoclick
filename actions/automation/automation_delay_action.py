"""
Automation Delay Action Module.

Delay and timing utilities for automation workflows including
fixed delays, randomized delays, and progressive backoff delays.

MIT License - Copyright (c) 2025 RabAi Research
"""

from __future__ import annotations

import asyncio
import logging
import random
import time
from dataclasses import dataclass
from enum import Enum
from typing import Any, Callable, Optional, TypeVar

logger = logging.getLogger(__name__)

T = TypeVar("T")


class DelayStrategy(Enum):
    """Delay strategies for automation."""
    FIXED = "fixed"
    RANDOM = "random"
    EXPONENTIAL = "exponential"
    LINEAR = "linear"
    FIBONACCI = "fibonacci"


@dataclass
class DelayConfig:
    """Configuration for delay behavior."""
    base_delay_ms: float = 1000.0
    max_delay_ms: float = 30000.0
    jitter_percent: float = 10.0
    strategy: DelayStrategy = DelayStrategy.FIXED


class DelayTracker:
    """Tracks delays for a named automation task."""

    def __init__(self, name: str) -> None:
        self.name = name
        self.total_delays = 0
        self.total_delay_ms = 0.0
        self.last_delay_ms = 0.0

    def record(self, delay_ms: float) -> None:
        """Record a delay."""
        self.total_delays += 1
        self.total_delay_ms += delay_ms
        self.last_delay_ms = delay_ms

    @property
    def average_delay_ms(self) -> float:
        """Average delay in milliseconds."""
        if self.total_delays == 0:
            return 0.0
        return self.total_delay_ms / self.total_delays


class AutomationDelayAction:
    """
    Delay management for automation workflows.

    Provides various delay strategies for rate limiting, retry backoff,
    and workflow pacing.

    Example:
        delay_action = AutomationDelayAction()

        # Fixed delay
        await delay_action.wait(base_delay_ms=1000)

        # Random delay between 500-1500ms
        await delay_action.wait_random(min_ms=500, max_ms=1500)

        # Exponential backoff
        await delay_action.wait_exponential(attempt=3, base_ms=1000)
    """

    def __init__(self) -> None:
        self._trackers: dict[str, DelayTracker] = {}
        self._default_config = DelayConfig()

    def get_tracker(self, name: str) -> DelayTracker:
        """Get or create a delay tracker for a named task."""
        if name not in self._trackers:
            self._trackers[name] = DelayTracker(name)
        return self._trackers[name]

    async def wait(
        self,
        delay_ms: float,
        task_name: Optional[str] = None,
    ) -> None:
        """Wait for a fixed duration."""
        await asyncio.sleep(delay_ms / 1000.0)
        if task_name:
            self.get_tracker(task_name).record(delay_ms)

    async def wait_random(
        self,
        min_ms: float = 0.0,
        max_ms: float = 1000.0,
        task_name: Optional[str] = None,
    ) -> float:
        """Wait for a random duration within a range."""
        delay_ms = random.uniform(min_ms, max_ms)
        await asyncio.sleep(delay_ms / 1000.0)
        if task_name:
            self.get_tracker(task_name).record(delay_ms)
        return delay_ms

    async def wait_exponential(
        self,
        attempt: int,
        base_ms: float = 1000.0,
        max_ms: float = 30000.0,
        jitter: bool = True,
        task_name: Optional[str] = None,
    ) -> float:
        """
        Wait with exponential backoff.

        delay = min(base * 2^attempt, max) [+ jitter]
        """
        delay_ms = min(base_ms * (2 ** attempt), max_ms)

        if jitter:
            jitter_range = delay_ms * 0.1
            delay_ms += random.uniform(-jitter_range, jitter_range)

        delay_ms = max(0, delay_ms)
        await asyncio.sleep(delay_ms / 1000.0)

        if task_name:
            self.get_tracker(task_name).record(delay_ms)

        return delay_ms

    async def wait_linear(
        self,
        step: int,
        base_ms: float = 1000.0,
        increment_ms: float = 500.0,
        max_ms: float = 30000.0,
        task_name: Optional[str] = None,
    ) -> float:
        """Wait with linear backoff (step * base + increment)."""
        delay_ms = min(base_ms + (step * increment_ms), max_ms)
        await asyncio.sleep(delay_ms / 1000.0)

        if task_name:
            self.get_tracker(task_name).record(delay_ms)

        return delay_ms

    async def wait_fibonacci(
        self,
        n: int,
        base_ms: float = 1000.0,
        max_ms: float = 30000.0,
        task_name: Optional[str] = None,
    ) -> float:
        """Wait with Fibonacci backoff."""
        fib = self._fibonacci(n)
        delay_ms = min(base_ms * fib, max_ms)
        await asyncio.sleep(delay_ms / 1000.0)

        if task_name:
            self.get_tracker(task_name).record(delay_ms)

        return delay_ms

    def _fibonacci(self, n: int) -> int:
        """Calculate the nth Fibonacci number."""
        if n <= 1:
            return max(1, n)
        a, b = 1, 1
        for _ in range(n - 1):
            a, b = b, a + b
        return b

    async def wait_with_progress(
        self,
        total_steps: int,
        step: int,
        base_ms: float = 1000.0,
        task_name: Optional[str] = None,
    ) -> float:
        """Wait with progress bar-like delay (decreases as progress increases)."""
        progress = step / total_steps
        remaining_progress = 1.0 - progress
        delay_ms = base_ms * remaining_progress

        await asyncio.sleep(delay_ms / 1000.0)

        if task_name:
            self.get_tracker(task_name).record(delay_ms)

        return delay_ms

    async def wait_until(
        self,
        target_time: float,
        task_name: Optional[str] = None,
    ) -> float:
        """Wait until a specific timestamp."""
        now = time.time()
        if target_time <= now:
            return 0.0

        delay_seconds = target_time - now
        delay_ms = delay_seconds * 1000.0
        await asyncio.sleep(delay_seconds)

        if task_name:
            self.get_tracker(task_name).record(delay_ms)

        return delay_ms

    def get_tracker_stats(self, task_name: str) -> Optional[dict[str, Any]]:
        """Get delay statistics for a task."""
        tracker = self._trackers.get(task_name)
        if not tracker:
            return None
        return {
            "total_delays": tracker.total_delays,
            "total_delay_ms": tracker.total_delay_ms,
            "average_delay_ms": tracker.average_delay_ms,
            "last_delay_ms": tracker.last_delay_ms,
        }

    def get_all_tracker_stats(self) -> dict[str, dict[str, Any]]:
        """Get statistics for all tracked tasks."""
        return {
            name: {
                "total_delays": t.total_delays,
                "total_delay_ms": t.total_delay_ms,
                "average_delay_ms": t.average_delay_ms,
                "last_delay_ms": t.last_delay_ms,
            }
            for name, t in self._trackers.items()
        }


def create_backoff_delayer(
    base_ms: float = 1000.0,
    max_ms: float = 30000.0,
    strategy: DelayStrategy = DelayStrategy.EXPONENTIAL,
) -> Callable[[int], float]:
    """Create a backoff delay function with the given strategy."""
    async def delay(attempt: int) -> float:
        if strategy == DelayStrategy.EXPONENTIAL:
            return min(base_ms * (2 ** attempt), max_ms)
        elif strategy == DelayStrategy.LINEAR:
            return min(base_ms + (attempt * base_ms), max_ms)
        elif strategy == DelayStrategy.FIBONACCI:
            fib = 1
            a, b = 1, 1
            for _ in range(attempt - 1):
                a, b = b, a + b
                fib = b
            return min(base_ms * fib, max_ms)
        else:
            return base_ms

    return delay
