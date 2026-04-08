"""
Periodic Task Utilities

Provides utilities for running periodic tasks
in automation workflows.

Author: Agent3
"""
from __future__ import annotations

from typing import Any, Callable
import asyncio
import time


class PeriodicTask:
    """
    Runs a task periodically at fixed intervals.
    
    Provides start/stop control and tracks
    execution statistics.
    """

    def __init__(
        self,
        interval_seconds: float,
        task: Callable[..., Any],
    ) -> None:
        self._interval = interval_seconds
        self._task = task
        self._running = False
        self._execution_count = 0
        self._last_run: float | None = None

    async def start(self) -> None:
        """Start the periodic task."""
        self._running = True
        while self._running:
            self._last_run = time.time()
            try:
                result = self._task()
                if asyncio.iscoroutine(result):
                    await result
                self._execution_count += 1
            except Exception:
                pass
            await asyncio.sleep(self._interval)

    def stop(self) -> None:
        """Stop the periodic task."""
        self._running = False

    def get_execution_count(self) -> int:
        """Get number of executions."""
        return self._execution_count

    def get_last_run_time(self) -> float | None:
        """Get timestamp of last execution."""
        return self._last_run
