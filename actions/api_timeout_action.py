"""API Timeout Action Module.

Request timeout handling with graceful degradation.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any, Callable, Generic, TypeVar

T = TypeVar("T")


@dataclass
class TimeoutConfig:
    """Timeout configuration."""
    default_timeout: float = 30.0
    connect_timeout: float = 5.0
    read_timeout: float = 30.0
    write_timeout: float = 10.0


class TimeoutError(Exception):
    """Request timeout error."""
    pass


class APITimeoutHandler(Generic[T]):
    """Handle API timeouts gracefully."""

    def __init__(self, config: TimeoutConfig | None = None) -> None:
        self.config = config or TimeoutConfig()
        self._timeouts: dict[str, asyncio.Task] = {}

    async def with_timeout(
        self,
        func: Callable[[], T | asyncio.coroutine],
        timeout: float | None = None,
        fallback: Callable | None = None
    ) -> T:
        """Execute function with timeout."""
        timeout_val = timeout or self.config.default_timeout
        try:
            result = await asyncio.wait_for(
                asyncio.coroutine(func)() if not asyncio.iscoroutinefunction(func) else func(),
                timeout=timeout_val
            )
            return result
        except asyncio.TimeoutError:
            if fallback:
                return fallback()
            raise TimeoutError(f"Operation timed out after {timeout_val}s")

    async def with_connect_timeout(
        self,
        func: Callable[[], T | asyncio.coroutine],
        timeout: float | None = None
    ) -> T:
        """Execute with connection timeout."""
        timeout_val = timeout or self.config.connect_timeout
        try:
            return await asyncio.wait_for(
                asyncio.coroutine(func)() if not asyncio.iscoroutinefunction(func) else func(),
                timeout=timeout_val
            )
        except asyncio.TimeoutError:
            raise TimeoutError(f"Connection timed out after {timeout_val}s")

    async def race(
        self,
        funcs: list[Callable[[], T | asyncio.coroutine]],
        timeout: float | None = None
    ) -> tuple[int, T]:
        """Race multiple functions, return first to complete."""
        timeout_val = timeout or self.config.default_timeout
        async def run_with_index(func: Callable, index: int):
            result = func()
            if asyncio.iscoroutine(result):
                result = await result
            return index, result
        tasks = [run_with_index(f, i) for i, f in enumerate(funcs)]
        try:
            done, pending = await asyncio.wait(
                [asyncio.create_task(t) for t in tasks],
                timeout=timeout_val,
                return_when=asyncio.FIRST_COMPLETED
            )
            for p in pending:
                p.cancel()
            result = done.pop().result()
            return result
        except asyncio.TimeoutError:
            raise TimeoutError(f"No function completed within {timeout_val}s")

    def create_timeout_task(
        self,
        task_id: str,
        func: Callable[[], T | asyncio.coroutine]
    ) -> asyncio.Task:
        """Create a tracked timeout task."""
        async def run():
            try:
                result = await asyncio.wait_for(
                    asyncio.coroutine(func)() if not asyncio.iscoroutinefunction(func) else func(),
                    timeout=self.config.default_timeout
                )
                return result
            except asyncio.TimeoutError:
                raise TimeoutError(f"Task {task_id} timed out")
        task = asyncio.create_task(run())
        self._timeouts[task_id] = task
        return task

    async def cancel_task(self, task_id: str) -> bool:
        """Cancel a tracked timeout task."""
        task = self._timeouts.get(task_id)
        if task:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
            return True
        return False
