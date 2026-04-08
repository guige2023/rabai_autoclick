"""
Parallel Executor Utilities

Provides utilities for parallel task execution
in automation workflows.

Author: Agent3
"""
from __future__ import annotations

from typing import Any, Callable, Awaitable
import asyncio


class ParallelExecutor:
    """
    Executes tasks in parallel with concurrency control.
    
    Manages worker pools and distributes work
    across available workers.
    """

    def __init__(self, max_workers: int = 4) -> None:
        self._max_workers = max_workers
        self._semaphore: asyncio.Semaphore | None = None

    async def execute(
        self,
        tasks: list[Callable[..., Awaitable[Any] | Any]],
    ) -> list[Any]:
        """
        Execute tasks in parallel.
        
        Args:
            tasks: List of callable tasks.
            
        Returns:
            List of results in same order as tasks.
        """
        if self._semaphore is None:
            self._semaphore = asyncio.Semaphore(self._max_workers)

        async def run_with_limit(task: Callable) -> Any:
            async with self._semaphore:  # type: ignore
                result = task()
                if asyncio.iscoroutine(result):
                    result = await result
                return result

        return await asyncio.gather(*[run_with_limit(t) for t in tasks])

    async def map(
        self,
        func: Callable[..., Awaitable[Any] | Any],
        items: list[Any],
    ) -> list[Any]:
        """
        Map function over items in parallel.
        
        Args:
            func: Function to apply.
            items: Items to process.
            
        Returns:
            List of results.
        """
        tasks = [lambda item=item: func(item) for item in items]
        return await self.execute(tasks)
