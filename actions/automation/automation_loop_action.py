"""
Automation Loop Action Module.

Loop execution utilities for automation workflows supporting
conditional loops, counted loops, and iterator-based loops.

MIT License - Copyright (c) 2025 RabAi Research
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from typing import Any, Callable, Generator, Iterator, List, Optional, TypeVar

logger = logging.getLogger(__name__)

T = TypeVar("T")
T_result = TypeVar("T_result")


@dataclass
class LoopStats:
    """Statistics for loop execution."""
    iterations_completed: int = 0
    iterations_failed: int = 0
    total_duration_ms: float = 0.0
    last_iteration_duration_ms: float = 0.0
    early_exit: bool = False


@dataclass
class LoopResult:
    """Result of a loop execution."""
    success: bool
    iterations: int
    results: List[Any]
    stats: LoopStats


class AutomationLoopAction:
    """
    Loop execution utilities for automation.

    Supports various loop patterns with early exit, error handling,
    and statistics tracking.

    Example:
        loop_action = AutomationLoopAction()

        # Counted loop
        results = await loop_action.counted_loop(
            func=fetch_page,
            count=10,
            delay_ms=1000,
        )

        # Conditional loop
        results = await loop_action.while_loop(
            func=check_condition,
            body=process_task,
            max_iterations=100,
        )

        # Iterator loop
        async for item in loop_action.iterate(data_source):
            await process(item)
    """

    def __init__(self) -> None:
        self._stats = LoopStats()

    async def counted_loop(
        self,
        func: Callable[[int], T_result],
        count: int,
        delay_ms: float = 0.0,
        stop_on_error: bool = False,
        start_index: int = 0,
    ) -> LoopResult:
        """
        Execute a function a fixed number of times.

        Args:
            func: Function to execute, receives iteration index (0-based)
            count: Number of iterations
            delay_ms: Delay between iterations
            stop_on_error: Stop loop if an iteration fails
            start_index: Starting index value
        """
        import time
        start = time.time()
        results: List[Any] = []
        iterations = 0
        failed = 0

        for i in range(count):
            iter_start = time.time()
            try:
                result = func(start_index + i)
                if asyncio.iscoroutinefunction(func):
                    result = await result
                results.append(result)
                iterations += 1

            except Exception as e:
                logger.error(f"Loop iteration {i} failed: {e}")
                failed += 1
                if stop_on_error:
                    break

            iter_duration = (time.time() - iter_start) * 1000
            self._stats.last_iteration_duration_ms = iter_duration

            if delay_ms > 0 and i < count - 1:
                await asyncio.sleep(delay_ms / 1000.0)

        duration = (time.time() - start) * 1000
        self._stats.iterations_completed = iterations
        self._stats.iterations_failed = failed
        self._stats.total_duration_ms = duration
        self._stats.early_exit = stop_on_error and failed > 0

        return LoopResult(
            success=failed == 0,
            iterations=iterations,
            results=results,
            stats=self._stats,
        )

    async def while_loop(
        self,
        condition: Callable[[], bool],
        body: Callable[[], T_result],
        max_iterations: int = 1000,
        delay_ms: float = 0.0,
        timeout_seconds: Optional[float] = None,
    ) -> LoopResult:
        """
        Execute body while condition is true.

        Args:
            condition: Function that returns True to continue
            body: Function to execute each iteration
            max_iterations: Maximum iterations before forced exit
            delay_ms: Delay between iterations
            timeout_seconds: Overall timeout for the loop
        """
        import time
        start = time.time()
        results: List[Any] = []
        iterations = 0
        failed = 0

        while iterations < max_iterations:
            # Check timeout
            if timeout_seconds and (time.time() - start) >= timeout_seconds:
                logger.warning(f"While loop timed out after {iterations} iterations")
                break

            # Check condition
            try:
                cond_result = condition()
                if asyncio.iscoroutinefunction(condition):
                    cond_result = await cond_result
                if not cond_result:
                    break
            except Exception as e:
                logger.error(f"Condition check failed: {e}")
                failed += 1
                break

            # Execute body
            iter_start = time.time()
            try:
                result = body()
                if asyncio.iscoroutinefunction(body):
                    result = await result
                results.append(result)
                iterations += 1

            except Exception as e:
                logger.error(f"While loop iteration {iterations} failed: {e}")
                failed += 1
                break

            if delay_ms > 0:
                await asyncio.sleep(delay_ms / 1000.0)

        duration = (time.time() - start) * 1000
        self._stats.iterations_completed = iterations
        self._stats.iterations_failed = failed
        self._stats.total_duration_ms = duration

        return LoopResult(
            success=failed == 0,
            iterations=iterations,
            results=results,
            stats=self._stats,
        )

    async def iterate(
        self,
        iterable: Iterator[T],
        body: Callable[[T], Any],
        delay_ms: float = 0.0,
    ) -> Generator[T, None, None]:
        """
        Iterate over items and apply body function.

        Yields results from body function for each item.

        Example:
            async for result in loop_action.iterate(items, process_item):
                print(result)
        """
        for item in iterable:
            try:
                result = body(item)
                if asyncio.iscoroutinefunction(body):
                    result = await result
                yield result

                if delay_ms > 0:
                    await asyncio.sleep(delay_ms / 1000.0)

            except Exception as e:
                logger.error(f"Iteration failed for item: {e}")
                continue

    async def parallel_loop(
        self,
        items: List[T],
        body: Callable[[T], T_result],
        max_concurrency: int = 10,
    ) -> LoopResult:
        """
        Execute body function over items in parallel batches.
        """
        import time
        start = time.time()
        results: List[Any] = []
        semaphore = asyncio.Semaphore(max_concurrency)

        async def process(item: T) -> Any:
            async with semaphore:
                result = body(item)
                if asyncio.iscoroutinefunction(body):
                    return await result
                return result

        batch_results = await asyncio.gather(
            *[process(item) for item in items],
            return_exceptions=True,
        )

        for res in batch_results:
            if isinstance(res, Exception):
                results.append(None)
            else:
                results.append(res)

        duration = (time.time() - start) * 1000
        failed = sum(1 for r in batch_results if isinstance(r, Exception))

        self._stats.iterations_completed = len(items) - failed
        self._stats.iterations_failed = failed
        self._stats.total_duration_ms = duration

        return LoopResult(
            success=failed == 0,
            iterations=len(items),
            results=results,
            stats=self._stats,
        )

    def get_stats(self) -> LoopStats:
        """Get loop execution statistics."""
        return self._stats

    def reset_stats(self) -> None:
        """Reset statistics."""
        self._stats = LoopStats()
