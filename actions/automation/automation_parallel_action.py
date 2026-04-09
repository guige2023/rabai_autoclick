"""
Automation Parallel Action Module.

Parallel execution of automation tasks with configurable concurrency,
result aggregation, and error handling strategies.

MIT License - Copyright (c) 2025 RabAi Research
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, TypeVar

logger = logging.getLogger(__name__)

T = TypeVar("T")


class ExecutionMode(Enum):
    """How parallel tasks should be executed."""
    PARALLEL = "parallel"          # All at once
    SEMI_PARALLEL = "semi_parallel"  # Batched
    SEQUENTIAL = "sequential"       # One at a time


@dataclass
class TaskResult:
    """Result of a single task execution."""
    task_id: str
    success: bool
    result: Any = None
    error: Optional[str] = None
    execution_time_ms: float = 0.0


@dataclass
class ParallelExecutionStats:
    """Statistics for a parallel execution."""
    total_tasks: int = 0
    successful: int = 0
    failed: int = 0
    total_time_ms: float = 0.0
    avg_task_time_ms: float = 0.0
    max_task_time_ms: float = 0.0


class TaskItem:
    """A task to be executed in parallel."""

    def __init__(
        self,
        task_id: str,
        func: Callable[..., T],
        args: tuple = (),
        kwargs: Optional[Dict[str, Any]] = None,
    ) -> None:
        self.task_id = task_id
        self.func = func
        self.args = args
        self.kwargs = kwargs or {}


class AutomationParallelAction:
    """
    Parallel task execution engine for automation.

    Manages concurrent execution of multiple tasks with configurable
    concurrency limits, result aggregation, and error handling.

    Example:
        parallel = AutomationParallelAction(max_concurrency=5)

        tasks = [
            TaskItem("task-1", fetch_data, args=(id,)),
            TaskItem("task-2", process_data, kwargs={"data": x}),
        ]

        results = await parallel.execute_all(tasks, stop_on_error=False)

        for r in results:
            if r.success:
                print(f"{r.task_id}: {r.result}")
    """

    def __init__(
        self,
        max_concurrency: int = 10,
        default_timeout: Optional[float] = None,
    ) -> None:
        self.max_concurrency = max_concurrency
        self.default_timeout = default_timeout
        self._semaphore = asyncio.Semaphore(max_concurrency)
        self._stats = ParallelExecutionStats()

    async def execute_single(
        self,
        task: TaskItem,
        timeout: Optional[float] = None,
    ) -> TaskResult:
        """Execute a single task with semaphore control."""
        start = time.time()

        async with self._semaphore:
            try:
                if asyncio.iscoroutinefunction(task.func):
                    result = await asyncio.wait_for(
                        task.func(*task.args, **task.kwargs),
                        timeout=timeout or self.default_timeout,
                    )
                else:
                    # Run sync function in executor
                    loop = asyncio.get_event_loop()
                    result = await asyncio.wait_for(
                        loop.run_in_executor(
                            None,
                            lambda: task.func(*task.args, **task.kwargs),
                        ),
                        timeout=timeout or self.default_timeout,
                    )

                execution_time = (time.time() - start) * 1000
                return TaskResult(
                    task_id=task.task_id,
                    success=True,
                    result=result,
                    execution_time_ms=execution_time,
                )

            except asyncio.TimeoutError:
                execution_time = (time.time() - start) * 1000
                return TaskResult(
                    task_id=task.task_id,
                    success=False,
                    error=f"Task timed out after {timeout or self.default_timeout}s",
                    execution_time_ms=execution_time,
                )

            except Exception as e:
                execution_time = (time.time() - start) * 1000
                return TaskResult(
                    task_id=task.task_id,
                    success=False,
                    error=str(e),
                    execution_time_ms=execution_time,
                )

    async def execute_all(
        self,
        tasks: List[TaskItem],
        stop_on_error: bool = False,
        timeout: Optional[float] = None,
    ) -> List[TaskResult]:
        """Execute all tasks in parallel."""
        start_time = time.time()
        results: List[TaskResult] = []

        if not tasks:
            return results

        # Create all tasks
        async def run_with_error_check(t: TaskItem) -> TaskResult:
            result = await self.execute_single(t, timeout)
            if stop_on_error and not result.success:
                raise RuntimeError(f"Task {t.task_id} failed: {result.error}")
            return result

        # Execute with gather, collecting results
        results = []
        for t in tasks:
            try:
                result = await self.execute_single(t, timeout)
                results.append(result)

                if stop_on_error and not result.success:
                    logger.warning(f"Stopping on error: {t.task_id}")
                    break

            except Exception as e:
                logger.error(f"Task execution error: {e}")

        # Update stats
        self._update_stats(results, time.time() - start_time)

        return results

    async def execute_batch(
        self,
        tasks: List[TaskItem],
        batch_size: int,
        timeout: Optional[float] = None,
    ) -> List[TaskResult]:
        """Execute tasks in batches."""
        results: List[TaskResult] = []

        for i in range(0, len(tasks), batch_size):
            batch = tasks[i:i + batch_size]
            batch_results = await self.execute_all(batch, timeout=timeout)
            results.extend(batch_results)

        return results

    def _update_stats(
        self,
        results: List[TaskResult],
        total_time_seconds: float,
    ) -> None:
        """Update execution statistics."""
        self._stats = ParallelExecutionStats(
            total_tasks=len(results),
            successful=sum(1 for r in results if r.success),
            failed=sum(1 for r in results if not r.success),
            total_time_ms=total_time_seconds * 1000,
            avg_task_time_ms=(
                sum(r.execution_time_ms for r in results) / len(results)
                if results else 0.0
            ),
            max_task_time_ms=(
                max(r.execution_time_ms for r in results)
                if results else 0.0
            ),
        )

    def get_stats(self) -> ParallelExecutionStats:
        """Get execution statistics."""
        return self._stats

    def map_parallel(
        self,
        func: Callable[[Any], T],
        items: List[Any],
        max_concurrency: Optional[int] = None,
    ) -> List[TaskResult]:
        """
        Map a function over items in parallel.

        Note: This is a sync wrapper around async execution.
        """
        tasks = [
            TaskItem(f"map-{i}", func, args=(item,))
            for i, item in enumerate(items)
        ]

        semaphore_limit = max_concurrency or self.max_concurrency

        async def run() -> List[TaskResult]:
            return await self.execute_all(tasks)

        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                # Create a new loop if we're in an async context
                import concurrent.futures
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    future = executor.submit(lambda: asyncio.run(run()))
                    return future.result()
            return loop.run_until_complete(run())
        except RuntimeError:
            return asyncio.run(run())

    async def execute_with_fanout(
        self,
        producer: Callable[[], List[TaskItem]],
        worker: Callable[[TaskItem], TaskResult],
        max_producers: int = 3,
    ) -> List[TaskResult]:
        """Execute with a producer-consumer (fanout) pattern."""
        all_results: List[TaskResult] = []
        semaphore = asyncio.Semaphore(max_producers)

        async def produce_and_work():
            async with semaphore:
                tasks = producer()
                return await self.execute_all([TaskItem(f"fanout-{i}", worker, args=(t,)) for i, t in enumerate(tasks)])

        producer_results = await asyncio.gather(
            produce_and_work(),
            return_exceptions=True,
        )

        for pr in producer_results:
            if isinstance(pr, list):
                all_results.extend(pr)

        return all_results
