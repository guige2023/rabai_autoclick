"""
API Fan-out Action Module

Provides fan-out pattern for executing API calls across multiple services or endpoints.
Supports parallel execution, result aggregation, partial failure handling, and 
configurable concurrency control.

MIT License - Copyright (c) 2025 RabAi Research
"""

from __future__ import annotations

import asyncio
import logging
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set

logger = logging.getLogger(__name__)


class FanoutStatus(Enum):
    """Fan-out operation status."""

    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    PARTIAL_FAILURE = "partial_failure"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass
class FanoutTask:
    """A single task in a fan-out operation."""

    task_id: str
    endpoint: str
    method: str = "GET"
    payload: Optional[Dict[str, Any]] = None
    headers: Optional[Dict[str, str]] = None
    timeout_seconds: float = 30.0
    priority: int = 0
    retry_count: int = 0
    max_retries: int = 3


@dataclass
class FanoutResult:
    """Result of a fan-out task."""

    task_id: str
    endpoint: str
    status: str
    response: Optional[Any] = None
    error: Optional[str] = None
    latency_ms: float = 0.0
    started_at: Optional[float] = None
    completed_at: Optional[float] = None


@dataclass
class FanoutOperation:
    """Complete fan-out operation."""

    operation_id: str
    name: str
    tasks: List[FanoutTask]
    status: FanoutStatus = FanoutStatus.PENDING
    results: List[FanoutResult] = field(default_factory=list)
    started_at: Optional[float] = None
    completed_at: Optional[float] = None
    total_duration_ms: float = 0.0


@dataclass
class FanoutConfig:
    """Configuration for fan-out operations."""

    max_concurrency: int = 10
    timeout_seconds: float = 60.0
    continue_on_failure: bool = True
    fail_fast_on_first_error: bool = False
    aggregate_results: bool = True
    result_aggregation_strategy: str = "all"
    enable_cancellation: bool = True


class ResultAggregator:
    """Aggregates results from fan-out operations."""

    def aggregate_all(
        self,
        results: List[FanoutResult],
    ) -> Dict[str, Any]:
        """Aggregate all results."""
        successful = [r for r in results if r.status == "success"]
        failed = [r for r in results if r.status != "success"]

        return {
            "total": len(results),
            "successful": len(successful),
            "failed": len(failed),
            "success_rate": len(successful) / len(results) * 100 if results else 0,
            "results": [r.__dict__ for r in results],
        }

    def aggregate_first(
        self,
        results: List[FanoutResult],
    ) -> Dict[str, Any]:
        """Return first result only."""
        return {
            "total": len(results),
            "results": results[0].__dict__ if results else None,
        }

    def aggregate_successful(
        self,
        results: List[FanoutResult],
    ) -> Dict[str, Any]:
        """Aggregate only successful results."""
        successful = [r for r in results if r.status == "success"]

        return {
            "total": len(results),
            "successful": len(successful),
            "results": [r.__dict__ for r in successful],
        }


class APIFanoutAction:
    """
    Fan-out action for executing API calls across multiple services.

    Features:
    - Parallel task execution with configurable concurrency
    - Task priority ordering
    - Retry logic with backoff
    - Partial failure handling
    - Result aggregation strategies
    - Timeout management
    - Operation cancellation

    Usage:
        fanout = APIFanoutAction(config)
        
        tasks = [
            FanoutTask(task_id="1", endpoint="/service-a"),
            FanoutTask(task_id="2", endpoint="/service-b"),
        ]
        
        result = await fanout.execute("multi-fetch", tasks, executor_func)
    """

    def __init__(self, config: Optional[FanoutConfig] = None):
        self.config = config or FanoutConfig()
        self._aggregator = ResultAggregator()
        self._operations: Dict[str, FanoutOperation] = {}
        self._active_operations: Set[str] = set()
        self._stats = {
            "operations_started": 0,
            "operations_completed": 0,
            "tasks_executed": 0,
            "tasks_succeeded": 0,
            "tasks_failed": 0,
        }

    def create_task(
        self,
        task_id: str,
        endpoint: str,
        method: str = "GET",
        payload: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
        timeout_seconds: float = 30.0,
        priority: int = 0,
    ) -> FanoutTask:
        """Create a fan-out task."""
        return FanoutTask(
            task_id=task_id,
            endpoint=endpoint,
            method=method,
            payload=payload,
            headers=headers,
            timeout_seconds=timeout_seconds,
            priority=priority,
        )

    async def execute(
        self,
        operation_name: str,
        tasks: List[FanoutTask],
        executor: Callable[..., Any],
        config: Optional[FanoutConfig] = None,
    ) -> FanoutOperation:
        """
        Execute a fan-out operation.

        Args:
            operation_name: Name for this operation
            tasks: List of tasks to execute
            executor: Async function to execute each task
            config: Optional override config

        Returns:
            FanoutOperation with results
        """
        cfg = config or self.config
        operation_id = f"fanout_{uuid.uuid4().hex[:12]}"

        operation = FanoutOperation(
            operation_id=operation_id,
            name=operation_name,
            tasks=tasks,
        )
        self._operations[operation_id] = operation
        self._active_operations.add(operation_id)
        self._stats["operations_started"] += 1

        operation.status = FanoutStatus.RUNNING
        operation.started_at = time.time()

        logger.info(f"Starting fan-out operation {operation_id} with {len(tasks)} tasks")

        try:
            results = await self._execute_tasks(operation.tasks, executor, cfg)
            operation.results = results

            failed_count = sum(1 for r in results if r.status == "failed")
            if failed_count == 0:
                operation.status = FanoutStatus.COMPLETED
            elif failed_count < len(results):
                operation.status = FanoutStatus.PARTIAL_FAILURE
            else:
                operation.status = FanoutStatus.FAILED

        except asyncio.CancelledError:
            operation.status = FanoutStatus.CANCELLED
            logger.warning(f"Fan-out operation {operation_id} cancelled")
            raise

        finally:
            operation.completed_at = time.time()
            operation.total_duration_ms = (operation.completed_at - operation.started_at) * 1000
            self._active_operations.discard(operation_id)
            self._stats["operations_completed"] += 1

        return operation

    async def _execute_tasks(
        self,
        tasks: List[FanoutTask],
        executor: Callable[..., Any],
        config: FanoutConfig,
    ) -> List[FanoutResult]:
        """Execute tasks with concurrency control."""
        # Sort by priority
        sorted_tasks = sorted(tasks, key=lambda t: -t.priority)

        semaphore = asyncio.Semaphore(config.max_concurrency)
        results: List[FanoutResult] = []

        async def execute_with_semaphore(task: FanoutTask) -> FanoutResult:
            async with semaphore:
                return await self._execute_single_task(task, executor, config)

        # Use gather for parallel execution
        if config.fail_fast_on_first_error:
            # Execute with return_exceptions to catch first error
            tasks_coroutines = [execute_with_semaphore(t) for t in sorted_tasks]
            results = await asyncio.gather(*tasks_coroutines, return_exceptions=True)

            # Filter out exceptions and handle them
            processed_results = []
            for i, r in enumerate(results):
                if isinstance(r, Exception):
                    processed_results.append(FanoutResult(
                        task_id=sorted_tasks[i].task_id,
                        endpoint=sorted_tasks[i].endpoint,
                        status="failed",
                        error=str(r),
                    ))
                else:
                    processed_results.append(r)
            results = processed_results
        else:
            results = await asyncio.gather(
                *[execute_with_semaphore(t) for t in sorted_tasks],
                return_exceptions=True,
            )

            # Process exceptions
            processed_results = []
            for i, r in enumerate(results):
                if isinstance(r, Exception):
                    processed_results.append(FanoutResult(
                        task_id=sorted_tasks[i].task_id,
                        endpoint=sorted_tasks[i].endpoint,
                        status="failed",
                        error=str(r),
                    ))
                else:
                    processed_results.append(r)
            results = processed_results

        return results

    async def _execute_single_task(
        self,
        task: FanoutTask,
        executor: Callable[..., Any],
        config: FanoutConfig,
    ) -> FanoutResult:
        """Execute a single task with retry logic."""
        result = FanoutResult(
            task_id=task.task_id,
            endpoint=task.endpoint,
            status="pending",
        )
        result.started_at = time.time()

        self._stats["tasks_executed"] += 1

        for attempt in range(task.max_retries + 1):
            try:
                response = await asyncio.wait_for(
                    executor(task),
                    timeout=task.timeout_seconds,
                )

                result.status = "success"
                result.response = response
                result.completed_at = time.time()
                result.latency_ms = (result.completed_at - result.started_at) * 1000
                self._stats["tasks_succeeded"] += 1
                return result

            except asyncio.TimeoutError:
                result.error = f"Timeout after {task.timeout_seconds}s"
                if attempt >= task.max_retries:
                    result.status = "failed"
                    self._stats["tasks_failed"] += 1
                else:
                    await asyncio.sleep(0.5 * (attempt + 1))

            except Exception as e:
                result.error = str(e)
                if attempt >= task.max_retries:
                    result.status = "failed"
                    self._stats["tasks_failed"] += 1
                else:
                    await asyncio.sleep(0.5 * (attempt + 1))

        result.completed_at = time.time()
        result.latency_ms = (result.completed_at - result.started_at) * 1000
        return result

    def aggregate_results(
        self,
        operation: FanoutOperation,
        strategy: str = "all",
    ) -> Dict[str, Any]:
        """
        Aggregate results from a fan-out operation.

        Args:
            operation: Completed operation
            strategy: Aggregation strategy (all, first, successful)

        Returns:
            Aggregated results
        """
        if strategy == "first":
            return self._aggregator.aggregate_first(operation.results)
        elif strategy == "successful":
            return self._aggregator.aggregate_successful(operation.results)
        else:
            return self._aggregator.aggregate_all(operation.results)

    def get_operation(self, operation_id: str) -> Optional[FanoutOperation]:
        """Get an operation by ID."""
        return self._operations.get(operation_id)

    def get_active_operations(self) -> List[FanoutOperation]:
        """Get all active operations."""
        return [
            op for op in self._operations.values()
            if op.status == FanoutStatus.RUNNING
        ]

    def cancel_operation(self, operation_id: str) -> bool:
        """Cancel an active operation."""
        if not self.config.enable_cancellation:
            return False

        operation = self._operations.get(operation_id)
        if operation and operation.status == FanoutStatus.RUNNING:
            operation.status = FanoutStatus.CANCELLED
            return True
        return False

    def get_stats(self) -> Dict[str, Any]:
        """Get fan-out statistics."""
        return {
            **self._stats.copy(),
            "active_operations": len(self._active_operations),
            "total_operations": len(self._operations),
        }


async def demo_fanout():
    """Demonstrate fan-out execution."""

    async def mock_executor(task: FanoutTask) -> Dict[str, Any]:
        await asyncio.sleep(0.05)
        return {"task_id": task.task_id, "endpoint": task.endpoint, "data": "ok"}

    config = FanoutConfig(max_concurrency=5)
    fanout = APIFanoutAction(config)

    tasks = [
        fanout.create_task(f"task_{i}", f"/service-{i}", priority=i % 3)
        for i in range(20)
    ]

    operation = await fanout.execute("batch-fetch", tasks, mock_executor)

    print(f"Operation status: {operation.status.value}")
    print(f"Results: {len(operation.results)}")
    print(f"Stats: {fanout.get_stats()}")

    aggregated = fanout.aggregate_results(operation)
    print(f"Success rate: {aggregated['success_rate']:.1f}%")


if __name__ == "__main__":
    asyncio.run(demo_fanout())
