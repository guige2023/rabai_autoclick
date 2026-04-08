"""
Parallel Execution Action Module.

Provides parallel and concurrent task execution with configurable worker pools,
rate limiting, result aggregation, and error handling strategies.

Author: RabAi Team
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import json
import threading
import time
import uuid
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set, Tuple, TypeVar

T = TypeVar("T")
R = TypeVar("R")


class ExecutionMode(Enum):
    """Parallel execution modes."""
    THREAD_POOL = "thread_pool"
    PROCESS_POOL = "process_pool"
    ASYNC = "async"
    SEQUENTIAL = "sequential"


class TaskStatus(Enum):
    """Individual task status."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass
class TaskResult:
    """Result of a single task execution."""
    task_id: str
    status: TaskStatus
    result: Any = None
    error: Optional[str] = None
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    duration_ms: Optional[float] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    @property
    def is_success(self) -> bool:
        return self.status == TaskStatus.COMPLETED

    def to_dict(self) -> Dict[str, Any]:
        return {
            "task_id": self.task_id,
            "status": self.status.value,
            "result": self.result,
            "error": self.error,
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "duration_ms": self.duration_ms,
            "metadata": self.metadata,
        }


@dataclass
class BatchResult:
    """Result of a batch parallel execution."""
    batch_id: str
    total_tasks: int
    successful: int
    failed: int
    cancelled: int
    results: List[TaskResult]
    total_duration_ms: float
    started_at: datetime
    completed_at: datetime = field(default_factory=datetime.now)
    metadata: Dict[str, Any] = field(default_factory=dict)

    @property
    def success_rate(self) -> float:
        return self.successful / self.total_tasks if self.total_tasks > 0 else 0.0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "batch_id": self.batch_id,
            "total_tasks": self.total_tasks,
            "successful": self.successful,
            "failed": self.failed,
            "cancelled": self.cancelled,
            "success_rate": self.success_rate,
            "total_duration_ms": self.total_duration_ms,
            "started_at": self.started_at.isoformat(),
            "completed_at": self.completed_at.isoformat(),
            "metadata": self.metadata,
        }

    def get_results(self, status: Optional[TaskStatus] = None) -> List[TaskResult]:
        """Get results filtered by status."""
        if status is None:
            return self.results
        return [r for r in self.results if r.status == status]

    def get_successful(self) -> List[TaskResult]:
        return self.get_results(TaskStatus.COMPLETED)

    def get_failed(self) -> List[TaskResult]:
        return self.get_results(TaskStatus.FAILED)


@dataclass
class ExecutionConfig:
    """Configuration for parallel execution."""
    max_workers: int = 4
    mode: ExecutionMode = ExecutionMode.THREAD_POOL
    timeout_seconds: Optional[float] = None
    max_retries: int = 0
    retry_delay: float = 0.0
    rate_limit_per_second: Optional[float] = None
    stop_on_first_error: bool = False


class TaskBuilder:
    """Builder for creating tasks."""

    def __init__(self, task_id: str, func: Callable):
        self.task_id = task_id
        self.func = func
        self.args: Tuple = ()
        self.kwargs: Dict[str, Any] = {}
        self.metadata: Dict[str, Any] = {}

    def with_args(self, *args, **kwargs) -> "TaskBuilder":
        self.args = args
        self.kwargs = kwargs
        return self

    def with_metadata(self, **kwargs) -> "TaskBuilder":
        self.metadata.update(kwargs)
        return self

    def build(self) -> Callable:
        def wrapper():
            return self.func(*self.args, **self.kwargs)
        return wrapper


class ParallelExecutor:
    """
    Parallel task execution engine with configurable worker pools.

    Supports thread-based, process-based, and async execution modes
    with built-in rate limiting, retry logic, and result aggregation.

    Example:
        >>> executor = ParallelExecutor(max_workers=8, mode=ExecutionMode.THREAD_POOL)
        >>> batch_id = executor.submit_batch([{"fn": func1, "args": (1,)}, {"fn": func2}])
        >>> result = executor.get_result(batch_id)
    """

    def __init__(self, config: Optional[ExecutionConfig] = None):
        self.config = config or ExecutionConfig()
        self._executor: Optional[concurrent.futures.Executor] = None
        self._async_loop: Optional[asyncio.AbstractEventLoop] = None
        self._batch_results: Dict[str, BatchResult] = {}
        self._task_results: Dict[str, TaskResult] = {}
        self._running_tasks: Set[str] = set()
        self._lock = threading.Lock()
        self._rate_limiter_last_call: Dict[str, float] = {}
        self._cancelled_tasks: Set[str] = set()

    def submit_batch(
        self,
        tasks: List[Dict[str, Any]],
        batch_metadata: Optional[Dict[str, Any]] = None,
    ) -> str:
        """Submit a batch of tasks for parallel execution."""
        batch_id = str(uuid.uuid4())
        started_at = datetime.now()

        if self.config.mode == ExecutionMode.SEQUENTIAL:
            return self._execute_sequential(batch_id, tasks, started_at, batch_metadata)

        self._ensure_executor()

        futures_map: Dict[str, concurrent.futures.Future] = {}
        task_results: List[TaskResult] = []

        for task_def in tasks:
            task_id = task_def.get("id", str(uuid.uuid4()))
            func = task_def["fn"]
            args = task_def.get("args", ())
            kwargs = task_def.get("kwargs", {})

            task_result = TaskResult(
                task_id=task_id,
                status=TaskStatus.PENDING,
                metadata=task_def.get("metadata", {}),
            )
            self._task_results[task_id] = task_result
            task_results.append(task_result)

            future = self._executor.submit(
                self._execute_task,
                task_id,
                func,
                args,
                kwargs,
            )
            futures_map[task_id] = future

        # Wait for all futures
        concurrent.futures.wait(futures_map.values())

        completed_at = datetime.now()
        total_duration = (completed_at - started_at).total_seconds() * 1000

        successful = sum(1 for r in task_results if r.status == TaskStatus.COMPLETED)
        failed = sum(1 for r in task_results if r.status == TaskStatus.FAILED)
        cancelled = sum(1 for r in task_results if r.status == TaskStatus.CANCELLED)

        batch_result = BatchResult(
            batch_id=batch_id,
            total_tasks=len(tasks),
            successful=successful,
            failed=failed,
            cancelled=cancelled,
            results=task_results,
            total_duration_ms=total_duration,
            started_at=started_at,
            completed_at=completed_at,
            metadata=batch_metadata or {},
        )
        self._batch_results[batch_id] = batch_result
        return batch_id

    def submit_task(
        self,
        func: Callable,
        *args,
        task_id: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> str:
        """Submit a single task for execution."""
        task_id = task_id or str(uuid.uuid4())
        batch_id = self.submit_batch([
            {"id": task_id, "fn": func, "args": args, "metadata": metadata or {}}
        ])
        return task_id

    def get_result(self, batch_id: str) -> Optional[BatchResult]:
        """Get result of a batch execution."""
        return self._batch_results.get(batch_id)

    def get_task_result(self, task_id: str) -> Optional[TaskResult]:
        """Get result of a specific task."""
        return self._task_results.get(task_id)

    def cancel_task(self, task_id: str) -> bool:
        """Cancel a pending or running task."""
        with self._lock:
            if task_id in self._cancelled_tasks:
                return False
            self._cancelled_tasks.add(task_id)
            if task_id in self._task_results:
                self._task_results[task_id].status = TaskStatus.CANCELLED
                return True
        return False

    def map_parallel(
        self,
        func: Callable[[T], R],
        items: List[T],
        max_workers: Optional[int] = None,
    ) -> List[R]:
        """Map a function over items in parallel."""
        if not items:
            return []
        workers = max_workers or self.config.max_workers
        with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
            return list(executor.map(func, items))

    def _execute_task(
        self,
        task_id: str,
        func: Callable,
        args: Tuple,
        kwargs: Dict,
    ) -> TaskResult:
        """Execute a single task with error handling."""
        if task_id in self._cancelled_tasks:
            return TaskResult(task_id=task_id, status=TaskStatus.CANCELLED)

        task_result = self._task_results.get(task_id, TaskResult(task_id=task_id, status=TaskStatus.PENDING))
        task_result.status = TaskStatus.RUNNING
        task_result.started_at = datetime.now()

        self._apply_rate_limit()
        start_time = time.time()

        try:
            if self.config.timeout_seconds:
                import signal
                def timeout_handler(signum, frame):
                    raise TimeoutError(f"Task exceeded timeout of {self.config.timeout_seconds}s")
                old_handler = signal.signal(signal.SIGALRM, timeout_handler)
                signal.alarm(int(self.config.timeout_seconds))

            result = func(*args, **kwargs)

            if self.config.timeout_seconds:
                signal.alarm(0)
                signal.signal(signal.SIGALRM, old_handler)

            task_result.result = result
            task_result.status = TaskStatus.COMPLETED

        except Exception as e:
            task_result.error = str(e)
            task_result.status = TaskStatus.FAILED

        finally:
            task_result.completed_at = datetime.now()
            task_result.duration_ms = (time.time() - start_time) * 1000

        return task_result

    def _execute_sequential(
        self,
        batch_id: str,
        tasks: List[Dict[str, Any]],
        started_at: datetime,
        batch_metadata: Optional[Dict[str, Any]],
    ) -> str:
        """Execute tasks sequentially."""
        task_results = []
        for task_def in tasks:
            task_id = task_def.get("id", str(uuid.uuid4()))
            func = task_def["fn"]
            args = task_def.get("args", ())
            kwargs = task_def.get("kwargs", {})
            task_result = self._execute_task(task_id, func, args, kwargs)
            task_results.append(task_result)

            if self.config.stop_on_first_error and task_result.status == TaskStatus.FAILED:
                break

        completed_at = datetime.now()
        total_duration = (completed_at - started_at).total_seconds() * 1000
        successful = sum(1 for r in task_results if r.status == TaskStatus.COMPLETED)
        failed = sum(1 for r in task_results if r.status == TaskStatus.FAILED)

        batch_result = BatchResult(
            batch_id=batch_id,
            total_tasks=len(tasks),
            successful=successful,
            failed=failed,
            cancelled=0,
            results=task_results,
            total_duration_ms=total_duration,
            started_at=started_at,
            completed_at=completed_at,
            metadata=batch_metadata or {},
        )
        self._batch_results[batch_id] = batch_result
        return batch_id

    def _ensure_executor(self) -> None:
        """Ensure executor is initialized."""
        if self._executor is None:
            if self.config.mode == ExecutionMode.PROCESS_POOL:
                self._executor = concurrent.futures.ProcessPoolExecutor(
                    max_workers=self.config.max_workers
                )
            else:
                self._executor = concurrent.futures.ThreadPoolExecutor(
                    max_workers=self.config.max_workers
                )

    def _apply_rate_limit(self) -> None:
        """Apply rate limiting if configured."""
        if not self.config.rate_limit_per_second:
            return
        min_interval = 1.0 / self.config.rate_limit_per_second
        thread_id = threading.current_thread().ident
        last_call = self._rate_limiter_last_call.get(thread_id, 0)
        elapsed = time.time() - last_call
        if elapsed < min_interval:
            time.sleep(min_interval - elapsed)
        self._rate_limiter_last_call[thread_id] = time.time()

    def shutdown(self, wait: bool = True) -> None:
        """Shutdown the executor."""
        if self._executor:
            self._executor.shutdown(wait=wait)
            self._executor = None


def create_parallel_executor(
    max_workers: int = 4,
    mode: str = "thread_pool",
    timeout_seconds: Optional[float] = None,
) -> ParallelExecutor:
    """Factory to create a configured parallel executor."""
    config = ExecutionConfig(
        max_workers=max_workers,
        mode=ExecutionMode(mode),
        timeout_seconds=timeout_seconds,
    )
    return ParallelExecutor(config=config)
