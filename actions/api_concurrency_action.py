"""API Concurrency Action Module.

Provides concurrency control for API requests including semaphore-based
limiting, worker pools, request batching, and parallel execution with
timeout and cancellation support.
"""

from __future__ import annotations

import asyncio
import logging
import threading
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, Future, TimeoutError
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set, Tuple, TypeVar

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


logger = logging.getLogger(__name__)

T = TypeVar("T")


class ConcurrencyMode(Enum):
    """Concurrency execution modes."""
    SEQUENTIAL = "sequential"
    PARALLEL = "parallel"
    SEMAPHORE = "semaphore"
    WORKER_POOL = "worker_pool"
    BATCH = "batch"


@dataclass
class ConcurrencyConfig:
    """Configuration for concurrency control."""
    mode: ConcurrencyMode = ConcurrencyMode.PARALLEL
    max_workers: int = 10
    semaphore_limit: int = 5
    batch_size: int = 10
    timeout_seconds: float = 30.0
    retry_attempts: int = 3
    retry_delay: float = 1.0


@dataclass
class ConcurrencyTask:
    """A task to be executed with concurrency control."""
    task_id: str
    func: Callable[..., T]
    args: Tuple[Any, ...] = field(default_factory=tuple)
    kwargs: Dict[str, Any] = field(default_factory=dict)
    priority: int = 0
    timeout: Optional[float] = None


@dataclass
class ConcurrencyResult:
    """Result of a concurrency operation."""
    task_id: str
    success: bool
    result: Optional[Any] = None
    error: Optional[str] = None
    duration_ms: float = 0.0
    attempts: int = 1


class SemaphoreLimiter:
    """Thread-safe semaphore-based rate limiter."""

    def __init__(self, limit: int):
        self._semaphore = threading.Semaphore(limit)
        self._lock = threading.Lock()
        self._acquired_count = 0
        self._waiters = 0

    def acquire(self, timeout: Optional[float] = None) -> bool:
        """Acquire a semaphore permit."""
        self._waiters += 1
        try:
            result = self._semaphore.acquire(timeout=timeout if timeout else -1)
            if result:
                with self._lock:
                    self._acquired_count += 1
            return result
        finally:
            self._waiters -= 1

    def release(self):
        """Release a semaphore permit."""
        with self._lock:
            self._acquired_count = max(0, self._acquired_count - 1)
        self._semaphore.release()

    @property
    def acquired(self) -> int:
        """Current number of acquired permits."""
        with self._lock:
            return self._acquired_count


class WorkerPool:
    """Thread pool with task tracking."""

    def __init__(self, max_workers: int, name: str = "worker_pool"):
        self._max_workers = max_workers
        self._name = name
        self._executor = ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix=name)
        self._futures: Dict[str, Future] = {}
        self._lock = threading.Lock()
        self._results: Dict[str, ConcurrencyResult] = {}

    def submit(self, task: ConcurrencyTask) -> Future:
        """Submit a task to the worker pool."""
        future = self._executor.submit(self._execute_task, task)
        with self._lock:
            self._futures[task.task_id] = future
        return future

    def _execute_task(self, task: ConcurrencyTask) -> ConcurrencyResult:
        """Execute a single task with retry logic."""
        start_time = time.time()
        last_error = None

        for attempt in range(task.timeout or 1):
            try:
                result = task.func(*task.args, **task.kwargs)
                duration_ms = (time.time() - start_time) * 1000
                return ConcurrencyResult(
                    task_id=task.task_id,
                    success=True,
                    result=result,
                    duration_ms=duration_ms,
                    attempts=attempt + 1
                )
            except Exception as e:
                last_error = str(e)
                logger.warning(f"Task {task.task_id} failed on attempt {attempt + 1}: {e}")

        duration_ms = (time.time() - start_time) * 1000
        return ConcurrencyResult(
            task_id=task.task_id,
            success=False,
            error=last_error,
            duration_ms=duration_ms,
            attempts=task.timeout or 1
        )

    def shutdown(self, wait: bool = True):
        """Shutdown the worker pool."""
        self._executor.shutdown(wait=wait)

    def get_results(self) -> Dict[str, ConcurrencyResult]:
        """Get all completed results."""
        with self._lock:
            completed = {}
            for task_id, future in list(self._futures.items()):
                if future.done():
                    try:
                        completed[task_id] = future.result()
                    except Exception as e:
                        completed[task_id] = ConcurrencyResult(
                            task_id=task_id,
                            success=False,
                            error=str(e)
                        )
            return completed


class ApiConcurrencyAction(BaseAction):
    """Action for managing API request concurrency."""

    def __init__(self):
        super().__init__(name="api_concurrency")
        self._config = ConcurrencyConfig()
        self._semaphore_limiter = SemaphoreLimiter(self._config.semaphore_limit)
        self._worker_pool: Optional[WorkerPool] = None
        self._lock = threading.Lock()
        self._execution_history: List[ConcurrencyResult] = []

    def configure(self, config: ConcurrencyConfig):
        """Configure concurrency settings."""
        self._config = config
        if config.mode == ConcurrencyMode.WORKER_POOL:
            self._worker_pool = WorkerPool(config.max_workers)

    def execute(self, tasks: List[ConcurrencyTask]) -> ActionResult:
        """Execute tasks with configured concurrency mode."""
        try:
            if not tasks:
                return ActionResult(success=False, error="No tasks provided")

            start_time = time.time()

            if self._config.mode == ConcurrencyMode.SEQUENTIAL:
                results = self._execute_sequential(tasks)
            elif self._config.mode == ConcurrencyMode.PARALLEL:
                results = self._execute_parallel(tasks)
            elif self._config.mode == ConcurrencyMode.SEMAPHORE:
                results = self._execute_with_semaphore(tasks)
            elif self._config.mode == ConcurrencyMode.WORKER_POOL:
                results = self._execute_with_worker_pool(tasks)
            elif self._config.mode == ConcurrencyMode.BATCH:
                results = self._execute_batched(tasks)
            else:
                return ActionResult(success=False, error=f"Unknown mode: {self._config.mode}")

            duration_ms = (time.time() - start_time) * 1000
            success_count = sum(1 for r in results if r.success)
            total_count = len(results)

            with self._lock:
                self._execution_history.extend(results)

            return ActionResult(
                success=success_count == total_count,
                data={
                    "total": total_count,
                    "successful": success_count,
                    "failed": total_count - success_count,
                    "duration_ms": duration_ms,
                    "results": [
                        {"task_id": r.task_id, "success": r.success, "error": r.error}
                        for r in results
                    ]
                }
            )
        except Exception as e:
            logger.exception("Concurrency execution failed")
            return ActionResult(success=False, error=str(e))

    def _execute_sequential(self, tasks: List[ConcurrencyTask]) -> List[ConcurrencyResult]:
        """Execute tasks sequentially."""
        results = []
        for task in tasks:
            result = self._run_single_task(task)
            results.append(result)
        return results

    def _execute_parallel(self, tasks: List[ConcurrencyTask]) -> List[ConcurrencyResult]:
        """Execute tasks in parallel using ThreadPoolExecutor."""
        with ThreadPoolExecutor(max_workers=self._config.max_workers) as executor:
            futures = {executor.submit(self._run_single_task, task): task for task in tasks}
            results = []
            for future in futures:
                try:
                    results.append(future.result(timeout=self._config.timeout_seconds))
                except TimeoutError:
                    results.append(ConcurrencyResult(
                        task_id=futures[future].task_id,
                        success=False,
                        error="Task timed out"
                    ))
            return results

    def _execute_with_semaphore(self, tasks: List[ConcurrencyTask]) -> List[ConcurrencyResult]:
        """Execute tasks with semaphore-based concurrency control."""
        results = []

        def run_with_semaphore(task: ConcurrencyTask) -> ConcurrencyResult:
            if not self._semaphore_limiter.acquire(timeout=self._config.timeout_seconds):
                return ConcurrencyResult(
                    task_id=task.task_id,
                    success=False,
                    error="Failed to acquire semaphore"
                )
            try:
                return self._run_single_task(task)
            finally:
                self._semaphore_limiter.release()

        with ThreadPoolExecutor(max_workers=self._config.max_workers) as executor:
            futures = {executor.submit(run_with_semaphore, task): task for task in tasks}
            for future in futures:
                try:
                    results.append(future.result(timeout=self._config.timeout_seconds))
                except TimeoutError:
                    results.append(ConcurrencyResult(
                        task_id=futures[future].task_id,
                        success=False,
                        error="Task timed out"
                    ))
        return results

    def _execute_with_worker_pool(self, tasks: List[ConcurrencyTask]) -> List[ConcurrencyResult]:
        """Execute tasks using a managed worker pool."""
        if not self._worker_pool:
            self._worker_pool = WorkerPool(self._config.max_workers)

        futures = []
        for task in tasks:
            future = self._worker_pool.submit(task)
            futures.append(future)

        results = []
        for future in futures:
            try:
                results.append(future.result(timeout=self._config.timeout_seconds))
            except TimeoutError:
                results.append(ConcurrencyResult(
                    task_id="unknown",
                    success=False,
                    error="Task timed out"
                ))
        return results

    def _execute_batched(self, tasks: List[ConcurrencyTask]) -> List[ConcurrencyResult]:
        """Execute tasks in batches."""
        results = []
        batch_size = self._config.batch_size

        for i in range(0, len(tasks), batch_size):
            batch = tasks[i:i + batch_size]
            batch_results = self._execute_parallel(batch)
            results.extend(batch_results)

        return results

    def _run_single_task(self, task: ConcurrencyTask) -> ConcurrencyResult:
        """Run a single task with retry logic."""
        start_time = time.time()
        last_error = None

        for attempt in range(self._config.retry_attempts):
            try:
                timeout = task.timeout or self._config.timeout_seconds
                result = task.func(*task.args, **task.kwargs)
                duration_ms = (time.time() - start_time) * 1000
                return ConcurrencyResult(
                    task_id=task.task_id,
                    success=True,
                    result=result,
                    duration_ms=duration_ms,
                    attempts=attempt + 1
                )
            except Exception as e:
                last_error = str(e)
                if attempt < self._config.retry_attempts - 1:
                    time.sleep(self._config.retry_delay * (attempt + 1))

        duration_ms = (time.time() - start_time) * 1000
        return ConcurrencyResult(
            task_id=task.task_id,
            success=False,
            error=last_error,
            duration_ms=duration_ms,
            attempts=self._config.retry_attempts
        )

    def get_stats(self) -> Dict[str, Any]:
        """Get concurrency statistics."""
        with self._lock:
            total = len(self._execution_history)
            successful = sum(1 for r in self._execution_history if r.success)
            return {
                "total_executions": total,
                "successful": successful,
                "failed": total - successful,
                "success_rate": successful / total if total > 0 else 0.0,
                "avg_duration_ms": sum(r.duration_ms for r in self._execution_history) / total if total > 0 else 0.0,
                "current_semaphore_acquired": self._semaphore_limiter.acquired if hasattr(self, '_semaphore_limiter') else 0,
            }
