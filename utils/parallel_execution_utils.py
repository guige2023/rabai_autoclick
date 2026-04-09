"""
Parallel Execution Utilities for UI Automation.

This module provides utilities for running automation tasks in parallel,
managing worker pools, and coordinating concurrent operations.

Author: AI Assistant
License: MIT
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import threading
import time
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Callable, Optional
from collections import deque


class TaskStatus(Enum):
    """Status of a parallel task."""
    PENDING = auto()
    RUNNING = auto()
    COMPLETED = auto()
    FAILED = auto()
    CANCELLED = auto()


@dataclass
class Task:
    """
    A task for parallel execution.
    
    Attributes:
        task_id: Unique identifier
        func: Function to execute
        args: Positional arguments
        kwargs: Keyword arguments
        priority: Task priority (higher = sooner)
    """
    task_id: str
    func: Callable
    args: tuple = field(default_factory=tuple)
    kwargs: dict = field(default_factory=dict)
    priority: int = 0
    
    # Runtime state
    status: TaskStatus = TaskStatus.PENDING
    result: Any = None
    error: Optional[str] = None
    start_time: Optional[float] = None
    end_time: Optional[float] = None
    
    @property
    def duration_ms(self) -> Optional[float]:
        """Get task duration in milliseconds."""
        if self.start_time and self.end_time:
            return (self.end_time - self.start_time) * 1000
        return None


class WorkerPool:
    """
    A thread pool for parallel task execution.
    
    Example:
        pool = WorkerPool(max_workers=4)
        pool.submit(my_task)
        results = pool.wait_all()
    """
    
    def __init__(self, max_workers: int = 4):
        self.max_workers = max_workers
        self._executor = concurrent.futures.ThreadPoolExecutor(max_workers=max_workers)
        self._futures: dict[concurrent.futures.Future, Task] = {}
        self._lock = threading.Lock()
    
    def submit(
        self,
        func: Callable,
        *args,
        task_id: Optional[str] = None,
        priority: int = 0,
        **kwargs
    ) -> Task:
        """
        Submit a task for execution.
        
        Args:
            func: Function to execute
            args: Positional arguments
            task_id: Optional task ID (auto-generated if not provided)
            priority: Task priority
            kwargs: Keyword arguments
            
        Returns:
            Task object
        """
        import uuid
        
        task = Task(
            task_id=task_id or str(uuid.uuid4()),
            func=func,
            args=args,
            kwargs=kwargs,
            priority=priority
        )
        
        future = self._executor.submit(self._execute_task, task)
        
        with self._lock:
            self._futures[future] = task
        
        return task
    
    def _execute_task(self, task: Task) -> Any:
        """Execute a task."""
        task.status = TaskStatus.RUNNING
        task.start_time = time.time()
        
        try:
            result = task.func(*task.args, **task.kwargs)
            task.result = result
            task.status = TaskStatus.COMPLETED
            return result
        except Exception as e:
            task.error = f"{type(e).__name__}: {str(e)}"
            task.status = TaskStatus.FAILED
            return None
        finally:
            task.end_time = time.time()
    
    def get_task_status(self, task: Task) -> TaskStatus:
        """Get the status of a task."""
        with self._lock:
            for future, t in self._futures.items():
                if t.task_id == task.task_id:
                    if future.done():
                        if future.exception():
                            return TaskStatus.FAILED
                        return TaskStatus.COMPLETED
                    elif future.running():
                        return TaskStatus.RUNNING
                    return TaskStatus.PENDING
        return task.status
    
    def wait_all(self, timeout: Optional[float] = None) -> list[Any]:
        """
        Wait for all submitted tasks to complete.
        
        Args:
            timeout: Optional timeout in seconds
            
        Returns:
            List of results
        """
        results = []
        
        with self._lock:
            futures = list(self._futures.keys())
        
        for future in concurrent.futures.as_completed(futures, timeout=timeout):
            try:
                result = future.result()
                results.append(result)
            except Exception:
                results.append(None)
        
        return results
    
    def cancel_all(self) -> None:
        """Cancel all pending tasks."""
        with self._lock:
            for future in self._futures:
                future.cancel()
            self._futures.clear()
    
    def shutdown(self, wait: bool = True) -> None:
        """Shutdown the worker pool."""
        self._executor.shutdown(wait=wait)


class AsyncWorkerPool:
    """
    An async worker pool for asynchronous task execution.
    
    Example:
        pool = AsyncWorkerPool(max_workers=4)
        await pool.submit_async(coro_function())
        results = await pool.wait_all()
    """
    
    def __init__(self, max_workers: int = 4):
        self.max_workers = max_workers
        self._semaphore = asyncio.Semaphore(max_workers)
        self._tasks: set[asyncio.Task] = set()
    
    async def submit_async(self, coro) -> Any:
        """Submit an async task."""
        async def run_with_semaphore():
            async with self._semaphore:
                return await coro
        
        task = asyncio.create_task(run_with_semaphore())
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)
        
        return await task
    
    async def wait_all(self) -> list[Any]:
        """Wait for all tasks to complete."""
        results = []
        
        if self._tasks:
            done, pending = await asyncio.wait(
                self._tasks,
                return_when=asyncio.ALL_COMPLETED
            )
            
            for task in done:
                try:
                    results.append(task.result())
                except Exception:
                    results.append(None)
        
        return results
    
    async def cancel_all(self) -> None:
        """Cancel all pending tasks."""
        for task in self._tasks:
            task.cancel()
        
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
        
        self._tasks.clear()


class ParallelBatchExecutor:
    """
    Executes tasks in parallel batches.
    
    Example:
        executor = ParallelBatchExecutor(batch_size=10, max_workers=4)
        items = [1, 2, 3, 4, 5]
        results = executor.execute_batch(process_item, items)
    """
    
    def __init__(self, batch_size: int = 10, max_workers: int = 4):
        self.batch_size = batch_size
        self.pool = WorkerPool(max_workers=max_workers)
    
    def execute_batch(
        self,
        func: Callable[[Any], Any],
        items: list[Any],
        timeout: Optional[float] = None
    ) -> list[Any]:
        """
        Execute a function on items in parallel batches.
        
        Args:
            func: Function to apply to each item
            items: List of items to process
            timeout: Optional timeout per item
            
        Returns:
            List of results in same order as items
        """
        results = [None] * len(items)
        pending = {}
        
        for i, item in enumerate(items):
            task = self.pool.submit(func, item)
            pending[task.task_id] = (i, task)
        
        # Collect results
        completed = self.pool.wait_all(timeout=timeout)
        
        # Map results back to original positions
        for task in self.pool._futures.values():
            if task.task_id in pending:
                idx, _ = pending[task.task_id]
                results[idx] = task.result
        
        return results
    
    def shutdown(self) -> None:
        """Shutdown the executor."""
        self.pool.shutdown()


class RateLimiter:
    """
    Rate limiter for controlling execution frequency.
    
    Example:
        limiter = RateLimiter(max_calls=10, per_seconds=1.0)
        for item in items:
            limiter.acquire()
            process(item)
    """
    
    def __init__(self, max_calls: int, per_seconds: float):
        self.max_calls = max_calls
        self.per_seconds = per_seconds
        self._calls = deque()
        self._lock = threading.Lock()
    
    def acquire(self, blocking: bool = True, timeout: Optional[float] = None) -> bool:
        """
        Acquire a rate limit token.
        
        Args:
            blocking: Whether to block if no tokens available
            timeout: Optional timeout
            
        Returns:
            True if acquired, False if timeout
        """
        start_time = time.time()
        
        while True:
            with self._lock:
                # Remove expired calls
                now = time.time()
                while self._calls and self._calls[0] < now:
                    self._calls.popleft()
                
                if len(self._calls) < self.max_calls:
                    self._calls.append(now + self.per_seconds)
                    return True
                
                if not blocking:
                    return False
                
                # Calculate wait time
                wait_time = self._calls[0] - now
                if timeout:
                    wait_time = min(wait_time, timeout)
                    if time.time() - start_time >= timeout:
                        return False
            
            time.sleep(min(wait_time, 0.1))
    
    def __enter__(self) -> 'RateLimiter':
        self.acquire()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        pass
