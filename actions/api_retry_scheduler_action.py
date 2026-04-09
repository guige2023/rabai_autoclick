"""API Retry Scheduler Action Module.

Provides intelligent retry scheduling with exponential backoff,
jitter, and priority-based queuing for API requests.

Author: rabai_autoclick team
"""

from __future__ import annotations

import asyncio
import logging
import random
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum, auto
from typing import Any, Callable, Dict, List, Optional, Set

logger = logging.getLogger(__name__)


class RetryStrategy(Enum):
    """Retry backoff strategies."""
    EXPONENTIAL = auto()
    LINEAR = auto()
    FIBONACCI = auto()
    CONSTANT = auto()


@dataclass
class RetryConfig:
    """Configuration for retry behavior."""
    max_attempts: int = 3
    initial_delay_seconds: float = 1.0
    max_delay_seconds: float = 60.0
    strategy: RetryStrategy = RetryStrategy.EXPONENTIAL
    jitter: bool = True
    jitter_factor: float = 0.3
    retryable_exceptions: tuple = (Exception,)
    timeout_seconds: Optional[float] = None


@dataclass
class RetryTask:
    """A task scheduled for retry."""
    id: str
    func: Callable
    args: tuple = field(default_factory=tuple)
    kwargs: Dict[str, Any] = field(default_factory=dict)
    priority: int = 0
    attempt: int = 0
    next_retry_time: float = field(default_factory=time.time)
    created_at: float = field(default_factory=time.time)
    last_error: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class RetryResult:
    """Result of a retry operation."""
    success: bool
    result: Any = None
    attempts: int = 0
    total_time_seconds: float = 0.0
    errors: List[str] = field(default_factory=list)
    final_error: Optional[str] = None


class RetryScheduler:
    """Schedules and executes retries with intelligent backoff.
    
    Features:
    - Multiple backoff strategies
    - Configurable jitter
    - Priority queuing
    - Resource-aware scheduling
    - Retry statistics tracking
    """
    
    def __init__(self, config: Optional[RetryConfig] = None):
        self.config = config or RetryConfig()
        self._queue: deque = deque()
        self._scheduled_tasks: Dict[str, RetryTask] = {}
        self._running = False
        self._processor_task: Optional[asyncio.Task] = None
        self._metrics = {
            "total_retries": 0,
            "successful_retries": 0,
            "failed_retries": 0,
            "total_attempts": 0
        }
        self._lock = asyncio.Lock()
    
    async def execute_with_retry(
        self,
        func: Callable,
        *args,
        config: Optional[RetryConfig] = None,
        priority: int = 0,
        **kwargs
    ) -> RetryResult:
        """Execute function with automatic retry on failure.
        
        Args:
            func: Async function to execute
            *args: Positional arguments
            config: Optional retry configuration override
            priority: Priority for scheduling
            **kwargs: Keyword arguments
            
        Returns:
            Retry result with statistics
        """
        retry_config = config or self.config
        task_id = f"task_{int(time.time() * 1000000)}"
        start_time = time.time()
        errors = []
        last_error = None
        
        for attempt in range(retry_config.max_attempts):
            self._metrics["total_attempts"] += 1
            
            try:
                if retry_config.timeout_seconds:
                    result = await asyncio.wait_for(
                        func(*args, **kwargs),
                        timeout=retry_config.timeout_seconds
                    )
                else:
                    result = await func(*args, **kwargs)
                
                self._metrics["successful_retries"] += 1
                
                return RetryResult(
                    success=True,
                    result=result,
                    attempts=attempt + 1,
                    total_time_seconds=time.time() - start_time,
                    errors=errors
                )
                
            except retry_config.retryable_exceptions as e:
                last_error = str(e)
                errors.append(last_error)
                self._metrics["total_retries"] += 1
                
                if attempt < retry_config.max_attempts - 1:
                    delay = self._calculate_delay(attempt, retry_config)
                    logger.debug(f"Task {task_id} retry {attempt + 1} after {delay:.2f}s: {last_error}")
                    await asyncio.sleep(delay)
                else:
                    self._metrics["failed_retries"] += 1
            
            except asyncio.TimeoutError as e:
                last_error = f"Timeout after {retry_config.timeout_seconds}s"
                errors.append(last_error)
                
                if attempt < retry_config.max_attempts - 1:
                    delay = self._calculate_delay(attempt, retry_config)
                    await asyncio.sleep(delay)
                else:
                    self._metrics["failed_retries"] += 1
        
        return RetryResult(
            success=False,
            attempts=retry_config.max_attempts,
            total_time_seconds=time.time() - start_time,
            errors=errors,
            final_error=last_error
        )
    
    def _calculate_delay(self, attempt: int, config: RetryConfig) -> float:
        """Calculate delay for given attempt using configured strategy.
        
        Args:
            attempt: Current attempt number (0-indexed)
            config: Retry configuration
            
        Returns:
            Delay in seconds
        """
        if config.strategy == RetryStrategy.EXPONENTIAL:
            delay = config.initial_delay_seconds * (2 ** attempt)
        elif config.strategy == RetryStrategy.LINEAR:
            delay = config.initial_delay_seconds * (attempt + 1)
        elif config.strategy == RetryStrategy.FIBONACCI:
            a, b = 1, 1
            for _ in range(attempt):
                a, b = b, a + b
            delay = config.initial_delay_seconds * a
        elif config.strategy == RetryStrategy.CONSTANT:
            delay = config.initial_delay_seconds
        else:
            delay = config.initial_delay_seconds
        
        delay = min(delay, config.max_delay_seconds)
        
        if config.jitter:
            jitter_range = delay * config.jitter_factor
            delay += random.uniform(-jitter_range, jitter_range)
            delay = max(0.1, delay)
        
        return delay
    
    async def schedule_task(
        self,
        func: Callable,
        *args,
        priority: int = 0,
        metadata: Optional[Dict[str, Any]] = None,
        **kwargs
    ) -> str:
        """Schedule a task for deferred retry execution.
        
        Args:
            func: Async function to execute
            *args: Positional arguments
            priority: Task priority (higher = earlier)
            metadata: Optional metadata
            **kwargs: Keyword arguments
            
        Returns:
            Task ID
        """
        task_id = f"scheduled_{int(time.time() * 1000000)}"
        
        task = RetryTask(
            id=task_id,
            func=func,
            args=args,
            kwargs=kwargs,
            priority=priority,
            metadata=metadata or {}
        )
        
        async with self._lock:
            self._scheduled_tasks[task_id] = task
            self._queue.append(task_id)
            self._queue = deque(sorted(self._queue, key=lambda tid: self._scheduled_tasks[tid].priority, reverse=True))
        
        if not self._running:
            await self.start()
        
        return task_id
    
    async def start(self) -> None:
        """Start the retry scheduler processor."""
        self._running = True
        self._processor_task = asyncio.create_task(self._process_loop())
        logger.info("Retry scheduler started")
    
    async def stop(self) -> None:
        """Stop the retry scheduler processor."""
        self._running = False
        
        if self._processor_task:
            self._processor_task.cancel()
            try:
                await self._processor_task
            except asyncio.CancelledError:
                pass
        
        logger.info("Retry scheduler stopped")
    
    async def _process_loop(self) -> None:
        """Main scheduler processing loop."""
        while self._running:
            try:
                now = time.time()
                
                async with self._lock:
                    ready_tasks = [
                        tid for tid in list(self._queue)
                        if tid in self._scheduled_tasks and
                        self._scheduled_tasks[tid].next_retry_time <= now
                    ]
                
                for task_id in ready_tasks:
                    task = self._scheduled_tasks[task_id]
                    
                    try:
                        result = await asyncio.wait_for(
                            task.func(*task.args, **task.kwargs),
                            timeout=30.0
                        )
                        
                        async with self._lock:
                            self._scheduled_tasks.pop(task_id, None)
                            if task_id in self._queue:
                                self._queue.remove(task_id)
                        
                        logger.debug(f"Scheduled task {task_id} completed successfully")
                        
                    except Exception as e:
                        task.attempt += 1
                        task.last_error = str(e)
                        
                        if task.attempt >= self.config.max_attempts:
                            async with self._lock:
                                self._scheduled_tasks.pop(task_id, None)
                                if task_id in self._queue:
                                    self._queue.remove(task_id)
                            logger.warning(f"Scheduled task {task_id} failed after {task.attempt} attempts")
                        else:
                            delay = self._calculate_delay(task.attempt, self.config)
                            task.next_retry_time = time.time() + delay
                
                await asyncio.sleep(0.1)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in retry scheduler: {e}")
    
    def cancel_task(self, task_id: str) -> bool:
        """Cancel a scheduled task.
        
        Args:
            task_id: Task ID to cancel
            
        Returns:
            True if cancelled
        """
        async with self._lock:
            if task_id in self._scheduled_tasks:
                self._scheduled_tasks.pop(task_id)
                if task_id in self._queue:
                    self._queue.remove(task_id)
                return True
        return False
    
    def get_task_status(self, task_id: str) -> Optional[Dict[str, Any]]:
        """Get status of a scheduled task.
        
        Args:
            task_id: Task ID
            
        Returns:
            Task status dictionary
        """
        if task_id not in self._scheduled_tasks:
            return None
        
        task = self._scheduled_tasks[task_id]
        return {
            "id": task.id,
            "priority": task.priority,
            "attempt": task.attempt,
            "next_retry_time": task.next_retry_time,
            "last_error": task.last_error,
            "metadata": task.metadata
        }
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get retry scheduler metrics."""
        return {
            **self._metrics,
            "pending_tasks": len(self._scheduled_tasks),
            "queue_size": len(self._queue)
        }


class AdaptiveRetryScheduler(RetryScheduler):
    """Adaptive retry scheduler that adjusts based on API responses.
    
    Monitors API behavior and adjusts retry parameters dynamically.
    """
    
    def __init__(self, config: Optional[RetryConfig] = None):
        super().__init__(config)
        self._rate_limit_count = 0
        self._server_error_count = 0
        self._success_count = 0
        self._consecutive_failures = 0
        self._adjusted_config = RetryConfig() if config is None else config
    
    async def execute_with_adaptive_retry(
        self,
        func: Callable,
        *args,
        **kwargs
    ) -> RetryResult:
        """Execute with adaptive retry based on response."""
        self._adjusted_config = RetryConfig() if self.config is None else self.config
        
        if self._rate_limit_count > 3:
            self._adjusted_config.initial_delay_seconds *= 2
            self._adjusted_config.max_delay_seconds *= 2
            logger.info(f"Increased delay due to rate limits: {self._adjusted_config.initial_delay_seconds}s")
        
        if self._consecutive_failures > 5:
            self._adjusted_config.max_attempts = min(self._adjusted_config.max_attempts + 1, 10)
            logger.info(f"Increased max attempts to {self._adjusted_config.max_attempts}")
        
        result = await self.execute_with_retry(
            func, *args, config=self._adjusted_config, **kwargs
        )
        
        if result.success:
            self._success_count += 1
            self._consecutive_failures = 0
            self._rate_limit_count = max(0, self._rate_limit_count - 1)
            self._server_error_count = max(0, self._server_error_count - 1)
        else:
            self._consecutive_failures += 1
            
            if "429" in str(result.final_error) or "rate" in str(result.final_error).lower():
                self._rate_limit_count += 1
            elif "500" in str(result.final_error) or "502" in str(result.final_error) or "503" in str(result.final_error):
                self._server_error_count += 1
        
        if self._success_count > 10 and self._consecutive_failures == 0:
            self._adjusted_config.initial_delay_seconds = max(1.0, self._adjusted_config.initial_delay_seconds * 0.9)
        
        return result


class PriorityQueue:
    """Priority queue for managing retry tasks."""
    
    def __init__(self):
        self._items: List[RetryTask] = []
        self._lock = asyncio.Lock()
    
    async def enqueue(self, task: RetryTask) -> None:
        """Add task to queue.
        
        Args:
            task: Retry task to add
        """
        async with self._lock:
            self._items.append(task)
            self._items.sort(key=lambda t: t.priority, reverse=True)
    
    async def dequeue(self) -> Optional[RetryTask]:
        """Remove and return highest priority task.
        
        Returns:
            Highest priority task or None
        """
        async with self._lock:
            if self._items:
                return self._items.pop(0)
            return None
    
    async def peek(self) -> Optional[RetryTask]:
        """Get highest priority task without removing.
        
        Returns:
            Highest priority task or None
        """
        async with self._lock:
            if self._items:
                return self._items[0]
            return None
    
    def size(self) -> int:
        """Get queue size."""
        return len(self._items)
    
    def is_empty(self) -> bool:
        """Check if queue is empty."""
        return len(self._items) == 0
