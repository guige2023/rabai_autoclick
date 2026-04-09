"""Automation Job Queue and Worker.

This module provides a job queue for automation tasks:
- Priority-based job scheduling
- Worker pool management
- Job retry and timeout handling
- Progress tracking

Example:
    >>> from actions.automation_job_queue_action import JobQueue, Job
    >>> queue = JobQueue(num_workers=4)
    >>> job = queue.enqueue("scrape_data", {"url": "https://example.com"})
"""

from __future__ import annotations

import time
import logging
import threading
import traceback
import uuid
from dataclasses import dataclass, field
from typing import Any, Callable, Optional
from enum import IntEnum
from collections import defaultdict

logger = logging.getLogger(__name__)


class JobPriority(IntEnum):
    """Job priority levels (lower = higher priority)."""
    CRITICAL = 0
    HIGH = 1
    NORMAL = 2
    LOW = 3
    BACKGROUND = 4


class JobStatus:
    """Job status constants."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass
class Job:
    """A queued job."""
    job_id: str
    func_name: str
    args: tuple = field(default_factory=tuple)
    kwargs: dict[str, Any] = field(default_factory=dict)
    priority: JobPriority = JobPriority.NORMAL
    status: str = JobStatus.PENDING
    created_at: float = field(default_factory=time.time)
    started_at: Optional[float] = None
    completed_at: Optional[float] = None
    result: Any = None
    error: Optional[str] = None
    retries: int = 0
    max_retries: int = 3
    timeout_seconds: float = 300.0


class JobQueue:
    """Automation job queue with worker pool."""

    def __init__(
        self,
        num_workers: int = 4,
        max_queue_size: int = 10000,
    ) -> None:
        """Initialize the job queue.

        Args:
            num_workers: Number of worker threads.
            max_queue_size: Maximum jobs in queue.
        """
        self._num_workers = num_workers
        self._max_queue_size = max_queue_size
        self._queue: list[Job] = []
        self._jobs: dict[str, Job] = {}
        self._lock = threading.RLock()
        self._running = True
        self._workers: list[threading.Thread] = []
        self._funcs: dict[str, Callable] = {}
        self._stats: dict[str, int] = defaultdict(int)

    def register(self, func_name: str, func: Callable) -> None:
        """Register a function to be executable as a job.

        Args:
            func_name: Name to register the function under.
            func: The callable to register.
        """
        with self._lock:
            self._funcs[func_name] = func
            logger.info("Registered job function: %s", func_name)

    def enqueue(
        self,
        func_name: str,
        args: Optional[tuple] = None,
        kwargs: Optional[dict[str, Any]] = None,
        priority: JobPriority = JobPriority.NORMAL,
        max_retries: int = 3,
        timeout_seconds: float = 300.0,
    ) -> str:
        """Enqueue a job for execution.

        Args:
            func_name: Name of registered function.
            args: Positional args for the function.
            kwargs: Keyword args for the function.
            priority: Job priority.
            max_retries: Max retry attempts on failure.
            timeout_seconds: Job timeout.

        Returns:
            The job ID.
        """
        with self._lock:
            if len(self._queue) >= self._max_queue_size:
                raise RuntimeError(f"Queue full (max={self._max_queue_size})")

            if func_name not in self._funcs:
                raise ValueError(f"Function not registered: {func_name}")

            job_id = str(uuid.uuid4())[:8]
            job = Job(
                job_id=job_id,
                func_name=func_name,
                args=args or (),
                kwargs=kwargs or {},
                priority=priority,
                max_retries=max_retries,
                timeout_seconds=timeout_seconds,
            )

            self._jobs[job_id] = job
            self._queue.append(job)
            self._queue.sort(key=lambda j: (j.priority, j.created_at))
            self._stats["enqueued"] += 1

            logger.info("Enqueued job %s (%s)", job_id, func_name)
            return job_id

    def get_job(self, job_id: str) -> Optional[Job]:
        """Get a job by ID.

        Args:
            job_id: The job ID.

        Returns:
            Job if found, None otherwise.
        """
        with self._lock:
            return self._jobs.get(job_id)

    def cancel_job(self, job_id: str) -> bool:
        """Cancel a pending job.

        Args:
            job_id: The job ID.

        Returns:
            True if cancelled, False if not found or already running.
        """
        with self._lock:
            job = self._jobs.get(job_id)
            if job is None:
                return False
            if job.status == JobStatus.RUNNING:
                return False
            job.status = JobStatus.CANCELLED
            if job in self._queue:
                self._queue.remove(job)
            self._stats["cancelled"] += 1
            logger.info("Cancelled job %s", job_id)
            return True

    def start(self) -> None:
        """Start the worker threads."""
        with self._lock:
            if self._workers:
                return
            self._running = True
            for i in range(self._num_workers):
                t = threading.Thread(target=self._worker, args=(i,), daemon=True)
                t.start()
                self._workers.append(t)
            logger.info("Started %d job queue workers", self._num_workers)

    def stop(self, timeout: float = 10.0) -> None:
        """Stop the worker threads.

        Args:
            timeout: Seconds to wait for workers to finish.
        """
        with self._lock:
            self._running = False
        for t in self._workers:
            t.join(timeout=timeout)
        self._workers.clear()
        logger.info("Stopped job queue workers")

    def get_stats(self) -> dict[str, Any]:
        """Get queue statistics."""
        with self._lock:
            pending = sum(1 for j in self._queue if j.status == JobStatus.PENDING)
            running = sum(1 for j in self._jobs.values() if j.status == JobStatus.RUNNING)
            return {
                **self._stats,
                "queue_size": len(self._queue),
                "pending": pending,
                "running": running,
                "total_jobs": len(self._jobs),
            }

    def _worker(self, worker_id: int) -> None:
        """Worker thread main loop."""
        logger.debug("Worker %d started", worker_id)
        while self._running:
            job = self._get_next_job()
            if job is None:
                time.sleep(0.1)
                continue
            self._execute_job(job, worker_id)

    def _get_next_job(self) -> Optional[Job]:
        """Get the next job from the queue."""
        with self._lock:
            for job in self._queue:
                if job.status == JobStatus.PENDING:
                    job.status = JobStatus.RUNNING
                    job.started_at = time.time()
                    return job
            return None

    def _execute_job(self, job: Job, worker_id: int) -> None:
        """Execute a job."""
        func = self._funcs.get(job.func_name)
        if func is None:
            job.status = JobStatus.FAILED
            job.error = f"Function not found: {job.func_name}"
            return

        try:
            result = func(*job.args, **job.kwargs)
            job.result = result
            job.status = JobStatus.COMPLETED
            job.completed_at = time.time()
            self._stats["completed"] += 1
            logger.info("Job %s completed on worker %d", job.job_id, worker_id)
        except Exception as e:
            logger.error("Job %s failed: %s", job.job_id, e)
            job.error = f"{type(e).__name__}: {e}"
            job.retries += 1
            if job.retries < job.max_retries:
                job.status = JobStatus.PENDING
                job.started_at = None
                with self._lock:
                    if job not in self._queue:
                        self._queue.append(job)
                    self._queue.sort(key=lambda j: (j.priority, j.created_at))
                self._stats["retried"] += 1
            else:
                job.status = JobStatus.FAILED
                job.completed_at = time.time()
                self._stats["failed"] += 1
