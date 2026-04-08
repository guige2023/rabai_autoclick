"""Worker Pool Action Module.

Provides worker pool management for concurrent task execution,
including pool sizing, worker lifecycle, and load balancing.
"""

import time
import hashlib
import asyncio
import threading
from typing import Any, Dict, List, Optional, Callable
from dataclasses import dataclass, field
from enum import Enum
from collections import deque
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class WorkerStatus(Enum):
    """Worker status."""
    IDLE = "idle"
    BUSY = "busy"
    STOPPED = "stopped"


class LoadBalancingStrategy(Enum):
    """Load balancing strategy."""
    ROUND_ROBIN = "round_robin"
    LEAST_BUSY = "least_busy"
    RANDOM = "random"


@dataclass
class Worker:
    """A worker in the pool."""
    worker_id: str
    name: str
    status: WorkerStatus = WorkerStatus.IDLE
    current_task: Optional[str] = None
    tasks_completed: int = 0
    tasks_failed: int = 0
    started_at: float = field(default_factory=time.time)
    last_active_at: Optional[float] = None


@dataclass
class PoolConfig:
    """Worker pool configuration."""
    min_workers: int = 1
    max_workers: int = 10
    max_queue_size: int = 100
    idle_timeout_seconds: float = 300.0
    health_check_interval_seconds: float = 60.0


@dataclass
class Task:
    """A task to be executed by worker."""
    task_id: str
    name: str
    handler: Callable
    params: Dict[str, Any] = field(default_factory=dict)
    priority: int = 0
    timeout_seconds: Optional[float] = None
    created_at: float = field(default_factory=time.time)
    result: Any = None
    error: Optional[str] = None
    completed_at: Optional[float] = None


@dataclass
class PoolMetrics:
    """Pool metrics."""
    total_workers: int = 0
    active_workers: int = 0
    idle_workers: int = 0
    busy_workers: int = 0
    total_tasks_completed: int = 0
    total_tasks_failed: int = 0
    pending_tasks: int = 0
    average_wait_time: float = 0.0


class WorkerPool:
    """Manages a pool of workers."""

    def __init__(
        self,
        pool_id: str,
        name: str,
        config: Optional[PoolConfig] = None
    ):
        self.pool_id = pool_id
        self.name = name
        self.config = config or PoolConfig()
        self._workers: Dict[str, Worker] = {}
        self._task_queue: List[Task] = []
        self._running_tasks: Dict[str, Task] = {}
        self._completed_tasks: List[Task] = []
        self._lock = threading.RLock()
        self._strategy = LoadBalancingStrategy.ROUND_ROBIN
        self._round_robin_index = 0
        self._metrics = PoolMetrics()
        self._stop_event = threading.Event()

    def set_strategy(self, strategy: LoadBalancingStrategy) -> None:
        """Set load balancing strategy."""
        self._strategy = strategy

    def add_worker(self, name: Optional[str] = None) -> str:
        """Add a worker to the pool."""
        with self._lock:
            if len(self._workers) >= self.config.max_workers:
                raise RuntimeError("Maximum workers reached")

            worker_id = hashlib.md5(
                f"{self.pool_id}{time.time()}".encode()
            ).hexdigest()[:8]

            worker = Worker(
                worker_id=worker_id,
                name=name or f"Worker-{worker_id[:6]}"
            )

            self._workers[worker_id] = worker
            self._update_metrics()

            return worker_id

    def remove_worker(self, worker_id: str, force: bool = False) -> bool:
        """Remove a worker from the pool."""
        with self._lock:
            if worker_id not in self._workers:
                return False

            worker = self._workers[worker_id]

            if worker.status == WorkerStatus.BUSY and not force:
                return False

            del self._workers[worker_id]
            self._update_metrics()
            return True

    def get_worker(self, worker_id: str) -> Optional[Worker]:
        """Get worker by ID."""
        return self._workers.get(worker_id)

    def submit_task(
        self,
        name: str,
        handler: Callable,
        params: Optional[Dict[str, Any]] = None,
        priority: int = 0,
        timeout_seconds: Optional[float] = None
    ) -> str:
        """Submit a task to the pool."""
        with self._lock:
            if len(self._task_queue) >= self.config.max_queue_size:
                raise RuntimeError("Task queue full")

            task_id = hashlib.md5(
                f"{name}{time.time()}".encode()
            ).hexdigest()[:8]

            task = Task(
                task_id=task_id,
                name=name,
                handler=handler,
                params=params or {},
                priority=priority,
                timeout_seconds=timeout_seconds
            )

            self._task_queue.append(task)
            self._task_queue.sort(key=lambda t: -t.priority)
            self._update_metrics()

            return task_id

    def _select_worker(self) -> Optional[Worker]:
        """Select a worker based on load balancing strategy."""
        idle_workers = [
            w for w in self._workers.values()
            if w.status == WorkerStatus.IDLE
        ]

        if not idle_workers:
            return None

        if self._strategy == LoadBalancingStrategy.ROUND_ROBIN:
            idx = self._round_robin_index % len(idle_workers)
            self._round_robin_index += 1
            return idle_workers[idx]

        elif self._strategy == LoadBalancingStrategy.LEAST_BUSY:
            return min(idle_workers, key=lambda w: w.tasks_completed)

        elif self._strategy == LoadBalancingStrategy.RANDOM:
            import random
            return random.choice(idle_workers)

        return idle_workers[0]

    def process_next_task(self) -> Optional[Task]:
        """Process the next task in queue."""
        with self._lock:
            if not self._task_queue:
                return None

            worker = self._select_worker()
            if not worker:
                return None

            task = self._task_queue.pop(0)
            worker.status = WorkerStatus.BUSY
            worker.current_task = task.task_id
            worker.last_active_at = time.time()

            self._running_tasks[task.task_id] = task

            try:
                start_time = time.time()

                if asyncio.iscoroutinefunction(task.handler):
                    result = asyncio.run(
                        task.handler(task.params)
                    )
                else:
                    result = task.handler(task.params)

                task.result = result
                task.completed_at = time.time()

                worker.tasks_completed += 1

            except Exception as e:
                task.error = str(e)
                task.completed_at = time.time()
                worker.tasks_failed += 1

            finally:
                worker.status = WorkerStatus.IDLE
                worker.current_task = None
                worker.last_active_at = time.time()

                if task.task_id in self._running_tasks:
                    del self._running_tasks[task.task_id]

                self._completed_tasks.append(task)

                if len(self._completed_tasks) > 1000:
                    self._completed_tasks = self._completed_tasks[-500:]

            self._update_metrics()
            return task

    def get_task(self, task_id: str) -> Optional[Task]:
        """Get task by ID."""
        for task in self._task_queue:
            if task.task_id == task_id:
                return task

        if task_id in self._running_tasks:
            return self._running_tasks[task_id]

        for task in self._completed_tasks:
            if task.task_id == task_id:
                return task

        return None

    def get_pending_tasks(self) -> List[Dict[str, Any]]:
        """Get pending tasks."""
        return [
            {
                "task_id": t.task_id,
                "name": t.name,
                "priority": t.priority,
                "created_at": t.created_at
            }
            for t in self._task_queue
        ]

    def get_completed_tasks(self, limit: int = 100) -> List[Dict[str, Any]]:
        """Get completed tasks."""
        tasks = self._completed_tasks[-limit:]

        return [
            {
                "task_id": t.task_id,
                "name": t.name,
                "result": t.result,
                "error": t.error,
                "completed_at": t.completed_at
            }
            for t in tasks
        ]

    def get_metrics(self) -> PoolMetrics:
        """Get pool metrics."""
        return self._metrics

    def _update_metrics(self) -> None:
        """Update pool metrics."""
        self._metrics.total_workers = len(self._workers)
        self._metrics.active_workers = len(self._workers)
        self._metrics.idle_workers = sum(
            1 for w in self._workers.values()
            if w.status == WorkerStatus.IDLE
        )
        self._metrics.busy_workers = sum(
            1 for w in self._workers.values()
            if w.status == WorkerStatus.BUSY
        )
        self._metrics.pending_tasks = len(self._task_queue)

        total_completed = sum(w.tasks_completed for w in self._workers.values())
        total_failed = sum(w.tasks_failed for w in self._workers.values())
        self._metrics.total_tasks_completed = total_completed
        self._metrics.total_tasks_failed = total_failed

    def scale_workers(self, count: int) -> int:
        """Scale worker count."""
        with self._lock:
            current = len(self._workers)

            if count > current:
                added = 0
                for _ in range(count - current):
                    try:
                        self.add_worker()
                        added += 1
                    except RuntimeError:
                        break
                return added

            elif count < current:
                removed = 0
                for _ in range(current - count):
                    try:
                        if self.remove_worker(
                            list(self._workers.keys())[-1],
                            force=True
                        ):
                            removed += 1
                    except IndexError:
                        break
                return -removed

        return 0

    def get_worker_stats(self) -> List[Dict[str, Any]]:
        """Get worker statistics."""
        return [
            {
                "worker_id": w.worker_id,
                "name": w.name,
                "status": w.status.value,
                "current_task": w.current_task,
                "tasks_completed": w.tasks_completed,
                "tasks_failed": w.tasks_failed,
                "uptime_seconds": time.time() - w.started_at
            }
            for w in self._workers.values()
        ]


class WorkerPoolAction(BaseAction):
    """Action for worker pool operations."""

    def __init__(self):
        super().__init__("worker_pool")
        self._pools: Dict[str, WorkerPool] = {}

    def execute(self, params: Dict[str, Any]) -> ActionResult:
        """Execute worker pool action."""
        try:
            operation = params.get("operation", "create")

            if operation == "create":
                return self._create_pool(params)
            elif operation == "add_worker":
                return self._add_worker(params)
            elif operation == "remove_worker":
                return self._remove_worker(params)
            elif operation == "submit":
                return self._submit_task(params)
            elif operation == "process":
                return self._process_task(params)
            elif operation == "scale":
                return self._scale_pool(params)
            elif operation == "get_pool":
                return self._get_pool(params)
            elif operation == "metrics":
                return self._get_metrics(params)
            elif operation == "workers":
                return self._get_workers(params)
            else:
                return ActionResult(
                    success=False,
                    message=f"Unknown operation: {operation}"
                )

        except Exception as e:
            return ActionResult(success=False, message=str(e))

    def _create_pool(self, params: Dict[str, Any]) -> ActionResult:
        """Create a worker pool."""
        pool_id = params.get("pool_id")
        name = params.get("name", "Unnamed Pool")

        if not pool_id:
            return ActionResult(success=False, message="pool_id required")

        config = PoolConfig(
            min_workers=params.get("min_workers", 1),
            max_workers=params.get("max_workers", 10),
            max_queue_size=params.get("max_queue_size", 100)
        )

        pool = WorkerPool(pool_id=pool_id, name=name, config=config)

        if "strategy" in params:
            pool.set_strategy(LoadBalancingStrategy(params["strategy"]))

        self._pools[pool_id] = pool

        for _ in range(config.min_workers):
            pool.add_worker()

        return ActionResult(
            success=True,
            message=f"Pool created: {pool_id}"
        )

    def _add_worker(self, params: Dict[str, Any]) -> ActionResult:
        """Add a worker to pool."""
        pool_id = params.get("pool_id")

        if not pool_id or pool_id not in self._pools:
            return ActionResult(success=False, message="Invalid pool_id")

        pool = self._pools[pool_id]

        try:
            worker_id = pool.add_worker(params.get("name"))
            return ActionResult(
                success=True,
                data={"worker_id": worker_id}
            )
        except RuntimeError as e:
            return ActionResult(success=False, message=str(e))

    def _remove_worker(self, params: Dict[str, Any]) -> ActionResult:
        """Remove a worker from pool."""
        pool_id = params.get("pool_id")
        worker_id = params.get("worker_id")
        force = params.get("force", False)

        if not pool_id or pool_id not in self._pools:
            return ActionResult(success=False, message="Invalid pool_id")

        pool = self._pools[pool_id]
        success = pool.remove_worker(worker_id, force)

        return ActionResult(
            success=success,
            message="Worker removed" if success else "Could not remove worker"
        )

    def _submit_task(self, params: Dict[str, Any]) -> ActionResult:
        """Submit a task to pool."""
        pool_id = params.get("pool_id")

        if not pool_id or pool_id not in self._pools:
            return ActionResult(success=False, message="Invalid pool_id")

        pool = self._pools[pool_id]

        def placeholder_handler(params):
            return {"status": "completed"}

        try:
            task_id = pool.submit_task(
                name=params.get("name", "task"),
                handler=params.get("handler") or placeholder_handler,
                params=params.get("params", {}),
                priority=params.get("priority", 0),
                timeout_seconds=params.get("timeout_seconds")
            )

            return ActionResult(
                success=True,
                data={"task_id": task_id}
            )

        except RuntimeError as e:
            return ActionResult(success=False, message=str(e))

    def _process_task(self, params: Dict[str, Any]) -> ActionResult:
        """Process next task in pool."""
        pool_id = params.get("pool_id")

        if not pool_id or pool_id not in self._pools:
            return ActionResult(success=False, message="Invalid pool_id")

        pool = self._pools[pool_id]
        task = pool.process_next_task()

        if not task:
            return ActionResult(
                success=True,
                message="No tasks to process"
            )

        return ActionResult(
            success=task.error is None,
            data={
                "task_id": task.task_id,
                "result": task.result,
                "error": task.error
            }
        )

    def _scale_pool(self, params: Dict[str, Any]) -> ActionResult:
        """Scale pool worker count."""
        pool_id = params.get("pool_id")
        count = params.get("count", 5)

        if not pool_id or pool_id not in self._pools:
            return ActionResult(success=False, message="Invalid pool_id")

        pool = self._pools[pool_id]
        scaled = pool.scale_workers(count)

        return ActionResult(
            success=True,
            data={"workers_scaled": scaled}
        )

    def _get_pool(self, params: Dict[str, Any]) -> ActionResult:
        """Get pool details."""
        pool_id = params.get("pool_id")

        if not pool_id:
            return ActionResult(
                success=True,
                data={
                    "pools": [
                        {"pool_id": p.pool_id, "name": p.name}
                        for p in self._pools.values()
                    ]
                }
            )

        if pool_id not in self._pools:
            return ActionResult(success=False, message="Pool not found")

        pool = self._pools[pool_id]
        metrics = pool.get_metrics()

        return ActionResult(
            success=True,
            data={
                "pool_id": pool.pool_id,
                "name": pool.name,
                "total_workers": metrics.total_workers,
                "active_workers": metrics.active_workers,
                "pending_tasks": metrics.pending_tasks
            }
        )

    def _get_metrics(self, params: Dict[str, Any]) -> ActionResult:
        """Get pool metrics."""
        pool_id = params.get("pool_id")

        if pool_id and pool_id in self._pools:
            pool = self._pools[pool_id]
            metrics = pool.get_metrics()
        else:
            metrics = PoolMetrics()

        return ActionResult(
            success=True,
            data={
                "total_workers": metrics.total_workers,
                "active_workers": metrics.active_workers,
                "idle_workers": metrics.idle_workers,
                "busy_workers": metrics.busy_workers,
                "pending_tasks": metrics.pending_tasks,
                "total_tasks_completed": metrics.total_tasks_completed,
                "total_tasks_failed": metrics.total_tasks_failed
            }
        )

    def _get_workers(self, params: Dict[str, Any]) -> ActionResult:
        """Get worker statistics."""
        pool_id = params.get("pool_id")

        if not pool_id or pool_id not in self._pools:
            return ActionResult(success=False, message="Invalid pool_id")

        pool = self._pools[pool_id]
        workers = pool.get_worker_stats()

        return ActionResult(
            success=True,
            data={"workers": workers}
        )
