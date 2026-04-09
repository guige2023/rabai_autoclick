"""Automation Coordinator Action Module.

Coordinates multiple automation tasks with dependency graphs,
resource pooling, and execution tracking.
"""

from __future__ import annotations

import sys
import os
import time
import threading
import uuid
from typing import Any, Dict, List, Optional, Set, Callable
from dataclasses import dataclass, field
from enum import Enum
from collections import defaultdict, deque
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class TaskStatus(Enum):
    """Status of a coordinated task."""
    IDLE = "idle"
    QUEUED = "queued"
    RUNNING = "running"
    WAITING = "waiting"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    PAUSED = "paused"


class TaskPriority(Enum):
    """Priority levels for tasks."""
    CRITICAL = 0
    HIGH = 1
    NORMAL = 2
    LOW = 3


@dataclass
class AutomationTask:
    """An automatable task with dependencies."""
    task_id: str
    name: str
    task_type: str
    config: Dict[str, Any] = field(default_factory=dict)
    depends_on: List[str] = field(default_factory=list)
    priority: TaskPriority = TaskPriority.NORMAL
    status: TaskStatus = TaskStatus.IDLE
    created_at: float = field(default_factory=time.time)
    started_at: Optional[float] = None
    completed_at: Optional[float] = None
    worker_id: Optional[str] = None
    result: Any = None
    error: Optional[str] = None
    retry_count: int = 0
    max_retries: int = 3
    tags: List[str] = field(default_factory=list)


@dataclass
class ResourcePool:
    """Pool of shared resources for tasks."""
    pool_id: str
    resource_type: str
    capacity: int
    acquired: Set[str] = field(default_factory=set)
    _lock: threading.Lock = field(default_factory=threading.Lock)

    def acquire(self, task_id: str) -> bool:
        with self._lock:
            if len(self.acquired) < self.capacity:
                self.acquired.add(task_id)
                return True
            return False

    def release(self, task_id: str) -> None:
        with self._lock:
            self.acquired.discard(task_id)

    def available(self) -> int:
        with self._lock:
            return self.capacity - len(self.acquired)


class AutomationCoordinatorAction(BaseAction):
    """Coordinate multiple automation tasks.

    Manages task dependencies, resource pools, parallel execution,
    and provides execution monitoring and reporting.
    """
    action_type = "automation_coordinator"
    display_name = "自动化协调器"
    description = "协调多个自动化任务，管理依赖关系和资源池"

    def __init__(self):
        super().__init__()
        self._tasks: Dict[str, AutomationTask] = {}
        self._task_graph: Dict[str, Set[str]] = defaultdict(set)
        self._reverse_graph: Dict[str, Set[str]] = defaultdict(set)
        self._resource_pools: Dict[str, ResourcePool] = {}
        self._workers: Dict[str, Dict] = {}
        self._lock = threading.Lock()
        self._execution_order: List[str] = []
        self._stats = defaultdict(lambda: {
            "total": 0, "completed": 0, "failed": 0, "running": 0
        })

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute coordination operation.

        Args:
            context: Execution context.
            params: Dict with keys: action, task_id, config, etc.

        Returns:
            ActionResult with coordination result.
        """
        action = params.get("action", "create")

        if action == "create":
            return self._create_task(context, params)
        elif action == "run":
            return self._run_tasks(context, params)
        elif action == "status":
            return self._get_status(params)
        elif action == "cancel":
            return self._cancel_task(params)
        elif action == "resource":
            return self._manage_resource(params)
        elif action == "worker":
            return self._register_worker(params)
        elif action == "dependency":
            return self._add_dependency(params)
        elif action == "plan":
            return self._plan_execution(params)
        else:
            return ActionResult(
                success=False,
                message=f"Unknown action: {action}"
            )

    def _create_task(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Create a new automation task."""
        import uuid

        name = params.get("name", "Unnamed Task")
        task_type = params.get("task_type", "generic")
        config = params.get("config", {})
        priority_str = params.get("priority", "normal").upper()
        depends_on = params.get("depends_on", [])
        tags = params.get("tags", [])
        save_to_var = params.get("save_to_var", None)

        try:
            priority = TaskPriority[priority_str]
        except KeyError:
            priority = TaskPriority.NORMAL

        task_id = params.get("task_id") or str(uuid.uuid4())[:8]

        task = AutomationTask(
            task_id=task_id,
            name=name,
            task_type=task_type,
            config=config,
            depends_on=depends_on,
            priority=priority,
            tags=tags
        )

        with self._lock:
            self._tasks[task_id] = task
            self._stats["total"] += 1

            for dep in depends_on:
                self._task_graph[dep].add(task_id)
                self._reverse_graph[task_id].add(dep)

        result_data = {
            "task_id": task_id,
            "name": name,
            "task_type": task_type,
            "priority": priority.name,
            "depends_on": depends_on
        }

        if save_to_var:
            context.variables[save_to_var] = result_data

        return ActionResult(
            success=True,
            message=f"Task '{task_id}' created: {name} ({task_type})",
            data=result_data
        )

    def _add_dependency(self, params: Dict[str, Any]) -> ActionResult:
        """Add a dependency between tasks."""
        task_id = params.get("task_id", "")
        depends_on = params.get("depends_on", [])

        if not task_id:
            return ActionResult(success=False, message="task_id is required")

        with self._lock:
            task = self._tasks.get(task_id)
            if not task:
                return ActionResult(
                    success=False,
                    message=f"Task '{task_id}' not found"
                )

            for dep_id in depends_on:
                if dep_id not in self._tasks:
                    return ActionResult(
                        success=False,
                        message=f"Dependency '{dep_id}' not found"
                    )
                if dep_id not in task.depends_on:
                    task.depends_on.append(dep_id)
                self._task_graph[dep_id].add(task_id)
                self._reverse_graph[task_id].add(dep_id)

        return ActionResult(
            success=True,
            message=f"Added dependencies {depends_on} to task '{task_id}'"
        )

    def _plan_execution(self, params: Dict[str, Any]) -> ActionResult:
        """Compute topological execution order for all tasks."""
        task_ids = params.get("task_ids", list(self._tasks.keys()))
        save_to_var = params.get("save_to_var", None)

        in_degree: Dict[str, int] = {tid: 0 for tid in task_ids}
        for tid in task_ids:
            task = self._tasks.get(tid)
            if task:
                for dep in task.depends_on:
                    if dep in in_degree:
                        pass

        for tid in task_ids:
            task = self._tasks.get(tid)
            if task:
                for dep in task.depends_on:
                    if dep in in_degree:
                        pass

        queue = deque([tid for tid, deg in in_degree.items() if deg == 0])
        sorted_order = []
        visited = set()

        temp_deps = {}
        for tid in task_ids:
            task = self._tasks.get(tid)
            temp_deps[tid] = set(task.depends_on) if task else set()

        while queue:
            node = queue.popleft()
            if node in visited:
                continue
            visited.add(node)
            sorted_order.append(node)

            for dependent in self._task_graph.get(node, []):
                if dependent in temp_deps:
                    temp_deps[dependent].discard(node)
                    if len(temp_deps[dependent]) == 0:
                        queue.append(dependent)

        remaining = [tid for tid in task_ids if tid not in visited]
        circular = remaining

        result_data = {
            "execution_order": sorted_order,
            "total_tasks": len(task_ids),
            "circular_dependencies": circular if circular else None
        }

        if save_to_var:
            context.variables[save_to_var] = result_data

        return ActionResult(
            success=not circular,
            message=f"Execution plan: {len(sorted_order)} tasks ordered, "
                    f"{len(circular)} circular" if circular else "No circular deps",
            data=result_data
        )

    def _run_tasks(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute tasks respecting dependencies."""
        task_ids = params.get("task_ids", [])
        max_parallel = int(params.get("max_parallel", 5))
        save_to_var = params.get("save_to_var", None)

        if not task_ids:
            return ActionResult(
                success=False,
                message="No task_ids provided"
            )

        with self._lock:
            tasks = {tid: self._tasks[tid] for tid in task_ids if tid in self._tasks}
            missing = [tid for tid in task_ids if tid not in self._tasks]
            if missing:
                return ActionResult(
                    success=False,
                    message=f"Tasks not found: {missing}"
                )

        completed: Set[str] = set()
        failed: Set[str] = set()
        running: Dict[str, AutomationTask] = {}
        start_time = time.time()

        ready_queue = deque(
            sorted(
                [t for tid, t in tasks.items() if not t.depends_on],
                key=lambda x: x.priority.value
            )
        )

        while (ready_queue or running) and len(completed) + len(failed) < len(tasks):
            while len(running) < max_parallel and ready_queue:
                task = ready_queue.popleft()
                task.status = TaskStatus.RUNNING
                task.started_at = time.time()
                task.worker_id = f"worker-{len(running) + 1}"
                running[task.task_id] = task
                self._stats["running"] += 1

                threading.Thread(
                    target=self._execute_task,
                    args=(task, completed, failed, tasks),
                    daemon=True
                ).start()

            time.sleep(0.05)

            done = [tid for tid, t in running.items()
                    if t.status in (TaskStatus.COMPLETED, TaskStatus.FAILED)]
            for tid in done:
                task = running.pop(tid)
                self._stats["running"] = max(0, self._stats["running"] - 1)
                if task.status == TaskStatus.COMPLETED:
                    completed.add(tid)
                    self._stats["completed"] += 1
                else:
                    failed.add(tid)
                    self._stats["failed"] += 1

                if task.status == TaskStatus.COMPLETED:
                    for next_tid in self._task_graph.get(tid, []):
                        next_task = tasks.get(next_tid)
                        if next_task and next_task.status == TaskStatus.IDLE:
                            if all(dep in completed for dep in next_task.depends_on):
                                ready_queue.append(next_task)

        elapsed = time.time() - start_time
        result_data = {
            "total": len(tasks),
            "completed": len(completed),
            "failed": len(failed),
            "elapsed": elapsed,
            "completed_tasks": list(completed),
            "failed_tasks": list(failed)
        }

        if save_to_var:
            context.variables[save_to_var] = result_data

        return ActionResult(
            success=len(failed) == 0,
            message=f"Execution: {len(completed)}/{len(tasks)} completed "
                    f"in {elapsed:.2f}s",
            data=result_data
        )

    def _execute_task(self, task: AutomationTask,
                      completed: Set, failed: Set, tasks: Dict) -> None:
        """Execute a single task (runs in thread)."""
        try:
            task_type = task.task_type
            config = task.config

            if task_type == "delay":
                delay = config.get("duration", 1.0)
                time.sleep(min(delay, 10.0))
                task.result = {"delayed": delay}
                task.status = TaskStatus.COMPLETED
            elif task_type == "log":
                message = config.get("message", f"Task {task.task_id} executed")
                task.result = {"logged": message}
                task.status = TaskStatus.COMPLETED
            elif task_type == "variable_set":
                var_name = config.get("name")
                var_value = config.get("value")
                if var_name:
                    task.result = {"set": var_name, "value": var_value}
                    task.status = TaskStatus.COMPLETED
            else:
                task.result = {"executed": True, "type": task_type}
                task.status = TaskStatus.COMPLETED

        except Exception as e:
            task.error = str(e)
            task.status = TaskStatus.FAILED

    def _cancel_task(self, params: Dict[str, Any]) -> ActionResult:
        """Cancel a task."""
        task_id = params.get("task_id", "")
        with self._lock:
            task = self._tasks.get(task_id)
            if not task:
                return ActionResult(
                    success=False,
                    message=f"Task '{task_id}' not found"
                )
            if task.status == TaskStatus.RUNNING:
                return ActionResult(
                    success=False,
                    message=f"Cannot cancel running task '{task_id}'"
                )
            task.status = TaskStatus.CANCELLED

        return ActionResult(
            success=True,
            message=f"Task '{task_id}' cancelled"
        )

    def _manage_resource(self, params: Dict[str, Any]) -> ActionResult:
        """Create, configure, or query resource pools."""
        sub_action = params.get("sub_action", "create")
        pool_id = params.get("pool_id", "")
        resource_type = params.get("resource_type", "generic")
        capacity = int(params.get("capacity", 10))

        if sub_action == "create":
            pool = ResourcePool(
                pool_id=pool_id,
                resource_type=resource_type,
                capacity=capacity
            )
            with self._lock:
                self._resource_pools[pool_id] = pool

            return ActionResult(
                success=True,
                message=f"Resource pool '{pool_id}' created "
                        f"(capacity: {capacity})"
            )

        elif sub_action == "acquire":
            task_id = params.get("task_id", "")
            with self._lock:
                pool = self._resource_pools.get(pool_id)
                if not pool:
                    return ActionResult(
                        success=False,
                        message=f"Pool '{pool_id}' not found"
                    )
                acquired = pool.acquire(task_id)
                if acquired:
                    return ActionResult(
                        success=True,
                        message=f"Task '{task_id}' acquired resource from '{pool_id}'"
                    )
                return ActionResult(
                    success=False,
                    message=f"Pool '{pool_id}' at capacity"
                )

        elif sub_action == "release":
            task_id = params.get("task_id", "")
            with self._lock:
                pool = self._resource_pools.get(pool_id)
                if pool:
                    pool.release(task_id)
            return ActionResult(
                success=True,
                message=f"Released from '{pool_id}'"
            )

        elif sub_action == "status":
            with self._lock:
                pool = self._resource_pools.get(pool_id)
                if not pool:
                    return ActionResult(
                        success=False,
                        message=f"Pool '{pool_id}' not found"
                    )
                return ActionResult(
                    success=True,
                    message=f"Pool '{pool_id}'",
                    data={"pool_id": pool_id, "available": pool.available(),
                          "capacity": pool.capacity}
                )

    def _register_worker(self, params: Dict[str, Any]) -> ActionResult:
        """Register a worker process."""
        worker_id = params.get("worker_id", f"worker-{len(self._workers)}")
        capabilities = params.get("capabilities", [])
        with self._lock:
            self._workers[worker_id] = {
                "capabilities": capabilities,
                "registered_at": time.time(),
                "active_tasks": []
            }

        return ActionResult(
            success=True,
            message=f"Worker '{worker_id}' registered"
        )

    def _get_status(self, params: Dict[str, Any]) -> ActionResult:
        """Get overall coordination status."""
        task_id = params.get("task_id", None)
        save_to_var = params.get("save_to_var", None)

        if task_id:
            with self._lock:
                task = self._tasks.get(task_id)
                if not task:
                    return ActionResult(
                        success=False,
                        message=f"Task '{task_id}' not found"
                    )
                data = {
                    "task_id": task.task_id,
                    "name": task.name,
                    "status": task.status.value,
                    "priority": task.priority.name,
                    "created_at": task.created_at,
                    "started_at": task.started_at,
                    "completed_at": task.completed_at,
                    "error": task.error,
                    "result": task.result
                }
        else:
            with self._lock:
                data = {
                    "total_tasks": len(self._tasks),
                    "stats": dict(self._stats),
                    "resource_pools": len(self._resource_pools),
                    "workers": len(self._workers),
                    "tasks_by_status": {
                        s.value: sum(1 for t in self._tasks.values()
                                     if t.status == s)
                        for s in TaskStatus
                    }
                }

        if save_to_var:
            context.variables[save_to_var] = data

        return ActionResult(success=True, message="Status retrieved", data=data)

    def get_required_params(self) -> List[str]:
        return ["action"]

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            "task_id": None,
            "name": "Task",
            "task_type": "generic",
            "config": {},
            "priority": "normal",
            "depends_on": [],
            "tags": [],
            "max_parallel": 5,
            "task_ids": [],
            "save_to_var": None
        }
