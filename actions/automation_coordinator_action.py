"""Automation coordinator action module for RabAI AutoClick.

Provides coordination operations for multi-step automations:
- TaskCoordinatorAction: Coordinate multiple automation tasks
- DependencyResolverAction: Resolve task dependencies
- ResourceAllocatorAction: Allocate resources for tasks
- ExecutionTrackerAction: Track execution progress
"""

import sys
import os
import logging
import threading
from typing import Any, Dict, List, Optional, Callable
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from collections import defaultdict, deque

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult

logger = logging.getLogger(__name__)


class TaskStatus(Enum):
    """Status of a coordinated task."""
    PENDING = "pending"
    READY = "ready"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    BLOCKED = "blocked"


@dataclass
class Task:
    """A coordinated task definition."""
    task_id: str
    name: str
    action_type: str
    params: Dict[str, Any]
    status: TaskStatus = TaskStatus.PENDING
    dependencies: List[str] = field(default_factory=list)
    resources: List[str] = field(default_factory=list)
    result: Any = None
    error: Optional[str] = None
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None


class DependencyGraph:
    """Task dependency graph."""

    def __init__(self) -> None:
        self._tasks: Dict[str, Task] = {}
        self._dependents: Dict[str, List[str]] = defaultdict(list)
        self._dependencies: Dict[str, List[str]] = defaultdict(list)

    def add_task(self, task: Task) -> None:
        self._tasks[task.task_id] = task
        for dep in task.dependencies:
            self._dependents[dep].append(task.task_id)
            self._dependencies[task.task_id].append(dep)

    def get_ready_tasks(self) -> List[Task]:
        ready = []
        for task in self._tasks.values():
            if task.status != TaskStatus.PENDING:
                continue
            deps = self._dependencies[task.task_id]
            if all(self._tasks[d].status == TaskStatus.COMPLETED for d in deps):
                task.status = TaskStatus.READY
                ready.append(task)
        return ready

    def get_blocked_tasks(self) -> List[Task]:
        blocked = []
        for task in self._tasks.values():
            if task.status != TaskStatus.PENDING:
                continue
            deps = self._dependencies[task.task_id]
            if not all(self._tasks[d].status == TaskStatus.COMPLETED for d in deps):
                blocked.append(task)
        return blocked

    def get_task(self, task_id: str) -> Optional[Task]:
        return self._tasks.get(task_id)


class ResourcePool:
    """Resource allocation pool."""

    def __init__(self) -> None:
        self._resources: Dict[str, int] = {}
        self._allocations: Dict[str, Dict[str, int]] = defaultdict(dict)
        self._lock = threading.Lock()

    def add_resource(self, name: str, capacity: int) -> None:
        with self._lock:
            self._resources[name] = capacity

    def allocate(self, task_id: str, resources: Dict[str, int]) -> bool:
        with self._lock:
            for name, amount in resources.items():
                available = self._resources.get(name, 0)
                used = self._allocations.get(name, {}).get(task_id, 0)
                if available - used < amount:
                    return False

            for name, amount in resources.items():
                self._allocations[name][task_id] = self._allocations[name].get(task_id, 0) + amount
            return True

    def release(self, task_id: str) -> None:
        with self._lock:
            for name, allocations in self._allocations.items():
                if task_id in allocations:
                    del allocations[task_id]

    def get_available(self, name: str) -> int:
        with self._lock:
            capacity = self._resources.get(name, 0)
            used = sum(self._allocations.get(name, {}).values())
            return capacity - used


_graph = DependencyGraph()
_pool = ResourcePool()


class TaskCoordinatorAction(BaseAction):
    """Coordinate multiple automation tasks."""
    action_type = "automation_coordinator"
    display_name = "任务协调器"
    description = "协调多个自动化任务的执行"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        operation = params.get("operation", "add")
        task_id = params.get("task_id", "")
        task_name = params.get("task_name", "")
        action_type = params.get("action_type", "")
        task_params = params.get("params", {})
        dependencies = params.get("dependencies", [])

        if operation == "add":
            if not task_id or not task_name:
                return ActionResult(success=False, message="task_id和task_name是必需的")

            task = Task(
                task_id=task_id,
                name=task_name,
                action_type=action_type,
                params=task_params,
                dependencies=dependencies
            )
            _graph.add_task(task)

            ready_count = len(_graph.get_ready_tasks())
            blocked_count = len(_graph.get_blocked_tasks())

            return ActionResult(
                success=True,
                message=f"任务 {task_name} 已添加，就绪: {ready_count}，阻塞: {blocked_count}",
                data={"task_id": task_id, "ready": ready_count, "blocked": blocked_count}
            )

        if operation == "list":
            tasks = list(_graph._tasks.values())
            return ActionResult(
                success=True,
                message=f"共 {len(tasks)} 个任务",
                data={"tasks": [{"id": t.task_id, "name": t.name, "status": t.status.value} for t in tasks]}
            )

        if operation == "status":
            task = _graph.get_task(task_id)
            if not task:
                return ActionResult(success=False, message=f"任务 {task_id} 不存在")

            return ActionResult(
                success=True,
                message=f"任务 {task_id}: {task.status.value}",
                data={
                    "task_id": task.task_id,
                    "name": task.name,
                    "status": task.status.value,
                    "result": task.result,
                    "error": task.error
                }
            )

        if operation == "complete":
            task = _graph.get_task(task_id)
            if not task:
                return ActionResult(success=False, message=f"任务 {task_id} 不存在")

            task.status = TaskStatus.COMPLETED
            task.end_time = datetime.now()
            _pool.release(task_id)

            ready = _graph.get_ready_tasks()
            return ActionResult(
                success=True,
                message=f"任务 {task_id} 完成，{len(ready)} 个任务就绪",
                data={"completed": task_id, "ready_count": len(ready)}
            )

        return ActionResult(success=False, message=f"未知操作: {operation}")


class DependencyResolverAction(BaseAction):
    """Resolve task dependencies."""
    action_type = "automation_dependency_resolver"
    display_name = "依赖解析器"
    description = "解析任务间的依赖关系"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        operation = params.get("operation", "resolve")

        if operation == "resolve":
            ready = _graph.get_ready_tasks()
            blocked = _graph.get_blocked_tasks()

            execution_order = []
            temp_graph = DependencyGraph()
            for task in _graph._tasks.values():
                temp_graph.add_task(task)

            while temp_graph._tasks:
                ready_tasks = temp_graph.get_ready_tasks()
                if not ready_tasks:
                    break
                for task in ready_tasks:
                    execution_order.append(task.task_id)
                    task.status = TaskStatus.COMPLETED

            return ActionResult(
                success=True,
                message=f"依赖解析完成，{len(execution_order)} 个任务可执行",
                data={
                    "execution_order": execution_order,
                    "ready_count": len(ready),
                    "blocked_count": len(blocked)
                }
            )

        if operation == "check":
            task_id = params.get("task_id", "")
            task = _graph.get_task(task_id)
            if not task:
                return ActionResult(success=False, message=f"任务 {task_id} 不存在")

            deps_status = {}
            for dep in task.dependencies:
                dep_task = _graph.get_task(dep)
                if dep_task:
                    deps_status[dep] = dep_task.status.value

            unmet = [d for d in task.dependencies if deps_status.get(d) != TaskStatus.COMPLETED.value]

            return ActionResult(
                success=True,
                message=f"依赖检查完成，{len(unmet)} 个未满足",
                data={"dependencies": deps_status, "unmet": unmet}
            )

        return ActionResult(success=False, message=f"未知操作: {operation}")


class ResourceAllocatorAction(BaseAction):
    """Allocate resources for tasks."""
    action_type = "automation_resource_allocator"
    display_name = "资源分配器"
    description = "为任务分配执行资源"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        operation = params.get("operation", "allocate")
        resource_name = params.get("resource_name", "")
        capacity = params.get("capacity", 1)
        task_id = params.get("task_id", "")
        resources = params.get("resources", {})

        if operation == "add":
            if not resource_name:
                return ActionResult(success=False, message="resource_name是必需的")

            _pool.add_resource(resource_name, capacity)
            return ActionResult(
                success=True,
                message=f"资源 {resource_name} 已添加，容量: {capacity}",
                data={"resource": resource_name, "capacity": capacity}
            )

        if operation == "allocate":
            if not task_id or not resources:
                return ActionResult(success=False, message="task_id和resources是必需的")

            success = _pool.allocate(task_id, resources)
            if success:
                return ActionResult(
                    success=True,
                    message=f"资源分配成功",
                    data={"task_id": task_id, "resources": resources}
                )
            return ActionResult(success=False, message="资源分配失败：资源不足")

        if operation == "status":
            if resource_name:
                available = _pool.get_available(resource_name)
                return ActionResult(
                    success=True,
                    message=f"资源 {resource_name} 可用: {available}",
                    data={"resource": resource_name, "available": available}
                )

            all_status = {name: _pool.get_available(name) for name in _pool._resources}
            return ActionResult(
                success=True,
                message=f"共 {len(all_status)} 个资源",
                data={"resources": all_status}
            )

        if operation == "release":
            if not task_id:
                return ActionResult(success=False, message="task_id是必需的")

            _pool.release(task_id)
            return ActionResult(success=True, message=f"任务 {task_id} 的资源已释放")

        return ActionResult(success=False, message=f"未知操作: {operation}")


class ExecutionTrackerAction(BaseAction):
    """Track execution progress."""
    action_type = "automation_execution_tracker"
    display_name = "执行追踪器"
    description = "追踪自动化执行进度"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        operation = params.get("operation", "progress")

        if operation == "progress":
            tasks = list(_graph._tasks.values())
            total = len(tasks)
            completed = sum(1 for t in tasks if t.status == TaskStatus.COMPLETED)
            running = sum(1 for t in tasks if t.status == TaskStatus.RUNNING)
            failed = sum(1 for t in tasks if t.status == TaskStatus.FAILED)
            pending = sum(1 for t in tasks if t.status in (TaskStatus.PENDING, TaskStatus.READY))

            progress = (completed / total * 100) if total > 0 else 0

            return ActionResult(
                success=True,
                message=f"执行进度: {progress:.1f}% ({completed}/{total})",
                data={
                    "total": total,
                    "completed": completed,
                    "running": running,
                    "failed": failed,
                    "pending": pending,
                    "progress_percent": round(progress, 2)
                }
            )

        if operation == "timeline":
            tasks = list(_graph._tasks.values())
            timeline = []
            for t in tasks:
                if t.start_time and t.end_time:
                    duration = (t.end_time - t.start_time).total_seconds()
                    timeline.append({
                        "task_id": t.task_id,
                        "name": t.name,
                        "start": t.start_time.isoformat(),
                        "end": t.end_time.isoformat(),
                        "duration_seconds": round(duration, 4)
                    })

            return ActionResult(
                success=True,
                message=f"时间线: {len(timeline)} 个任务",
                data={"timeline": timeline}
            )

        if operation == "failed":
            failed_tasks = [t for t in _graph._tasks.values() if t.status == TaskStatus.FAILED]
            return ActionResult(
                success=True,
                message=f"失败任务: {len(failed_tasks)}",
                data={"failed": [{"task_id": t.task_id, "error": t.error} for t in failed_tasks]}
            )

        return ActionResult(success=False, message=f"未知操作: {operation}")
